# data_engine.py - Layer 1: 数据引擎
#
# 整合 CTP tick + HL WebSocket + 汇率, 输出标准化价格 (NormalizedPrice)
# 所有价格统一转换为 RMB/kg 进行价差比较

import asyncio
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Optional, Callable, Any, List

import websockets

from exchanges.ctp_gateway import CTPGateway, TickData
from exchanges.hyperliquid import HyperliquidClient
from forex_feed import ForexFeed
from session_manager import SessionManager
from unit_converter import hl_usd_oz_to_cny_kg
from config import TradingPair

import logging
logger = logging.getLogger(__name__)


@dataclass
class NormalizedPrice:
    """标准化价格 (两腿统一为 RMB/kg)"""
    # AG 侧 (CTP)
    ag_price: float          # 最新价 (RMB/kg)
    ag_bid: float            # 买一价
    ag_ask: float            # 卖一价
    ag_bid_vol: int          # 买一量
    ag_ask_vol: int          # 卖一量
    # HL 侧
    hl_price_usd: float      # 原始价格 (USD/oz)
    hl_price_cny_kg: float   # 换算后价格 (RMB/kg)
    # 汇率
    usdcny: float
    # 元信息
    timestamp: float
    ag_stale: bool = False   # AG tick 是否过期
    hl_stale: bool = False   # HL 价格是否过期
    forex_stale: bool = False  # 汇率是否过期 (>1h 未更新)


class DataEngine:
    """数据引擎: CTP + HL + 汇率 → NormalizedPrice"""

    STALE_THRESHOLD = 5.0  # 价格超过5秒视为过期

    def __init__(
        self,
        ctp_gateway: CTPGateway,
        hl_client: HyperliquidClient,
        forex_feed: ForexFeed,
        session_manager: SessionManager,
        pair: TradingPair,
        hl_data_mode: str = "local",
        remote_quote_url: str = "",
        remote_quote_ws_url: str = "",
        remote_quote_timeout_sec: float = 1.0,
        remote_quote_poll_sec: float = 1.0,
    ):
        self._ctp = ctp_gateway
        self._hl = hl_client
        self._forex = forex_feed
        self._session_mgr = session_manager
        self._pair = pair
        self._hl_data_mode = (hl_data_mode or "local").strip().lower()
        self._remote_quote_url = (remote_quote_url or "").strip()
        self._remote_quote_ws_url = (remote_quote_ws_url or "").strip()
        self._remote_quote_timeout_sec = max(0.2, float(remote_quote_timeout_sec))
        self._remote_quote_poll_sec = max(0.2, float(remote_quote_poll_sec))

        # 最新数据
        self._ag_tick: Optional[TickData] = None
        self._hl_mid: float = 0
        self._hl_update_time: float = 0
        self._latest: Optional[NormalizedPrice] = None

        # 回调
        self._callbacks: List[Callable] = []

        # 状态
        self._running = False
        self._ctp_stream_active = False
        self._remote_quote_ws_connected = False
        self._remote_quote_ws_last_error = ""

    @property
    def latest(self) -> Optional[NormalizedPrice]:
        return self._latest

    @property
    def hl_last_update_age(self) -> float:
        """HL 价格数据年龄 (秒), 未收到过数据时返回 -1"""
        if self._hl_update_time == 0:
            return -1
        return time.time() - self._hl_update_time

    @property
    def ag_last_tick_age(self) -> float:
        """AG tick 数据年龄 (秒), 未收到过数据时返回 -1"""
        if self._ag_tick is None:
            return -1
        return time.time() - self._ag_tick.timestamp

    @property
    def remote_quote_ws_connected(self) -> bool:
        return self._remote_quote_ws_connected

    @property
    def remote_quote_ws_last_error(self) -> str:
        return self._remote_quote_ws_last_error

    def on_price(self, callback: Callable[[NormalizedPrice], Any]):
        """注册价格更新回调"""
        self._callbacks.append(callback)

    async def start(self, include_ctp: bool = True):
        """启动数据源"""
        self._running = True

        # 1. CTP: 订阅行情, 注册 tick 回调
        if include_ctp:
            self.activate_ctp_stream()
        else:
            logger.info("当前未启用 CTP 行情订阅（等待交易时段）")

        # 2. HL: 启动远程行情拉取或本地 WebSocket
        if self._hl_data_mode == "remote":
            if self._remote_quote_ws_url:
                asyncio.create_task(self._run_hl_remote_quote_ws())
                logger.info(
                    "HL data source: remote quote websocket "
                    f"url={self._remote_quote_ws_url} "
                    f"fallback_http={self._remote_quote_url or 'N/A'}"
                )
            else:
                if not self._remote_quote_url:
                    raise ValueError("HL_DATA_MODE=remote but HL_REMOTE_QUOTE_URL is empty")
                asyncio.create_task(self._run_hl_remote_quote())
                logger.info(
                    "HL data source: remote quote http polling "
                    f"url={self._remote_quote_url} "
                    f"poll={self._remote_quote_poll_sec:.2f}s timeout={self._remote_quote_timeout_sec:.2f}s"
                )
        else:
            asyncio.create_task(self._run_hl_ws())
            logger.info("HL data source: local websocket")

        # 3. 汇率: 启动定时获取
        asyncio.create_task(self._forex.start())

        logger.info("DataEngine 启动完成")

    def activate_ctp_stream(self):
        """确保 CTP 订阅与回调已激活（用于首次启动或重连后重订阅）"""
        instrument = self._pair.ctp_instrument
        logger.info(f"DataEngine CTP instrument={instrument!r} type={type(instrument).__name__}")
        self._ctp.subscribe(instrument)
        self._ctp.on_tick(instrument, self._on_ag_tick)
        self._ctp_stream_active = True
        logger.info(f"已订阅 CTP 行情: {instrument}")

    def deactivate_ctp_stream(self):
        """停用 CTP 数据流（不关闭 HL / Forex）"""
        self._ctp_stream_active = False
        self._ag_tick = None

    async def _on_ag_tick(self, tick: TickData):
        """CTP tick 回调 (由 CTP 线程通过 call_soon_threadsafe 调度)"""
        self._ag_tick = tick
        await self._emit_price()

    async def _run_hl_ws(self):
        """HL WebSocket 连接和接收 (指数退避重连)"""
        hl_symbol = self._pair.hl_symbol
        if self._hl.dex and ":" not in hl_symbol:
            hl_symbol = f"{self._hl.dex}:{hl_symbol}"
        backoff = 2  # 初始重连间隔 (秒)
        max_backoff = 30  # 最大重连间隔

        while self._running:
            try:
                async with websockets.connect(self._hl.ws_url) as ws:
                    logger.info("HL WebSocket 已连接")
                    backoff = 2  # 连接成功, 重置退避

                    # 订阅 allMids
                    sub = {"type": "allMids"}
                    if self._hl.dex:
                        sub["dex"] = self._hl.dex
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": sub
                    }))

                    async for message in ws:
                        if not self._running:
                            break
                        data = json.loads(message)
                        if data.get("channel") == "allMids":
                            mids = data.get("data", {}).get("mids", {})
                            if hl_symbol in mids:
                                self._hl_mid = float(mids[hl_symbol])
                                self._hl_update_time = time.time()
                                await self._emit_price()

            except Exception as e:
                if self._running:
                    logger.error(f"HL WebSocket 错误: {e}, {backoff}s 后重连")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

    async def _run_hl_remote_quote_ws(self):
        """Consume remote quote websocket stream from Tokyo, fallback to HTTP polling."""
        if not self._remote_quote_ws_url:
            await self._run_hl_remote_quote()
            return

        backoff = 1.0
        max_backoff = 10.0
        hl_symbol = self._pair.hl_symbol
        while self._running:
            try:
                stream_url = self._remote_quote_ws_url
                if "?" in stream_url:
                    stream_url = f"{stream_url}&symbol={hl_symbol}"
                else:
                    stream_url = f"{stream_url}?symbol={hl_symbol}"

                async with websockets.connect(stream_url, ping_interval=15, ping_timeout=10) as ws:
                    logger.info(f"HL remote quote websocket connected: {self._remote_quote_ws_url}")
                    self._remote_quote_ws_connected = True
                    self._remote_quote_ws_last_error = ""
                    backoff = 1.0
                    async for message in ws:
                        if not self._running:
                            break
                        data = json.loads(message)
                        if not isinstance(data, dict):
                            continue
                        if not data.get("ok", False):
                            logger.warning(f"HL remote quote ws not ok: {data}")
                            continue

                        price = float(data.get("price", 0) or 0)
                        if price <= 0:
                            continue

                        src_ts = data.get("ts")
                        if isinstance(src_ts, (int, float)) and src_ts > 0:
                            self._hl_update_time = float(src_ts)
                        else:
                            self._hl_update_time = time.time()
                        self._hl_mid = price
                        await self._emit_price()
            except Exception as e:
                self._remote_quote_ws_connected = False
                self._remote_quote_ws_last_error = str(e)
                if self._running:
                    logger.warning(f"HL remote quote websocket error: {e}")
                    if self._remote_quote_url:
                        logger.info("HL remote quote websocket fallback to HTTP polling")
                        try:
                            await self._run_hl_remote_quote()
                        except Exception as poll_e:
                            logger.warning(f"HL remote quote fallback polling error: {poll_e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

    async def _run_hl_remote_quote(self):
        """从东京网关轮询 HL 行情。"""
        hl_symbol = self._pair.hl_symbol
        backoff = 1.0
        max_backoff = 10.0

        while self._running:
            try:
                query = urllib.parse.urlencode({"symbol": hl_symbol})
                req = urllib.request.Request(
                    url=f"{self._remote_quote_url}?{query}",
                    method="GET",
                )
                with urllib.request.urlopen(req, timeout=self._remote_quote_timeout_sec) as resp:
                    status = int(getattr(resp, "status", 200))
                    if status < 200 or status >= 300:
                        raise RuntimeError(f"HTTP {status}")

                    data = json.loads(resp.read().decode("utf-8"))
                    if not data.get("ok", False):
                        raise RuntimeError(f"quote not ok: {data}")

                    price = float(data.get("price", 0) or 0)
                    if price <= 0:
                        raise RuntimeError(f"invalid quote price: {price}")

                    src_ts = data.get("ts")
                    if isinstance(src_ts, (int, float)) and src_ts > 0:
                        self._hl_update_time = float(src_ts)
                    else:
                        self._hl_update_time = time.time()
                    self._hl_mid = price

                    if data.get("stale"):
                        logger.warning(
                            "HL remote quote stale: "
                            f"symbol={data.get('symbol')} age={data.get('age_sec')}s"
                        )

                    await self._emit_price()
                    backoff = 1.0
            except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, ValueError, RuntimeError) as e:
                if self._running:
                    logger.warning(f"HL remote quote error: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue
            except Exception as e:
                if self._running:
                    logger.warning(f"HL remote quote unexpected error: {e}")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                    continue

            await asyncio.sleep(self._remote_quote_poll_sec)

    async def _emit_price(self):
        """构建 NormalizedPrice 并通知回调"""
        if not self._ag_tick or self._hl_mid <= 0:
            return

        # 汇率尚未获取过 (last_update_time==0), 等待首次获取完成
        if self._forex.last_update_time == 0:
            return

        now = time.time()
        forex_stale = self._forex.is_stale
        if forex_stale:
            logger.warning(
                f"汇率数据过期! 上次更新: {now - self._forex.last_update_time:.0f}s 前, "
                f"当前使用: {self._forex.usdcny:.4f}"
            )

        usdcny = self._forex.usdcny
        hl_cny_kg = hl_usd_oz_to_cny_kg(self._hl_mid, usdcny)

        self._latest = NormalizedPrice(
            ag_price=self._ag_tick.last_price,
            ag_bid=self._ag_tick.bid_price1,
            ag_ask=self._ag_tick.ask_price1,
            ag_bid_vol=self._ag_tick.bid_volume1,
            ag_ask_vol=self._ag_tick.ask_volume1,
            hl_price_usd=self._hl_mid,
            hl_price_cny_kg=hl_cny_kg,
            usdcny=usdcny,
            timestamp=now,
            ag_stale=(now - self._ag_tick.timestamp) > self.STALE_THRESHOLD,
            hl_stale=(now - self._hl_update_time) > self.STALE_THRESHOLD,
            forex_stale=forex_stale,
        )

        for cb in self._callbacks:
            try:
                await cb(self._latest)
            except Exception as e:
                logger.error(f"价格回调错误: {e}")

    async def stop(self):
        """停止"""
        self._running = False
        self._remote_quote_ws_connected = False
        await self._forex.stop()
        logger.info("DataEngine 已停止")
