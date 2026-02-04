# price_feed.py - WebSocket 实时价格推送

import asyncio
import json
import time
from typing import Dict, Callable, Optional
from dataclasses import dataclass
import websockets

import logging
logger = logging.getLogger(__name__)


@dataclass
class PriceUpdate:
    """价格更新"""
    symbol: str
    price: float
    timestamp: float
    source: str  # "xyz" or "lighter"


class PriceFeed:
    """
    实时价格推送管理器

    使用 WebSocket 替代 HTTP 轮询:
    - XYZ: wss://api.hyperliquid.xyz/ws (allMids 订阅)
    - Lighter: wss://mainnet.zklighter.elliot.ai/ws (ticker 订阅)

    延迟: HTTP轮询 ~1000ms → WebSocket ~10-50ms
    """

    def __init__(self):
        self._xyz_ws_url = "wss://api.hyperliquid.xyz/ws"
        self._lighter_ws_url = "wss://mainnet.zklighter.elliot.ai/ws"

        self._xyz_ws: Optional[websockets.WebSocketClientProtocol] = None
        self._lighter_ws: Optional[websockets.WebSocketClientProtocol] = None

        # 最新价格缓存
        self._prices: Dict[str, Dict[str, float]] = {}
        # {pair_name: {"xyz": price, "lighter": price, "timestamp": ts}}

        # 回调
        self._on_price_update: Optional[Callable] = None

        self._running = False
        self._subscribed_pairs: Dict[str, dict] = {}
        # {pair_name: {"xyz_symbol": "xyz:SILVER", "lighter_market_id": 93}}

    def subscribe(self, pair_name: str, xyz_symbol: str, lighter_market_id: int):
        """订阅交易对"""
        self._subscribed_pairs[pair_name] = {
            "xyz_symbol": xyz_symbol,
            "lighter_market_id": lighter_market_id,
        }
        self._prices[pair_name] = {"xyz": 0, "lighter": 0, "timestamp": 0}

    def on_price_update(self, callback: Callable[[str, float, float], None]):
        """
        注册价格更新回调

        callback(pair_name, xyz_price, lighter_price)
        """
        self._on_price_update = callback

    def get_price(self, pair_name: str) -> tuple[float, float, float]:
        """获取最新价格 (xyz_price, lighter_price, timestamp)"""
        p = self._prices.get(pair_name, {})
        return p.get("xyz", 0), p.get("lighter", 0), p.get("timestamp", 0)

    async def start(self):
        """启动价格推送"""
        self._running = True

        # 并行启动两个 WebSocket
        await asyncio.gather(
            self._run_xyz_ws(),
            self._run_lighter_ws(),
        )

    async def stop(self):
        """停止"""
        self._running = False
        if self._xyz_ws:
            await self._xyz_ws.close()
        if self._lighter_ws:
            await self._lighter_ws.close()

    async def _run_xyz_ws(self):
        """XYZ WebSocket 连接"""
        while self._running:
            try:
                async with websockets.connect(self._xyz_ws_url) as ws:
                    self._xyz_ws = ws
                    logger.info("XYZ WebSocket connected")

                    # 订阅 allMids
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "allMids"}
                    }))

                    async for message in ws:
                        await self._handle_xyz_message(json.loads(message))

            except Exception as e:
                logger.error(f"XYZ WebSocket error: {e}")
                await asyncio.sleep(1)

    async def _run_lighter_ws(self):
        """Lighter WebSocket 连接"""
        while self._running:
            try:
                async with websockets.connect(self._lighter_ws_url) as ws:
                    self._lighter_ws = ws
                    logger.info("Lighter WebSocket connected")

                    # 订阅每个市场的 ticker
                    for pair_name, info in self._subscribed_pairs.items():
                        market_id = info["lighter_market_id"]
                        await ws.send(json.dumps({
                            "type": "subscribe",
                            "channel": "ticker",
                            "market_id": market_id
                        }))

                    async for message in ws:
                        await self._handle_lighter_message(json.loads(message))

            except Exception as e:
                logger.error(f"Lighter WebSocket error: {e}")
                await asyncio.sleep(1)

    async def _handle_xyz_message(self, data: dict):
        """处理 XYZ 消息"""
        if data.get("channel") != "allMids":
            return

        mids = data.get("data", {}).get("mids", {})
        ts = time.time()

        for pair_name, info in self._subscribed_pairs.items():
            xyz_symbol = info["xyz_symbol"]
            if xyz_symbol in mids:
                price = float(mids[xyz_symbol])
                self._prices[pair_name]["xyz"] = price
                self._prices[pair_name]["timestamp"] = ts

                # 触发回调
                await self._trigger_callback(pair_name)

    async def _handle_lighter_message(self, data: dict):
        """处理 Lighter 消息"""
        channel = data.get("channel", "")
        if not channel.startswith("ticker"):
            return

        market_id = data.get("market_id")
        price = float(data.get("data", {}).get("last_price", 0))
        ts = time.time()

        for pair_name, info in self._subscribed_pairs.items():
            if info["lighter_market_id"] == market_id:
                self._prices[pair_name]["lighter"] = price
                self._prices[pair_name]["timestamp"] = ts

                await self._trigger_callback(pair_name)
                break

    async def _trigger_callback(self, pair_name: str):
        """触发价格更新回调"""
        if not self._on_price_update:
            return

        p = self._prices[pair_name]
        xyz_price = p.get("xyz", 0)
        lighter_price = p.get("lighter", 0)

        # 只有两边价格都有效时才回调
        if xyz_price > 0 and lighter_price > 0:
            try:
                await self._on_price_update(pair_name, xyz_price, lighter_price)
            except Exception as e:
                logger.error(f"Price callback error: {e}")


class OrderbookCache:
    """
    订单簿缓存

    持续更新订单簿，避免每次交易时重新获取
    """

    def __init__(self):
        self._xyz_books: Dict[str, list] = {}  # {symbol: [(price, size), ...]}
        self._lighter_books: Dict[int, dict] = {}  # {market_id: {"bids": [], "asks": []}}
        self._last_update: Dict[str, float] = {}

    def update_xyz_book(self, symbol: str, bids: list, asks: list):
        """更新 XYZ 订单簿"""
        self._xyz_books[symbol] = {"bids": bids, "asks": asks}
        self._last_update[f"xyz:{symbol}"] = time.time()

    def update_lighter_book(self, market_id: int, bids: list, asks: list):
        """更新 Lighter 订单簿"""
        self._lighter_books[market_id] = {"bids": bids, "asks": asks}
        self._last_update[f"lighter:{market_id}"] = time.time()

    def get_xyz_book(self, symbol: str) -> dict:
        return self._xyz_books.get(symbol, {"bids": [], "asks": []})

    def get_lighter_book(self, market_id: int) -> dict:
        return self._lighter_books.get(market_id, {"bids": [], "asks": []})

    def is_fresh(self, key: str, max_age_ms: int = 500) -> bool:
        """检查数据是否新鲜"""
        last = self._last_update.get(key, 0)
        return (time.time() - last) * 1000 < max_age_ms
