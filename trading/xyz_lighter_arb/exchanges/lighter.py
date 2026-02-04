# exchanges/lighter.py - Lighter 交易接口

import asyncio
import json
import time
from typing import Optional, Dict, Any, Callable, List
from decimal import Decimal
import aiohttp
import websockets

import logging
logger = logging.getLogger(__name__)


class LighterClient:
    """Lighter API 客户端"""

    def __init__(self, api_url: str, ws_url: str, private_key: str = "", api_key: str = ""):
        self.api_url = api_url
        self.ws_url = ws_url
        self.private_key = private_key
        self.api_key = api_key

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_callbacks: Dict[str, Callable] = {}
        self._running = False

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {}
            if self.api_key:
                headers["X-API-Key"] = self.api_key
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
                headers=headers
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._ws:
            await self._ws.close()
        self._running = False

    # ==================== 公共API ====================

    async def get_orderbooks(self) -> List[Dict]:
        """获取所有订单簿信息"""
        session = await self._get_session()
        async with session.get(f"{self.api_url}/api/v1/orderBooks") as resp:
            data = await resp.json()
            return data.get("order_books", [])

    async def get_orderbook_details(self, market_id: int) -> Dict:
        """获取订单簿详情"""
        session = await self._get_session()
        async with session.get(
            f"{self.api_url}/api/v1/orderBookDetails",
            params={"market_id": market_id}
        ) as resp:
            data = await resp.json()
            details = data.get("order_book_details", [])
            return details[0] if details else {}

    async def get_orderbook_orders(self, market_id: int, depth: int = 20) -> Dict:
        """获取订单簿深度"""
        session = await self._get_session()
        async with session.get(
            f"{self.api_url}/api/v1/orderBookOrders",
            params={"market_id": market_id, "depth": depth}
        ) as resp:
            return await resp.json()

    async def get_candles(
        self,
        market_id: int,
        resolution: str = "1h",
        start_timestamp: int = None,
        end_timestamp: int = None,
        count_back: int = 500
    ) -> List[Dict]:
        """获取K线数据"""
        if end_timestamp is None:
            end_timestamp = int(time.time() * 1000)
        if start_timestamp is None:
            start_timestamp = end_timestamp - 86400000 * 7

        session = await self._get_session()
        async with session.get(
            f"{self.api_url}/api/v1/candles",
            params={
                "market_id": market_id,
                "resolution": resolution,
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "count_back": count_back
            }
        ) as resp:
            data = await resp.json()
            return data.get("c", [])

    async def get_ticker(self, market_id: int) -> Dict:
        """获取行情数据"""
        details = await self.get_orderbook_details(market_id)
        return {
            "price": float(details.get("last_trade_price", 0)),
            "high_24h": float(details.get("daily_price_high", 0)),
            "low_24h": float(details.get("daily_price_low", 0)),
            "volume_24h": float(details.get("daily_quote_token_volume", 0)),
            "open_interest": float(details.get("open_interest", 0)),
        }

    async def get_funding_rates(self) -> List[Dict]:
        """获取资金费率"""
        session = await self._get_session()
        async with session.get(f"{self.api_url}/api/v1/funding-rates") as resp:
            data = await resp.json()
            return data.get("funding_rates", [])

    # ==================== 账户API ====================

    async def get_account_info(self, account_index: int) -> Dict:
        """获取账户信息"""
        session = await self._get_session()
        async with session.get(
            f"{self.api_url}/api/v1/account",
            params={"account_index": account_index}
        ) as resp:
            return await resp.json()

    async def get_positions(self, account_index: int) -> List[Dict]:
        """获取持仓"""
        session = await self._get_session()
        async with session.get(
            f"{self.api_url}/api/v1/positions",
            params={"account_index": account_index}
        ) as resp:
            data = await resp.json()
            return data.get("positions", [])

    async def get_open_orders(self, account_index: int) -> List[Dict]:
        """获取未成交订单"""
        session = await self._get_session()
        async with session.get(
            f"{self.api_url}/api/v1/orders",
            params={"account_index": account_index, "status": "open"}
        ) as resp:
            data = await resp.json()
            return data.get("orders", [])

    # ==================== 交易API ====================

    async def place_order(
        self,
        market_id: int,
        is_buy: bool,
        size: float,
        price: float,
        order_type: str = "limit",  # limit, market
        reduce_only: bool = False,
        post_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Dict:
        """下单

        注意: Lighter 需要签名，这里是简化版本
        实际使用需要参考 Lighter SDK 的签名逻辑
        """
        session = await self._get_session()

        order_data = {
            "market_id": market_id,
            "side": "buy" if is_buy else "sell",
            "size": str(size),
            "price": str(price),
            "type": order_type,
            "reduce_only": reduce_only,
            "post_only": post_only,
        }

        if client_order_id:
            order_data["client_order_id"] = client_order_id

        # 需要签名 - 这里仅展示结构
        # 实际需要使用 Lighter 的签名库
        async with session.post(
            f"{self.api_url}/api/v1/order",
            json=order_data
        ) as resp:
            result = await resp.json()
            logger.info(f"Lighter order result: {result}")
            return result

    async def cancel_order(self, market_id: int, order_id: str) -> Dict:
        """取消订单"""
        session = await self._get_session()
        async with session.delete(
            f"{self.api_url}/api/v1/order",
            params={"market_id": market_id, "order_id": order_id}
        ) as resp:
            return await resp.json()

    async def cancel_all_orders(self, market_id: int) -> Dict:
        """取消所有订单"""
        session = await self._get_session()
        async with session.delete(
            f"{self.api_url}/api/v1/orders",
            params={"market_id": market_id}
        ) as resp:
            return await resp.json()

    async def market_order(self, market_id: int, is_buy: bool, size: float) -> Dict:
        """市价单"""
        # 获取当前价格
        ticker = await self.get_ticker(market_id)
        price = ticker["price"]

        # 设置滑点保护
        slippage = 0.005
        if is_buy:
            price = price * (1 + slippage)
        else:
            price = price * (1 - slippage)

        return await self.place_order(
            market_id=market_id,
            is_buy=is_buy,
            size=size,
            price=round(price, 4),
            order_type="limit",  # 用限价单模拟
        )

    # ==================== WebSocket ====================

    async def connect_ws(self):
        """连接WebSocket"""
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self._ws = ws
                    logger.info("Lighter WebSocket connected")

                    # 接收消息
                    async for message in ws:
                        await self._handle_message(json.loads(message))

            except Exception as e:
                logger.error(f"Lighter WebSocket error: {e}")
                await asyncio.sleep(5)

    async def subscribe_orderbook(self, market_id: int):
        """订阅订单簿"""
        if self._ws:
            await self._ws.send(json.dumps({
                "type": "subscribe",
                "channel": "orderbook",
                "market_id": market_id
            }))

    async def subscribe_trades(self, market_id: int):
        """订阅成交"""
        if self._ws:
            await self._ws.send(json.dumps({
                "type": "subscribe",
                "channel": "trades",
                "market_id": market_id
            }))

    async def _handle_message(self, data: Dict):
        """处理WebSocket消息"""
        channel = data.get("channel", "")
        if channel in self._ws_callbacks:
            await self._ws_callbacks[channel](data)

    def on_orderbook_update(self, market_id: int, callback: Callable):
        """注册订单簿更新回调"""
        self._ws_callbacks[f"orderbook:{market_id}"] = callback

    def on_trade(self, market_id: int, callback: Callable):
        """注册成交回调"""
        self._ws_callbacks[f"trades:{market_id}"] = callback
