# exchanges/hyperliquid.py - Hyperliquid (XYZ) 交易接口

import asyncio
import json
import time
import hashlib
import hmac
from typing import Optional, Dict, Any, Callable
from decimal import Decimal
import aiohttp
import websockets
from eth_account import Account
from eth_account.messages import encode_typed_data

import logging
logger = logging.getLogger(__name__)


class HyperliquidClient:
    """Hyperliquid API 客户端"""

    def __init__(self, api_url: str, ws_url: str, private_key: str, wallet_address: str):
        self.api_url = api_url
        self.ws_url = ws_url
        self.private_key = private_key
        self.wallet_address = wallet_address
        self.account = Account.from_key(private_key) if private_key else None

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_callbacks: Dict[str, Callable] = {}
        self._running = False

        # asset_id 缓存
        self._asset_id_cache: Dict[str, int] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._ws:
            await self._ws.close()
        self._running = False

    # ==================== 公共API ====================

    async def get_all_mids(self) -> Dict[str, float]:
        """获取所有交易对的中间价"""
        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/info",
            json={"type": "allMids"}
        ) as resp:
            data = await resp.json()
            return {k: float(v) for k, v in data.items()}

    async def get_orderbook(self, coin: str, depth: int = 20) -> Dict:
        """获取订单簿"""
        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/info",
            json={"type": "l2Book", "coin": coin, "nSigFigs": 5}
        ) as resp:
            return await resp.json()

    async def get_candles(self, coin: str, interval: str = "1h",
                          start_time: int = None, end_time: int = None) -> list:
        """获取K线数据"""
        if end_time is None:
            end_time = int(time.time() * 1000)
        if start_time is None:
            start_time = end_time - 86400000 * 7  # 默认7天

        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/info",
            json={
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": start_time,
                    "endTime": end_time
                }
            }
        ) as resp:
            return await resp.json()

    async def get_user_state(self) -> Dict:
        """获取账户状态"""
        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/info",
            json={"type": "clearinghouseState", "user": self.wallet_address}
        ) as resp:
            return await resp.json()

    async def get_open_orders(self) -> list:
        """获取未成交订单"""
        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/info",
            json={"type": "openOrders", "user": self.wallet_address}
        ) as resp:
            return await resp.json()

    # ==================== 交易API ====================

    def _sign_action(self, action: Dict, nonce: int, vault_address: Optional[str] = None) -> Dict:
        """签名交易动作"""
        is_mainnet = "api.hyperliquid.xyz" in self.api_url

        # EIP-712 签名
        domain = {
            "name": "HyperliquidSignTransaction",
            "version": "1",
            "chainId": 42161 if is_mainnet else 421614,
            "verifyingContract": "0x0000000000000000000000000000000000000000",
        }

        types = {
            "HyperliquidTransaction:Approve": [
                {"name": "hyperliquidChain", "type": "string"},
                {"name": "signatureChainId", "type": "uint64"},
                {"name": "nonce", "type": "uint64"},
            ]
        }

        message = {
            "hyperliquidChain": "Mainnet" if is_mainnet else "Testnet",
            "signatureChainId": 42161 if is_mainnet else 421614,
            "nonce": nonce,
        }

        signable_message = encode_typed_data(domain, types, message)
        signed = self.account.sign_message(signable_message)

        return {
            "action": action,
            "nonce": nonce,
            "signature": {
                "r": hex(signed.r),
                "s": hex(signed.s),
                "v": signed.v,
            },
            "vaultAddress": vault_address,
        }

    async def place_order(
        self,
        coin: str,
        is_buy: bool,
        size: float,
        price: float,
        order_type: str = "Limit",  # Limit, Market
        reduce_only: bool = False,
        client_oid: Optional[str] = None,
    ) -> Dict:
        """下单"""
        await self._ensure_asset_ids()
        nonce = int(time.time() * 1000)

        # 构建订单
        order = {
            "a": self._coin_to_asset_id(coin),  # asset id
            "b": is_buy,
            "p": str(price),
            "s": str(size),
            "r": reduce_only,
            "t": {"limit": {"tif": "Gtc"}} if order_type == "Limit" else {"market": {}},
        }

        if client_oid:
            order["c"] = client_oid

        action = {
            "type": "order",
            "orders": [order],
            "grouping": "na",
        }

        payload = self._sign_action(action, nonce)

        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/exchange",
            json=payload
        ) as resp:
            result = await resp.json()
            logger.info(f"HL order result: {result}")
            return result

    async def cancel_order(self, coin: str, oid: int) -> Dict:
        """取消订单"""
        nonce = int(time.time() * 1000)

        action = {
            "type": "cancel",
            "cancels": [{"a": self._coin_to_asset_id(coin), "o": oid}],
        }

        payload = self._sign_action(action, nonce)

        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/exchange",
            json=payload
        ) as resp:
            return await resp.json()

    async def cancel_all_orders(self, coin: str) -> list:
        """取消指定合约的所有挂单"""
        orders = await self.get_open_orders()
        name = coin.replace("xyz:", "")
        to_cancel = [o for o in orders if o.get("coin") == name]
        results = []
        for o in to_cancel:
            oid = o.get("oid")
            if oid is not None:
                r = await self.cancel_order(coin, oid)
                results.append(r)
                logger.info(f"已取消 HL 挂单: oid={oid}, coin={name}")
        if not to_cancel:
            logger.info(f"HL 无 {name} 挂单需要取消")
        return results

    async def market_order(self, coin: str, is_buy: bool, size: float) -> Dict:
        """市价单 (使用滑点保护价格)"""
        # 获取当前价格
        mids = await self.get_all_mids()
        mid_price = mids.get(coin, 0)

        if mid_price == 0:
            raise ValueError(f"Cannot get price for {coin}")

        # 设置滑点保护价格 (0.5%)
        slippage = 0.005
        if is_buy:
            price = mid_price * (1 + slippage)
        else:
            price = mid_price * (1 - slippage)

        return await self.place_order(
            coin=coin,
            is_buy=is_buy,
            size=size,
            price=round(price, 4),
            order_type="Limit",  # 用限价单模拟市价单
        )

    async def _ensure_asset_ids(self):
        """从 meta API 预缓存 asset_id 映射"""
        if self._asset_id_cache:
            return

        session = await self._get_session()

        # 查询永续合约 meta
        async with session.post(
            f"{self.api_url}/info",
            json={"type": "metaAndAssetCtxs"}
        ) as resp:
            data = await resp.json()

        universe = data[0].get("universe", []) if isinstance(data, list) else []
        for i, asset in enumerate(universe):
            name = asset.get("name", "")
            self._asset_id_cache[name] = i

        logger.info(f"Cached {len(self._asset_id_cache)} asset IDs")

    def _coin_to_asset_id(self, coin: str) -> int:
        """查找 coin → asset_id"""
        # 处理 "xyz:SILVER" → "SILVER" 格式
        name = coin.replace("xyz:", "")
        if name in self._asset_id_cache:
            return self._asset_id_cache[name]
        # 尝试原名
        if coin in self._asset_id_cache:
            return self._asset_id_cache[coin]
        raise ValueError(
            f"Unknown coin: {coin}, available: {list(self._asset_id_cache.keys())[:20]}"
        )

    async def get_funding_rate(self, coin: str) -> float:
        """获取指定合约的当前 funding rate"""
        session = await self._get_session()
        async with session.post(
            f"{self.api_url}/info",
            json={"type": "metaAndAssetCtxs"}
        ) as resp:
            data = await resp.json()

        if not isinstance(data, list) or len(data) < 2:
            return 0.0

        universe = data[0].get("universe", [])
        ctxs = data[1]

        name = coin.replace("xyz:", "")
        for i, asset in enumerate(universe):
            if asset.get("name") == name and i < len(ctxs):
                return float(ctxs[i].get("funding", 0))

        return 0.0

    # ==================== WebSocket ====================

    async def connect_ws(self):
        """连接WebSocket"""
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self._ws = ws
                    logger.info("HL WebSocket connected")

                    # 订阅
                    for channel, callback in self._ws_callbacks.items():
                        await self._subscribe(channel)

                    # 接收消息
                    async for message in ws:
                        await self._handle_message(json.loads(message))

            except Exception as e:
                logger.error(f"HL WebSocket error: {e}")
                await asyncio.sleep(5)

    async def _subscribe(self, channel: str):
        """订阅频道"""
        if self._ws:
            await self._ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": channel}
            }))

    async def _handle_message(self, data: Dict):
        """处理WebSocket消息"""
        channel = data.get("channel", "")
        if channel in self._ws_callbacks:
            await self._ws_callbacks[channel](data)

    def on_price_update(self, callback: Callable):
        """注册价格更新回调"""
        self._ws_callbacks["allMids"] = callback

    def on_orderbook_update(self, coin: str, callback: Callable):
        """注册订单簿更新回调"""
        self._ws_callbacks[f"l2Book:{coin}"] = callback
