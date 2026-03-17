# forex_feed.py - USD/CNY 实时汇率获取
#
# 使用 open.er-api.com 免费 API 获取汇率
# 每 60 秒更新一次, 失败时使用上次成功的汇率或 fallback 值

import asyncio
import time
from typing import Optional

import aiohttp

import logging
logger = logging.getLogger(__name__)


class ForexFeed:
    """USD/CNY 实时汇率"""

    DEFAULT_USDCNY = 7.25
    API_URL = "https://open.er-api.com/v6/latest/USD"

    def __init__(
        self,
        fallback_rate: float = 7.25,
        update_interval: int = 60,
    ):
        self._rate: float = fallback_rate
        self._fallback: float = fallback_rate
        self._interval: int = update_interval
        self._last_update: float = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._stale_threshold: int = 3600  # 超过1小时未更新视为过期

    @property
    def usdcny(self) -> float:
        """当前 USD/CNY 汇率"""
        return self._rate

    @property
    def is_stale(self) -> bool:
        """汇率是否过期 (超过1小时未更新)"""
        if self._last_update == 0:
            return True
        return (time.time() - self._last_update) > self._stale_threshold

    @property
    def last_update_time(self) -> float:
        return self._last_update

    async def start(self):
        """启动汇率更新循环"""
        self._running = True
        logger.info("ForexFeed 启动, 开始获取 USD/CNY 汇率...")

        # 立即获取一次
        await self._fetch_rate()

        # 定期更新
        while self._running:
            await asyncio.sleep(self._interval)
            if self._running:
                await self._fetch_rate()

    async def _fetch_rate(self):
        """从 API 获取汇率"""
        try:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10)
                )

            async with self._session.get(self.API_URL) as resp:
                if resp.status != 200:
                    logger.warning(f"汇率 API 返回 {resp.status}")
                    return

                data = await resp.json()
                rates = data.get("rates", {})
                cny_rate = rates.get("CNY")

                if cny_rate and cny_rate > 0:
                    old_rate = self._rate
                    self._rate = float(cny_rate)
                    self._last_update = time.time()

                    if abs(old_rate - self._rate) > 0.01:
                        logger.info(f"USD/CNY 汇率更新: {old_rate:.4f} → {self._rate:.4f}")
                else:
                    logger.warning("API 返回数据中无 CNY 汇率")

        except Exception as e:
            logger.warning(f"获取汇率失败 (使用 {self._rate:.4f}): {e}")

    async def fetch_once(self) -> float:
        """单次获取汇率 (用于初始化)"""
        await self._fetch_rate()
        return self._rate

    async def stop(self):
        """停止"""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info(f"ForexFeed 停止, 最终汇率: {self._rate:.4f}")
