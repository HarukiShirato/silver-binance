# DEPRECATED: 已被 signal_engine.py 替代 (百分比价差 + SHFE时段管理)
# strategy/spread_calculator.py - 旧版价差计算 (仅保留作参考)

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import numpy as np
import time

import logging
logger = logging.getLogger(__name__)


@dataclass
class SpreadSignal:
    """价差信号"""
    pair_name: str
    timestamp: float
    xyz_price: float
    lighter_price: float
    spread: float
    spread_mean: float
    spread_std: float
    zscore: float
    signal: str  # "LONG", "SHORT", "EXIT_LONG", "EXIT_SHORT", "HOLD"


class SpreadCalculator:
    """价差计算器"""

    def __init__(
        self,
        pair_name: str,
        window_size: int = 100,
        entry_zscore: float = 2.5,
        exit_zscore: float = 0.3,
        stop_loss_zscore: float = 4.0,
    ):
        self.pair_name = pair_name
        self.window_size = window_size
        self.entry_zscore = entry_zscore
        self.exit_zscore = exit_zscore
        self.stop_loss_zscore = stop_loss_zscore

        # 价格历史
        self._xyz_prices: deque = deque(maxlen=window_size)
        self._lighter_prices: deque = deque(maxlen=window_size)
        self._spreads: deque = deque(maxlen=window_size)
        self._timestamps: deque = deque(maxlen=window_size)

        # 当前状态
        self._current_position: str = "NONE"  # NONE, LONG, SHORT

        # 统计
        self._spread_mean: float = 0
        self._spread_std: float = 0

    def update_prices(self, xyz_price: float, lighter_price: float) -> Optional[SpreadSignal]:
        """更新价格并生成信号"""
        timestamp = time.time()

        # 价格有效性检查
        if xyz_price <= 0 or lighter_price <= 0:
            logger.warning(f"Invalid prices: xyz={xyz_price}, lighter={lighter_price}")
            return None

        # 计算价差
        spread = xyz_price - lighter_price

        # 保存历史
        self._xyz_prices.append(xyz_price)
        self._lighter_prices.append(lighter_price)
        self._spreads.append(spread)
        self._timestamps.append(timestamp)

        # 需要足够的历史数据
        if len(self._spreads) < 20:
            return SpreadSignal(
                pair_name=self.pair_name,
                timestamp=timestamp,
                xyz_price=xyz_price,
                lighter_price=lighter_price,
                spread=spread,
                spread_mean=0,
                spread_std=0,
                zscore=0,
                signal="HOLD"
            )

        # 计算统计量
        spreads_arr = np.array(self._spreads)
        self._spread_mean = np.mean(spreads_arr)
        self._spread_std = np.std(spreads_arr)

        # 避免除零
        if self._spread_std < 1e-8:
            zscore = 0
        else:
            zscore = (spread - self._spread_mean) / self._spread_std

        # 生成信号
        signal = self._generate_signal(zscore)

        return SpreadSignal(
            pair_name=self.pair_name,
            timestamp=timestamp,
            xyz_price=xyz_price,
            lighter_price=lighter_price,
            spread=spread,
            spread_mean=self._spread_mean,
            spread_std=self._spread_std,
            zscore=zscore,
            signal=signal
        )

    def _generate_signal(self, zscore: float) -> str:
        """根据Z-score生成交易信号"""

        # 止损检查
        if self._current_position == "LONG" and zscore < -self.stop_loss_zscore:
            logger.warning(f"STOP LOSS triggered for LONG position, zscore={zscore:.2f}")
            self._current_position = "NONE"
            return "EXIT_LONG"

        if self._current_position == "SHORT" and zscore > self.stop_loss_zscore:
            logger.warning(f"STOP LOSS triggered for SHORT position, zscore={zscore:.2f}")
            self._current_position = "NONE"
            return "EXIT_SHORT"

        # 出场信号
        if self._current_position == "LONG" and zscore >= -self.exit_zscore:
            self._current_position = "NONE"
            return "EXIT_LONG"

        if self._current_position == "SHORT" and zscore <= self.exit_zscore:
            self._current_position = "NONE"
            return "EXIT_SHORT"

        # 入场信号 (仅在无持仓时)
        if self._current_position == "NONE":
            if zscore > self.entry_zscore:
                self._current_position = "SHORT"
                return "SHORT"  # 做空价差: 卖XYZ, 买Lighter

            if zscore < -self.entry_zscore:
                self._current_position = "LONG"
                return "LONG"  # 做多价差: 买XYZ, 卖Lighter

        return "HOLD"

    def set_position(self, position: str):
        """手动设置当前持仓 (用于恢复状态)"""
        self._current_position = position

    @property
    def current_position(self) -> str:
        return self._current_position

    @property
    def spread_stats(self) -> Tuple[float, float]:
        """返回价差均值和标准差"""
        return self._spread_mean, self._spread_std

    def get_current_zscore(self) -> float:
        """获取当前Z-score"""
        if len(self._spreads) < 20 or self._spread_std < 1e-8:
            return 0
        current_spread = self._spreads[-1]
        return (current_spread - self._spread_mean) / self._spread_std

    async def warmup_from_candles(
        self,
        xyz_candles: list,
        lighter_candles: list
    ):
        """使用历史K线数据预热"""
        # 按时间对齐
        xyz_dict = {c['t']: float(c['c']) for c in xyz_candles}
        lighter_dict = {c['t']: float(c['c']) for c in lighter_candles}

        common_times = sorted(set(xyz_dict.keys()) & set(lighter_dict.keys()))

        for t in common_times[-self.window_size:]:
            xyz_price = xyz_dict[t]
            lighter_price = lighter_dict[t]
            spread = xyz_price - lighter_price

            self._xyz_prices.append(xyz_price)
            self._lighter_prices.append(lighter_price)
            self._spreads.append(spread)
            self._timestamps.append(t / 1000)

        logger.info(f"Warmed up {self.pair_name} with {len(self._spreads)} data points")
        logger.info(f"Spread mean: {np.mean(self._spreads):.4f}, std: {np.std(self._spreads):.4f}")
