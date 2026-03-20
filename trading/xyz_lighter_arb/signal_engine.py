# signal_engine.py - Layer 2: 信号引擎
#
# 百分比价差 → 滚动 Z-score → 交易信号
# 公式来自 backtest/silver/spread_backtest_1m.py:
#   spread_pct = (ag_price - hl_cny_price) / hl_cny_price * 100
#
# 采样方式: 每 sample_interval 秒取一个数据点加入滚动窗口,
#   与回测中1分钟K线的统计特性保持一致.
#   中间的 tick 仍然实时计算 z-score 用于信号判断, 但不写入窗口.
#
# 信号方向:
#   LONG  = 做多价差 = 买AG + 卖HL (AG 便宜时)
#   SHORT = 做空价差 = 卖AG + 买HL (AG 贵时)

import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import numpy as np

from data_engine import NormalizedPrice
from session_manager import SessionManager

import logging
logger = logging.getLogger(__name__)


class Signal(str, Enum):
    LONG = "LONG"                # 买AG, 卖HL
    SHORT = "SHORT"              # 卖AG, 买HL
    EXIT_LONG = "EXIT_LONG"      # 平多: 卖AG, 买HL
    EXIT_SHORT = "EXIT_SHORT"    # 平空: 买AG, 卖HL
    HOLD = "HOLD"


@dataclass
class SignalResult:
    """信号结果"""
    signal: Signal
    # 价格
    ag_price: float              # RMB/kg
    hl_price_cny_kg: float       # 换算后 RMB/kg
    hl_price_usd_oz: float       # 原始 USD/oz
    usdcny: float
    # 价差统计
    spread_pct: float            # (ag - hl_cny) / hl_cny * 100
    spread_mean: float
    spread_std: float
    zscore: float
    # 其他
    funding_rate: float
    timestamp: float


class SignalEngine:
    """价差信号引擎 (按时间采样, 与回测1分钟K线对齐)"""

    MIN_DATA_POINTS = 30  # 最少需要的数据点才开始出信号

    def __init__(
        self,
        pair_name: str,
        window_size: int = 60,
        entry_zscore: float = 1.5,
        exit_zscore: float = 0.5,
        stop_loss_zscore: float = 4.0,
        sample_interval: int = 60,
        session_manager: Optional[SessionManager] = None,
    ):
        self._pair_name = pair_name
        self._window_size = window_size
        self._entry_z = entry_zscore
        self._exit_z = exit_zscore
        self._stop_z = stop_loss_zscore
        self._sample_interval = sample_interval  # 采样间隔 (秒)
        self._session_mgr = session_manager

        # 滚动窗口 (按时间采样的数据点)
        self._spread_pcts: deque = deque(maxlen=window_size)

        # 采样控制
        self._last_sample_time: float = 0  # 上次采样时间戳
        self._latest_spread_pct: float = 0  # 当前最新价差 (用于采样时写入窗口)

        # 缓存的统计量 (避免每个 tick 都重新计算)
        self._cached_mean: float = 0
        self._cached_std: float = 0

        # 当前持仓方向
        self._position: str = "NONE"  # NONE, LONG, SHORT

        # Funding rate 跟踪
        self._last_funding_rate: float = 0
        self._cumulative_funding: float = 0

    @property
    def position(self) -> str:
        return self._position

    @property
    def data_ready(self) -> bool:
        return len(self._spread_pcts) >= self.MIN_DATA_POINTS

    def update(self, price: NormalizedPrice) -> Optional[SignalResult]:
        """
        接收 NormalizedPrice, 计算价差和信号

        采样逻辑:
        - 每个 tick 都实时计算 spread_pct 和 zscore (用于信号判断)
        - 但只有每隔 sample_interval 秒才将一个数据点写入滚动窗口
        - 这样 window=60 + interval=60s = 60分钟窗口, 与回测一致

        Returns:
            SignalResult 或 None (数据不足或非交易时段)
        """
        # 检查交易时段
        if self._session_mgr and not self._session_mgr.is_trading_time():
            return None

        # 价格有效性
        if price.ag_price <= 0 or price.hl_price_cny_kg <= 0:
            return None

        # 跳过过期数据
        if price.ag_stale or price.hl_stale:
            logger.debug("价格数据过期, 跳过信号生成")
            return None

        # 汇率过期时只允许平仓信号, 禁止开新仓
        if price.forex_stale and self._position == "NONE":
            logger.warning("汇率过期, 禁止开新仓")
            return None

        # 百分比价差: (AG - HL_CNY) / HL_CNY * 100
        spread_pct = (price.ag_price - price.hl_price_cny_kg) / price.hl_price_cny_kg * 100
        self._latest_spread_pct = spread_pct

        # === 按时间采样: 每 sample_interval 秒写入一个数据点 ===
        now = price.timestamp
        if self._last_sample_time == 0:
            # 首次: 直接采样
            self._spread_pcts.append(spread_pct)
            self._last_sample_time = now
            self._update_stats()
            logger.info(f"首次采样, spread_pct={spread_pct:.4f}%, 窗口大小={len(self._spread_pcts)}")
        elif (now - self._last_sample_time) >= self._sample_interval:
            # 到达采样间隔: 将最新价差写入窗口
            self._spread_pcts.append(spread_pct)
            self._last_sample_time = now
            self._update_stats()
            logger.debug(
                f"采样: spread_pct={spread_pct:.4f}%, "
                f"窗口={len(self._spread_pcts)}/{self._window_size}, "
                f"mean={self._cached_mean:.4f}%, std={self._cached_std:.4f}%"
            )

        # 数据不足
        if not self.data_ready:
            return SignalResult(
                signal=Signal.HOLD,
                ag_price=price.ag_price,
                hl_price_cny_kg=price.hl_price_cny_kg,
                hl_price_usd_oz=price.hl_price_usd,
                usdcny=price.usdcny,
                spread_pct=spread_pct,
                spread_mean=self._cached_mean,
                spread_std=self._cached_std,
                zscore=0,
                funding_rate=self._last_funding_rate,
                timestamp=price.timestamp,
            )

        # 实时计算 Z-score (用窗口的 mean/std, 但用当前最新 spread_pct)
        zscore = (spread_pct - self._cached_mean) / self._cached_std if self._cached_std > 1e-8 else 0

        # 生成信号
        signal = self._generate_signal(zscore)

        return SignalResult(
            signal=signal,
            ag_price=price.ag_price,
            hl_price_cny_kg=price.hl_price_cny_kg,
            hl_price_usd_oz=price.hl_price_usd,
            usdcny=price.usdcny,
            spread_pct=spread_pct,
            spread_mean=self._cached_mean,
            spread_std=self._cached_std,
            zscore=zscore,
            funding_rate=self._last_funding_rate,
            timestamp=price.timestamp,
        )

    def _update_stats(self):
        """更新缓存的统计量 (仅在新数据点采样时调用)"""
        if len(self._spread_pcts) < 2:
            self._cached_mean = self._spread_pcts[0] if self._spread_pcts else 0
            self._cached_std = 0
            return
        arr = np.array(self._spread_pcts)
        self._cached_mean = float(np.mean(arr))
        self._cached_std = float(np.std(arr))

    def _generate_signal(self, zscore: float) -> Signal:
        """
        根据 Z-score 生成交易信号

        zscore > 0: AG 相对 HL 偏贵
        zscore < 0: AG 相对 HL 偏便宜

        LONG  (zscore << -entry): AG便宜 → 买AG, 卖HL → 等价差回归
        SHORT (zscore >> +entry): AG贵   → 卖AG, 买HL → 等价差回归
        """
        # 止损
        if self._position == "LONG" and zscore < -self._stop_z:
            logger.warning(f"止损平多, zscore={zscore:.2f}")
            self._position = "NONE"
            return Signal.EXIT_LONG

        if self._position == "SHORT" and zscore > self._stop_z:
            logger.warning(f"止损平空, zscore={zscore:.2f}")
            self._position = "NONE"
            return Signal.EXIT_SHORT

        # 出场 (价差均值回归)
        if self._position == "LONG" and zscore >= -self._exit_z:
            self._position = "NONE"
            return Signal.EXIT_LONG

        if self._position == "SHORT" and zscore <= self._exit_z:
            self._position = "NONE"
            return Signal.EXIT_SHORT

        # 入场 (仅在空仓时)
        if self._position == "NONE":
            # 临近收盘不开新仓
            if self._session_mgr and self._session_mgr.is_near_boundary():
                return Signal.HOLD

            if zscore < -self._entry_z:
                self._position = "LONG"
                return Signal.LONG

            if zscore > self._entry_z:
                self._position = "SHORT"
                return Signal.SHORT

        return Signal.HOLD

    def update_funding_rate(self, rate: float):
        """更新 HL funding rate"""
        self._last_funding_rate = rate
        # 如果持仓中, 累计 funding 成本
        if self._position != "NONE":
            self._cumulative_funding += rate

    def set_position(self, position: str):
        """恢复持仓状态 (重启后)"""
        self._position = position

    def reset_funding(self):
        """重置 funding 累计"""
        self._cumulative_funding = 0

    @property
    def cumulative_funding(self) -> float:
        return self._cumulative_funding

    def get_current_zscore(self) -> float:
        """获取最新 Z-score"""
        if not self.data_ready:
            return 0
        if self._cached_std < 1e-8:
            return 0
        return (self._latest_spread_pct - self._cached_mean) / self._cached_std
