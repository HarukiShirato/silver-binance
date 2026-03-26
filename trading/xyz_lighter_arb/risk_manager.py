# risk_manager.py - 风控模块

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, date
import json
import os

import logging
logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """交易记录"""
    timestamp: float
    pair_name: str
    signal: str
    xyz_price: float
    lighter_price: float
    spread: float
    zscore: float
    pnl: float = 0.0
    status: str = "pending"  # pending, filled, failed


@dataclass
class DailyStats:
    """每日统计"""
    date: str
    trade_count: int = 0
    total_pnl: float = 0.0
    win_count: int = 0
    loss_count: int = 0
    max_drawdown: float = 0.0
    peak_pnl: float = 0.0


class RiskManager:
    """风险管理器"""

    def __init__(
        self,
        max_daily_trades: int = 50,
        max_daily_loss: float = 2000,
        leg_timeout_ms: int = 2000,
        leg_retry_times: int = 2,
        emergency_spread_pct: float = 25.0,
    ):
        self.max_daily_trades = max_daily_trades
        self.max_daily_loss = max_daily_loss
        self.leg_timeout_ms = leg_timeout_ms
        self.leg_retry_times = leg_retry_times
        self.emergency_spread_pct = emergency_spread_pct

        # 当日统计
        self._today: str = ""
        self._daily_stats: DailyStats = DailyStats(date="")

        # 交易记录
        self._trades: List[TradeRecord] = []
        self._open_positions: Dict[str, TradeRecord] = {}

        # 状态标志
        self._emergency_stop = False
        self._cooldown_until: float = 0

        # 仓位状态 (供重启恢复)
        self._position_state: Dict[str, dict] = {}
        # {"SILVER": {"direction": "LONG", "lots": 3}}

        # 持久化路径
        self._state_file = "data/risk_state.json"

    def _check_new_day(self):
        """检查是否新的一天"""
        today = date.today().isoformat()
        if today != self._today:
            logger.info(f"New day started: {today}")
            self._today = today
            self._daily_stats = DailyStats(date=today)

    def can_trade(self, pair_name: str) -> tuple[bool, str]:
        """检查是否可以交易"""
        self._check_new_day()

        # 紧急停止
        if self._emergency_stop:
            return False, "Emergency stop activated"

        # 冷却期
        if time.time() < self._cooldown_until:
            remaining = self._cooldown_until - time.time()
            return False, f"Cooldown period, {remaining:.0f}s remaining"

        # 每日交易次数限制
        if self.max_daily_trades > 0 and self._daily_stats.trade_count >= self.max_daily_trades:
            return False, f"Daily trade limit reached: {self.max_daily_trades}"

        # 每日亏损限制
        if self._daily_stats.total_pnl <= -self.max_daily_loss:
            return False, f"Daily loss limit reached: ${self.max_daily_loss}"

        return True, "OK"

    def check_spread_safety(self, xyz_price: float, lighter_price: float) -> tuple[bool, str]:
        """检查价差是否安全"""
        if xyz_price <= 0 or lighter_price <= 0:
            return False, "Invalid prices"

        mid_price = (xyz_price + lighter_price) / 2
        spread_pct = abs(xyz_price - lighter_price) / mid_price * 100

        if spread_pct > self.emergency_spread_pct:
            self._emergency_stop = True
            return False, f"Emergency: spread {spread_pct:.2f}% > {self.emergency_spread_pct}%"

        return True, "OK"

    def record_trade_start(self, pair_name: str, signal: str,
                          xyz_price: float, lighter_price: float,
                          spread: float, zscore: float) -> TradeRecord:
        """记录交易开始"""
        self._check_new_day()

        trade = TradeRecord(
            timestamp=time.time(),
            pair_name=pair_name,
            signal=signal,
            xyz_price=xyz_price,
            lighter_price=lighter_price,
            spread=spread,
            zscore=zscore,
        )

        self._trades.append(trade)
        self._daily_stats.trade_count += 1

        if signal in ["LONG", "SHORT"]:
            self._open_positions[pair_name] = trade

        return trade

    def record_trade_result(self, pair_name: str, pnl: float, status: str = "filled"):
        """记录交易结果"""
        # 更新最近的交易记录
        for trade in reversed(self._trades):
            if trade.pair_name == pair_name and trade.status == "pending":
                trade.pnl = pnl
                trade.status = status
                break

        # 更新每日统计
        self._daily_stats.total_pnl += pnl
        if pnl > 0:
            self._daily_stats.win_count += 1
        elif pnl < 0:
            self._daily_stats.loss_count += 1

        # 更新最大回撤
        if self._daily_stats.total_pnl > self._daily_stats.peak_pnl:
            self._daily_stats.peak_pnl = self._daily_stats.total_pnl
        drawdown = self._daily_stats.peak_pnl - self._daily_stats.total_pnl
        if drawdown > self._daily_stats.max_drawdown:
            self._daily_stats.max_drawdown = drawdown

        # 清除平仓记录
        if pair_name in self._open_positions:
            del self._open_positions[pair_name]

        logger.info(f"Trade result: {pair_name} PnL=${pnl:.2f}, Daily PnL=${self._daily_stats.total_pnl:.2f}")

    def set_cooldown(self, seconds: int):
        """设置冷却期"""
        self._cooldown_until = time.time() + seconds

    def trigger_emergency_stop(self, reason: str):
        """触发紧急停止"""
        self._emergency_stop = True
        logger.critical(f"EMERGENCY STOP: {reason}")

    def reset_emergency_stop(self):
        """重置紧急停止"""
        self._emergency_stop = False
        logger.info("Emergency stop reset")

    @property
    def is_emergency(self) -> bool:
        return self._emergency_stop

    @property
    def daily_stats(self) -> DailyStats:
        self._check_new_day()
        return self._daily_stats

    @property
    def open_positions(self) -> Dict[str, TradeRecord]:
        return self._open_positions

    def save_position_state(self, pair_name: str, direction: str, lots: int):
        """保存仓位状态 (供崩溃恢复)"""
        if direction == "NONE" or lots <= 0:
            self._position_state.pop(pair_name, None)
        else:
            self._position_state[pair_name] = {"direction": direction, "lots": lots}

    def get_position_state(self, pair_name: str) -> Optional[dict]:
        """获取保存的仓位状态"""
        return self._position_state.get(pair_name)

    def get_summary(self) -> str:
        """获取状态摘要"""
        stats = self.daily_stats
        return (
            f"Date: {stats.date}\n"
            f"Trades: {stats.trade_count}/{self.max_daily_trades}\n"
            f"PnL: ${stats.total_pnl:.2f}\n"
            f"Win/Loss: {stats.win_count}/{stats.loss_count}\n"
            f"Max Drawdown: ${stats.max_drawdown:.2f}\n"
            f"Emergency: {self._emergency_stop}"
        )

    def save_state(self):
        """保存状态"""
        state = {
            "today": self._today,
            "daily_stats": {
                "date": self._daily_stats.date,
                "trade_count": self._daily_stats.trade_count,
                "total_pnl": self._daily_stats.total_pnl,
                "win_count": self._daily_stats.win_count,
                "loss_count": self._daily_stats.loss_count,
                "max_drawdown": self._daily_stats.max_drawdown,
                "peak_pnl": self._daily_stats.peak_pnl,
            },
            "emergency_stop": self._emergency_stop,
            "open_positions": {
                k: {
                    "timestamp": v.timestamp,
                    "pair_name": v.pair_name,
                    "signal": v.signal,
                    "xyz_price": v.xyz_price,
                    "lighter_price": v.lighter_price,
                    "spread": v.spread,
                    "zscore": v.zscore,
                    "pnl": v.pnl,
                    "status": v.status,
                } for k, v in self._open_positions.items()
            },
            "position_state": self._position_state,
        }

        os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
        with open(self._state_file, 'w') as f:
            json.dump(state, f, indent=2)

    def load_state(self):
        """加载状态"""
        if not os.path.exists(self._state_file):
            return

        try:
            with open(self._state_file, 'r') as f:
                state = json.load(f)

            self._today = state.get("today", "")
            ds = state.get("daily_stats", {})
            self._daily_stats = DailyStats(
                date=ds.get("date", ""),
                trade_count=ds.get("trade_count", 0),
                total_pnl=ds.get("total_pnl", 0),
                win_count=ds.get("win_count", 0),
                loss_count=ds.get("loss_count", 0),
                max_drawdown=ds.get("max_drawdown", 0),
                peak_pnl=ds.get("peak_pnl", 0),
            )
            self._emergency_stop = state.get("emergency_stop", False)

            # 恢复持仓
            for k, v in state.get("open_positions", {}).items():
                self._open_positions[k] = TradeRecord(**v)

            # 恢复仓位状态
            self._position_state = state.get("position_state", {})

            logger.info(f"Loaded risk state: {self.get_summary()}")

        except Exception as e:
            logger.error(f"Failed to load risk state: {e}")



# PositionManager 已拆分至 position_manager.py
