# position_manager.py - 仓位管理器
#
# 核心约束:
#   1. AG 最小开仓 = 1手, 加仓逐手 (1手1手加)
#   2. 双账户保证金实时监控 (CTP + HL 分别预警)

import threading
from dataclasses import dataclass
from typing import Optional

import logging
logger = logging.getLogger(__name__)


class MarginLevel:
    """保证金等级"""
    NORMAL = "normal"           # 正常 (< 70%)
    WARNING = "warning"         # 预警 (>= 70%)
    DANGER = "danger"           # 危险 (>= 85%)
    CRITICAL = "critical"       # 临界 (>= 95%), 禁止开新仓


@dataclass
class AccountMargin:
    """单账户保证金状态"""
    name: str                   # "CTP" 或 "HL"
    balance: float = 0          # 总资金
    available: float = 0        # 可用资金
    used_margin: float = 0      # 已用保证金
    unrealized_pnl: float = 0   # 浮动盈亏

    @property
    def margin_ratio(self) -> float:
        """保证金使用率"""
        if self.balance <= 0:
            return 1.0
        return self.used_margin / self.balance

    @property
    def risk_ratio(self) -> float:
        """风险度 = 已用保证金 / (总资金 + 浮盈浮亏)"""
        equity = self.balance + self.unrealized_pnl
        if equity <= 0:
            return 1.0
        return self.used_margin / equity


class PositionManager:
    """
    仓位管理器 (AG 整手约束 + 双账户保证金预警)

    关键规则:
    - 开仓: 最小1手, 每次只加1手
    - CTP 和 HL 各自独立监控保证金
    - 任一账户触发 CRITICAL → 禁止开新仓
    - 任一账户触发 DANGER → 不再加仓
    """

    def __init__(
        self,
        capital: float,
        ctp_margin_rate: float = 0.09,
        hl_leverage: int = 5,
        ctp_multiplier: int = 15,
        max_position_lots: int = 10,
        # 保证金预警阈值 (两个账户共用)
        margin_warning_pct: float = 0.70,    # 70% 预警
        margin_danger_pct: float = 0.85,     # 85% 危险
        margin_critical_pct: float = 0.95,   # 95% 禁止开仓
    ):
        self.capital = capital
        self.ctp_margin_rate = ctp_margin_rate
        self.hl_leverage = hl_leverage
        self.ctp_multiplier = ctp_multiplier  # kg per lot
        self.max_position_lots = max_position_lots

        # 保证金阈值
        self.margin_warning_pct = margin_warning_pct
        self.margin_danger_pct = margin_danger_pct
        self.margin_critical_pct = margin_critical_pct

        # 当前持仓
        self._current_lots: int = 0

        # 双账户保证金状态
        self.ctp_margin = AccountMargin(name="CTP")
        self.hl_margin = AccountMargin(name="HL")

        # 估算值 (用于开仓前检查)
        self._est_margin_per_lot_ctp: float = 0
        self._est_margin_per_lot_hl: float = 0

        # 上次报警级别 (避免重复报警)
        self._last_ctp_level: str = MarginLevel.NORMAL
        self._last_hl_level: str = MarginLevel.NORMAL

        # 保证金更新锁 (防止并发读写不一致)
        self._margin_lock = threading.Lock()

    # ==================== 属性 ====================

    @property
    def current_lots(self) -> int:
        return self._current_lots

    @current_lots.setter
    def current_lots(self, value: int):
        self._current_lots = max(0, value)

    @property
    def available_lots(self) -> int:
        """剩余可开手数"""
        return max(0, self.max_position_lots - self._current_lots)

    # ==================== 保证金更新 (来自真实账户查询) ====================

    def update_ctp_margin(
        self,
        balance: float,
        available: float,
        used_margin: float,
        unrealized_pnl: float = 0,
    ) -> str:
        """
        更新 CTP 账户保证金 (从 ctp_gateway.query_account() 获取)
        使用快照模式: 先构建完整快照, 再原子替换, 防止并发读到中间状态

        Returns:
            MarginLevel
        """
        with self._margin_lock:
            snapshot = AccountMargin(
                name="CTP", balance=balance, available=available,
                used_margin=used_margin, unrealized_pnl=unrealized_pnl,
            )
            self.ctp_margin = snapshot
            level = self._check_level(snapshot)
            self._log_level_change("CTP", level, self._last_ctp_level, snapshot)
            self._last_ctp_level = level
        return level

    def update_hl_margin(
        self,
        account_value: float,
        available: float,
        used_margin: float,
        unrealized_pnl: float = 0,
    ) -> str:
        """
        更新 HL 账户保证金 (从 hl_client.get_user_state() 获取)
        使用快照模式: 先构建完整快照, 再原子替换

        Returns:
            MarginLevel
        """
        with self._margin_lock:
            snapshot = AccountMargin(
                name="HL", balance=account_value, available=available,
                used_margin=used_margin, unrealized_pnl=unrealized_pnl,
            )
            self.hl_margin = snapshot
            level = self._check_level(snapshot)
            self._log_level_change("HL", level, self._last_hl_level, snapshot)
            self._last_hl_level = level
        return level

    def _check_level(self, acct: AccountMargin) -> str:
        """检查保证金等级"""
        ratio = acct.margin_ratio
        if ratio >= self.margin_critical_pct:
            return MarginLevel.CRITICAL
        elif ratio >= self.margin_danger_pct:
            return MarginLevel.DANGER
        elif ratio >= self.margin_warning_pct:
            return MarginLevel.WARNING
        return MarginLevel.NORMAL

    def _log_level_change(
        self, name: str, new_level: str, old_level: str, acct: AccountMargin
    ):
        """等级变化时记录日志"""
        if new_level == old_level:
            return

        ratio = acct.margin_ratio
        msg = (
            f"{name} 保证金{new_level}: "
            f"使用率={ratio:.1%}, "
            f"已用={acct.used_margin:,.0f}, "
            f"可用={acct.available:,.0f}, "
            f"总额={acct.balance:,.0f}"
        )

        if new_level == MarginLevel.CRITICAL:
            logger.critical(msg)
        elif new_level == MarginLevel.DANGER:
            logger.warning(msg)
        elif new_level == MarginLevel.WARNING:
            logger.warning(msg)
        else:
            logger.info(msg)

    @property
    def ctp_level(self) -> str:
        with self._margin_lock:
            return self._last_ctp_level

    @property
    def hl_level(self) -> str:
        with self._margin_lock:
            return self._last_hl_level

    @property
    def worst_level(self) -> str:
        """两个账户中更差的那个等级"""
        levels = [MarginLevel.NORMAL, MarginLevel.WARNING,
                  MarginLevel.DANGER, MarginLevel.CRITICAL]
        with self._margin_lock:
            ctp_idx = levels.index(self._last_ctp_level) if self._last_ctp_level in levels else 0
            hl_idx = levels.index(self._last_hl_level) if self._last_hl_level in levels else 0
        return levels[max(ctp_idx, hl_idx)]

    def has_margin_alert(self) -> bool:
        """任一账户有预警"""
        return self.worst_level != MarginLevel.NORMAL

    # ==================== 估算保证金 (用于开仓前检查) ====================

    def estimate_margin_per_lot(
        self,
        ag_price_cny_kg: float,
        hl_price_usd_oz: float,
        usdcny: float,
    ):
        """估算每手所需保证金 (开仓前调用)"""
        from unit_converter import ag_lots_to_hl_oz

        self._est_margin_per_lot_ctp = (
            ag_price_cny_kg * self.ctp_multiplier * self.ctp_margin_rate
        )

        hl_oz = ag_lots_to_hl_oz(1)
        self._est_margin_per_lot_hl = (
            hl_price_usd_oz * hl_oz / self.hl_leverage
        )  # USD

    # ==================== 下单手数计算 ====================

    def calculate_order_lots(
        self,
        ag_price_cny_kg: float,
        hl_price_usd_oz: float,
        usdcny: float,
    ) -> int:
        """
        计算本次开仓/加仓手数

        规则:
        1. 最小开仓 = 1手
        2. 加仓 = 每次固定1手 (逐手加仓)
        3. 任一账户 CRITICAL/DANGER → 禁止开仓
        4. CTP可用保证金不够1手 → 禁止
        5. HL可用保证金不够1手 → 禁止

        Returns:
            1 (可开仓) 或 0 (不可开仓)
        """
        self.estimate_margin_per_lot(ag_price_cny_kg, hl_price_usd_oz, usdcny)

        # 检查1: 已到最大持仓
        if self._current_lots >= self.max_position_lots:
            logger.info(f"已达最大持仓 {self.max_position_lots}手, 不可加仓")
            return 0

        # 检查2: 任一账户 DANGER/CRITICAL → 禁止加仓
        if self.worst_level in (MarginLevel.DANGER, MarginLevel.CRITICAL):
            logger.warning(
                f"保证金状态异常 (CTP={self._last_ctp_level}, HL={self._last_hl_level}), "
                f"禁止开新仓"
            )
            return 0

        # 检查3: CTP 可用资金够不够1手
        if self.ctp_margin.available > 0:
            if self._est_margin_per_lot_ctp > self.ctp_margin.available:
                logger.warning(
                    f"CTP 可用资金不足: 需要 ¥{self._est_margin_per_lot_ctp:,.0f}, "
                    f"可用 ¥{self.ctp_margin.available:,.0f}"
                )
                return 0

        # 检查4: HL 可用资金够不够1手
        if self.hl_margin.available > 0:
            if self._est_margin_per_lot_hl > self.hl_margin.available:
                logger.warning(
                    f"HL 可用保证金不足: 需要 ${self._est_margin_per_lot_hl:,.2f}, "
                    f"可用 ${self.hl_margin.available:,.2f}"
                )
                return 0

        # 逐手加仓: 永远返回1手
        logger.info(
            f"加仓计算: 当前={self._current_lots}手 → +1手 | "
            f"CTP保证金/手=¥{self._est_margin_per_lot_ctp:,.0f} "
            f"(可用¥{self.ctp_margin.available:,.0f}) | "
            f"HL保证金/手=${self._est_margin_per_lot_hl:,.2f} "
            f"(可用${self.hl_margin.available:,.2f})"
        )
        return 1

    # ==================== 持仓更新 ====================

    def on_open(self, lots: int):
        """开仓后更新"""
        self._current_lots += lots
        logger.info(
            f"开仓 +{lots}手 | 总持仓={self._current_lots}手"
        )

    def on_close(self, lots: int):
        """平仓后更新"""
        self._current_lots = max(0, self._current_lots - lots)
        logger.info(
            f"平仓 -{lots}手 | 总持仓={self._current_lots}手"
        )

    # ==================== 状态 ====================

    def get_state(self) -> dict:
        return {
            "current_lots": self._current_lots,
        }

    def load_state(self, state: dict):
        self._current_lots = state.get("current_lots", 0)

    def get_margin_summary(self) -> str:
        """保证金摘要 (用于飞书日报)"""
        ctp = self.ctp_margin
        hl = self.hl_margin
        return (
            f"持仓={self._current_lots}手\n"
            f"CTP: ¥{ctp.used_margin:,.0f}/¥{ctp.balance:,.0f} "
            f"({ctp.margin_ratio:.1%}) [{self._last_ctp_level}]\n"
            f"HL: ${hl.used_margin:,.2f}/${hl.balance:,.2f} "
            f"({hl.margin_ratio:.1%}) [{self._last_hl_level}]"
        )
