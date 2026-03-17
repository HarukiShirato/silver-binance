# execution_engine.py - Layer 3: 执行引擎
#
# 双腿并发下单: CTP AG + HL SILVER
# 处理: 开平仓语义, 整手约束, 腿失败应急平仓

import asyncio
import csv
import os
import time
from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple

from exchanges.ctp_gateway import CTPGateway, OrderResult as CTPOrderResult
from exchanges.hyperliquid import HyperliquidClient
from signal_engine import Signal, SignalResult
from unit_converter import (
    ag_lots_to_hl_oz, calculate_ag_fee, calculate_hl_fee,
    AG_LOT_KG, HL_FEE_RATE, AG_FEE_RATE,
)
from config import TradingPair, STRATEGY
from notifier import FeishuNotifier

import logging
logger = logging.getLogger(__name__)


@dataclass
class DualLegResult:
    """双腿交易结果"""
    # CTP 侧
    ag_order: Optional[CTPOrderResult] = None
    ag_fill_price: float = 0
    # HL 侧
    hl_order: Optional[dict] = None
    hl_fill_price: float = 0
    # 数量
    lots: int = 0
    hl_size_oz: float = 0
    # 费用
    ag_fee_rmb: float = 0
    hl_fee_usd: float = 0
    total_fee_rmb: float = 0
    # 状态
    status: str = "pending"  # filled, partial, leg_failure, timeout, error
    error: str = ""


class ExecutionEngine:
    """执行引擎: 双腿并发下单"""

    def __init__(
        self,
        ctp_gateway: CTPGateway,
        hl_client: HyperliquidClient,
        pair: TradingPair,
        notifier: Optional[FeishuNotifier] = None,
        leg_timeout_sec: float = 3.0,
    ):
        self._ctp = ctp_gateway
        self._hl = hl_client
        self._pair = pair
        self._notifier = notifier
        self._timeout = leg_timeout_sec
        self._audit_dir = "logs"
        os.makedirs(self._audit_dir, exist_ok=True)

    async def execute(self, signal: SignalResult, lots: int) -> DualLegResult:
        """
        执行双腿套利交易

        LONG  → CTP BUY OPEN  + HL SELL
        SHORT → CTP SELL OPEN + HL BUY
        EXIT_LONG  → CTP SELL CLOSE_TODAY + HL BUY (reduce_only)
        EXIT_SHORT → CTP BUY CLOSE_TODAY  + HL SELL (reduce_only)
        """
        hl_size_oz = ag_lots_to_hl_oz(lots)

        # CTP 方向和开平
        ag_direction, ag_offset = self._signal_to_ctp_params(signal.signal)

        # HL 方向
        hl_is_buy = signal.signal in (Signal.SHORT, Signal.EXIT_SHORT)
        hl_reduce_only = signal.signal.name.startswith("EXIT")

        # CTP 价格: 对手价 + buffer
        ag_price = self._calculate_ag_price(signal, ag_direction)

        # HL 价格: mid + 滑点
        hl_price = self._calculate_hl_price(signal, hl_is_buy)

        logger.info(
            f"执行 {signal.signal.value}: "
            f"CTP {ag_direction} {ag_offset} {self._pair.ctp_instrument} "
            f"{lots}手 @¥{ag_price:.1f} | "
            f"HL {'BUY' if hl_is_buy else 'SELL'} "
            f"{hl_size_oz:.2f}oz @${hl_price:.4f}"
        )

        # 双腿并发执行
        t_start = time.monotonic()
        ag_coro = self._ctp.place_order(
            instrument_id=self._pair.ctp_instrument,
            direction=ag_direction,
            offset=ag_offset,
            price=ag_price,
            volume=lots,
            timeout=self._timeout,
        )

        hl_coro = self._hl.place_order(
            coin=f"xyz:{self._pair.hl_symbol}",
            is_buy=hl_is_buy,
            size=hl_size_oz,
            price=round(hl_price, 4),
            reduce_only=hl_reduce_only,
        )

        ag_task = asyncio.ensure_future(ag_coro)
        hl_task = asyncio.ensure_future(hl_coro)

        try:
            # CTP 内部有 self._timeout 超时, HL 无内置超时
            # 总超时 = 单腿超时 + 2s 缓冲 (避免 CTP 超时后再等太久)
            results = await asyncio.wait_for(
                asyncio.gather(ag_task, hl_task, return_exceptions=True),
                timeout=self._timeout + 2,
            )

            ag_result, hl_result = results

        except asyncio.TimeoutError:
            # 显式取消仍在执行的任务, 防止后台残留下单
            ag_task.cancel()
            hl_task.cancel()
            logger.error("双腿执行超时! 已取消残余任务")
            self._audit_trade(
                signal=signal, lots=lots, hl_size_oz=hl_size_oz,
                ag_fill=0, hl_fill=0, fees=0,
                status="timeout", latency_ms=(time.monotonic() - t_start) * 1000,
            )
            return DualLegResult(
                lots=lots, hl_size_oz=hl_size_oz,
                status="timeout", error="双腿执行超时"
            )

        # 检查腿失败(异常级别)
        ag_failed = isinstance(ag_result, Exception)
        hl_failed = isinstance(hl_result, Exception)

        if ag_failed and hl_failed:
            error = f"双腿均失败: AG={ag_result}, HL={hl_result}"
            logger.error(error)
            return DualLegResult(
                lots=lots, hl_size_oz=hl_size_oz,
                status="error", error=error
            )

        if ag_failed or hl_failed:
            return await self._handle_leg_failure(
                ag_result, hl_result, signal, lots, hl_size_oz,
                ag_direction, hl_is_buy,
            )

        # CTP 成交状态
        ag_order = ag_result if isinstance(ag_result, CTPOrderResult) else None
        ag_status = ag_order.status if ag_order else "error"
        ag_filled = (
            ag_order is not None and
            (ag_order.status == "filled" or ag_order.filled_volume > 0)
        )
        ag_fill = (
            ag_order.filled_price
            if ag_order and ag_order.filled_price > 0
            else ag_price
        )

        if ag_status not in ("filled", "partial"):
            return await self._handle_leg_failure(
                ag_result, hl_result, signal, lots, hl_size_oz,
                ag_direction, hl_is_buy,
                ag_filled=ag_filled,
            )

        # HL 成交状态
        hl_order = hl_result if isinstance(hl_result, dict) else None
        hl_accepted, hl_filled, hl_fill_price, hl_error, hl_resting = self._parse_hl_order(hl_order)

        # 如果 HL 订单 resting (挂单未成交), 先尝试取消再走腿失败流程
        if hl_resting:
            logger.warning("HL 订单处于 resting 状态, 尝试取消挂单")
            try:
                await self._hl.cancel_all_orders(coin=f"xyz:{self._pair.hl_symbol}")
            except Exception as e:
                logger.error(f"取消 HL resting 订单失败: {e}")

        if not hl_accepted or not hl_filled:
            reason = hl_error or "HL order not filled"
            return await self._handle_leg_failure(
                ag_result, hl_result, signal, lots, hl_size_oz,
                ag_direction, hl_is_buy,
                ag_filled=ag_filled,
                hl_filled=hl_filled,
                extra_error=f"HL状态异常: {reason}",
            )

        # 计算费用
        ag_fee = calculate_ag_fee(ag_fill, lots)
        final_hl_fill = hl_fill_price if hl_fill_price > 0 else hl_price
        hl_fee = calculate_hl_fee(final_hl_fill, hl_size_oz)
        total_fee_rmb = ag_fee + hl_fee * signal.usdcny

        result = DualLegResult(
            ag_order=ag_order,
            ag_fill_price=ag_fill,
            hl_order=hl_order,
            hl_fill_price=final_hl_fill,
            lots=lots,
            hl_size_oz=hl_size_oz,
            ag_fee_rmb=ag_fee,
            hl_fee_usd=hl_fee,
            total_fee_rmb=total_fee_rmb,
            status="filled",
        )

        latency_ms = (time.monotonic() - t_start) * 1000
        logger.info(
            f"交易完成: AG @¥{ag_fill:.1f}, HL @${final_hl_fill:.4f}, "
            f"费用=¥{total_fee_rmb:.2f}, 延迟={latency_ms:.0f}ms"
        )

        self._audit_trade(
            signal=signal, lots=lots, hl_size_oz=hl_size_oz,
            ag_fill=ag_fill, hl_fill=final_hl_fill, fees=total_fee_rmb,
            status="filled", latency_ms=latency_ms,
            ag_slippage=ag_fill - ag_price,
            hl_slippage=final_hl_fill - hl_price,
        )

        return result

    def _parse_hl_order(self, result: Optional[dict]) -> Tuple[bool, bool, float, str, bool]:
        """解析 Hyperliquid 下单结果: (accepted, filled, fill_price, error, resting)."""
        if not isinstance(result, dict):
            return False, False, 0.0, "invalid response type", False

        top_status = str(result.get("status", "")).lower()
        if top_status and top_status != "ok":
            return False, False, 0.0, f"status={top_status}", False

        response = result.get("response", {})
        data = response.get("data", {}) if isinstance(response, dict) else {}
        statuses = data.get("statuses", []) if isinstance(data, dict) else []
        if not statuses:
            return False, False, 0.0, "missing statuses", False

        accepted = True
        filled = False
        resting = False
        fill_price = 0.0
        errors = []

        for st in statuses:
            if not isinstance(st, dict):
                continue

            if "error" in st:
                accepted = False
                errors.append(str(st.get("error")))
                continue

            if "filled" in st and isinstance(st["filled"], dict):
                filled = True
                px = st["filled"].get("avgPx") or st["filled"].get("px")
                if px is not None:
                    try:
                        fill_price = float(px)
                    except (TypeError, ValueError):
                        pass
                continue

            if "resting" in st:
                # 订单挂在盘口未立即成交, 需要取消
                accepted = True
                resting = True

        if not filled:
            error_msg = "; ".join(errors) if errors else ("resting (not filled)" if resting else "not filled")
            return accepted, False, 0.0, error_msg, resting

        return accepted, True, fill_price, "", False

    def _signal_to_ctp_params(self, signal: Signal) -> Tuple[str, str]:
        """信号 → CTP 方向和开平标志"""
        mapping = {
            Signal.LONG: ("BUY", "OPEN"),
            Signal.SHORT: ("SELL", "OPEN"),
            Signal.EXIT_LONG: ("SELL", "CLOSE_TODAY"),
            Signal.EXIT_SHORT: ("BUY", "CLOSE_TODAY"),
        }
        return mapping[signal]

    def _calculate_ag_price(self, signal: SignalResult, direction: str) -> float:
        """计算 CTP 下单价格 (对手价 + buffer)"""
        if direction == "BUY":
            # 买入: 用卖一价 + 1 tick
            price = signal.ag_price + 1  # AG tick = 1
        else:
            # 卖出: 用买一价 - 1 tick
            price = signal.ag_price - 1

        # AG 价格精度: 整数
        return round(price, 0)

    def _calculate_hl_price(self, signal: SignalResult, is_buy: bool) -> float:
        """计算 HL 下单价格 (mid + slippage)"""
        slippage = STRATEGY.hl_order_slippage_pct
        if is_buy:
            return signal.hl_price_usd_oz * (1 + slippage)
        else:
            return signal.hl_price_usd_oz * (1 - slippage)

    async def _handle_leg_failure(
        self,
        ag_result,
        hl_result,
        signal: SignalResult,
        lots: int,
        hl_size_oz: float,
        ag_direction: str,
        hl_is_buy: bool,
        ag_filled: Optional[bool] = None,
        hl_filled: Optional[bool] = None,
        extra_error: str = "",
    ) -> DualLegResult:
        """
        处理单腿失败: 已成交的腿紧急平掉

        这是关键的风控逻辑:
        - AG 成交但 HL 失败 → 立即反向平 AG
        - HL 成交但 AG 失败 → 立即反向平 HL
        """
        ag_failed = isinstance(ag_result, Exception)
        hl_failed = isinstance(hl_result, Exception)
        if ag_filled is None:
            ag_order = ag_result if isinstance(ag_result, CTPOrderResult) else None
            ag_filled = (
                ag_order is not None and
                (ag_order.status == "filled" or ag_order.filled_volume > 0)
            )
        if hl_filled is None:
            hl_order = hl_result if isinstance(hl_result, dict) else None
            _, hl_filled, _, _, _ = self._parse_hl_order(hl_order)

        error_msg = (
            f"腿失败: AG={'失败' if ag_failed else '成功'}(成交={ag_filled}), "
            f"HL={'失败' if hl_failed else '成功'}(成交={hl_filled})"
        )
        if extra_error:
            error_msg = f"{error_msg} | {extra_error}"
        logger.error(error_msg)

        if self._notifier:
            await self._notifier.notify_emergency(
                f"{error_msg}\nAG: {ag_result}\nHL: {hl_result}"
            )

        # 紧急平仓
        emergency_close_failed = False
        if ag_filled:
            # AG 成交了, HL 失败 → 反向平 AG
            reverse_dir = "SELL" if ag_direction == "BUY" else "BUY"
            try:
                logger.warning(f"紧急平仓 AG: {reverse_dir} {lots}手")
                await self._ctp.market_order(
                    instrument_id=self._pair.ctp_instrument,
                    direction=reverse_dir,
                    offset="CLOSE_TODAY",
                    volume=lots,
                )
            except Exception as e:
                emergency_close_failed = True
                logger.critical(f"紧急平仓 AG 失败! {e}")

        if hl_filled:
            # HL 成交了, AG 失败 → 反向平 HL
            try:
                logger.warning(f"紧急平仓 HL: {'SELL' if hl_is_buy else 'BUY'} {hl_size_oz}oz")
                await self._hl.market_order(
                    coin=f"xyz:{self._pair.hl_symbol}",
                    is_buy=not hl_is_buy,
                    size=hl_size_oz,
                )
            except Exception as e:
                emergency_close_failed = True
                logger.critical(f"紧急平仓 HL 失败! {e}")

        if emergency_close_failed and self._notifier:
            await self._notifier.notify_emergency(
                f"紧急平仓失败! 需要人工介入!\n{error_msg}"
            )

        self._audit_trade(
            signal=signal, lots=lots, hl_size_oz=hl_size_oz,
            ag_fill=0, hl_fill=0, fees=0,
            status="leg_failure", latency_ms=0,
            extra_info=error_msg,
        )

        return DualLegResult(
            lots=lots,
            hl_size_oz=hl_size_oz,
            status="leg_failure",
            error=error_msg,
        )

    def _audit_trade(
        self,
        signal: SignalResult,
        lots: int,
        hl_size_oz: float,
        ag_fill: float,
        hl_fill: float,
        fees: float,
        status: str,
        latency_ms: float = 0,
        ag_slippage: float = 0,
        hl_slippage: float = 0,
        extra_info: str = "",
    ):
        """写入交易审计 CSV 日志"""
        filepath = os.path.join(self._audit_dir, f"trades_{date.today().isoformat()}.csv")
        write_header = not os.path.exists(filepath)
        try:
            with open(filepath, 'a', newline='') as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow([
                        "timestamp", "signal", "lots", "hl_size_oz",
                        "ag_expected", "ag_fill", "ag_slippage",
                        "hl_expected", "hl_fill", "hl_slippage",
                        "spread_pct", "zscore", "usdcny",
                        "fees_rmb", "status", "latency_ms", "extra",
                    ])
                writer.writerow([
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    signal.signal.value,
                    lots,
                    f"{hl_size_oz:.2f}",
                    f"{signal.ag_price:.1f}",
                    f"{ag_fill:.1f}",
                    f"{ag_slippage:.1f}",
                    f"{signal.hl_price_usd_oz:.4f}",
                    f"{hl_fill:.4f}",
                    f"{hl_slippage:.4f}",
                    f"{signal.spread_pct:.4f}",
                    f"{signal.zscore:.3f}",
                    f"{signal.usdcny:.4f}",
                    f"{fees:.2f}",
                    status,
                    f"{latency_ms:.0f}",
                    extra_info,
                ])
        except Exception as e:
            logger.warning(f"写入审计日志失败: {e}")
