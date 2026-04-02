# execution_engine.py - Layer 3 dual-leg execution

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
from unit_converter import ag_lots_to_hl_oz, calculate_ag_fee, calculate_hl_fee
from config import TradingPair, STRATEGY
from notifier import FeishuNotifier

import logging
logger = logging.getLogger(__name__)


@dataclass
class DualLegResult:
    ag_order: Optional[CTPOrderResult] = None
    ag_fill_price: float = 0
    hl_order: Optional[dict] = None
    hl_fill_price: float = 0
    lots: int = 0
    hl_size_oz: float = 0
    ag_fee_rmb: float = 0
    hl_fee_usd: float = 0
    total_fee_rmb: float = 0
    status: str = "pending"  # filled, rolled_back, leg_failure, timeout, error
    error: str = ""
    exposure_unresolved: bool = False


class ExecutionEngine:
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
        self._ctp_wait_repair_sec = 60
        self._hl_repair_timeout_sec = 60
        self._hl_repair_interval_sec = 1.0
        self._audit_dir = "logs"
        os.makedirs(self._audit_dir, exist_ok=True)

    async def execute(self, signal: SignalResult, lots: int) -> DualLegResult:
        hl_size_oz = ag_lots_to_hl_oz(lots)

        ag_direction, ag_offset = self._signal_to_ctp_params(signal.signal)
        hl_is_buy = signal.signal in (Signal.SHORT, Signal.EXIT_SHORT)
        hl_reduce_only = signal.signal.name.startswith("EXIT")

        ag_price = self._calculate_ag_price(signal, ag_direction)
        hl_price = self._calculate_hl_price(signal, hl_is_buy)

        logger.info(
            f"执行 {signal.signal.value}: CTP {ag_direction} {ag_offset} {self._pair.ctp_instrument} "
            f"{lots}手 @¥{ag_price:.1f} | HL {'BUY' if hl_is_buy else 'SELL'} {hl_size_oz:.2f}oz @${hl_price:.4f}"
        )

        t_start = time.monotonic()
        ag_task = asyncio.ensure_future(
            self._ctp.place_order(
                instrument_id=self._pair.ctp_instrument,
                direction=ag_direction,
                offset=ag_offset,
                price=ag_price,
                volume=lots,
                timeout=self._timeout,
            )
        )
        hl_task = asyncio.ensure_future(
            self._hl.place_order(
                coin=f"xyz:{self._pair.hl_symbol}",
                is_buy=hl_is_buy,
                size=hl_size_oz,
                price=round(hl_price, 4),
                reduce_only=hl_reduce_only,
                tif=STRATEGY.hl_order_tif,
            )
        )

        try:
            ag_result, hl_result = await asyncio.wait_for(
                asyncio.gather(ag_task, hl_task, return_exceptions=True),
                timeout=self._timeout + 2,
            )
        except asyncio.TimeoutError:
            ag_task.cancel()
            hl_task.cancel()
            logger.error("双腿执行超时")
            self._audit_trade(
                signal=signal,
                lots=lots,
                hl_size_oz=hl_size_oz,
                ag_fill=0,
                hl_fill=0,
                fees=0,
                status="timeout",
                latency_ms=(time.monotonic() - t_start) * 1000,
            )
            return DualLegResult(
                lots=lots,
                hl_size_oz=hl_size_oz,
                status="timeout",
                error="双腿执行超时",
                exposure_unresolved=True,
            )

        ag_failed = isinstance(ag_result, Exception)
        hl_failed = isinstance(hl_result, Exception)

        if ag_failed and hl_failed:
            err = f"双腿均失败: AG={ag_result}, HL={hl_result}"
            logger.error(err)
            return DualLegResult(
                lots=lots,
                hl_size_oz=hl_size_oz,
                status="error",
                error=err,
                exposure_unresolved=True,
            )

        if ag_failed or hl_failed:
            return await self._handle_leg_failure(
                ag_result=ag_result,
                hl_result=hl_result,
                signal=signal,
                lots=lots,
                hl_size_oz=hl_size_oz,
                ag_direction=ag_direction,
                hl_is_buy=hl_is_buy,
            )

        ag_order = ag_result if isinstance(ag_result, CTPOrderResult) else None
        ag_status = ag_order.status if ag_order else "error"
        ag_filled = bool(ag_order and (ag_order.status == "filled" or ag_order.filled_volume > 0))
        ag_fill = ag_order.filled_price if ag_order and ag_order.filled_price > 0 else ag_price

        hl_order = hl_result if isinstance(hl_result, dict) else None
        hl_accepted, hl_filled, hl_filled_size_oz, hl_fill_price, hl_error, hl_resting = self._parse_hl_order(hl_order)
        if hl_filled and hl_filled_size_oz <= 0:
            hl_filled_size_oz = hl_size_oz

        if hl_resting:
            try:
                await self._hl.cancel_all_orders(coin=f"xyz:{self._pair.hl_symbol}")
            except Exception as e:
                logger.warning(f"取消 HL resting 订单失败: {e}")

        # treat partial HL fill as failure needing repair
        hl_partial = hl_filled and (hl_filled_size_oz + 1e-6 < hl_size_oz)
        if ag_status not in ("filled", "partial") or (not hl_accepted) or (not hl_filled) or hl_partial:
            reason = hl_error or ("HL partial fill" if hl_partial else "leg status abnormal")
            return await self._handle_leg_failure(
                ag_result=ag_result,
                hl_result=hl_result,
                signal=signal,
                lots=lots,
                hl_size_oz=hl_size_oz,
                ag_direction=ag_direction,
                hl_is_buy=hl_is_buy,
                ag_filled=ag_filled,
                hl_filled=hl_filled,
                hl_filled_size_oz=hl_filled_size_oz,
                extra_error=reason,
            )

        effective_hl_oz = hl_filled_size_oz if hl_filled_size_oz > 0 else hl_size_oz
        final_hl_fill = hl_fill_price if hl_fill_price > 0 else hl_price
        ag_fee = calculate_ag_fee(ag_fill, lots)
        hl_fee = calculate_hl_fee(final_hl_fill, effective_hl_oz)
        total_fee_rmb = ag_fee + hl_fee * signal.usdcny

        latency_ms = (time.monotonic() - t_start) * 1000
        self._audit_trade(
            signal=signal,
            lots=lots,
            hl_size_oz=effective_hl_oz,
            ag_fill=ag_fill,
            hl_fill=final_hl_fill,
            fees=total_fee_rmb,
            status="filled",
            latency_ms=latency_ms,
            ag_slippage=ag_fill - ag_price,
            hl_slippage=final_hl_fill - hl_price,
        )

        return DualLegResult(
            ag_order=ag_order,
            ag_fill_price=ag_fill,
            hl_order=hl_order,
            hl_fill_price=final_hl_fill,
            lots=lots,
            hl_size_oz=effective_hl_oz,
            ag_fee_rmb=ag_fee,
            hl_fee_usd=hl_fee,
            total_fee_rmb=total_fee_rmb,
            status="filled",
            exposure_unresolved=False,
        )

    def _parse_hl_order(self, result: Optional[dict]) -> Tuple[bool, bool, float, float, str, bool]:
        if not isinstance(result, dict):
            return False, False, 0.0, 0.0, "invalid response type", False

        top_status = str(result.get("status", "")).lower()
        if top_status and top_status != "ok":
            return False, False, 0.0, 0.0, f"status={top_status}", False

        response = result.get("response", {})
        data = response.get("data", {}) if isinstance(response, dict) else {}
        statuses = data.get("statuses", []) if isinstance(data, dict) else []
        if not statuses:
            return False, False, 0.0, 0.0, "missing statuses", False

        accepted = True
        filled = False
        resting = False
        fill_price = 0.0
        filled_size_oz = 0.0
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
                fp = st["filled"].get("avgPx") or st["filled"].get("px")
                if fp is not None:
                    try:
                        fill_price = float(fp)
                    except Exception:
                        pass
                fsz = st["filled"].get("totalSz") or st["filled"].get("sz")
                if fsz is not None:
                    try:
                        filled_size_oz += float(fsz)
                    except Exception:
                        pass
                continue
            if "resting" in st:
                resting = True

        if not filled:
            err = "; ".join(errors) if errors else ("resting (not filled)" if resting else "not filled")
            return accepted, False, 0.0, 0.0, err, resting

        return accepted, True, filled_size_oz, fill_price, "", resting

    def _signal_to_ctp_params(self, signal: Signal) -> Tuple[str, str]:
        mapping = {
            Signal.LONG: ("BUY", "OPEN"),
            Signal.SHORT: ("SELL", "OPEN"),
            Signal.EXIT_LONG: ("SELL", "CLOSE_TODAY"),
            Signal.EXIT_SHORT: ("BUY", "CLOSE_TODAY"),
        }
        return mapping[signal]

    def _calculate_ag_price(self, signal: SignalResult, direction: str) -> float:
        if direction == "BUY":
            price = signal.ag_price + 1
        else:
            price = signal.ag_price - 1
        return round(price, 0)

    def _calculate_hl_price(self, signal: SignalResult, is_buy: bool) -> float:
        slippage = STRATEGY.hl_order_slippage_pct
        if is_buy:
            return signal.hl_price_usd_oz * (1 + slippage)
        return signal.hl_price_usd_oz * (1 - slippage)

    async def _wait_ctp_fill_for_seconds(self, ag_order: Optional[CTPOrderResult], wait_sec: int) -> int:
        if ag_order is None:
            return 0
        deadline = time.time() + max(1, int(wait_sec))
        while time.time() < deadline:
            fv = int(getattr(ag_order, "filled_volume", 0) or 0)
            st = str(getattr(ag_order, "status", "") or "").lower()
            if fv > 0:
                return fv
            if st == "filled":
                return 1
            await asyncio.sleep(1.0)
        return int(getattr(ag_order, "filled_volume", 0) or 0)

    async def _repair_hl_with_ioc(
        self,
        signal: SignalResult,
        is_buy: bool,
        target_size_oz: float,
        reduce_only: bool,
        timeout_sec: int,
    ) -> tuple[bool, float, float]:
        if target_size_oz <= 1e-8:
            return True, 0.0, 0.0

        filled_sum = 0.0
        notional_sum = 0.0
        coin = f"xyz:{self._pair.hl_symbol}"
        deadline = time.time() + max(1, int(timeout_sec))

        while time.time() < deadline and (target_size_oz - filled_sum) > 1e-6:
            remain = target_size_oz - filled_sum
            size = round(remain, 2)
            if size <= 0:
                break
            px = round(self._calculate_hl_price(signal, is_buy), 4)
            try:
                r = await self._hl.place_order(
                    coin=coin,
                    is_buy=is_buy,
                    size=size,
                    price=px,
                    order_type="Limit",
                    tif="Ioc",
                    reduce_only=reduce_only,
                )
            except Exception as e:
                logger.warning(f"HL IOC 修复异常: {e}")
                await asyncio.sleep(self._hl_repair_interval_sec)
                continue

            accepted, _, fsz, fpx, err, resting = self._parse_hl_order(r)
            if resting:
                try:
                    await self._hl.cancel_all_orders(coin=coin)
                except Exception:
                    pass
            if (not accepted) and fsz <= 1e-8:
                logger.warning(f"HL IOC 修复未成交: {err}")
                await asyncio.sleep(self._hl_repair_interval_sec)
                continue
            if fsz > 0:
                filled_sum += fsz
                px_used = fpx if fpx > 0 else px
                notional_sum += fsz * px_used
            else:
                await asyncio.sleep(self._hl_repair_interval_sec)

        ok = (target_size_oz - filled_sum) <= 1e-6
        avg_px = (notional_sum / filled_sum) if filled_sum > 0 else 0.0
        return ok, filled_sum, avg_px

    async def _safe_cancel_ctp_order(self, ag_order: Optional[CTPOrderResult]):
        if ag_order is None:
            return
        try:
            await self._ctp.cancel_order(
                instrument_id=self._pair.ctp_instrument,
                order_ref=ag_order.order_ref,
                order_sys_id=ag_order.order_sys_id or "",
            )
        except Exception as e:
            logger.warning(f"CTP 撤单异常(忽略): {e}")

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
        hl_filled_size_oz: float = 0.0,
        extra_error: str = "",
    ) -> DualLegResult:
        ag_failed = isinstance(ag_result, Exception)
        hl_failed = isinstance(hl_result, Exception)

        ag_order = ag_result if isinstance(ag_result, CTPOrderResult) else None
        if ag_filled is None:
            ag_filled = bool(ag_order and (ag_order.status == "filled" or ag_order.filled_volume > 0))
        ag_filled_lots = int(ag_order.filled_volume or 0) if ag_order else 0
        if ag_filled and ag_filled_lots <= 0:
            ag_filled_lots = lots

        hl_order = hl_result if isinstance(hl_result, dict) else None
        if hl_filled is None:
            _, hl_filled, parsed_hl_oz, _, _, _ = self._parse_hl_order(hl_order)
            if hl_filled_size_oz <= 0:
                hl_filled_size_oz = parsed_hl_oz
        if hl_filled and hl_filled_size_oz <= 0:
            hl_filled_size_oz = hl_size_oz

        err = (
            f"腿失败: AG={'失败' if ag_failed else '成功'}(成交={ag_filled_lots}), "
            f"HL={'失败' if hl_failed else '成功'}(成交oz={hl_filled_size_oz:.4f})"
        )
        if extra_error:
            err = f"{err} | {extra_error}"
        logger.error(err)
        if self._notifier:
            await self._notifier.notify_emergency(f"{err}\nAG: {ag_result}\nHL: {hl_result}")

        # Branch A: CTP filled, HL not enough -> keep CTP, repair HL via IOC.
        if ag_filled_lots > 0:
            target_hl_oz = ag_lots_to_hl_oz(ag_filled_lots)
            gap_oz = max(0.0, target_hl_oz - hl_filled_size_oz)
            if gap_oz > 1e-6:
                ok, repaired_oz, repaired_px = await self._repair_hl_with_ioc(
                    signal=signal,
                    is_buy=hl_is_buy,
                    target_size_oz=gap_oz,
                    reduce_only=False,
                    timeout_sec=self._hl_repair_timeout_sec,
                )
                total_hl_oz = hl_filled_size_oz + repaired_oz
                if ok:
                    ag_fill = ag_order.filled_price if ag_order and ag_order.filled_price > 0 else self._calculate_ag_price(signal, ag_direction)
                    hl_fill = repaired_px if repaired_px > 0 else self._calculate_hl_price(signal, hl_is_buy)
                    ag_fee = calculate_ag_fee(ag_fill, ag_filled_lots)
                    hl_fee = calculate_hl_fee(hl_fill, total_hl_oz)
                    total_fee_rmb = ag_fee + hl_fee * signal.usdcny
                    return DualLegResult(
                        ag_order=ag_order,
                        ag_fill_price=ag_fill,
                        hl_order=hl_order,
                        hl_fill_price=hl_fill,
                        lots=ag_filled_lots,
                        hl_size_oz=total_hl_oz,
                        ag_fee_rmb=ag_fee,
                        hl_fee_usd=hl_fee,
                        total_fee_rmb=total_fee_rmb,
                        status="filled",
                        exposure_unresolved=False,
                    )

                final_err = f"{err} | HL补单失败, 残余gap={max(0.0, target_hl_oz-total_hl_oz):.4f}oz"
                self._audit_trade(signal, lots, hl_size_oz, 0, 0, 0, "leg_failure", extra_info=final_err)
                return DualLegResult(
                    ag_order=ag_order,
                    lots=lots,
                    hl_size_oz=hl_size_oz,
                    status="leg_failure",
                    error=final_err,
                    exposure_unresolved=True,
                )

        # Branch B: HL filled first, CTP not filled -> wait CTP, else cancel CTP and rollback HL.
        if hl_filled_size_oz > 1e-6 and ag_filled_lots <= 0:
            filled_lots_after_wait = await self._wait_ctp_fill_for_seconds(ag_order, self._ctp_wait_repair_sec)
            if filled_lots_after_wait > 0:
                target_hl_oz = ag_lots_to_hl_oz(filled_lots_after_wait)
                gap_oz = max(0.0, target_hl_oz - hl_filled_size_oz)
                if gap_oz <= 1e-6:
                    return DualLegResult(
                        ag_order=ag_order,
                        lots=filled_lots_after_wait,
                        hl_size_oz=target_hl_oz,
                        status="filled",
                        exposure_unresolved=False,
                    )
                ok, repaired_oz, _ = await self._repair_hl_with_ioc(
                    signal=signal,
                    is_buy=hl_is_buy,
                    target_size_oz=gap_oz,
                    reduce_only=False,
                    timeout_sec=self._hl_repair_timeout_sec,
                )
                if ok:
                    return DualLegResult(
                        ag_order=ag_order,
                        lots=filled_lots_after_wait,
                        hl_size_oz=hl_filled_size_oz + repaired_oz,
                        status="filled",
                        exposure_unresolved=False,
                    )

            await self._safe_cancel_ctp_order(ag_order)
            rollback_ok, rollback_oz, _ = await self._repair_hl_with_ioc(
                signal=signal,
                is_buy=not hl_is_buy,
                target_size_oz=hl_filled_size_oz,
                reduce_only=True,
                timeout_sec=self._hl_repair_timeout_sec,
            )
            if rollback_ok:
                info = f"{err} | CTP未成, 已撤CTP并平HL {rollback_oz:.4f}oz"
                self._audit_trade(signal, lots, hl_size_oz, 0, 0, 0, "rolled_back", extra_info=info)
                return DualLegResult(
                    ag_order=ag_order,
                    lots=lots,
                    hl_size_oz=hl_size_oz,
                    status="rolled_back",
                    error=info,
                    exposure_unresolved=False,
                )

            final_err = f"{err} | CTP未成, HL回滚失败"
            self._audit_trade(signal, lots, hl_size_oz, 0, 0, 0, "leg_failure", extra_info=final_err)
            return DualLegResult(
                ag_order=ag_order,
                lots=lots,
                hl_size_oz=hl_size_oz,
                status="leg_failure",
                error=final_err,
                exposure_unresolved=True,
            )

        self._audit_trade(signal, lots, hl_size_oz, 0, 0, 0, "leg_failure", extra_info=err)
        return DualLegResult(
            ag_order=ag_order,
            lots=lots,
            hl_size_oz=hl_size_oz,
            status="leg_failure",
            error=err,
            exposure_unresolved=True,
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
        filepath = os.path.join(self._audit_dir, f"trades_{date.today().isoformat()}.csv")
        write_header = not os.path.exists(filepath)
        try:
            with open(filepath, "a", newline="") as f:
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
