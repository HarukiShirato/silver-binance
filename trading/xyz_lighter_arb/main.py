#!/usr/bin/env python3
# main.py - SHFE AG vs HL SILVER 瀵瑰啿绯荤粺鍏ュ彛
#
# 钖勭紪鎺掑眰: 鎶?DataEngine + SignalEngine + ExecutionEngine 涓茶捣鏉?

import asyncio
import signal
import os
import csv
import uuid
import time
from collections import Counter
from datetime import datetime

import logging
from logging.handlers import RotatingFileHandler
# 纭繚鏃ュ織鐩綍鍦?FileHandler 鍒濆鍖栧墠瀛樺湪
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("logs/hedge.log", maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"),
    ]
)
logger = logging.getLogger("SilverHedge")

from config import (
    TRADING_PAIRS, STRATEGY, API, RISK, FOREX, NOTIFY, RUNTIME,
    load_api_keys, load_notify_config, validate_api_keys, load_runtime_config, get_ctp_front_candidates,
)
from exchanges import CTPGateway, HyperliquidClient
from forex_feed import ForexFeed
from session_manager import SessionManager, SessionType
from data_engine import DataEngine, NormalizedPrice
from signal_engine import SignalEngine, Signal, SignalResult
from execution_engine import ExecutionEngine
from remote_hl_executor import RemoteHLExecutorClient
from risk_manager import RiskManager
from position_manager import PositionManager, MarginLevel
from notifier import FeishuNotifier


class SilverHedgeBot:
    """SHFE AG vs HL SILVER hedge bot."""

    def __init__(self):
        load_runtime_config()
        load_api_keys(dry_run=RUNTIME.dry_run, hl_exec_mode=RUNTIME.hl_exec_mode)
        validate_api_keys(dry_run=RUNTIME.dry_run, hl_exec_mode=RUNTIME.hl_exec_mode)
        load_notify_config()

        pair = TRADING_PAIRS['SILVER']

        # 浜ゆ槗鎵€
        self.ctp_gateway = CTPGateway(
            broker_id=API.ctp_broker_id,
            user_id=API.ctp_user_id,
            password=API.ctp_password,
            md_front=API.ctp_md_front,
            td_front=API.ctp_td_front,
            app_id=API.ctp_app_id,
            auth_code=API.ctp_auth_code,
        )
        self.hl_client = HyperliquidClient(
            api_url=API.hl_api_url,
            ws_url=API.hl_ws_url,
            dex=API.hl_dex,
            private_key=API.hl_api_wallet_private_key,
            wallet_address=API.hl_wallet_address,
        )

        # 宸ュ叿妯″潡
        self.session_mgr = SessionManager()
        self.forex_feed = ForexFeed(
            fallback_rate=FOREX.fallback_usdcny,
            update_interval=FOREX.update_interval,
        )

        # 涓夊眰寮曟搸
        self.data_engine = DataEngine(
            ctp_gateway=self.ctp_gateway,
            hl_client=self.hl_client,
            forex_feed=self.forex_feed,
            session_manager=self.session_mgr,
            pair=pair,
            hl_data_mode=RUNTIME.hl_data_mode,
            remote_quote_url=RUNTIME.hl_remote_quote_url,
            remote_quote_ws_url=RUNTIME.hl_remote_quote_ws_url,
            remote_quote_timeout_sec=RUNTIME.remote_quote_timeout_sec,
            remote_quote_poll_sec=RUNTIME.remote_quote_poll_sec,
        )
        self.signal_engine = SignalEngine(
            pair_name='SILVER',
            window_size=STRATEGY.spread_window,
            entry_zscore=STRATEGY.entry_zscore,
            exit_zscore=STRATEGY.exit_zscore,
            stop_loss_zscore=STRATEGY.stop_loss_zscore,
            sample_interval=STRATEGY.sample_interval,
            session_manager=self.session_mgr,
        )

        # 椋庢帶鍜屼粨绠?
        self.risk_manager = RiskManager(
            max_daily_trades=RISK.max_daily_trades,
            max_daily_loss=RISK.max_daily_loss,
            leg_timeout_ms=int(RISK.leg_timeout_sec * 1000),
            leg_retry_times=RISK.leg_retry_times,
            emergency_spread_pct=RISK.emergency_spread_pct,
        )
        self.position_manager = PositionManager(
            capital=pair.capital,
            ctp_margin_rate=pair.ctp_margin_rate,
            hl_leverage=pair.hl_leverage,
            ctp_multiplier=pair.ctp_multiplier,
            max_position_lots=STRATEGY.max_position_lots,
            margin_warning_pct=RISK.margin_warning_pct,
            margin_danger_pct=RISK.margin_danger_pct,
            margin_critical_pct=RISK.margin_critical_pct,
        )

        # 閫氱煡
        self.notifier = FeishuNotifier(
            webhook_url=NOTIFY.feishu_trade_webhook_url,
            enabled=NOTIFY.enable_feishu,
        )
        self.margin_notifier = FeishuNotifier(
            webhook_url=NOTIFY.feishu_margin_webhook_url or NOTIFY.feishu_trade_webhook_url,
            enabled=NOTIFY.enable_feishu,
        )

        # 鎵ц寮曟搸
        self.execution_engine = ExecutionEngine(
            ctp_gateway=self.ctp_gateway,
            hl_client=self.hl_client,
            pair=pair,
            notifier=self.notifier,
            leg_timeout_sec=RISK.leg_timeout_sec,
        )
        self.remote_hl_executor = None
        if RUNTIME.hl_exec_mode == "remote" and RUNTIME.hl_remote_url:
            self.remote_hl_executor = RemoteHLExecutorClient(
                base_url=RUNTIME.hl_remote_url,
                timeout_sec=RUNTIME.remote_exec_timeout_sec,
            )

        self._running = False
        self._total_fees_today = 0.0
        self._last_session_type = None
        self._decision_stats = Counter()
        self._trade_log_file = os.path.join("data", "trade_events.csv")
        self._window_full_notified = False
        self._last_remote_ok_ts = ""
        self._last_remote_err = ""
        self._margin_risk_last_notified = {"CTP": 0.0, "HL": 0.0}
        self._alert_last_notified: dict[str, float] = {}
        self._ctp_reconnect_lock = asyncio.Lock()
        self._ctp_connected_at: float = 0.0
        self._ctp_paused_by_session = False
        self._last_position_mismatch_msg = ""
        self._position_phase = "NONE"  # NONE | PENDING_ENTRY_* | PENDING_EXIT_*
        self._pending_signal_id = ""
        self._pending_since = 0.0

    async def initialize(self):
        """Initialize exchanges and data sources."""
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data", exist_ok=True)
        self._ensure_trade_log_file()
        self._log_runtime_config()

        current_session = self.session_mgr.get_session_type()
        should_connect_ctp = not (
            RISK.disconnect_ctp_when_closed and current_session == SessionType.CLOSED
        )
        if should_connect_ctp:
            logger.info("正在连接 CTP 网关...")
            await self._connect_ctp_with_fallback()
        else:
            logger.info("当前为非交易时段，暂不连接 CTP，等待开盘后自动连接")
            self._ctp_paused_by_session = True

        if RUNTIME.dry_run:
            logger.info("当前模式: DRY_RUN=true（只跑信号与风控，不发真实下单）")

        logger.info("正在缓存 HL asset_id...")
        await self.hl_client._ensure_asset_ids()

        if self.remote_hl_executor is not None:
            health = self.remote_hl_executor.health_check()
            if health.ok:
                logger.info(
                    f"远程HL网关探活成功: code={health.status_code} latency={health.latency_ms:.1f}ms"
                )
            else:
                logger.warning(
                    f"远程HL网关探活失败: code={health.status_code} latency={health.latency_ms:.1f}ms detail={health.detail}"
                )

        restored = self.signal_engine.load_window_state(load_points=20)
        if restored > 0:
            logger.info(
                f"已恢复信号窗口样本: {restored}/{self.signal_engine.window_size} (warm start)"
            )
            if self.signal_engine.sample_count >= self.signal_engine.window_size:
                self._window_full_notified = True

        # 鍔犺浇椋庢帶鐘舵€?
        self.risk_manager.load_state()

        # 启动先做一次外部仓位对账：以外部真实仓位为准
        await self._sync_local_position_from_external_on_startup()

        # 娉ㄥ唽浠锋牸鍥炶皟
        self.data_engine.on_price(self._on_price)

        # 鍚姩鏁版嵁寮曟搸
        await self.data_engine.start(include_ctp=should_connect_ctp)

        # 鍙戦€佸惎鍔ㄩ€氱煡
        await self.notifier.notify_startup(
            pairs=['SILVER'],
            capital=TRADING_PAIRS['SILVER'].capital,
        )

        self._last_session_type = self.session_mgr.get_session_type()
        if self._last_session_type != SessionType.CLOSED:
            await self._notify_session_transition("START", self._last_session_type)

        logger.info("SilverHedgeBot initialized")

    def _log_runtime_config(self):
        pair = TRADING_PAIRS['SILVER']
        logger.info(
            "运行配置摘要: "
            f"mode={'DRY_RUN' if RUNTIME.dry_run else 'LIVE'} "
            f"hl_exec_mode={RUNTIME.hl_exec_mode} "
            f"hl_remote_url={RUNTIME.hl_remote_url or 'N/A'} "
            f"hl_data_mode={RUNTIME.hl_data_mode} "
            f"hl_remote_quote_url={RUNTIME.hl_remote_quote_url or 'N/A'} "
            f"hl_remote_quote_ws_url={RUNTIME.hl_remote_quote_ws_url or 'N/A'} "
            f"entry_z={STRATEGY.entry_zscore} exit_z={STRATEGY.exit_zscore} stop_z={STRATEGY.stop_loss_zscore} "
            f"window={STRATEGY.spread_window} sample_interval={STRATEGY.sample_interval}s "
            f"max_lots={STRATEGY.max_position_lots} emergency_spread_pct={RISK.emergency_spread_pct} "
            f"ctp_instrument={pair.ctp_instrument} hl_symbol={pair.hl_symbol}"
        )

    async def _connect_ctp_with_fallback(self):
        fronts = get_ctp_front_candidates()
        if not fronts:
            raise ConnectionError("未配置可用的 CTP 前置地址")

        last_error: Exception = None
        total = len(fronts)
        for idx, (md_front, td_front) in enumerate(fronts, start=1):
            self.ctp_gateway.md_front = md_front
            self.ctp_gateway.td_front = td_front
            logger.info(
                f"尝试 CTP 前置 [{idx}/{total}] "
                f"MD={md_front}, TD={td_front}"
            )
            try:
                await self.ctp_gateway.connect()
                self._ctp_connected_at = time.time()
                self._ctp_paused_by_session = False
                logger.info(f"CTP 已连接, 使用前置: MD={md_front}, TD={td_front}")
                return
            except Exception as e:
                last_error = e
                logger.warning(
                    f"前置连接失败 [{idx}/{total}] MD={md_front}, TD={td_front}, err={e}"
                )
                try:
                    await self.ctp_gateway.close()
                except Exception:
                    pass

        raise ConnectionError(f"所有 CTP 前置连接失败, last_error={last_error}")

    def _should_send_alert(self, key: str, cooldown_sec: int) -> bool:
        now = time.time()
        last = self._alert_last_notified.get(key, 0.0)
        if now - last >= cooldown_sec:
            self._alert_last_notified[key] = now
            return True
        return False

    async def _ensure_ctp_online_for_session(self):
        if self.ctp_gateway.is_connected:
            if self._ctp_paused_by_session:
                logger.info("交易时段开始，恢复 CTP 行情订阅")
            self.data_engine.activate_ctp_stream()
            self._ctp_paused_by_session = False
            if self._ctp_connected_at <= 0:
                self._ctp_connected_at = time.time()
            return
        async with self._ctp_reconnect_lock:
            if self.ctp_gateway.is_connected:
                if self._ctp_paused_by_session:
                    logger.info("交易时段开始，恢复 CTP 行情订阅")
                self.data_engine.activate_ctp_stream()
                self._ctp_paused_by_session = False
                if self._ctp_connected_at <= 0:
                    self._ctp_connected_at = time.time()
                return
            logger.info("交易时段开始，自动连接 CTP")
            await self._connect_ctp_with_fallback()
            self.data_engine.activate_ctp_stream()

    async def _pause_ctp_for_closed_session(self):
        if not RISK.disconnect_ctp_when_closed:
            return
        if not self.ctp_gateway.is_connected:
            self._ctp_paused_by_session = True
            self._ctp_connected_at = 0.0
            self.data_engine.deactivate_ctp_stream()
            return
        async with self._ctp_reconnect_lock:
            if not self.ctp_gateway.is_connected:
                self._ctp_paused_by_session = True
                self._ctp_connected_at = 0.0
                self.data_engine.deactivate_ctp_stream()
                return
            logger.info("非交易时段，自动断开 CTP")
            self.data_engine.deactivate_ctp_stream()
            try:
                await asyncio.wait_for(self.ctp_gateway.close(), timeout=15)
            except asyncio.TimeoutError:
                logger.warning("非交易时段断开CTP超时(15s)，继续后续流程")
            except Exception as e:
                logger.warning(f"非交易时段断开CTP异常(忽略): {e}")
            self._ctp_paused_by_session = True
            self._ctp_connected_at = 0.0

    async def _reconnect_ctp_stream(self, reason: str):
        async with self._ctp_reconnect_lock:
            logger.warning(f"触发 CTP 重连: reason={reason}")
            try:
                await asyncio.wait_for(self.ctp_gateway.close(), timeout=15)
            except asyncio.TimeoutError:
                logger.warning("CTP重连前关闭超时(15s)，继续重连")
            except Exception as e:
                logger.warning(f"CTP 关闭异常(忽略): {e}")
            await self._connect_ctp_with_fallback()
            self.data_engine.activate_ctp_stream()

    async def _sync_local_position_from_external_on_startup(self):
        truth = await self._read_external_position_truth()
        if truth is None:
            self.signal_engine.set_position("NONE")
            self.position_manager.current_lots = 0
            self.risk_manager.save_position_state("SILVER", "NONE", 0)
            logger.info("启动对账: 外部仓位不可用，默认本地清仓态")
            return

        truth_side = truth["truth_side"]
        truth_lots = truth["truth_lots"]
        self.signal_engine.set_position(truth_side)
        self.position_manager.current_lots = int(round(truth_lots))
        self.risk_manager.save_position_state("SILVER", truth_side, self.position_manager.current_lots)
        logger.info(
            "启动对账完成: "
            f"truth={truth_side}/{truth_lots:.2f}, "
            f"ctp={truth['ctp_side']}/{truth['ctp_lots']:.2f}, "
            f"hl={truth['hl_side']}/{truth['hl_lots']:.2f}"
        )

    async def _on_price(self, price: NormalizedPrice):
        """DataEngine callback: price -> signal -> execution."""
        result = self.signal_engine.update(price)
        await self._maybe_notify_window_full(price)
        if not result:
            self._decision_stats[self.signal_engine.last_decision_reason] += 1
            return

        if result.signal == Signal.HOLD:
            self._decision_stats[self.signal_engine.last_decision_reason] += 1
            return

        await self._process_signal(result)

    async def _maybe_notify_window_full(self, price: NormalizedPrice):
        if self._window_full_notified:
            return
        if self.signal_engine.sample_count < self.signal_engine.window_size:
            return

        spread_pct = ((price.ag_price - price.hl_price_cny_kg) / price.hl_price_cny_kg * 100) if price.hl_price_cny_kg > 0 else None
        zscore = self.signal_engine.get_current_zscore()

        await self.notifier.notify_window_full(
            pair_name="SILVER",
            sample_count=self.signal_engine.sample_count,
            window_size=self.signal_engine.window_size,
            zscore=zscore,
            spread_pct=spread_pct,
            ag_price=price.ag_price,
            hl_price_usd=price.hl_price_usd,
            hl_price_cny_kg=price.hl_price_cny_kg,
            usdcny=price.usdcny,
        )
        self._window_full_notified = True
        logger.info(
            f"信号窗口已满: {self.signal_engine.sample_count}/{self.signal_engine.window_size}, "
            f"z={zscore:.2f}, spread={(spread_pct if spread_pct is not None else 0):.3f}%"
        )

    async def _process_signal(self, signal: SignalResult):
        signal_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{signal.signal.value}-{uuid.uuid4().hex[:8]}"
        logger.info(
            f"信号[{signal_id}]: {signal.signal.value} | "
            f"AG={signal.ag_price:.1f} CNY/kg HL=${signal.hl_price_usd_oz:.4f} | "
            f"spread={signal.spread_pct:.3f}% zscore={signal.zscore:.2f}"
        )

        is_entry = signal.signal in (Signal.LONG, Signal.SHORT)
        is_exit = signal.signal in (Signal.EXIT_LONG, Signal.EXIT_SHORT)

        if is_entry and self._position_phase.startswith("PENDING"):
            reason = f"pending_state:{self._position_phase}"
            self._decision_stats["blocked_pending"] += 1
            self._append_trade_event(signal.signal.value, 0, "blocked", 0.0, reason, signal, signal_id)
            return

        if (
            is_entry
            and RUNTIME.hl_data_mode == "remote"
            and bool(RUNTIME.hl_remote_quote_ws_url)
            and not self.data_engine.remote_quote_ws_connected
        ):
            reason = "hl_quote_ws_disconnected"
            self._decision_stats[f"blocked_{reason}"] += 1
            self._append_trade_event(signal.signal.value, 0, "blocked", 0.0, reason, signal, signal_id)
            if self._should_send_alert("hl_quote_ws_disconnected", max(30, int(RISK.hl_quote_ws_down_alert_cooldown_sec))):
                await self.margin_notifier.notify_error(
                    f"阻止开新仓: HL quote websocket 断开, last_err={self.data_engine.remote_quote_ws_last_error or 'N/A'}",
                    "SILVER",
                )
            return

        can_trade, reason = self.risk_manager.can_trade('SILVER', signal=signal.signal.value)
        if not can_trade:
            self._decision_stats[f"blocked_risk_{reason}"] += 1
            self._append_trade_event(signal.signal.value, 0, "blocked", 0.0, f"risk:{reason}", signal, signal_id)
            return

        safe, reason = self.risk_manager.check_spread_safety(signal.ag_price, signal.hl_price_cny_kg)
        if not safe:
            await self.notifier.notify_emergency(reason)
            self._decision_stats[f"blocked_spread_{reason}"] += 1
            self._append_trade_event(signal.signal.value, 0, "blocked", 0.0, f"spread:{reason}", signal, signal_id)
            return

        if is_entry:
            lots = self.position_manager.calculate_order_lots(
                ag_price_cny_kg=signal.ag_price,
                hl_price_usd_oz=signal.hl_price_usd_oz,
                usdcny=signal.usdcny,
            )
        elif is_exit:
            lots = self.position_manager.current_lots
        else:
            lots = 0

        if lots <= 0:
            self._decision_stats["blocked_lots_zero"] += 1
            self._append_trade_event(signal.signal.value, 0, "blocked", 0.0, "lots_zero", signal, signal_id)
            return

        self._set_pending_phase(signal.signal.value, signal_id)
        self.risk_manager.record_trade_start(
            pair_name='SILVER',
            signal=signal.signal.value,
            xyz_price=signal.ag_price,
            lighter_price=signal.hl_price_usd_oz,
            spread=signal.spread_pct,
            zscore=signal.zscore,
        )

        if NOTIFY.notify_on_trade:
            await self.notifier.notify_trade(
                pair_name='SILVER',
                signal=signal.signal.value,
                xyz_price=signal.ag_price,
                lighter_price=signal.hl_price_usd_oz,
                zscore=signal.zscore,
                size=lots,
                spread=signal.spread_pct,
            )

        if RUNTIME.dry_run:
            remote_ok = True
            remote_error = ""
            if self.remote_hl_executor is not None:
                payload = {
                    "signal_id": signal_id,
                    "signal": signal.signal.value,
                    "symbol": TRADING_PAIRS['SILVER'].hl_symbol,
                    "lots": lots,
                    "hl_size_oz": lots * TRADING_PAIRS['SILVER'].ctp_multiplier / 0.0311035,
                    "ag_price": signal.ag_price,
                    "hl_price_usd": signal.hl_price_usd_oz,
                    "usdcny": signal.usdcny,
                    "spread_pct": signal.spread_pct,
                    "zscore": signal.zscore,
                    "ts": signal.timestamp,
                }
                result = self.remote_hl_executor.send_dry_run(payload)
                if result.ok:
                    self._last_remote_ok_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._last_remote_err = ""
                else:
                    remote_ok = False
                    remote_error = f"code={result.status_code} detail={result.detail}"
                    self._last_remote_err = remote_error

            if remote_ok:
                self._apply_filled_position_state(signal.signal.value, lots)
                self.risk_manager.save_position_state('SILVER', self.signal_engine.position, self.position_manager.current_lots)
                self.risk_manager.record_trade_result('SILVER', 0, "filled")
                self._append_trade_event(signal.signal.value, lots, "filled", 0.0, "dry_run_fill", signal, signal_id)
                if NOTIFY.notify_on_trade:
                    try:
                        await self.notifier.notify_trade_result(
                            pair_name='SILVER',
                            signal=f"DRY_RUN_{signal.signal.value}",
                            xyz_fill_price=signal.ag_price,
                            lighter_fill_price=signal.hl_price_usd_oz,
                            size=lots,
                            fees=0,
                            status="filled",
                        )
                        await self.notifier.notify_dry_run_fill(
                            signal=signal.signal.value,
                            lots=lots,
                            zscore=signal.zscore,
                            spread_pct=signal.spread_pct,
                            ag_price=signal.ag_price,
                            hl_price_usd=signal.hl_price_usd_oz,
                            hl_price_cny_kg=signal.hl_price_cny_kg,
                            usdcny=signal.usdcny,
                            position_after=self.signal_engine.position,
                        )
                    except Exception as notify_err:
                        logger.error(f"DRY_RUN 通知失败(不影响成交记录): {notify_err}")
                self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)
            else:
                self.risk_manager.record_trade_result('SILVER', 0, "failed")
                self._decision_stats["dry_run_remote_failed"] += 1
                self._append_trade_event(signal.signal.value, 0, "failed", 0.0, f"dry_run_remote_failed:{remote_error}", signal, signal_id)
                if NOTIFY.notify_on_error:
                    await self.notifier.notify_error(f"DRY_RUN 远端回执失败，忽略本次成交: {remote_error}", 'SILVER')
            self._clear_pending_phase()
            return

        try:
            result = await self.execution_engine.execute(signal, lots)
            if result.status == "filled":
                self._apply_filled_position_state(signal.signal.value, lots)
                self.risk_manager.save_position_state('SILVER', self.signal_engine.position, self.position_manager.current_lots)
                self.risk_manager.record_trade_result('SILVER', -result.total_fee_rmb, "filled")
                self._total_fees_today += result.total_fee_rmb
                self._append_trade_event(signal.signal.value, lots, "filled", result.total_fee_rmb, "live_fill", signal, signal_id)
                if NOTIFY.notify_on_trade:
                    await self.notifier.notify_trade_result(
                        pair_name='SILVER',
                        signal=signal.signal.value,
                        xyz_fill_price=result.ag_fill_price,
                        lighter_fill_price=result.hl_fill_price,
                        size=lots,
                        fees=result.total_fee_rmb,
                        status="filled",
                    )
            else:
                self.risk_manager.record_trade_result('SILVER', 0, result.status)
                self._decision_stats[f"exec_status_{result.status}"] += 1
                self._append_trade_event(
                    signal.signal.value,
                    lots,
                    result.status,
                    0.0,
                    result.error or "live_execute_not_filled",
                    signal,
                    signal_id,
                )
                if NOTIFY.notify_on_error:
                    await self.notifier.notify_error(result.error, 'SILVER')
            self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)
            self._clear_pending_phase()
        except Exception as e:
            logger.error(f"交易执行异常: {e}")
            self.risk_manager.record_trade_result('SILVER', 0, "failed")
            self._decision_stats["exec_exception"] += 1
            self._append_trade_event(signal.signal.value, lots, "failed", 0.0, str(e), signal, signal_id)
            if NOTIFY.notify_on_error:
                await self.notifier.notify_error(str(e), 'SILVER')
            self._clear_pending_phase()

    def _set_pending_phase(self, signal_name: str, signal_id: str):
        if signal_name == "LONG":
            self._position_phase = "PENDING_ENTRY_LONG"
        elif signal_name == "SHORT":
            self._position_phase = "PENDING_ENTRY_SHORT"
        elif signal_name == "EXIT_LONG":
            self._position_phase = "PENDING_EXIT_LONG"
        elif signal_name == "EXIT_SHORT":
            self._position_phase = "PENDING_EXIT_SHORT"
        else:
            self._position_phase = "NONE"
        self._pending_signal_id = signal_id
        self._pending_since = time.time()

    def _clear_pending_phase(self):
        self._position_phase = "NONE"
        self._pending_signal_id = ""
        self._pending_since = 0.0

    def _apply_filled_position_state(self, signal_name: str, lots: int):
        if signal_name == "LONG":
            self.position_manager.on_open(lots)
            self.signal_engine.set_position("LONG")
            return
        if signal_name == "SHORT":
            self.position_manager.on_open(lots)
            self.signal_engine.set_position("SHORT")
            return
        if signal_name in ("EXIT_LONG", "EXIT_SHORT"):
            self.position_manager.on_close(lots)
            self.signal_engine.set_position("NONE")
            self.signal_engine.reset_funding()
            return

    async def run(self):
        """Start background tasks."""
        self._running = True

        tasks = [
            asyncio.create_task(self._save_state_loop()),
            asyncio.create_task(self._daily_summary_loop()),
            asyncio.create_task(self._funding_rate_loop()),
            asyncio.create_task(self._margin_monitor_loop()),
            asyncio.create_task(self._position_reconcile_loop()),
            asyncio.create_task(self._health_monitor_loop()),
            asyncio.create_task(self._md_watchdog_loop()),
            asyncio.create_task(self._session_transition_loop()),
            asyncio.create_task(self._status_heartbeat_loop()),
            asyncio.create_task(self._market_snapshot_loop()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass

    async def _funding_rate_loop(self):
        """Fetch funding rate periodically."""
        while self._running:
            try:
                rate = await self.hl_client.get_funding_rate(TRADING_PAIRS['SILVER'].hl_symbol)
                self.signal_engine.update_funding_rate(rate)
                logger.info(f"HL SILVER funding rate: {rate:.6f}")
            except Exception as e:
                logger.warning(f"获取 funding rate 失败: {e}")
            await asyncio.sleep(3600)

    async def _margin_monitor_loop(self):
        """Monitor CTP and HL margins periodically."""
        interval = RISK.margin_check_interval  # 榛樿30绉?
        while self._running:
            try:
                await self._check_ctp_margin()
            except Exception as e:
                logger.warning(f"CTP 保证金查询失败: {e}")

            try:
                await self._check_hl_margin()
            except Exception as e:
                logger.warning(f"HL 保证金查询失败: {e}")

            await asyncio.sleep(interval)

    async def _check_ctp_margin(self):
        """Query CTP account and update margin state."""
        if not self.ctp_gateway.is_connected:
            return

        acct = await self.ctp_gateway.query_account()
        if not acct:
            return

        old_level = self.position_manager.ctp_level
        new_level = self.position_manager.update_ctp_margin(
            balance=acct.balance,
            available=acct.available,
            used_margin=acct.margin,
            unrealized_pnl=acct.profit,
        )

        # 等级变化时通知一次；DANGER/CRITICAL 持续状态每60秒重复告警
        should_notify = (
            (new_level != old_level and new_level != MarginLevel.NORMAL)
            or self._should_repeat_margin_alert("CTP", new_level)
        )
        if should_notify:
            await self.margin_notifier.notify_margin_warning(
                account_name="CTP",
                level=new_level,
                margin_ratio=self.position_manager.ctp_margin.margin_ratio,
                used_margin=acct.margin,
                balance=acct.balance,
                available=acct.available,
            )

    async def _check_hl_margin(self):
        """Query HL account and update margin state."""
        if not API.hl_wallet_address:
            return
        state = await self.hl_client.get_user_state()
        if not state:
            return

        # Hyperliquid clearinghouseState 杩斿洖鏍煎紡:
        # {"marginSummary": {"accountValue": "...", "totalMarginUsed": "...", "withdrawable": "..."}}
        margin_summary = state.get("marginSummary", {})

        account_value = float(margin_summary.get("accountValue", 0))
        total_margin_used = float(margin_summary.get("totalMarginUsed", 0))
        withdrawable = float(margin_summary.get("withdrawable", 0))

        # 娴泩娴簭 = accountValue - totalRawUsd (杩戜技)
        total_raw_usd = float(margin_summary.get("totalRawUsd", account_value))
        unrealized_pnl = account_value - total_raw_usd

        old_level = self.position_manager.hl_level
        new_level = self.position_manager.update_hl_margin(
            account_value=account_value,
            available=withdrawable,
            used_margin=total_margin_used,
            unrealized_pnl=unrealized_pnl,
        )

        # 等级变化时通知一次；DANGER/CRITICAL 持续状态每60秒重复告警
        should_notify = (
            (new_level != old_level and new_level != MarginLevel.NORMAL)
            or self._should_repeat_margin_alert("HL", new_level)
        )
        if should_notify:
            await self.margin_notifier.notify_margin_warning(
                account_name="HL",
                level=new_level,
                margin_ratio=self.position_manager.hl_margin.margin_ratio,
                used_margin=total_margin_used,
                balance=account_value,
                available=withdrawable,
            )

    def _should_repeat_margin_alert(self, account_name: str, level: str) -> bool:
        """DANGER/CRITICAL level repeats every 60s; reset timer once risk is cleared."""
        if level in (MarginLevel.DANGER, MarginLevel.CRITICAL):
            now = time.time()
            last = self._margin_risk_last_notified.get(account_name, 0.0)
            if now - last >= 60:
                self._margin_risk_last_notified[account_name] = now
                return True
            return False

        # NORMAL/WARNING: clear repeat timer
        self._margin_risk_last_notified[account_name] = 0.0
        return False

    async def _save_state_loop(self):
        """Persist runtime state every minute."""
        while self._running:
            try:
                self.risk_manager.save_state()
            except Exception as e:
                logger.error(f"保存状态失败: {e}")
            await asyncio.sleep(60)

    async def _daily_summary_loop(self):
        """Send daily summary."""
        sent_today = False
        while self._running:
            now = datetime.now()
            if now.hour == NOTIFY.daily_summary_hour and not sent_today:
                if NOTIFY.notify_daily_summary:
                    stats = self.risk_manager.daily_stats
                    positions = {}
                    if self.position_manager.current_lots > 0:
                        positions['SILVER'] = (
                            f"{self.signal_engine.position} "
                            f"{self.position_manager.current_lots} lots"
                        )
                    await self.notifier.notify_daily_summary(
                        date=stats.date,
                        trade_count=stats.trade_count,
                        total_pnl=stats.total_pnl,
                        win_count=stats.win_count,
                        loss_count=stats.loss_count,
                        total_fees=self._total_fees_today,
                        max_drawdown=stats.max_drawdown,
                        positions=positions,
                    )
                sent_today = True

            if now.hour == 0 and sent_today:
                sent_today = False
                self._total_fees_today = 0.0

            await asyncio.sleep(60)

    async def _health_monitor_loop(self):
        """Run periodic health checks and alerts."""
        while self._running:
            issues = []
            current_session = self.session_mgr.get_session_type()
            in_session = current_session != SessionType.CLOSED

            # CTP 连接
            if in_session and not self.ctp_gateway.is_connected:
                issues.append("CTP 网关断开")

            if (
                RUNTIME.hl_data_mode == "remote"
                and bool(RUNTIME.hl_remote_quote_ws_url)
                and not self.data_engine.remote_quote_ws_connected
            ):
                issues.append("HL远端行情WS断开(已禁止开新仓)")

            # HL/AG 数据新鲜度
            latest = self.data_engine.latest
            if latest:
                if latest.hl_stale:
                    hl_age = self.data_engine.hl_last_update_age
                    issues.append(f"HL 价格过期 ({hl_age:.0f}s)")
                if in_session and latest.ag_stale:
                    issues.append("AG 行情过期")

            # 汇率
            if self.forex_feed.is_stale:
                issues.append(f"汇率过期 (当前 {self.forex_feed.usdcny:.4f})")

            # 信号引擎数据就绪
            if not self.signal_engine.data_ready:
                issues.append("信号引擎数据不足，等待更多 tick")

            # 紧急停止
            if self.risk_manager.is_emergency:
                issues.append("紧急停止已触发")

            if issues:
                msg = "健康检查异常:\n" + "\n".join(f"- {i}" for i in issues)
                logger.warning(msg)
                if any(kw in msg for kw in ("断开", "紧急")):
                    await self.notifier.notify_emergency(msg)

            await asyncio.sleep(60)

    async def _position_reconcile_loop(self):
        while self._running:
            try:
                truth = await self._read_external_position_truth()
                if truth is None:
                    await asyncio.sleep(max(10, int(RISK.position_reconcile_interval_sec)))
                    continue

                msg = self._build_position_mismatch_message(truth)
                if msg:
                    logger.warning(msg)
                    if msg != self._last_position_mismatch_msg or self._should_send_alert(
                        "position_mismatch",
                        max(30, int(RISK.position_mismatch_alert_cooldown_sec)),
                    ):
                        await self.margin_notifier.notify_error(msg, "SILVER")
                    self._last_position_mismatch_msg = msg
                else:
                    self._last_position_mismatch_msg = ""

                if truth["exposure_reason"]:
                    reason = f"单边暴露: {truth['exposure_reason']}"
                    if not self.risk_manager.is_emergency:
                        self.risk_manager.trigger_emergency_stop(reason)
                    if self._should_send_alert("single_leg_exposure", 30):
                        await self.notifier.notify_emergency(reason)
                    await self._try_flatten_single_leg(truth)

                if self._position_phase == "NONE":
                    target_side = truth["truth_side"]
                    target_lots = int(round(truth["truth_lots"]))
                    if (
                        self.signal_engine.position != target_side
                        or self.position_manager.current_lots != target_lots
                    ):
                        logger.warning(
                            "外部仓位修正本地状态: "
                            f"local={self.signal_engine.position}/{self.position_manager.current_lots} "
                            f"-> truth={target_side}/{target_lots}"
                        )
                        self.signal_engine.set_position(target_side)
                        self.position_manager.current_lots = target_lots
                        self.risk_manager.save_position_state("SILVER", target_side, target_lots)
            except Exception as e:
                logger.warning(f"仓位对账异常: {e}")

            await asyncio.sleep(max(10, int(RISK.position_reconcile_interval_sec)))

    def _build_position_mismatch_message(self, truth: dict) -> str:
        local_side = self.signal_engine.position
        local_lots = float(self.position_manager.current_lots)

        ctp_side = truth["ctp_side"]
        ctp_lots = truth["ctp_lots"]
        hl_side = truth["hl_side"]
        hl_lots = truth["hl_lots"]
        truth_side = truth["truth_side"]
        truth_lots = truth["truth_lots"]

        reasons = []
        if truth["exposure_reason"]:
            reasons.append(f"外部腿异常({truth['exposure_reason']})")

        if local_side != truth_side:
            reasons.append(f"本地方向不一致(local={local_side}, truth={truth_side})")
        if abs(local_lots - truth_lots) > 0.35:
            reasons.append(f"本地手数不一致(local={local_lots:.2f}, truth={truth_lots:.2f})")

        if not reasons:
            return ""

        return (
            "仓位不对等告警: "
            + "; ".join(reasons)
            + f" | local={local_side}/{local_lots:.2f} "
            + f"ctp={ctp_side}/{ctp_lots:.2f} hl={hl_side}/{hl_lots:.2f}"
        )

    async def _read_external_position_truth(self) -> dict | None:
        pair = TRADING_PAIRS["SILVER"]

        # CTP作为主腿，未连接时无法形成可靠真相源
        if not self.ctp_gateway.is_connected:
            return None

        ctp_side, ctp_lots = await self._get_ctp_position_side_lots(pair.ctp_instrument)
        hl_side, hl_lots = await self._get_hl_position_side_lots(pair.hl_symbol, pair.ctp_multiplier)

        truth_side = "NONE"
        truth_lots = 0.0
        exposure_reason = ""

        if ctp_side == "NONE" and hl_side == "NONE":
            truth_side = "NONE"
            truth_lots = 0.0
        elif ctp_side == "NONE" and hl_side != "NONE":
            exposure_reason = "ctp_flat_hl_exposed"
        elif ctp_side != "NONE" and hl_side == "NONE":
            exposure_reason = "ctp_exposed_hl_flat"
        else:
            if ctp_side == "LONG" and hl_side == "SHORT":
                truth_side = "LONG"
                truth_lots = min(ctp_lots, hl_lots)
            elif ctp_side == "SHORT" and hl_side == "LONG":
                truth_side = "SHORT"
                truth_lots = min(ctp_lots, hl_lots)
            else:
                exposure_reason = "ctp_hl_same_direction"

        return {
            "ctp_side": ctp_side,
            "ctp_lots": ctp_lots,
            "hl_side": hl_side,
            "hl_lots": hl_lots,
            "truth_side": truth_side,
            "truth_lots": truth_lots,
            "exposure_reason": exposure_reason,
        }

    async def _get_ctp_position_side_lots(self, instrument_id: str) -> tuple[str, float]:
        positions = await self.ctp_gateway.query_positions(instrument_id=instrument_id)
        long_lots = 0.0
        short_lots = 0.0
        for pos in positions:
            if pos.instrument_id != instrument_id:
                continue
            if pos.direction == "LONG":
                long_lots += float(pos.volume)
            elif pos.direction == "SHORT":
                short_lots += float(pos.volume)

        net = long_lots - short_lots
        if abs(net) < 1e-6:
            return ("NONE", 0.0)
        return ("LONG", net) if net > 0 else ("SHORT", abs(net))

    async def _get_hl_position_side_lots(self, hl_symbol: str, ctp_multiplier: int) -> tuple[str, float]:
        if not API.hl_wallet_address:
            return ("NONE", 0.0)

        state = await self.hl_client.get_user_state()
        if not isinstance(state, dict):
            return ("NONE", 0.0)

        symbol_variants = {hl_symbol}
        if API.hl_dex:
            symbol_variants.add(f"{API.hl_dex}:{hl_symbol}")

        net_size_oz = 0.0
        for entry in state.get("assetPositions", []) or []:
            pos = entry.get("position", entry) if isinstance(entry, dict) else {}
            coin = str(pos.get("coin") or pos.get("symbol") or pos.get("asset") or "").strip()
            if coin not in symbol_variants:
                continue
            raw_size = pos.get("szi")
            if raw_size is None:
                raw_size = pos.get("sz")
            if raw_size is None:
                raw_size = pos.get("size")
            if raw_size is None:
                raw_size = pos.get("position")
            try:
                net_size_oz += float(raw_size)
            except Exception:
                continue

        lots = abs(net_size_oz) * 0.0311035 / float(ctp_multiplier)
        if abs(net_size_oz) < 1e-8:
            return ("NONE", 0.0)
        return ("LONG", lots) if net_size_oz > 0 else ("SHORT", lots)

    async def _try_flatten_single_leg(self, truth: dict):
        pair = TRADING_PAIRS["SILVER"]
        ctp_side = truth["ctp_side"]
        ctp_lots = truth["ctp_lots"]
        hl_side = truth["hl_side"]
        hl_lots = truth["hl_lots"]

        if RUNTIME.dry_run:
            logger.warning(
                "DRY_RUN 模式仅告警，不执行单边暴露应急平仓: "
                f"ctp={ctp_side}/{ctp_lots:.2f}, hl={hl_side}/{hl_lots:.2f}"
            )
            return

        if ctp_side != "NONE" and hl_side == "NONE" and ctp_lots > 0:
            close_dir = "SELL" if ctp_side == "LONG" else "BUY"
            close_lots = max(1, int(round(ctp_lots)))
            if self._should_send_alert("single_leg_flatten_ctp", 30):
                logger.warning(f"单边暴露应急平仓CTP: {close_dir} CLOSE_TODAY {close_lots}手")
            try:
                await self.ctp_gateway.market_order(
                    instrument_id=pair.ctp_instrument,
                    direction=close_dir,
                    offset="CLOSE_TODAY",
                    volume=close_lots,
                )
            except Exception as e:
                logger.error(f"单边暴露应急平仓CTP失败: {e}")

        if hl_side != "NONE" and ctp_side == "NONE" and hl_lots > 0 and not RUNTIME.dry_run:
            is_buy = hl_side == "SHORT"
            hl_size_oz = hl_lots * pair.ctp_multiplier / 0.0311035
            if self._should_send_alert("single_leg_flatten_hl", 30):
                logger.warning(
                    f"单边暴露应急平仓HL: {'BUY' if is_buy else 'SELL'} {hl_size_oz:.2f}oz"
                )
            try:
                await self.hl_client.market_order(
                    coin=f"xyz:{pair.hl_symbol}",
                    is_buy=is_buy,
                    size=hl_size_oz,
                )
            except Exception as e:
                logger.error(f"单边暴露应急平仓HL失败: {e}")

    async def _md_watchdog_loop(self):
        """During trading sessions, reconnect CTP if AG ticks stop updating for too long."""
        while self._running:
            try:
                session_type = self.session_mgr.get_session_type()
                if session_type == SessionType.CLOSED:
                    await asyncio.sleep(5)
                    continue

                if not self.ctp_gateway.is_connected:
                    await asyncio.sleep(5)
                    continue

                threshold = max(5, int(RISK.md_watchdog_no_tick_sec))
                age = self.data_engine.ag_last_tick_age
                if age < 0 and self._ctp_connected_at > 0:
                    age = time.time() - self._ctp_connected_at

                if age >= threshold:
                    detail = (
                        f"MD watchdog触发: AG连续{age:.0f}s无新tick(阈值{threshold}s), "
                        f"session={session_type.value}, 自动重连CTP"
                    )
                    logger.warning(detail)
                    if self._should_send_alert(
                        "md_watchdog",
                        max(60, int(RISK.md_watchdog_alert_cooldown_sec)),
                    ):
                        await self.notifier.notify_error(detail, "SILVER")
                    await self._reconnect_ctp_stream("md_watchdog_no_tick")
            except Exception as e:
                logger.warning(f"MD watchdog异常: {e}")
            await asyncio.sleep(5)

    async def _session_transition_loop(self):
        """Detect trading-session transitions and send Feishu notifications."""
        while self._running:
            try:
                current = self.session_mgr.get_session_type()
                previous = self._last_session_type
                self._last_session_type = current

                if previous is None:
                    await asyncio.sleep(5)
                    continue

                if previous == SessionType.CLOSED and current != SessionType.CLOSED:
                    logger.info(f"检测到时段切换: {previous.value} -> {current.value}")
                    await self._ensure_ctp_online_for_session()
                    await self._notify_session_transition("START", current)
                elif previous != SessionType.CLOSED and current == SessionType.CLOSED:
                    logger.info(f"检测到时段切换: {previous.value} -> {current.value}")
                    await self._pause_ctp_for_closed_session()
                    await self._notify_session_transition("END", previous)
            except Exception as e:
                logger.warning(f"交易时段切换通知异常: {e}")

            await asyncio.sleep(5)

    async def _notify_session_transition(self, event: str, session_type: SessionType):
        latest = self.data_engine.latest
        ag = latest.ag_price if latest else None
        hl_usd = latest.hl_price_usd if latest else None
        hl_cny = latest.hl_price_cny_kg if latest else None
        usdcny = latest.usdcny if latest else None

        await self.notifier.notify_session_transition(
            event=event,
            session_type=session_type.value,
            ag_price=ag,
            hl_price_usd=hl_usd,
            hl_price_cny_kg=hl_cny,
            usdcny=usdcny,
        )

    def _ensure_trade_log_file(self):
        if os.path.exists(self._trade_log_file):
            return
        with open(self._trade_log_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "timestamp",
                "mode",
                "signal_id",
                "pair",
                "signal",
                "lots",
                "status",
                "fee_rmb",
                "reason",
                "remote_last_ok_ts",
                "remote_last_err",
                "ag_price",
                "hl_price_usd",
                "hl_price_cny_kg",
                "usdcny",
                "spread_pct",
                "zscore",
                "position_after",
            ])

    def _append_trade_event(
        self,
        signal: str,
        lots: float,
        status: str,
        fee_rmb: float,
        reason: str,
        signal_data: SignalResult,
        signal_id: str = "",
    ):
        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "DRY_RUN" if RUNTIME.dry_run else "LIVE",
            signal_id,
            "SILVER",
            signal,
            f"{lots:.4f}",
            status,
            f"{fee_rmb:.2f}",
            reason,
            self._last_remote_ok_ts,
            self._last_remote_err,
            f"{signal_data.ag_price:.4f}",
            f"{signal_data.hl_price_usd_oz:.6f}",
            f"{signal_data.hl_price_cny_kg:.4f}",
            f"{signal_data.usdcny:.6f}",
            f"{signal_data.spread_pct:.6f}",
            f"{signal_data.zscore:.6f}",
            self.signal_engine.position,
        ]
        with open(self._trade_log_file, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)

    async def _status_heartbeat_loop(self):
        """Emit status heartbeat every 30 minutes (log + Feishu)."""
        while self._running:
            try:
                latest = self.data_engine.latest
                session_type = self.session_mgr.get_session_type().value
                stats_snapshot = dict(self._decision_stats)
                zscore = self.signal_engine.get_current_zscore()

                ag = latest.ag_price if latest else None
                hl_usd = latest.hl_price_usd if latest else None
                hl_cny = latest.hl_price_cny_kg if latest else None
                usdcny = latest.usdcny if latest else None
                spread = latest and ((latest.ag_price - latest.hl_price_cny_kg) / latest.hl_price_cny_kg * 100)

                logger.info(
                    "状态心跳(30m): "
                    f"mode={'DRY_RUN' if RUNTIME.dry_run else 'LIVE'} "
                    f"session={session_type} "
                    f"data_ready={self.signal_engine.data_ready} "
                    f"window={self.signal_engine.sample_count}/{self.signal_engine.window_size} "
                    f"z={zscore:.2f} spread={(spread if spread is not None else 0):.3f}% "
                    f"AG={(ag if ag is not None else 0):.1f} HL={(hl_usd if hl_usd is not None else 0):.4f} "
                    f"decisions_30m={stats_snapshot} "
                    f"remote_last_ok={self._last_remote_ok_ts or 'N/A'} "
                    f"remote_last_err={self._last_remote_err or 'N/A'}"
                )

                await self.notifier.notify_status_heartbeat(
                    mode="DRY_RUN" if RUNTIME.dry_run else "LIVE",
                    session_type=session_type,
                    data_ready=self.signal_engine.data_ready,
                    sample_count=self.signal_engine.sample_count,
                    window_size=self.signal_engine.window_size,
                    zscore=zscore,
                    spread_pct=spread,
                    ag_price=ag,
                    hl_price_usd=hl_usd,
                    hl_price_cny_kg=hl_cny,
                    usdcny=usdcny,
                    decision_stats=stats_snapshot,
                )

                self._decision_stats.clear()
            except Exception as e:
                logger.warning(f"状态心跳发送失败: {e}")

            await asyncio.sleep(1800)

    async def _market_snapshot_loop(self):
        """Emit market snapshot every minute for operator visibility."""
        while self._running:
            try:
                latest = self.data_engine.latest
                session_type = self.session_mgr.get_session_type().value
                window = f"{self.signal_engine.sample_count}/{self.signal_engine.window_size}"
                zscore = self.signal_engine.get_current_zscore()

                if not latest:
                    logger.info(
                        "行情快照: latest=N/A "
                        f"session={session_type} window={window} "
                        f"data_ready={self.signal_engine.data_ready}"
                    )
                else:
                    spread = (
                        (latest.ag_price - latest.hl_price_cny_kg) / latest.hl_price_cny_kg * 100
                        if latest.hl_price_cny_kg > 0 else 0.0
                    )
                    logger.info(
                        "行情快照: "
                        f"AG={latest.ag_price:.1f} CNY/kg "
                        f"HL={latest.hl_price_usd:.4f} USD/oz "
                        f"HL_CNY={latest.hl_price_cny_kg:.1f} CNY/kg "
                        f"USDCNY={latest.usdcny:.4f} "
                        f"spread={spread:.3f}% z={zscore:.2f} "
                        f"window={window} "
                        f"session={session_type} "
                        f"stale(ag={latest.ag_stale},hl={latest.hl_stale},fx={latest.forex_stale})"
                    )
            except Exception as e:
                logger.warning(f"行情快照日志异常: {e}")

            await asyncio.sleep(60)

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("正在关闭...")
        self._running = False

        # 淇濆瓨浠撲綅鐘舵€?
        self.risk_manager.save_position_state(
            pair_name='SILVER',
            direction=self.signal_engine.position,
            lots=self.position_manager.current_lots,
        )
        self.signal_engine.save_window_state()
        self.risk_manager.save_state()

        await self.data_engine.stop()
        await self.ctp_gateway.close()
        await self.hl_client.close()
        if self.remote_hl_executor is not None:
            self.remote_hl_executor.close()
        await self.notifier.close()
        await self.margin_notifier.close()

        logger.info("关闭完成")


async def main():
    loop = asyncio.get_event_loop()
    stop_requested = False
    current_bot = None

    def signal_handler():
        nonlocal stop_requested, current_bot
        stop_requested = True
        if current_bot is not None:
            asyncio.create_task(current_bot.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    while not stop_requested:
        bot = SilverHedgeBot()
        current_bot = bot
        try:
            await bot.initialize()
            await bot.run()
        except KeyboardInterrupt:
            stop_requested = True
        except Exception as e:
            logger.exception(f"主循环异常: {e}")
            if not stop_requested:
                # 避免初始化失败时进程退出，让程序在后台持续自愈重试
                await asyncio.sleep(15)
        finally:
            try:
                await bot.shutdown()
            except Exception as e:
                logger.warning(f"关闭流程异常(忽略): {e}")
            current_bot = None


if __name__ == "__main__":
    asyncio.run(main())

