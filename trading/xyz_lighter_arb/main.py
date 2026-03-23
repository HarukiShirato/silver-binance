#!/usr/bin/env python3
# main.py - SHFE AG vs HL SILVER 瀵瑰啿绯荤粺鍏ュ彛
#
# 钖勭紪鎺掑眰: 鎶?DataEngine + SignalEngine + ExecutionEngine 涓茶捣鏉?

import asyncio
import signal
import os
import csv
from collections import Counter
from datetime import datetime

import logging
# 纭繚鏃ュ織鐩綍鍦?FileHandler 鍒濆鍖栧墠瀛樺湪
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/hedge.log"),
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
from risk_manager import RiskManager
from position_manager import PositionManager, MarginLevel
from notifier import FeishuNotifier


class SilverHedgeBot:
    """SHFE AG vs HL SILVER hedge bot."""

    def __init__(self):
        load_runtime_config()
        load_api_keys()
        validate_api_keys(dry_run=RUNTIME.dry_run)
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
            private_key=API.hl_private_key,
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
            webhook_url=NOTIFY.feishu_webhook_url,
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

        self._running = False
        self._total_fees_today = 0.0
        self._last_session_type = None
        self._decision_stats = Counter()
        self._trade_log_file = os.path.join("data", "trade_events.csv")
        self._window_full_notified = False

    async def initialize(self):
        """Initialize exchanges and data sources."""
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data", exist_ok=True)
        self._ensure_trade_log_file()

        logger.info("正在连接 CTP 网关...")
        await self._connect_ctp_with_fallback()

        if RUNTIME.dry_run:
            logger.info("当前模式: DRY_RUN=true（只跑信号与风控，不发真实下单）")

        logger.info("正在缓存 HL asset_id...")
        await self.hl_client._ensure_asset_ids()

        # 鍔犺浇椋庢帶鐘舵€?
        self.risk_manager.load_state()

        # 鎭㈠浠撲綅鐘舵€?(宕╂簝鎭㈠)
        saved_pos = self.risk_manager.get_position_state('SILVER')
        if saved_pos:
            direction = saved_pos.get("direction", "NONE")
            lots = saved_pos.get("lots", 0)
            if direction != "NONE" and lots > 0:
                self.signal_engine.set_position(direction)
                self.position_manager.current_lots = lots
                logger.warning(
                    f"恢复上次持仓: {direction} {lots} 手"
                    f"(请确认与实际持仓一致)"
                )

        # 娉ㄥ唽浠锋牸鍥炶皟
        self.data_engine.on_price(self._on_price)

        # 鍚姩鏁版嵁寮曟搸
        await self.data_engine.start()

        # 鍙戦€佸惎鍔ㄩ€氱煡
        await self.notifier.notify_startup(
            pairs=['SILVER'],
            capital=TRADING_PAIRS['SILVER'].capital,
        )

        self._last_session_type = self.session_mgr.get_session_type()
        if self._last_session_type != SessionType.CLOSED:
            await self._notify_session_transition("START", self._last_session_type)

        logger.info("SilverHedgeBot initialized")

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
        """Handle a trading signal."""
        logger.info(
            f"信号: {signal.signal.value} | "
            f"AG={signal.ag_price:.1f} CNY/kg HL=${signal.hl_price_usd_oz:.4f} | "
            f"spread={signal.spread_pct:.3f}% zscore={signal.zscore:.2f}"
        )

        # 椋庢帶妫€鏌?
        can_trade, reason = self.risk_manager.can_trade('SILVER')
        if not can_trade:
            logger.warning(f"交易被阻止: {reason}")
            self._decision_stats[f"blocked_risk_{reason}"] += 1
            return

        # 浠峰樊瀹夊叏妫€鏌?
        safe, reason = self.risk_manager.check_spread_safety(
            signal.ag_price, signal.hl_price_cny_kg
        )
        if not safe:
            await self.notifier.notify_emergency(reason)
            self._decision_stats[f"blocked_spread_{reason}"] += 1
            return

        # 璁＄畻鎵嬫暟
        is_entry = signal.signal in (Signal.LONG, Signal.SHORT)
        if is_entry:
            lots = self.position_manager.calculate_order_lots(
                ag_price_cny_kg=signal.ag_price,
                hl_price_usd_oz=signal.hl_price_usd_oz,
                usdcny=signal.usdcny,
            )
        else:
            # 骞充粨: 骞冲叏閮ㄦ寔浠?
            lots = self.position_manager.current_lots

        if lots <= 0:
            logger.warning("计算手数为 0，跳过")
            self._decision_stats["blocked_lots_zero"] += 1
            return

        # 璁板綍浜ゆ槗寮€濮?
        self.risk_manager.record_trade_start(
            pair_name='SILVER',
            signal=signal.signal.value,
            xyz_price=signal.ag_price,
            lighter_price=signal.hl_price_usd_oz,
            spread=signal.spread_pct,
            zscore=signal.zscore,
        )

        # 鎺ㄩ€佷氦鏄撲俊鍙?
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

        # 鎵ц浜ゆ槗
        if RUNTIME.dry_run:
            if is_entry:
                self.position_manager.on_open(lots)
            else:
                self.position_manager.on_close(lots)
                self.signal_engine.reset_funding()

            self.risk_manager.save_position_state(
                pair_name='SILVER',
                direction=self.signal_engine.position,
                lots=self.position_manager.current_lots,
            )
            self.risk_manager.record_trade_result('SILVER', 0, "filled")
            if NOTIFY.notify_on_trade:
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
            self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)
            self._append_trade_event(
                signal=signal.signal.value,
                lots=lots,
                status="filled",
                fee_rmb=0.0,
                reason="dry_run_fill",
                signal_data=signal,
            )
            logger.info(
                "DRY_RUN 成交模拟: "
                f"signal={signal.signal.value} lots={lots} "
                f"z={signal.zscore:.2f} spread={signal.spread_pct:.3f}% "
                f"AG={signal.ag_price:.1f} HL=${signal.hl_price_usd_oz:.4f} "
                f"HL_CNY={signal.hl_price_cny_kg:.1f} usdcny={signal.usdcny:.4f} "
                f"position_after={self.signal_engine.position}"
            )
            return

        try:
            result = await self.execution_engine.execute(signal, lots)

            if result.status == "filled":
                # 鏇存柊浠撲綅
                if is_entry:
                    self.position_manager.on_open(lots)
                else:
                    self.position_manager.on_close(lots)
                    self.signal_engine.reset_funding()

                # 鎸佷箙鍖栦粨浣嶇姸鎬?(渚涘穿婧冩仮澶?
                self.risk_manager.save_position_state(
                    pair_name='SILVER',
                    direction=self.signal_engine.position,
                    lots=self.position_manager.current_lots,
                )

                self.risk_manager.record_trade_result('SILVER', -result.total_fee_rmb, "filled")
                self._total_fees_today += result.total_fee_rmb
                self._append_trade_event(
                    signal=signal.signal.value,
                    lots=lots,
                    status="filled",
                    fee_rmb=result.total_fee_rmb,
                    reason="live_fill",
                    signal_data=signal,
                )

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
                    signal=signal.signal.value,
                    lots=lots,
                    status=result.status,
                    fee_rmb=0.0,
                    reason=result.error or "live_execute_not_filled",
                    signal_data=signal,
                )
                if NOTIFY.notify_on_error:
                    await self.notifier.notify_error(result.error, 'SILVER')

            # 鍐峰嵈鏈?
            self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)

        except Exception as e:
            logger.error(f"交易执行异常: {e}")
            self.risk_manager.record_trade_result('SILVER', 0, "failed")
            self._decision_stats["exec_exception"] += 1
            self._append_trade_event(
                signal=signal.signal.value,
                lots=lots,
                status="failed",
                fee_rmb=0.0,
                reason=str(e),
                signal_data=signal,
            )
            if NOTIFY.notify_on_error:
                await self.notifier.notify_error(str(e), 'SILVER')

    async def run(self):
        """Start background tasks."""
        self._running = True

        tasks = [
            asyncio.create_task(self._save_state_loop()),
            asyncio.create_task(self._daily_summary_loop()),
            asyncio.create_task(self._funding_rate_loop()),
            asyncio.create_task(self._margin_monitor_loop()),
            asyncio.create_task(self._health_monitor_loop()),
            asyncio.create_task(self._session_transition_loop()),
            asyncio.create_task(self._status_heartbeat_loop()),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass

    async def _funding_rate_loop(self):
        """Fetch funding rate periodically."""
        while self._running:
            try:
                rate = await self.hl_client.get_funding_rate("SILVER")
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

            if not RUNTIME.dry_run:
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

        # 绛夌骇鍗囬珮(鎭跺寲)鏃跺彂椋炰功閫氱煡
        if new_level != old_level and new_level != MarginLevel.NORMAL:
            await self.notifier.notify_margin_warning(
                account_name="CTP",
                level=new_level,
                margin_ratio=self.position_manager.ctp_margin.margin_ratio,
                used_margin=acct.margin,
                balance=acct.balance,
                available=acct.available,
            )

    async def _check_hl_margin(self):
        """Query HL account and update margin state."""
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

        # 绛夌骇鍗囬珮(鎭跺寲)鏃跺彂椋炰功閫氱煡
        if new_level != old_level and new_level != MarginLevel.NORMAL:
            await self.notifier.notify_margin_warning(
                account_name="HL",
                level=new_level,
                margin_ratio=self.position_manager.hl_margin.margin_ratio,
                used_margin=total_margin_used,
                balance=account_value,
                available=withdrawable,
            )

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

            # CTP 连接
            if not self.ctp_gateway.is_connected:
                issues.append("CTP 网关断开")

            # HL/AG 数据新鲜度
            latest = self.data_engine.latest
            if latest:
                if latest.hl_stale:
                    hl_age = self.data_engine.hl_last_update_age
                    issues.append(f"HL 价格过期 ({hl_age:.0f}s)")
                if latest.ag_stale:
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
                if any(kw in msg for kw in ("disconnect", "emergency")):
                    await self.notifier.notify_emergency(msg)

            await asyncio.sleep(60)

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
                    await self._notify_session_transition("START", current)
                elif previous != SessionType.CLOSED and current == SessionType.CLOSED:
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
                "pair",
                "signal",
                "lots",
                "status",
                "fee_rmb",
                "reason",
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
    ):
        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "DRY_RUN" if RUNTIME.dry_run else "LIVE",
            "SILVER",
            signal,
            f"{lots:.4f}",
            status,
            f"{fee_rmb:.2f}",
            reason,
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
                    f"decisions_30m={stats_snapshot}"
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
        self.risk_manager.save_state()

        await self.data_engine.stop()
        await self.ctp_gateway.close()
        await self.hl_client.close()
        await self.notifier.close()

        logger.info("关闭完成")


async def main():
    bot = SilverHedgeBot()

    loop = asyncio.get_event_loop()
    def signal_handler():
        asyncio.create_task(bot.shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await bot.initialize()
        await bot.run()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

