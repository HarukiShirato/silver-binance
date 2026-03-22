#!/usr/bin/env python3
# main.py - SHFE AG vs HL SILVER 瀵瑰啿绯荤粺鍏ュ彛
#
# 钖勭紪鎺掑眰: 鎶?DataEngine + SignalEngine + ExecutionEngine 涓茶捣鏉?

import asyncio
import signal
import os
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
    load_api_keys, load_notify_config, validate_api_keys, load_runtime_config,
)
from exchanges import CTPGateway, HyperliquidClient
from forex_feed import ForexFeed
from session_manager import SessionManager
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

    async def initialize(self):
        """Initialize exchanges and data sources."""
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data", exist_ok=True)

        logger.info("正在连接 CTP 网关...")
        await self.ctp_gateway.connect()

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
                    f"鎭㈠涓婃浠撲綅: {direction} {lots}鎵?"
                    f"(璇风‘璁や笌瀹為檯鎸佷粨涓€鑷?)"
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

        logger.info("SilverHedgeBot initialized")

    async def _on_price(self, price: NormalizedPrice):
        """DataEngine callback: price -> signal -> execution."""
        result = self.signal_engine.update(price)
        if not result or result.signal == Signal.HOLD:
            return

        await self._process_signal(result)

    async def _process_signal(self, signal: SignalResult):
        """Handle a trading signal."""
        logger.info(
            f"淇″彿: {signal.signal.value} | "
            f"AG=楼{signal.ag_price:.1f} HL=${signal.hl_price_usd_oz:.4f} | "
            f"spread={signal.spread_pct:.3f}% zscore={signal.zscore:.2f}"
        )

        # 椋庢帶妫€鏌?
        can_trade, reason = self.risk_manager.can_trade('SILVER')
        if not can_trade:
            logger.warning(f"交易被阻止: {reason}")
            return

        # 浠峰樊瀹夊叏妫€鏌?
        safe, reason = self.risk_manager.check_spread_safety(
            signal.ag_price, signal.hl_price_cny_kg
        )
        if not safe:
            await self.notifier.notify_emergency(reason)
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
            self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)
            logger.info(
                f"DRY_RUN 已拦截下单: {signal.signal.value} lots={lots}"
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
                if NOTIFY.notify_on_error:
                    await self.notifier.notify_error(result.error, 'SILVER')

            # 鍐峰嵈鏈?
            self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)

        except Exception as e:
            logger.error(f"交易执行异常: {e}")
            self.risk_manager.record_trade_result('SILVER', 0, "failed")
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

            # CTP 杩炴帴
            if not self.ctp_gateway.is_connected:
                issues.append("CTP 缃戝叧鏂紑")

            # HL/AG 鏁版嵁鏂伴矞搴?
            latest = self.data_engine.latest
            if latest:
                if latest.hl_stale:
                    hl_age = self.data_engine.hl_last_update_age
                    issues.append(f"HL 浠锋牸杩囨湡 ({hl_age:.0f}s)")
                if latest.ag_stale:
                    issues.append("AG 琛屾儏杩囨湡")

            # 姹囩巼
            if self.forex_feed.is_stale:
                issues.append(f"姹囩巼杩囨湡 (褰撳墠 {self.forex_feed.usdcny:.4f})")

            # 淇″彿寮曟搸鏁版嵁灏辩华
            if not self.signal_engine.data_ready:
                issues.append("淇″彿寮曟搸鏁版嵁涓嶈冻, 绛夊緟鏇村 tick")

            # 绱ф€ュ仠姝?
            if self.risk_manager.is_emergency:
                issues.append("绱ф€ュ仠姝㈠凡婵€娲?")

            if issues:
                msg = "鍋ュ悍妫€鏌ュ紓甯?\n" + "\n".join(f"- {i}" for i in issues)
                logger.warning(msg)
                if any(kw in msg for kw in ("disconnect", "emergency")):
                    await self.notifier.notify_emergency(msg)

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

