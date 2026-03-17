#!/usr/bin/env python3
# main.py - SHFE AG vs HL SILVER 对冲系统入口
#
# 薄编排层: 把 DataEngine + SignalEngine + ExecutionEngine 串起来

import asyncio
import signal
import os
from datetime import datetime

import logging
# 确保日志目录在 FileHandler 初始化前存在
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
    TRADING_PAIRS, STRATEGY, API, RISK, FOREX, NOTIFY,
    load_api_keys, load_notify_config, validate_api_keys,
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
    """SHFE AG vs HL SILVER 对冲机器人"""

    def __init__(self):
        load_api_keys()
        validate_api_keys()
        load_notify_config()

        pair = TRADING_PAIRS['SILVER']

        # 交易所
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

        # 工具模块
        self.session_mgr = SessionManager()
        self.forex_feed = ForexFeed(
            fallback_rate=FOREX.fallback_usdcny,
            update_interval=FOREX.update_interval,
        )

        # 三层引擎
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
            session_manager=self.session_mgr,
        )

        # 风控和仓管
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

        # 通知
        self.notifier = FeishuNotifier(
            webhook_url=NOTIFY.feishu_webhook_url,
            enabled=NOTIFY.enable_feishu,
        )

        # 执行引擎
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
        """初始化: 连接交易所, 启动数据源"""
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data", exist_ok=True)

        logger.info("正在连接 CTP 网关...")
        await self.ctp_gateway.connect()

        logger.info("正在缓存 HL asset_id...")
        await self.hl_client._ensure_asset_ids()

        # 加载风控状态
        self.risk_manager.load_state()

        # 恢复仓位状态 (崩溃恢复)
        saved_pos = self.risk_manager.get_position_state('SILVER')
        if saved_pos:
            direction = saved_pos.get("direction", "NONE")
            lots = saved_pos.get("lots", 0)
            if direction != "NONE" and lots > 0:
                self.signal_engine.set_position(direction)
                self.position_manager.current_lots = lots
                logger.warning(
                    f"恢复上次仓位: {direction} {lots}手 "
                    f"(请确认与实际持仓一致!)"
                )

        # 注册价格回调
        self.data_engine.on_price(self._on_price)

        # 启动数据引擎
        await self.data_engine.start()

        # 发送启动通知
        await self.notifier.notify_startup(
            pairs=['SILVER'],
            capital=TRADING_PAIRS['SILVER'].capital,
        )

        logger.info("SilverHedgeBot 初始化完成")

    async def _on_price(self, price: NormalizedPrice):
        """DataEngine 价格回调 → 信号 → 执行"""
        result = self.signal_engine.update(price)
        if not result or result.signal == Signal.HOLD:
            return

        await self._process_signal(result)

    async def _process_signal(self, signal: SignalResult):
        """处理交易信号"""
        logger.info(
            f"信号: {signal.signal.value} | "
            f"AG=¥{signal.ag_price:.1f} HL=${signal.hl_price_usd_oz:.4f} | "
            f"spread={signal.spread_pct:.3f}% zscore={signal.zscore:.2f}"
        )

        # 风控检查
        can_trade, reason = self.risk_manager.can_trade('SILVER')
        if not can_trade:
            logger.warning(f"交易被阻止: {reason}")
            return

        # 价差安全检查
        safe, reason = self.risk_manager.check_spread_safety(
            signal.ag_price, signal.hl_price_cny_kg
        )
        if not safe:
            await self.notifier.notify_emergency(reason)
            return

        # 计算手数
        is_entry = signal.signal in (Signal.LONG, Signal.SHORT)
        if is_entry:
            lots = self.position_manager.calculate_order_lots(
                ag_price_cny_kg=signal.ag_price,
                hl_price_usd_oz=signal.hl_price_usd_oz,
                usdcny=signal.usdcny,
            )
        else:
            # 平仓: 平全部持仓
            lots = self.position_manager.current_lots

        if lots <= 0:
            logger.warning("计算手数为0, 跳过")
            return

        # 记录交易开始
        self.risk_manager.record_trade_start(
            pair_name='SILVER',
            signal=signal.signal.value,
            xyz_price=signal.ag_price,
            lighter_price=signal.hl_price_usd_oz,
            spread=signal.spread_pct,
            zscore=signal.zscore,
        )

        # 推送交易信号
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

        # 执行交易
        try:
            result = await self.execution_engine.execute(signal, lots)

            if result.status == "filled":
                # 更新仓位
                if is_entry:
                    self.position_manager.on_open(lots)
                else:
                    self.position_manager.on_close(lots)
                    self.signal_engine.reset_funding()

                # 持久化仓位状态 (供崩溃恢复)
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

            # 冷却期
            self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)

        except Exception as e:
            logger.error(f"交易执行异常: {e}")
            self.risk_manager.record_trade_result('SILVER', 0, "failed")
            if NOTIFY.notify_on_error:
                await self.notifier.notify_error(str(e), 'SILVER')

    async def run(self):
        """启动后台任务"""
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
        """每小时获取 HL funding rate"""
        while self._running:
            try:
                rate = await self.hl_client.get_funding_rate("SILVER")
                self.signal_engine.update_funding_rate(rate)
                logger.info(f"HL SILVER funding rate: {rate:.6f}")
            except Exception as e:
                logger.warning(f"获取 funding rate 失败: {e}")
            await asyncio.sleep(3600)

    async def _margin_monitor_loop(self):
        """定期查询 CTP + HL 账户保证金, 触发预警通知"""
        interval = RISK.margin_check_interval  # 默认30秒
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
        """查询 CTP 资金账户并更新保证金状态"""
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

        # 等级升高(恶化)时发飞书通知
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
        """查询 HL 账户并更新保证金状态"""
        state = await self.hl_client.get_user_state()
        if not state:
            return

        # Hyperliquid clearinghouseState 返回格式:
        # {"marginSummary": {"accountValue": "...", "totalMarginUsed": "...", "withdrawable": "..."}}
        margin_summary = state.get("marginSummary", {})

        account_value = float(margin_summary.get("accountValue", 0))
        total_margin_used = float(margin_summary.get("totalMarginUsed", 0))
        withdrawable = float(margin_summary.get("withdrawable", 0))

        # 浮盈浮亏 = accountValue - totalRawUsd (近似)
        total_raw_usd = float(margin_summary.get("totalRawUsd", account_value))
        unrealized_pnl = account_value - total_raw_usd

        old_level = self.position_manager.hl_level
        new_level = self.position_manager.update_hl_margin(
            account_value=account_value,
            available=withdrawable,
            used_margin=total_margin_used,
            unrealized_pnl=unrealized_pnl,
        )

        # 等级升高(恶化)时发飞书通知
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
        """每分钟保存状态"""
        while self._running:
            try:
                self.risk_manager.save_state()
            except Exception as e:
                logger.error(f"保存状态失败: {e}")
            await asyncio.sleep(60)

    async def _daily_summary_loop(self):
        """每日汇总"""
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
                            f"{self.position_manager.current_lots}手"
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
        """定期检查系统健康状态, 异常时发飞书告警"""
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
                issues.append("信号引擎数据不足, 等待更多 tick")

            # 紧急停止
            if self.risk_manager.is_emergency:
                issues.append("紧急停止已激活!")

            if issues:
                msg = "健康检查异常:\n" + "\n".join(f"- {i}" for i in issues)
                logger.warning(msg)
                if any(kw in msg for kw in ("断开", "紧急")):
                    await self.notifier.notify_emergency(msg)

            await asyncio.sleep(60)

    async def shutdown(self):
        """优雅关闭"""
        logger.info("正在关闭...")
        self._running = False

        # 保存仓位状态
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
