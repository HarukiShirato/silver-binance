#!/usr/bin/env python3
# arbitrage_bot.py - 套利机器人主程序

import asyncio
import signal
import sys
import os
from typing import Dict, Optional
from datetime import datetime, time as dt_time

import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/arbitrage.log"),
    ]
)
logger = logging.getLogger("ArbitrageBot")

# 导入模块
from config import TRADING_PAIRS, STRATEGY, API, RISK, NOTIFY, load_api_keys, load_notify_config, TradingPair
from exchanges import HyperliquidClient, LighterClient
from strategy import SpreadCalculator, SpreadSignal
from risk_manager import RiskManager, PositionManager
from notifier import FeishuNotifier


class ArbitrageBot:
    """XYZ-Lighter 套利机器人"""

    def __init__(self):
        # 加载配置
        load_api_keys()
        load_notify_config()

        # 交易所客户端
        self.xyz_client: Optional[HyperliquidClient] = None
        self.lighter_client: Optional[LighterClient] = None

        # 策略计算器 (每个交易对一个)
        self.spread_calculators: Dict[str, SpreadCalculator] = {}

        # 仓位管理器 (每个交易对一个)
        self.position_managers: Dict[str, PositionManager] = {}

        # 风控
        self.risk_manager = RiskManager(
            max_daily_trades=RISK.max_daily_trades,
            max_daily_loss=RISK.max_daily_loss,
            leg_timeout_ms=RISK.leg_timeout_ms,
            leg_retry_times=RISK.leg_retry_times,
            emergency_spread_pct=RISK.emergency_spread_pct,
        )

        # 飞书通知
        self.notifier = FeishuNotifier(
            webhook_url=NOTIFY.feishu_trade_webhook_url,
            enabled=NOTIFY.enable_feishu,
        )

        # 运行状态
        self._running = False
        self._last_prices: Dict[str, Dict[str, float]] = {}
        self._daily_summary_sent = False
        self._total_fees_today = 0.0

    async def initialize(self):
        """初始化"""
        logger.info("Initializing ArbitrageBot...")

        # 创建日志目录
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data", exist_ok=True)

        # 初始化交易所客户端
        self.xyz_client = HyperliquidClient(
            api_url=API.hl_api_url,
            ws_url=API.hl_ws_url,
            private_key=API.hl_api_wallet_private_key,
            wallet_address=API.hl_wallet_address,
        )

        self.lighter_client = LighterClient(
            api_url=API.lighter_api_url,
            ws_url=API.lighter_ws_url,
            private_key=API.lighter_private_key,
            api_key=API.lighter_api_key,
        )

        # 初始化每个交易对的策略和仓位管理
        for name, pair in TRADING_PAIRS.items():
            self.spread_calculators[name] = SpreadCalculator(
                pair_name=name,
                window_size=STRATEGY.spread_window,
                entry_zscore=STRATEGY.entry_zscore,
                exit_zscore=STRATEGY.exit_zscore,
                stop_loss_zscore=STRATEGY.stop_loss_zscore,
            )

            self.position_managers[name] = PositionManager(
                capital=pair.capital,
                leverage=pair.leverage,
                max_position_pct=STRATEGY.max_position_pct,
                split_orders=STRATEGY.split_orders,
                max_single_order=STRATEGY.max_single_order,
                fixed_size=pair.fixed_size,
                max_book_pct=STRATEGY.max_book_pct,
                max_oi_pct=STRATEGY.max_oi_pct,
            )

        # 加载风控状态
        self.risk_manager.load_state()

        # 预热历史数据
        await self._warmup()

        # 发送启动通知
        await self.notifier.notify_startup(
            pairs=list(TRADING_PAIRS.keys()),
            capital=sum(p.capital for p in TRADING_PAIRS.values()),
        )

        logger.info("ArbitrageBot initialized")

    async def _warmup(self):
        """使用历史数据预热"""
        logger.info("Warming up with historical data...")

        for name, pair in TRADING_PAIRS.items():
            try:
                # 获取XYZ K线
                xyz_candles = await self.xyz_client.get_candles(
                    coin=pair.xyz_symbol,
                    interval="1h",
                )

                # 获取Lighter K线
                lighter_candles = await self.lighter_client.get_candles(
                    market_id=pair.lighter_market_id,
                    resolution="1h",
                )

                # 预热
                await self.spread_calculators[name].warmup_from_candles(
                    xyz_candles=xyz_candles,
                    lighter_candles=lighter_candles,
                )

                logger.info(f"Warmed up {name}")

            except Exception as e:
                logger.error(f"Failed to warmup {name}: {e}")

    def _calculate_executable_size(
        self,
        orderbook_levels: list,
        max_slippage_pct: float,
        is_buy: bool,
    ) -> tuple[float, float, float]:
        """
        计算在给定滑点限制下能执行的最大数量

        Args:
            orderbook_levels: [(price, size), ...] 订单簿档位
            max_slippage_pct: 最大允许滑点 (如 0.1 = 0.1%)
            is_buy: True=吃卖单, False=吃买单

        Returns:
            (max_size, avg_price, actual_slippage_pct)
        """
        if not orderbook_levels:
            return 0, 0, 0

        best_price = orderbook_levels[0][0]
        if best_price <= 0:
            return 0, 0, 0

        # 滑点限制价格
        if is_buy:
            limit_price = best_price * (1 + max_slippage_pct / 100)
        else:
            limit_price = best_price * (1 - max_slippage_pct / 100)

        # 累计可执行数量和成本
        total_size = 0
        total_cost = 0

        for price, size in orderbook_levels:
            if is_buy and price > limit_price:
                break
            if not is_buy and price < limit_price:
                break

            total_size += size
            total_cost += price * size

        if total_size == 0:
            return 0, best_price, 0

        avg_price = total_cost / total_size
        actual_slippage = abs(avg_price - best_price) / best_price * 100

        return total_size, avg_price, actual_slippage

    async def _get_executable_depth(
        self,
        pair: TradingPair,
        is_buy_xyz: bool,
    ) -> tuple[float, float, dict]:
        """
        获取两个交易所在滑点限制内的可执行深度

        Args:
            pair: 交易对
            is_buy_xyz: XYZ方向 (True=买XYZ卖Lighter)

        Returns:
            (max_executable_size, open_interest, depth_info)
        """
        try:
            # 获取 XYZ 订单簿 (10档)
            xyz_book = await self.xyz_client.get_orderbook(pair.xyz_symbol, depth=10)
            xyz_asks_raw = xyz_book.get('levels', [[]])[0] if xyz_book.get('levels') else []
            xyz_bids_raw = xyz_book.get('levels', [[]])[1] if xyz_book.get('levels') else []

            # 转换格式 [(price, size), ...]
            xyz_asks = [(float(l.get('px', 0)), float(l.get('sz', 0))) for l in xyz_asks_raw[:10]]
            xyz_bids = [(float(l.get('px', 0)), float(l.get('sz', 0))) for l in xyz_bids_raw[:10]]

            # 获取 Lighter 订单簿 (10档)
            lighter_book = await self.lighter_client.get_orderbook_orders(pair.lighter_market_id, depth=10)
            lighter_asks_raw = lighter_book.get('asks', [])[:10]
            lighter_bids_raw = lighter_book.get('bids', [])[:10]

            lighter_asks = [(float(l.get('price', 0)), float(l.get('size', 0))) for l in lighter_asks_raw]
            lighter_bids = [(float(l.get('price', 0)), float(l.get('size', 0))) for l in lighter_bids_raw]

            max_slippage = STRATEGY.max_slippage_pct

            if is_buy_xyz:
                # 买XYZ (吃卖单), 卖Lighter (吃买单)
                xyz_size, xyz_avg, xyz_slip = self._calculate_executable_size(xyz_asks, max_slippage, True)
                lighter_size, lighter_avg, lighter_slip = self._calculate_executable_size(lighter_bids, max_slippage, False)
            else:
                # 卖XYZ (吃买单), 买Lighter (吃卖单)
                xyz_size, xyz_avg, xyz_slip = self._calculate_executable_size(xyz_bids, max_slippage, False)
                lighter_size, lighter_avg, lighter_slip = self._calculate_executable_size(lighter_asks, max_slippage, True)

            # 取两边较小的可执行量
            max_executable = min(xyz_size, lighter_size) if xyz_size > 0 and lighter_size > 0 else 0

            # 获取 Lighter OI
            lighter_details = await self.lighter_client.get_orderbook_details(pair.lighter_market_id)
            open_interest = float(lighter_details.get('open_interest', 0))

            depth_info = {
                'xyz_size': xyz_size,
                'xyz_avg_price': xyz_avg,
                'xyz_slippage': xyz_slip,
                'lighter_size': lighter_size,
                'lighter_avg_price': lighter_avg,
                'lighter_slippage': lighter_slip,
            }

            logger.info(
                f"Executable depth {pair.name}: "
                f"XYZ={xyz_size:.2f}@{xyz_avg:.4f}({xyz_slip:.3f}%), "
                f"Lighter={lighter_size:.2f}@{lighter_avg:.4f}({lighter_slip:.3f}%), "
                f"max={max_executable:.2f}, OI=${open_interest:,.0f}"
            )

            return max_executable, open_interest, depth_info

        except Exception as e:
            logger.warning(f"Failed to get executable depth: {e}")
            return 0, 0, {}

    async def run(self):
        """运行主循环"""
        self._running = True
        logger.info("Starting main loop...")

        # 启动价格更新任务
        price_task = asyncio.create_task(self._price_loop())

        # 启动信号检查任务
        signal_task = asyncio.create_task(self._signal_loop())

        # 启动状态保存任务
        save_task = asyncio.create_task(self._save_state_loop())

        # 启动每日汇总任务
        daily_task = asyncio.create_task(self._daily_summary_loop())

        try:
            await asyncio.gather(price_task, signal_task, save_task, daily_task)
        except asyncio.CancelledError:
            logger.info("Tasks cancelled")

    async def _price_loop(self):
        """价格更新循环"""
        while self._running:
            try:
                for name, pair in TRADING_PAIRS.items():
                    # 获取XYZ价格
                    mids = await self.xyz_client.get_all_mids()
                    xyz_price = mids.get(pair.xyz_symbol, 0)

                    # 获取Lighter价格
                    ticker = await self.lighter_client.get_ticker(pair.lighter_market_id)
                    lighter_price = ticker.get("price", 0)

                    # 保存价格
                    self._last_prices[name] = {
                        "xyz": xyz_price,
                        "lighter": lighter_price,
                        "timestamp": datetime.now().timestamp(),
                    }

                # 每秒更新一次
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Price loop error: {e}")
                await asyncio.sleep(5)

    async def _signal_loop(self):
        """信号检查循环"""
        while self._running:
            try:
                for name, pair in TRADING_PAIRS.items():
                    prices = self._last_prices.get(name)
                    if not prices:
                        continue

                    xyz_price = prices["xyz"]
                    lighter_price = prices["lighter"]

                    # 更新价差计算器
                    signal = self.spread_calculators[name].update_prices(
                        xyz_price=xyz_price,
                        lighter_price=lighter_price,
                    )

                    if signal and signal.signal != "HOLD":
                        await self._process_signal(name, pair, signal)

                # 每秒检查一次
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Signal loop error: {e}")
                await asyncio.sleep(5)

    async def _process_signal(self, name: str, pair: TradingPair, signal: SpreadSignal):
        """处理交易信号"""
        logger.info(f"Signal: {name} {signal.signal} zscore={signal.zscore:.2f}")

        # 风控检查
        can_trade, reason = self.risk_manager.can_trade(name)
        if not can_trade:
            logger.warning(f"Trade blocked: {reason}")
            return

        # 价差安全检查
        safe, reason = self.risk_manager.check_spread_safety(
            signal.xyz_price, signal.lighter_price
        )
        if not safe:
            await self.notifier.notify_emergency(reason)
            return

        # 确定交易方向
        is_buy_xyz = signal.signal in ["LONG", "EXIT_SHORT"]

        # 获取滑点限制内的可执行深度
        executable_size, open_interest, depth_info = await self._get_executable_depth(pair, is_buy_xyz)

        # 计算订单大小 (考虑流动性约束)
        pm = self.position_managers[name]
        sizes = pm.calculate_order_size(
            price=signal.xyz_price,
            min_size=pair.min_order_size,
            size_decimals=pair.size_decimals,
            book_depth_size=executable_size,  # 使用可执行深度而非总深度
            open_interest=open_interest,
        )

        # 记录交易开始
        self.risk_manager.record_trade_start(
            pair_name=name,
            signal=signal.signal,
            xyz_price=signal.xyz_price,
            lighter_price=signal.lighter_price,
            spread=signal.spread,
            zscore=signal.zscore,
        )

        # 执行交易
        try:
            total_size = sum(sizes)

            # 先推送交易信号
            if NOTIFY.notify_on_trade:
                await self.notifier.notify_trade(
                    pair_name=name,
                    signal=signal.signal,
                    xyz_price=signal.xyz_price,
                    lighter_price=signal.lighter_price,
                    zscore=signal.zscore,
                    size=total_size,
                    spread=signal.spread,
                )

            # 执行交易 (使用 taker, 传入预计算的深度信息)
            pnl, fees, xyz_fill, lighter_fill = await self._execute_trade(
                name, pair, signal, total_size, depth_info
            )

            # 记录结果
            self.risk_manager.record_trade_result(name, pnl, "filled")
            self._total_fees_today += fees

            # 推送成交结果
            if NOTIFY.notify_on_trade:
                await self.notifier.notify_trade_result(
                    pair_name=name,
                    signal=signal.signal,
                    xyz_fill_price=xyz_fill,
                    lighter_fill_price=lighter_fill,
                    size=total_size,
                    fees=fees,
                    status="filled",
                )

            # 设置冷却期
            self.risk_manager.set_cooldown(STRATEGY.cooldown_seconds)

        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            self.risk_manager.record_trade_result(name, 0, "failed")

            if NOTIFY.notify_on_error:
                await self.notifier.notify_error(str(e), name)

    async def _execute_trade(
        self,
        name: str,
        pair: TradingPair,
        signal: SpreadSignal,
        size: float,
        depth_info: dict = None,
    ) -> tuple[float, float, float, float]:
        """
        执行套利交易 - 使用 TAKER 订单

        Args:
            depth_info: 预计算的深度信息，包含预估成交价

        Returns:
            (pnl, fees, xyz_fill_price, lighter_fill_price)
        """
        logger.info(f"Executing {signal.signal} for {name}, size={size} [TAKER]")

        # 确定方向
        # LONG spread = 买XYZ, 卖Lighter (价差偏低时)
        # SHORT spread = 卖XYZ, 买Lighter (价差偏高时)
        # EXIT_LONG = 卖XYZ, 买Lighter
        # EXIT_SHORT = 买XYZ, 卖Lighter

        xyz_is_buy = signal.signal in ["LONG", "EXIT_SHORT"]
        lighter_is_buy = signal.signal in ["SHORT", "EXIT_LONG"]

        # 使用预计算的深度信息或重新获取
        if depth_info and depth_info.get('xyz_avg_price') and depth_info.get('lighter_avg_price'):
            # 使用预计算的加权平均价 + 额外滑点保护
            extra_slippage = 1.0005  # 0.05% 额外保护
            if xyz_is_buy:
                xyz_price = depth_info['xyz_avg_price'] * extra_slippage
            else:
                xyz_price = depth_info['xyz_avg_price'] / extra_slippage

            if lighter_is_buy:
                lighter_price = depth_info['lighter_avg_price'] * extra_slippage
            else:
                lighter_price = depth_info['lighter_avg_price'] / extra_slippage

            logger.info(
                f"Using pre-calculated prices: XYZ={xyz_price:.4f}, Lighter={lighter_price:.4f}"
            )
        else:
            # 回退: 获取当前订单簿计算taker价格
            xyz_book = await self.xyz_client.get_orderbook(pair.xyz_symbol)
            lighter_book = await self.lighter_client.get_orderbook_orders(pair.lighter_market_id)

            slippage = STRATEGY.max_slippage_pct / 100

            if xyz_is_buy:
                xyz_ask = float(xyz_book['levels'][0][0]['px']) if xyz_book.get('levels') else signal.xyz_price
                xyz_price = xyz_ask * (1 + slippage)
            else:
                xyz_bid = float(xyz_book['levels'][1][0]['px']) if xyz_book.get('levels') else signal.xyz_price
                xyz_price = xyz_bid * (1 - slippage)

            if lighter_is_buy:
                lighter_asks = lighter_book.get('asks', [])
                lighter_ask = float(lighter_asks[0]['price']) if lighter_asks else signal.lighter_price
                lighter_price = lighter_ask * (1 + slippage)
            else:
                lighter_bids = lighter_book.get('bids', [])
                lighter_bid = float(lighter_bids[0]['price']) if lighter_bids else signal.lighter_price
                lighter_price = lighter_bid * (1 - slippage)

        # 并发执行两腿 (taker单 = 立即成交的限价单)
        xyz_task = self.xyz_client.place_order(
            coin=pair.xyz_symbol,
            is_buy=xyz_is_buy,
            size=size,
            price=round(xyz_price, pair.price_decimals),
            order_type="Limit",  # 用激进价格的限价单实现taker
            reduce_only=signal.signal.startswith("EXIT"),
        )

        lighter_task = self.lighter_client.place_order(
            market_id=pair.lighter_market_id,
            is_buy=lighter_is_buy,
            size=size,
            price=round(lighter_price, pair.price_decimals),
            order_type="limit",
            reduce_only=signal.signal.startswith("EXIT"),
        )

        # 设置超时
        try:
            xyz_result, lighter_result = await asyncio.wait_for(
                asyncio.gather(xyz_task, lighter_task),
                timeout=RISK.leg_timeout_ms / 1000,
            )
        except asyncio.TimeoutError:
            logger.error("Trade timeout!")
            # TODO: 处理单腿成交的情况
            raise

        logger.info(f"XYZ result: {xyz_result}")
        logger.info(f"Lighter result: {lighter_result}")

        # 计算手续费
        # XYZ taker fee: 0.0082%
        # Lighter fee: 0%
        xyz_fee = size * xyz_price * 0.000082
        lighter_fee = 0
        total_fees = xyz_fee + lighter_fee

        # 计算PnL
        # 入场交易: PnL = -手续费
        # 出场交易: PnL = 价差变化 * 仓位 - 手续费
        if signal.signal in ["LONG", "SHORT"]:
            pnl = -total_fees
        else:
            # 简化: 实际需要从持仓记录获取入场价格
            pnl = -total_fees

        return pnl, total_fees, xyz_price, lighter_price

    async def _daily_summary_loop(self):
        """每日汇总循环"""
        while self._running:
            try:
                now = datetime.now()
                target_hour = NOTIFY.daily_summary_hour

                # 检查是否到了汇总时间
                if now.hour == target_hour and not self._daily_summary_sent:
                    await self._send_daily_summary()
                    self._daily_summary_sent = True

                # 新的一天重置标志
                if now.hour == 0 and self._daily_summary_sent:
                    self._daily_summary_sent = False
                    self._total_fees_today = 0.0

                await asyncio.sleep(60)  # 每分钟检查一次

            except Exception as e:
                logger.error(f"Daily summary error: {e}")
                await asyncio.sleep(60)

    async def _send_daily_summary(self):
        """发送每日汇总"""
        if not NOTIFY.notify_daily_summary:
            return

        stats = self.risk_manager.daily_stats

        # 获取当前持仓
        positions = {}
        for name, calc in self.spread_calculators.items():
            pos = calc.current_position
            if pos != "NONE":
                positions[name] = pos

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

        logger.info(f"Daily summary sent: PnL=${stats.total_pnl:.2f}")

    async def _save_state_loop(self):
        """定期保存状态"""
        while self._running:
            try:
                self.risk_manager.save_state()
                await asyncio.sleep(60)  # 每分钟保存一次
            except Exception as e:
                logger.error(f"Save state error: {e}")
                await asyncio.sleep(60)

    async def shutdown(self):
        """关闭"""
        logger.info("Shutting down...")
        self._running = False

        # 保存状态
        self.risk_manager.save_state()

        # 关闭连接
        if self.xyz_client:
            await self.xyz_client.close()
        if self.lighter_client:
            await self.lighter_client.close()
        await self.notifier.close()

        logger.info("Shutdown complete")


async def main():
    """主函数"""
    bot = ArbitrageBot()

    # 信号处理
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
