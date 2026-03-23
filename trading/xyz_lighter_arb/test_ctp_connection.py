#!/usr/bin/env python3
"""
CTP 仿真环境连接测试脚本

国贸期货仿真环境:
  BrokerID: 0187
  行情前置: tcp://140.206.244.75:41213
  交易前置: tcp://140.206.244.75:41205

使用方法:
  # 设置环境变量
  export CTP_USER_ID="你的仿真账号"
  export CTP_PASSWORD="你的仿真密码"

  # 运行测试
  python3 test_ctp_connection.py
"""

import asyncio
import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("CTPTest")

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from exchanges.ctp_gateway import CTPGateway, TickData


async def on_tick(tick: TickData):
    """行情回调"""
    logger.info(
        f"[{tick.instrument_id}] "
        f"最新={tick.last_price:.1f} "
        f"买一={tick.bid_price1:.1f}×{tick.bid_volume1} "
        f"卖一={tick.ask_price1:.1f}×{tick.ask_volume1} "
        f"量={tick.volume} 仓={tick.open_interest:.0f} "
        f"时间={tick.update_time}"
    )


async def main():
    # 读取账号配置
    user_id = os.environ.get('CTP_USER_ID', '')
    password = os.environ.get('CTP_PASSWORD', '')
    auth_code = os.environ.get('CTP_AUTH_CODE', '')

    if not user_id or not password:
        logger.error("请设置环境变量 CTP_USER_ID, CTP_PASSWORD, CTP_AUTH_CODE")
        logger.info("  export CTP_USER_ID='55021'")
        logger.info("  export CTP_PASSWORD='your_password'")
        logger.info("  export CTP_AUTH_CODE='your_auth_code'")
        return

    front_candidates = [
        ("tcp://114.80.225.10:41213", "tcp://114.80.225.10:41205"),
        ("tcp://140.206.244.75:41213", "tcp://140.206.244.75:41205"),
        ("tcp://140.206.244.67:41213", "tcp://140.206.244.67:41205"),
        ("tcp://114.80.225.2:41213", "tcp://114.80.225.2:41205"),
    ]
    gateway = None

    try:
        # 1. 连接(自动轮询前置)
        logger.info("=" * 60)
        logger.info("步骤 1: 连接国贸期货仿真环境...")
        logger.info("=" * 60)
        connected = False
        for idx, (md_front, td_front) in enumerate(front_candidates, start=1):
            logger.info(f"尝试前置 [{idx}/{len(front_candidates)}] MD={md_front}, TD={td_front}")
            gateway = CTPGateway(
                broker_id="0187",
                user_id=user_id,
                password=password,
                md_front=md_front,
                td_front=td_front,
                app_id="client_Lavas_1.0.0",
                auth_code=auth_code,
            )
            try:
                await gateway.connect(timeout=30)
                connected = True
                logger.info(f"前置连接成功: MD={md_front}, TD={td_front}")
                break
            except Exception as e:
                logger.warning(f"前置连接失败: {e}")
                await gateway.close()
                gateway = None

        if not connected or gateway is None:
            raise ConnectionError("所有 CTP 前置均连接失败")

        logger.info(f"交易日: {gateway.trading_day}")

        # 2. 订阅行情 (白银主力 + 黄金主力)
        logger.info("=" * 60)
        logger.info("步骤 2: 订阅行情...")
        logger.info("=" * 60)
        instruments = ['ag2506', 'au2506']
        for inst in instruments:
            gateway.subscribe(inst)
            gateway.on_tick(inst, on_tick)

        # 等待接收行情
        logger.info("等待接收行情数据 (30秒)...")
        await asyncio.sleep(30)

        # 3. 查看最新行情
        logger.info("=" * 60)
        logger.info("步骤 3: 查看最新行情...")
        logger.info("=" * 60)
        for inst in instruments:
            tick = gateway.get_tick(inst)
            if tick:
                logger.info(f"{inst}: 最新价={tick.last_price}, "
                          f"买一={tick.bid_price1}, 卖一={tick.ask_price1}")
            else:
                logger.warning(f"{inst}: 未收到行情 (可能不在交易时段)")

        # 4. 查询资金
        logger.info("=" * 60)
        logger.info("步骤 4: 查询账户资金...")
        logger.info("=" * 60)
        account = await gateway.query_account()
        if account:
            logger.info(f"总资金: {account.balance:.2f}")
            logger.info(f"可用:   {account.available:.2f}")
            logger.info(f"冻结:   {account.frozen:.2f}")
            logger.info(f"保证金: {account.margin:.2f}")
            logger.info(f"盈亏:   {account.profit:.2f}")

        # 5. 查询持仓
        logger.info("=" * 60)
        logger.info("步骤 5: 查询持仓...")
        logger.info("=" * 60)
        positions = await gateway.query_positions()
        if positions:
            for pos in positions:
                if pos.volume > 0:
                    logger.info(f"{pos.instrument_id} {pos.direction}: "
                              f"{pos.volume}手, 均价={pos.avg_price:.2f}, "
                              f"盈亏={pos.profit:.2f}")
        else:
            logger.info("当前无持仓")

        logger.info("=" * 60)
        logger.info("测试完成!")
        logger.info("=" * 60)

    except ConnectionError as e:
        logger.error(f"连接失败: {e}")
        logger.info("请检查:")
        logger.info("  1. 网络是否可达 140.206.244.75")
        logger.info("  2. 账号密码是否正确")
        logger.info("  3. 是否在交易时段 (仿真可能有时间限制)")
    except Exception as e:
        logger.error(f"测试出错: {e}", exc_info=True)
    finally:
        if gateway:
            await gateway.close()


if __name__ == "__main__":
    asyncio.run(main())
