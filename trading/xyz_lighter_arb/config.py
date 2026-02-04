# config.py - 套利机器人配置文件

import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class TradingPair:
    """交易对配置"""
    name: str                    # 名称: SILVER, GOLD
    xyz_symbol: str              # XYZ合约代码
    lighter_market_id: int       # Lighter market_id
    capital: float               # 本金 (USDC)
    leverage: int                # 杠杆倍数
    min_order_size: float        # 最小下单量
    price_decimals: int          # 价格精度
    size_decimals: int           # 数量精度
    fixed_size: float = 0        # 固定下单数量，0表示根据资金动态计算

# ==================== 交易对配置 ====================
TRADING_PAIRS: Dict[str, TradingPair] = {
    'SILVER': TradingPair(
        name='SILVER',
        xyz_symbol='xyz:SILVER',
        lighter_market_id=93,
        capital=50000,
        leverage=3,
        min_order_size=0.15,
        price_decimals=4,
        size_decimals=2,
        fixed_size=0,            # 0=动态计算
    ),
    'GOLD': TradingPair(
        name='GOLD',
        xyz_symbol='xyz:GOLD',
        lighter_market_id=92,
        capital=50000,
        leverage=3,
        min_order_size=0.003,
        price_decimals=2,
        size_decimals=4,
        fixed_size=0,            # 0=动态计算
    ),
}

# ==================== 策略参数 ====================
@dataclass
class StrategyConfig:
    # 入场/出场阈值
    entry_zscore: float = 2.5       # Z-score入场阈值 (实盘用2.5更保守)
    exit_zscore: float = 0.3        # Z-score出场阈值
    stop_loss_zscore: float = 4.0   # 止损阈值

    # 价差计算窗口
    spread_window: int = 100        # 计算均值/标准差的窗口 (小时数)

    # 仓位管理
    max_position_pct: float = 1.0   # 最大仓位占本金比例
    split_orders: int = 3           # 大单拆分笔数
    max_single_order: float = 20000 # 单笔最大下单金额 (USDC)

    # 流动性约束
    max_book_pct: float = 0.2       # 单笔最大占订单簿前5档的比例 (20%)
    max_oi_pct: float = 0.005       # 单笔最大占OI的比例 (0.5%)

    # 时间控制
    max_hold_hours: int = 24        # 最大持仓时间
    cooldown_seconds: int = 60      # 交易后冷却时间

    # 滑点保护
    max_slippage_pct: float = 0.1   # 最大允许滑点 0.1%

STRATEGY = StrategyConfig()

# ==================== API配置 ====================
@dataclass
class APIConfig:
    # Hyperliquid (XYZ)
    hl_api_url: str = "https://api.hyperliquid.xyz"
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    hl_private_key: str = ""  # 从环境变量读取
    hl_wallet_address: str = ""

    # Lighter
    lighter_api_url: str = "https://mainnet.zklighter.elliot.ai"
    lighter_ws_url: str = "wss://mainnet.zklighter.elliot.ai/ws"
    lighter_private_key: str = ""
    lighter_api_key: str = ""

    # 超时设置
    request_timeout: int = 5000     # 请求超时 (ms)
    ws_ping_interval: int = 20      # WebSocket心跳间隔 (秒)

API = APIConfig()

# 从环境变量加载敏感信息
def load_api_keys():
    API.hl_private_key = os.environ.get('HL_PRIVATE_KEY', '')
    API.hl_wallet_address = os.environ.get('HL_WALLET_ADDRESS', '')
    API.lighter_private_key = os.environ.get('LIGHTER_PRIVATE_KEY', '')
    API.lighter_api_key = os.environ.get('LIGHTER_API_KEY', '')

# ==================== 风控配置 ====================
@dataclass
class RiskConfig:
    # 每日限制
    max_daily_trades: int = 50      # 每日最大交易次数
    max_daily_loss: float = 2000    # 每日最大亏损 (USDC)

    # 单腿保护
    leg_timeout_ms: int = 2000      # 第二腿超时时间
    leg_retry_times: int = 2        # 第二腿重试次数

    # 紧急停止
    emergency_spread_pct: float = 5.0  # 价差超过5%紧急停止

RISK = RiskConfig()

# ==================== 通知配置 ====================
@dataclass
class NotifyConfig:
    # 飞书 Webhook
    feishu_webhook_url: str = ""  # https://open.feishu.cn/open-apis/bot/v2/hook/xxx
    enable_feishu: bool = True

    # 通知级别
    notify_on_trade: bool = True       # 每笔交易推送
    notify_on_error: bool = True       # 错误推送
    notify_daily_summary: bool = True  # 每日汇总推送
    daily_summary_hour: int = 23       # 每日汇总时间 (24小时制，23点)

NOTIFY = NotifyConfig()

def load_notify_config():
    NOTIFY.feishu_webhook_url = os.environ.get('FEISHU_WEBHOOK_URL', '')

# ==================== 日志配置 ====================
LOG_LEVEL = "INFO"
LOG_FILE = "logs/arbitrage.log"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
