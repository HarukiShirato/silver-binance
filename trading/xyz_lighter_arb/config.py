# config.py - SHFE AG vs HL SILVER 对冲系统配置

import os
from dataclasses import dataclass
from typing import Dict


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "y")


@dataclass
class TradingPair:
    """交易对配置"""
    name: str                        # 名称: SILVER
    # Hyperliquid 侧
    hl_symbol: str = ""              # HL 合约代码 (如 SILVER)
    hl_leverage: int = 5             # HL 杠杆倍数
    # CTP 侧
    ctp_instrument: str = ""         # CTP 合约代码 (如 ag2506)
    ctp_exchange: str = "SHFE"       # 交易所代码
    ctp_multiplier: int = 15         # 合约乘数 (白银 15kg/手)
    ctp_margin_rate: float = 0.09    # 保证金率 (~9%)
    # 资金
    capital: float = 50000           # 本金 (RMB)


# ==================== 交易对配置 ====================
TRADING_PAIRS: Dict[str, TradingPair] = {
    'SILVER': TradingPair(
        name='SILVER',
        hl_symbol='SILVER',
        hl_leverage=5,
        ctp_instrument='ag2506',      # 白银主力合约 (需要根据实际主力切换)
        ctp_exchange='SHFE',
        ctp_multiplier=15,            # 白银 15kg/手
        ctp_margin_rate=0.09,         # ~9% 保证金率
        capital=50000,
    ),
}


# ==================== 策略参数 ====================
@dataclass
class StrategyConfig:
    # 入场/出场阈值 (来自 spread_backtest_1m.py 最优参数扫描)
    entry_zscore: float = 1.5       # Z-score入场阈值 (原2.5, 回测最优1.5)
    exit_zscore: float = 0.5        # Z-score出场阈值
    stop_loss_zscore: float = 4.0   # 止损阈值

    # 价差计算窗口
    spread_window: int = 60         # 滚动窗口大小 (分钟, 原240 tick)
    sample_interval: int = 60       # 采样间隔 (秒), 每60秒取一个数据点

    # 仓位管理
    max_position_lots: int = 10     # 最大持仓手数 (逐手加仓, 每次+1手)

    # 时间控制
    max_hold_hours: int = 24        # 最大持仓时间
    cooldown_seconds: int = 60      # 交易后冷却时间

    # 滑点保护
    max_slippage_pct: float = 0.1   # 最大允许滑点 0.1%
    hl_order_slippage_pct: float = 0.003  # HL 下单价格滑点保护 0.3%

STRATEGY = StrategyConfig()


# ==================== API 配置 ====================
@dataclass
class APIConfig:
    # Hyperliquid
    hl_api_url: str = "https://api.hyperliquid.xyz"
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    hl_private_key: str = ""
    hl_wallet_address: str = ""

    # CTP (国贸期货)
    ctp_broker_id: str = "0187"
    ctp_user_id: str = ""
    ctp_password: str = ""
    ctp_md_front: str = "tcp://140.206.244.75:41213"   # 行情前置
    ctp_td_front: str = "tcp://140.206.244.75:41205"   # 交易前置
    ctp_app_id: str = "client_Lavas_1.0.0"
    ctp_auth_code: str = ""

    # 超时设置
    request_timeout: int = 5000     # 请求超时 (ms)
    ws_ping_interval: int = 20      # WebSocket心跳间隔 (秒)

API = APIConfig()


@dataclass
class RuntimeConfig:
    dry_run: bool = True


RUNTIME = RuntimeConfig()


def load_runtime_config():
    RUNTIME.dry_run = _env_bool("DRY_RUN", True)


def load_api_keys():
    """从环境变量加载敏感信息"""
    # Hyperliquid
    API.hl_private_key = os.environ.get('HL_PRIVATE_KEY', '')
    API.hl_wallet_address = os.environ.get('HL_WALLET_ADDRESS', '')
    # CTP
    API.ctp_user_id = os.environ.get('CTP_USER_ID', '')
    API.ctp_password = os.environ.get('CTP_PASSWORD', '')
    API.ctp_auth_code = os.environ.get('CTP_AUTH_CODE', '')


def validate_api_keys(dry_run: bool = False):
    """校验 API 密钥是否已配置, 缺失则抛出异常"""
    missing = []
    if not API.ctp_user_id:
        missing.append('CTP_USER_ID')
    if not API.ctp_password:
        missing.append('CTP_PASSWORD')
    if not API.ctp_auth_code:
        missing.append('CTP_AUTH_CODE')
    if not dry_run:
        if not API.hl_private_key:
            missing.append('HL_PRIVATE_KEY')
        if not API.hl_wallet_address:
            missing.append('HL_WALLET_ADDRESS')
    if missing:
        raise EnvironmentError(
            f"缺少必要的 API 密钥环境变量: {', '.join(missing)}. "
            f"请在 .env 文件或环境变量中配置."
        )


# ==================== 汇率配置 ====================
@dataclass
class ForexConfig:
    api_source: str = "exchangerate-api"    # 汇率API来源
    fallback_usdcny: float = 7.25           # 默认汇率
    update_interval: int = 60               # 更新间隔 (秒)

FOREX = ForexConfig()


# ==================== 风控配置 ====================
@dataclass
class RiskConfig:
    max_daily_trades: int = 50      # 每日最大交易次数
    max_daily_loss: float = 5000    # 每日最大亏损 (RMB)
    leg_timeout_sec: float = 2.0    # 第二腿超时时间 (秒)
    leg_retry_times: int = 2        # 第二腿重试次数
    emergency_spread_pct: float = 5.0  # 价差超过5%紧急停止
    # 保证金预警阈值 (CTP 和 HL 共用)
    margin_warning_pct: float = 0.70   # 70% 预警
    margin_danger_pct: float = 0.85    # 85% 危险 (禁止加仓)
    margin_critical_pct: float = 0.90  # 90% 临界 (禁止开仓)
    margin_check_interval: int = 30    # 保证金检查间隔 (秒)

RISK = RiskConfig()


# ==================== 通知配置 ====================
@dataclass
class NotifyConfig:
    feishu_webhook_url: str = ""
    enable_feishu: bool = True
    notify_on_trade: bool = True
    notify_on_error: bool = True
    notify_daily_summary: bool = True
    daily_summary_hour: int = 23

NOTIFY = NotifyConfig()


def load_notify_config():
    NOTIFY.feishu_webhook_url = os.environ.get('FEISHU_WEBHOOK_URL', '')


# ==================== 日志配置 ====================
LOG_LEVEL = "INFO"
LOG_FILE = "logs/arbitrage.log"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
