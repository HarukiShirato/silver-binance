# config.py - SHFE AG vs HL SILVER 对冲系统配置

import os
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import logging


logger = logging.getLogger(__name__)


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
        ctp_instrument='ag2606',      # 白银主力合约 (需要根据实际主力切换)
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
    entry_zscore: float = 2.3       # Z-score入场阈值 (轻微放宽)
    exit_zscore: float = 0.8        # Z-score出场阈值
    stop_loss_zscore: float = 4.0   # 止损阈值

    # 价差计算窗口
    spread_window: int = 60         # 滚动窗口大小 (分钟, 原240 tick)
    sample_interval: int = 45       # 采样间隔 (秒)

    # 仓位管理
    max_position_lots: int = 1      # 最大持仓手数 (最小模式: 仅1手)

    # 时间控制
    max_hold_hours: int = 24        # 最大持仓时间
    cooldown_seconds: int = 120      # 交易后冷却时间

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
    hl_dex: str = "xyz"
    # HL signer key: 推荐使用 API wallet(代理钱包)私钥，而不是主钱包私钥
    hl_api_wallet_private_key: str = ""
    hl_wallet_address: str = ""
    hl_api_wallet_secret_id: str = ""      # Optional: AWS Secrets Manager secret id/name
    aws_region: str = ""                   # Optional: AWS region for secrets fetch

    # CTP (国贸期货)
    ctp_broker_id: str = "0187"
    ctp_user_id: str = ""
    ctp_password: str = ""
    ctp_md_front: str = "tcp://114.80.225.10:41213"   # 行情前置(主)
    ctp_td_front: str = "tcp://114.80.225.10:41205"   # 交易前置(主)
    ctp_md_front_backups: List[str] = field(default_factory=lambda: [
        "tcp://140.206.244.75:41213",
        "tcp://140.206.244.67:41213",
        "tcp://114.80.225.2:41213",
    ])
    ctp_td_front_backups: List[str] = field(default_factory=lambda: [
        "tcp://140.206.244.75:41205",
        "tcp://140.206.244.67:41205",
        "tcp://114.80.225.2:41205",
    ])
    ctp_app_id: str = "client_Lavas_1.0.0"
    ctp_auth_code: str = ""

    # 超时设置
    request_timeout: int = 5000     # 请求超时 (ms)
    ws_ping_interval: int = 20      # WebSocket心跳间隔 (秒)

API = APIConfig()


@dataclass
class RuntimeConfig:
    dry_run: bool = True
    hl_exec_mode: str = "local"          # local | remote
    hl_remote_url: str = ""              # e.g. http://52.193.85.209:18080
    remote_exec_timeout_sec: float = 2.0
    hl_data_mode: str = "local"          # local | remote
    hl_remote_quote_url: str = ""        # e.g. http://52.193.85.209:18080/quote
    hl_remote_quote_ws_url: str = ""     # e.g. ws://52.193.85.209:18081/quote
    remote_quote_timeout_sec: float = 1.0
    remote_quote_poll_sec: float = 1.0


RUNTIME = RuntimeConfig()


def load_runtime_config():
    RUNTIME.dry_run = _env_bool("DRY_RUN", True)
    RUNTIME.hl_exec_mode = os.environ.get("HL_EXEC_MODE", RUNTIME.hl_exec_mode).strip().lower()
    RUNTIME.hl_remote_url = os.environ.get("HL_REMOTE_URL", RUNTIME.hl_remote_url).strip()
    RUNTIME.hl_data_mode = os.environ.get("HL_DATA_MODE", RUNTIME.hl_data_mode).strip().lower()
    RUNTIME.hl_remote_quote_url = os.environ.get("HL_REMOTE_QUOTE_URL", RUNTIME.hl_remote_quote_url).strip()
    RUNTIME.hl_remote_quote_ws_url = os.environ.get("HL_REMOTE_QUOTE_WS_URL", RUNTIME.hl_remote_quote_ws_url).strip()
    timeout = os.environ.get("REMOTE_EXEC_TIMEOUT_SEC")
    if timeout:
        try:
            RUNTIME.remote_exec_timeout_sec = float(timeout)
        except ValueError:
            pass
    quote_timeout = os.environ.get("REMOTE_QUOTE_TIMEOUT_SEC")
    if quote_timeout:
        try:
            RUNTIME.remote_quote_timeout_sec = float(quote_timeout)
        except ValueError:
            pass
    quote_poll = os.environ.get("REMOTE_QUOTE_POLL_SEC")
    if quote_poll:
        try:
            RUNTIME.remote_quote_poll_sec = float(quote_poll)
        except ValueError:
            pass

    if (
        RUNTIME.hl_data_mode == "remote"
        and not RUNTIME.hl_remote_quote_url
        and RUNTIME.hl_remote_url
    ):
        RUNTIME.hl_remote_quote_url = RUNTIME.hl_remote_url.rstrip("/") + "/quote"
    if (
        RUNTIME.hl_data_mode == "remote"
        and not RUNTIME.hl_remote_quote_ws_url
        and RUNTIME.hl_remote_url
    ):
        base = RUNTIME.hl_remote_url.strip()
        if base.startswith("https://"):
            RUNTIME.hl_remote_quote_ws_url = "wss://" + base[len("https://") :].rstrip("/") + ":18081/quote"
        elif base.startswith("http://"):
            RUNTIME.hl_remote_quote_ws_url = "ws://" + base[len("http://") :].rstrip("/") + ":18081/quote"

    if RUNTIME.hl_exec_mode not in ("local", "remote"):
        logger.warning(f"Unknown HL_EXEC_MODE={RUNTIME.hl_exec_mode}, fallback to local")
        RUNTIME.hl_exec_mode = "local"
    if RUNTIME.hl_data_mode not in ("local", "remote"):
        logger.warning(f"Unknown HL_DATA_MODE={RUNTIME.hl_data_mode}, fallback to local")
        RUNTIME.hl_data_mode = "local"


def _load_hl_key_from_aws_secret(secret_id: str, region: str) -> str:
    """Fetch HL private key from AWS Secrets Manager and normalize it."""
    try:
        import boto3  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "AWS secret is configured but boto3 is not installed. "
            "Install with `pip install boto3`."
        ) from e

    kwargs = {}
    if region:
        kwargs["region_name"] = region

    client = boto3.client("secretsmanager", **kwargs)
    resp = client.get_secret_value(SecretId=secret_id)
    secret = (resp.get("SecretString") or "").strip()
    if not secret:
        raise RuntimeError("SecretString is empty")

    # Supports plain key or JSON payload.
    if secret.startswith("{") and secret.endswith("}"):
        import json

        obj = json.loads(secret)
        for k in ("private_key", "key", "secret"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        raise RuntimeError("JSON secret does not contain a supported private key field")
    return secret


def load_api_keys(dry_run: bool = False, hl_exec_mode: str = "local"):
    """从环境变量（或 AWS Secrets Manager）加载敏感信息"""
    # Hyperliquid
    API.hl_dex = os.environ.get('HL_DEX', API.hl_dex)
    API.hl_api_wallet_private_key = ""
    API.hl_wallet_address = os.environ.get('HL_WALLET_ADDRESS', '').strip()
    API.hl_api_wallet_secret_id = os.environ.get('HL_API_WALLET_SECRET_ID', '').strip()
    API.aws_region = (
        os.environ.get('AWS_REGION', '').strip()
        or os.environ.get('AWS_DEFAULT_REGION', '').strip()
    )

    # Only resolve secret when key is needed.
    needs_hl_key = (not dry_run) and (hl_exec_mode != "remote")
    if needs_hl_key:
        if not API.hl_api_wallet_secret_id:
            raise EnvironmentError(
                "缺少必要环境变量: HL_API_WALLET_SECRET_ID. "
                "LIVE本地HL模式下必须通过 AWS Secrets Manager 提供私钥。"
            )
        API.hl_api_wallet_private_key = _load_hl_key_from_aws_secret(
            API.hl_api_wallet_secret_id,
            API.aws_region,
        )
        logger.info("Loaded HL API wallet private key from AWS Secrets Manager")

    # CTP
    API.ctp_user_id = os.environ.get('CTP_USER_ID', '')
    API.ctp_password = os.environ.get('CTP_PASSWORD', '')
    API.ctp_auth_code = os.environ.get('CTP_AUTH_CODE', '')
    API.ctp_md_front = os.environ.get('CTP_MD_FRONT', API.ctp_md_front)
    API.ctp_td_front = os.environ.get('CTP_TD_FRONT', API.ctp_td_front)

    md_backups = os.environ.get('CTP_MD_FRONT_BACKUPS', '')
    if md_backups.strip():
        API.ctp_md_front_backups = [x.strip() for x in md_backups.split(',') if x.strip()]
    td_backups = os.environ.get('CTP_TD_FRONT_BACKUPS', '')
    if td_backups.strip():
        API.ctp_td_front_backups = [x.strip() for x in td_backups.split(',') if x.strip()]


def get_ctp_front_candidates() -> List[Tuple[str, str]]:
    """Return [(md_front, td_front), ...] with primary first then backups."""
    candidates: List[Tuple[str, str]] = []
    seen = set()

    primary = (API.ctp_md_front, API.ctp_td_front)
    if all(primary):
        candidates.append(primary)
        seen.add(primary)

    n = min(len(API.ctp_md_front_backups), len(API.ctp_td_front_backups))
    for i in range(n):
        pair = (API.ctp_md_front_backups[i], API.ctp_td_front_backups[i])
        if all(pair) and pair not in seen:
            candidates.append(pair)
            seen.add(pair)

    return candidates


def validate_api_keys(dry_run: bool = False, hl_exec_mode: str = "local"):
    """校验 API 密钥是否已配置, 缺失则抛出异常"""
    missing = []
    if not API.ctp_user_id:
        missing.append('CTP_USER_ID')
    if not API.ctp_password:
        missing.append('CTP_PASSWORD')
    if not API.ctp_auth_code:
        missing.append('CTP_AUTH_CODE')
    needs_hl_key = (not dry_run) and (hl_exec_mode != "remote")
    if needs_hl_key:
        if not API.hl_api_wallet_private_key:
            missing.append('HL_API_WALLET_SECRET_ID(secret)')
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
    max_daily_trades: int = 0      # 每日最大交易次数 (0=不限制)
    max_daily_loss: float = 5000    # 每日最大亏损 (RMB)
    leg_timeout_sec: float = 2.0    # 第二腿超时时间 (秒)
    leg_retry_times: int = 2        # 第二腿重试次数
    emergency_spread_pct: float = 25.0  # 价差超过25%紧急停止
    # 保证金预警阈值 (CTP 和 HL 共用)
    margin_warning_pct: float = 0.70   # 70% 预警
    margin_danger_pct: float = 0.85    # 85% 危险 (禁止加仓)
    margin_critical_pct: float = 0.90  # 90% 临界 (禁止开仓)
    margin_check_interval: int = 30    # 保证金检查间隔 (秒)
    md_watchdog_no_tick_sec: int = 60  # 开盘时段内 AG 超过该秒数无新 tick 则触发 watchdog
    md_watchdog_alert_cooldown_sec: int = 600  # watchdog 同类告警发送冷却 (秒)
    disconnect_ctp_when_closed: bool = True  # 非交易时段自动断开 CTP

    position_reconcile_interval_sec: int = 60
    position_mismatch_alert_cooldown_sec: int = 60
    hl_quote_ws_down_alert_cooldown_sec: int = 60
    position_reconcile_consistency_count: int = 3
    position_reconcile_correction_cooldown_sec: int = 120

RISK = RiskConfig()


# ==================== 通知配置 ====================
@dataclass
class NotifyConfig:
    feishu_trade_webhook_url: str = ""
    feishu_margin_webhook_url: str = ""
    enable_feishu: bool = True
    notify_on_trade: bool = True
    notify_on_error: bool = True
    notify_daily_summary: bool = True
    daily_summary_hour: int = 23

NOTIFY = NotifyConfig()


def load_notify_config():
    # 兼容旧变量 FEISHU_WEBHOOK_URL -> trade webhook
    trade = os.environ.get('FEISHU_TRADE_WEBHOOK_URL', '').strip()
    if not trade:
        trade = os.environ.get('FEISHU_WEBHOOK_URL', '').strip()
    NOTIFY.feishu_trade_webhook_url = trade
    NOTIFY.feishu_margin_webhook_url = os.environ.get('FEISHU_MARGIN_WEBHOOK_URL', '').strip()


# ==================== 日志配置 ====================
LOG_LEVEL = "INFO"
LOG_FILE = "logs/arbitrage.log"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
