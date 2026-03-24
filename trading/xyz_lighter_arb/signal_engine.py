import json
import logging
import os
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import numpy as np

from data_engine import NormalizedPrice
from session_manager import SessionManager

logger = logging.getLogger(__name__)


class Signal(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"
    HOLD = "HOLD"


@dataclass
class SignalResult:
    signal: Signal
    ag_price: float
    hl_price_cny_kg: float
    hl_price_usd_oz: float
    usdcny: float
    spread_pct: float
    spread_mean: float
    spread_std: float
    zscore: float
    funding_rate: float
    timestamp: float


class SignalEngine:
    MIN_DATA_POINTS = 30

    def __init__(
        self,
        pair_name: str,
        window_size: int = 60,
        entry_zscore: float = 1.5,
        exit_zscore: float = 0.5,
        stop_loss_zscore: float = 4.0,
        sample_interval: int = 60,
        session_manager: Optional[SessionManager] = None,
    ):
        self._pair_name = pair_name
        self._window_size = window_size
        self._entry_z = entry_zscore
        self._exit_z = exit_zscore
        self._stop_z = stop_loss_zscore
        self._sample_interval = sample_interval
        self._session_mgr = session_manager

        self._spread_pcts: deque = deque(maxlen=window_size)
        self._last_sample_time: float = 0.0
        self._latest_spread_pct: float = 0.0
        self._cached_mean: float = 0.0
        self._cached_std: float = 0.0

        self._position: str = "NONE"
        self._last_funding_rate: float = 0.0
        self._cumulative_funding: float = 0.0
        self._last_decision_reason: str = "init"

        self._state_file = os.path.join("data", "signal_window_state.json")

    @property
    def position(self) -> str:
        return self._position

    @property
    def data_ready(self) -> bool:
        return len(self._spread_pcts) >= self.MIN_DATA_POINTS

    @property
    def sample_count(self) -> int:
        return len(self._spread_pcts)

    @property
    def window_size(self) -> int:
        return self._window_size

    @property
    def last_decision_reason(self) -> str:
        return self._last_decision_reason

    def load_window_state(self, load_points: int = 20) -> int:
        """Load the latest N samples from disk for warm-start."""
        if not os.path.exists(self._state_file):
            return 0
        try:
            with open(self._state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            items = data.get("spread_pcts", [])
            if not isinstance(items, list):
                return 0
            n = max(0, min(int(load_points), self._window_size))
            restored = items[-n:] if n > 0 else []
            self._spread_pcts.clear()
            for x in restored:
                self._spread_pcts.append(float(x))
            self._last_sample_time = float(data.get("last_sample_time", 0.0) or 0.0)
            if self._spread_pcts:
                self._latest_spread_pct = float(self._spread_pcts[-1])
            self._update_stats()
            return len(self._spread_pcts)
        except Exception as e:
            logger.warning(f"加载信号窗口状态失败: {e}")
            return 0

    def save_window_state(self) -> None:
        try:
            os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
            payload = {
                "pair_name": self._pair_name,
                "spread_pcts": list(self._spread_pcts),
                "last_sample_time": self._last_sample_time,
            }
            with open(self._state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"保存信号窗口状态失败: {e}")

    def update(self, price: NormalizedPrice) -> Optional[SignalResult]:
        if self._session_mgr and not self._session_mgr.is_trading_time():
            self._last_decision_reason = "session_closed"
            return None

        if price.ag_price <= 0 or price.hl_price_cny_kg <= 0:
            self._last_decision_reason = "invalid_price"
            return None

        if price.ag_stale or price.hl_stale:
            self._last_decision_reason = "stale_price"
            return None

        if price.forex_stale and self._position == "NONE":
            self._last_decision_reason = "forex_stale_no_entry"
            return None

        spread_pct = (price.ag_price - price.hl_price_cny_kg) / price.hl_price_cny_kg * 100.0
        self._latest_spread_pct = spread_pct

        now = float(price.timestamp)
        should_sample = False
        if self._last_sample_time == 0.0:
            should_sample = True
        elif (now - self._last_sample_time) >= self._sample_interval:
            should_sample = True

        if should_sample:
            self._spread_pcts.append(spread_pct)
            self._last_sample_time = now
            self._update_stats()
            self.save_window_state()

        if not self.data_ready:
            self._last_decision_reason = "warmup"
            return SignalResult(
                signal=Signal.HOLD,
                ag_price=price.ag_price,
                hl_price_cny_kg=price.hl_price_cny_kg,
                hl_price_usd_oz=price.hl_price_usd,
                usdcny=price.usdcny,
                spread_pct=spread_pct,
                spread_mean=self._cached_mean,
                spread_std=self._cached_std,
                zscore=0.0,
                funding_rate=self._last_funding_rate,
                timestamp=price.timestamp,
            )

        zscore = (spread_pct - self._cached_mean) / self._cached_std if self._cached_std > 1e-8 else 0.0
        signal = self._generate_signal(zscore)
        self._last_decision_reason = f"signal_{signal.value.lower()}"

        return SignalResult(
            signal=signal,
            ag_price=price.ag_price,
            hl_price_cny_kg=price.hl_price_cny_kg,
            hl_price_usd_oz=price.hl_price_usd,
            usdcny=price.usdcny,
            spread_pct=spread_pct,
            spread_mean=self._cached_mean,
            spread_std=self._cached_std,
            zscore=zscore,
            funding_rate=self._last_funding_rate,
            timestamp=price.timestamp,
        )

    def _update_stats(self) -> None:
        if len(self._spread_pcts) < 2:
            self._cached_mean = self._spread_pcts[0] if self._spread_pcts else 0.0
            self._cached_std = 0.0
            return
        arr = np.array(self._spread_pcts, dtype=float)
        self._cached_mean = float(np.mean(arr))
        self._cached_std = float(np.std(arr))

    def _generate_signal(self, zscore: float) -> Signal:
        if self._position == "LONG" and zscore < -self._stop_z:
            self._position = "NONE"
            return Signal.EXIT_LONG

        if self._position == "SHORT" and zscore > self._stop_z:
            self._position = "NONE"
            return Signal.EXIT_SHORT

        if self._position == "LONG" and zscore >= -self._exit_z:
            self._position = "NONE"
            return Signal.EXIT_LONG

        if self._position == "SHORT" and zscore <= self._exit_z:
            self._position = "NONE"
            return Signal.EXIT_SHORT

        if self._position == "NONE":
            if self._session_mgr and self._session_mgr.is_near_boundary():
                return Signal.HOLD
            if zscore < -self._entry_z:
                self._position = "LONG"
                return Signal.LONG
            if zscore > self._entry_z:
                self._position = "SHORT"
                return Signal.SHORT

        return Signal.HOLD

    def update_funding_rate(self, rate: float):
        self._last_funding_rate = rate
        if self._position != "NONE":
            self._cumulative_funding += rate

    def set_position(self, position: str):
        self._position = position

    def reset_funding(self):
        self._cumulative_funding = 0.0

    @property
    def cumulative_funding(self) -> float:
        return self._cumulative_funding

    def get_current_zscore(self) -> float:
        if not self.data_ready:
            return 0.0
        if self._cached_std < 1e-8:
            return 0.0
        return (self._latest_spread_pct - self._cached_mean) / self._cached_std
