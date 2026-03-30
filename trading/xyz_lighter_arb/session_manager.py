# session_manager.py - SHFE 白银交易时段管理
#
# 上期所白银(AG)交易时间 (北京时间):
#   日盘: 09:00-10:15, 10:30-11:30, 13:30-15:00
#   夜盘: 21:00-次日02:30
#
# 注意: 夜盘21:00~02:30属于下一个交易日

from datetime import datetime, date, time as dt_time, timedelta
from enum import Enum
from typing import Optional


class SessionType(Enum):
    DAY = "day"
    NIGHT = "night"
    CLOSED = "closed"


# SHFE AG 交易时段 (start, end)
DAY_SESSIONS = [
    (dt_time(9, 0), dt_time(10, 15)),
    (dt_time(10, 30), dt_time(11, 30)),
    (dt_time(13, 30), dt_time(15, 0)),
]
NIGHT_SESSION_START = dt_time(21, 0)
NIGHT_SESSION_END = dt_time(2, 30)


class SessionManager:
    """SHFE 交易时段管理器"""

    def __init__(self, boundary_buffer_minutes: int = 3):
        self._buffer = boundary_buffer_minutes

    def is_trading_time(self, dt: Optional[datetime] = None) -> bool:
        """当前是否在交易时段内"""
        return self.get_session_type(dt) != SessionType.CLOSED

    def get_session_type(self, dt: Optional[datetime] = None) -> SessionType:
        """返回当前时段类型"""
        if dt is None:
            dt = datetime.now()

        t = dt.time()
        weekday = dt.weekday()  # 0=Monday, 6=Sunday

        # 日盘 (周一到周五)
        if weekday < 5:
            for start, end in DAY_SESSIONS:
                if start <= t < end:
                    return SessionType.DAY

        # 夜盘: 21:00~23:59
        # 有夜盘开盘的日期: 周日~周四晚 (周五晚不开, 周六晚不开)
        if (weekday == 6 or 0 <= weekday <= 3) and t >= NIGHT_SESSION_START:
            return SessionType.NIGHT

        # 夜盘延续: 00:00~02:30 (周一到周五凌晨)
        if 0 <= weekday <= 4 and t < NIGHT_SESSION_END:
            return SessionType.NIGHT

        return SessionType.CLOSED

    def is_near_boundary(self, dt: Optional[datetime] = None) -> bool:
        """是否临近开盘/收盘 (buffer分钟内), 避免在边界开新仓"""
        if dt is None:
            dt = datetime.now()

        t = dt.time()
        buf = timedelta(minutes=self._buffer)

        # 检查每个日盘时段的开盘/收盘边界
        for start, end in DAY_SESSIONS:
            start_dt = datetime.combine(dt.date(), start)
            end_dt = datetime.combine(dt.date(), end)
            if abs((datetime.combine(dt.date(), t) - start_dt).total_seconds()) < buf.total_seconds():
                return True
            if abs((datetime.combine(dt.date(), t) - end_dt).total_seconds()) < buf.total_seconds():
                return True

        # 夜盘开盘边界
        night_start_dt = datetime.combine(dt.date(), NIGHT_SESSION_START)
        if abs((datetime.combine(dt.date(), t) - night_start_dt).total_seconds()) < buf.total_seconds():
            return True

        # 夜盘收盘边界 (02:30)
        night_end_dt = datetime.combine(dt.date(), NIGHT_SESSION_END)
        if t < dt_time(3, 0):  # 凌晨时段
            if abs((datetime.combine(dt.date(), t) - night_end_dt).total_seconds()) < buf.total_seconds():
                return True

        return False

    def assign_trading_day(self, dt: datetime) -> date:
        """
        分配交易日:
        - 日盘 09:00-15:00 → 当天
        - 夜盘 21:00-23:59 → 下一个工作日
        - 夜盘 00:00-02:30 → 当天 (已经是下一天了)
        """
        t = dt.time()
        d = dt.date()

        if t >= NIGHT_SESSION_START:
            # 21:00~23:59 → 下一个工作日
            next_day = d + timedelta(days=1)
            # 跳过周末: 周五晚 → 下周一, 周六晚 → 下周一
            while next_day.weekday() >= 5:
                next_day += timedelta(days=1)
            return next_day
        else:
            # 00:00~15:00 → 当天
            return d

    def time_to_next_session(self, dt: Optional[datetime] = None) -> timedelta:
        """距离下一个交易时段开始的时间"""
        if dt is None:
            dt = datetime.now()

        if self.is_trading_time(dt):
            return timedelta(0)

        t = dt.time()
        today = dt.date()

        # 构建所有可能的下一个开盘时间
        candidates = []

        # 今天剩余的日盘
        for start, _ in DAY_SESSIONS:
            if t < start:
                candidates.append(datetime.combine(today, start))

        # 今天的夜盘
        if t < NIGHT_SESSION_START:
            candidates.append(datetime.combine(today, NIGHT_SESSION_START))

        # 明天的日盘和夜盘
        tomorrow = today + timedelta(days=1)
        for start, _ in DAY_SESSIONS:
            candidates.append(datetime.combine(tomorrow, start))
        candidates.append(datetime.combine(tomorrow, NIGHT_SESSION_START))

        # 过滤掉闭市时间
        valid = []
        for c in candidates:
            if c > dt and self.get_session_type(c) != SessionType.CLOSED:
                valid.append(c)

        if not valid:
            # 周末: 返回下周一 09:00
            days_ahead = 7 - dt.weekday()  # Monday
            next_monday = today + timedelta(days=days_ahead)
            return datetime.combine(next_monday, dt_time(9, 0)) - dt

        return min(valid) - dt
