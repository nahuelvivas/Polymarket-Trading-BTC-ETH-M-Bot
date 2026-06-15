"""UTC trading schedule — sleep outside configured windows."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from polybot5m.config import ScheduleConfig, ScheduleWindowConfig

WEEKDAY_NAMES = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalized_weekdays(weekdays: list[str]) -> set[str]:
    return {str(d).lower().strip() for d in weekdays if str(d).strip()}


def _time_minutes(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute


def _window_minutes(window: ScheduleWindowConfig) -> tuple[int, int]:
    return (
        window.start_hour * 60 + window.start_minute,
        window.end_hour * 60 + window.end_minute,
    )


def _window_start_on_day(day: datetime, window: ScheduleWindowConfig) -> datetime:
    base = day.replace(
        hour=window.start_hour,
        minute=window.start_minute,
        second=0,
        microsecond=0,
    )
    return base


def is_trading_active(now: datetime, schedule: ScheduleConfig) -> bool:
    """True when `now` falls inside a configured weekday + time window (UTC)."""
    if not schedule.enabled:
        return True
    weekday = WEEKDAY_NAMES[now.weekday()]
    if weekday not in _normalized_weekdays(schedule.weekdays):
        return False
    t = _time_minutes(now)
    for window in schedule.windows:
        start_m, end_m = _window_minutes(window)
        if start_m <= t < end_m:
            return True
    return False


def next_window_start(now: datetime, schedule: ScheduleConfig) -> datetime:
    """Next UTC datetime when a configured trading window opens."""
    allowed = _normalized_weekdays(schedule.weekdays)
    day_base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    candidates: list[datetime] = []
    for day_offset in range(8):
        day = day_base + timedelta(days=day_offset)
        weekday = WEEKDAY_NAMES[day.weekday()]
        if weekday not in allowed:
            continue
        for window in schedule.windows:
            start_at = _window_start_on_day(day, window)
            if start_at > now:
                candidates.append(start_at)
    if not candidates:
        raise RuntimeError("schedule has no upcoming trading windows")
    return min(candidates)


def format_schedule_reminder(now: datetime, next_start: datetime) -> str:
    """e.g. '459 min remind to 09/06/26 Monday 4 AM UTC'."""
    remaining_s = max(0.0, (next_start - now).total_seconds())
    minutes = int(remaining_s // 60)
    date_part = next_start.strftime("%d/%m/%y")
    weekday = next_start.strftime("%A")
    hour = next_start.hour
    minute = next_start.minute
    if hour == 0:
        h12, ampm = 12, "AM"
    elif hour < 12:
        h12, ampm = hour, "AM"
    elif hour == 12:
        h12, ampm = 12, "PM"
    else:
        h12, ampm = hour - 12, "PM"
    if minute == 0:
        time_str = f"{h12} {ampm}"
    else:
        time_str = f"{h12}:{minute:02d} {ampm}"
    return f"{minutes} min remind to {date_part} {weekday} {time_str} UTC"


def describe_schedule(schedule: ScheduleConfig) -> str:
    if not schedule.enabled:
        return "schedule=off (24/7)"
    days = ", ".join(schedule.weekdays)
    parts = []
    for w in schedule.windows:
        parts.append(
            f"{w.start_hour:02d}:{w.start_minute:02d}"
            f"–{w.end_hour:02d}:{w.end_minute:02d}"
        )
    return f"schedule=on UTC days=[{days}] windows=[{'; '.join(parts)}]"


async def wait_for_trading_window(schedule: ScheduleConfig) -> None:
    """Block (with periodic logs) until the next trading window opens."""
    if not schedule.enabled or is_trading_active(_utc_now(), schedule):
        return
    log_interval_s = max(1, int(schedule.sleep_log_interval_min)) * 60
    while not is_trading_active(_utc_now(), schedule):
        now = _utc_now()
        next_start = next_window_start(now, schedule)
        print(f"[SCHEDULE] {format_schedule_reminder(now, next_start)}")
        await asyncio.sleep(log_interval_s)
    print(f"[SCHEDULE] trading window active at {_utc_now().isoformat()}")
