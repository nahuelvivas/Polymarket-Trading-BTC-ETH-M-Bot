"""Compute current/next epoch slug for 5m (e.g. btc-updown-5m-{unix_ts})."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import NamedTuple

from polybot5m.constants import INTERVAL_SECONDS


class EpochSlugs(NamedTuple):
    current_slug: str
    current_start: datetime
    next_slug: str
    next_start: datetime


def epoch_start_ts(now_utc: datetime, interval_seconds: int, offset: int = 0) -> int:
    ts = int(now_utc.timestamp())
    return ((ts - offset) // interval_seconds) * interval_seconds + offset


def build_structured_slug(asset: str, interval: str, epoch_ts: int) -> str:
    return f"{asset}-updown-{interval}-{epoch_ts}"


def compute_epoch_slugs(
    asset: str,
    interval: str,
    now_utc: datetime | None = None,
) -> EpochSlugs:
    if now_utc is None:
        now_utc = datetime.now(UTC)
    seconds = INTERVAL_SECONDS.get(interval, 300)
    offset = 0
    current_ts = epoch_start_ts(now_utc, seconds, offset)
    next_ts = current_ts + seconds
    current_dt = datetime.fromtimestamp(current_ts, tz=UTC)
    next_dt = datetime.fromtimestamp(next_ts, tz=UTC)
    return EpochSlugs(
        current_slug=build_structured_slug(asset.lower(), interval, current_ts),
        current_start=current_dt,
        next_slug=build_structured_slug(asset.lower(), interval, next_ts),
        next_start=next_dt,
    )
