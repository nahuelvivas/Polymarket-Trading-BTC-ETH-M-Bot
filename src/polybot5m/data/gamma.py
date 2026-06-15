"""Gamma API client — fetch BTC 5m event by slug."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from polybot5m.constants import STRUCTURED_SLUG_INTERVALS
from polybot5m.data.models import CryptoMarketMeta, Event, Market

if TYPE_CHECKING:
    import aiohttp

log = structlog.get_logger(__name__)


def _parse_json_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except (json.JSONDecodeError, TypeError):
            pass
    return []


def _parse_slug_structured(slug: str) -> CryptoMarketMeta:
    """Parse btc-updown-5m-1707836100 style slug."""
    parts = slug.split("-")
    if len(parts) < 4 or not parts[-1].isdigit():
        raise ValueError(f"Invalid structured slug: {slug}")
    asset = parts[0].lower()
    interval = ""
    for c in (parts[2], parts[-2]):
        if c in STRUCTURED_SLUG_INTERVALS:
            interval = c
            break
    if not interval:
        raise ValueError(f"Unknown interval in slug: {slug}")
    expiry = datetime.fromtimestamp(int(parts[-1]), tz=UTC).replace(tzinfo=None)
    return CryptoMarketMeta(asset=asset, interval=interval, slug=slug, expiry=expiry)


def _classify_event(event_data: dict[str, Any]) -> Event | None:
    slug = event_data.get("slug", "")
    markets_raw = event_data.get("markets", [])
    if not markets_raw:
        markets_raw = [event_data]
    try:
        event_meta = _parse_slug_structured(slug)
    except ValueError:
        return None
    markets: list[Market] = []
    for m in markets_raw:
        clob_ids = _parse_json_string_list(m.get("clobTokenIds", m.get("asset_ids", [])))
        outcomes = _parse_json_string_list(m.get("outcomes", []))
        question = m.get("question", "")
        market_slug = m.get("slug", slug)
        try:
            meta = _parse_slug_structured(market_slug)
        except ValueError:
            meta = event_meta.model_copy()
        end_date_str = m.get("endDate", "")
        if end_date_str:
            try:
                meta = meta.model_copy(
                    update={
                        "expiry": datetime.fromisoformat(
                            end_date_str.replace("Z", "+00:00")
                        ).replace(tzinfo=None),
                    },
                )
            except ValueError:
                pass
        markets.append(
            Market(
                condition_id=m.get("conditionId", m.get("condition_id", "")),
                asset_ids=clob_ids,
                question=question,
                outcomes=outcomes,
                meta=meta,
            ),
        )
    return Event(
        id=str(event_data.get("id", event_data.get("conditionId", ""))),
        slug=slug,
        title=event_data.get("title", ""),
        markets=markets,
    )


class GammaClient:
    def __init__(self, base_url: str, session: "aiohttp.ClientSession | None") -> None:
        self._base_url = base_url.rstrip("/")
        self._session = session

    async def fetch_event_by_slug(self, slug: str) -> Event:
        url = f"{self._base_url}/events"
        params = {"slug": slug}
        if self._session is None:
            raise RuntimeError("GammaClient requires an aiohttp ClientSession")
        async with self._session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        if isinstance(data, list) and data:
            event = _classify_event(data[0])
        elif isinstance(data, dict):
            event = _classify_event(data)
        else:
            event = None
        if event is None:
            raise ValueError(f"No event found for slug: {slug}")
        return event
