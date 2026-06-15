"""Fetch resolution strike (\"Price to beat\") from Polymarket event page — matches Chainlink stream shown in UI."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

POLYMARKET_EVENT_URL = "https://polymarket.com/event"
USER_AGENT = "Mozilla/5.0 (compatible; polybot5m/1.0; +https://polymarket.com)"


def _find_price_to_beat(obj: Any, target_slug: str) -> float | None:
    """Find first object with matching slug and numeric priceToBeat (embedded Next.js data)."""
    if isinstance(obj, dict):
        if obj.get("slug") == target_slug:
            ptb = obj.get("priceToBeat")
            if isinstance(ptb, (int, float)) and ptb > 0:
                return float(ptb)
            markets = obj.get("markets")
            if isinstance(markets, list):
                for mkt in markets:
                    if isinstance(mkt, dict) and mkt.get("slug") == target_slug:
                        ptb = mkt.get("priceToBeat")
                        if isinstance(ptb, (int, float)) and ptb > 0:
                            return float(ptb)
        for v in obj.values():
            r = _find_price_to_beat(v, target_slug)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_price_to_beat(item, target_slug)
            if r is not None:
                return r
    return None


def _price_to_beat_regex_fallback(html: str, target_slug: str) -> float | None:
    """priceToBeat is often not on the event dict in __NEXT_DATA__; scan JSON after this slug's key."""
    for needle in (f'"slug":"{target_slug}"', f'\\"slug\\":\\"{target_slug}\\"'):
        idx = html.find(needle)
        if idx < 0:
            continue
        # First priceToBeat after this slug (same market block) — crypto strike >> 1000 USD
        window = html[idx : idx + 8000]
        for pm in re.finditer(r'"priceToBeat"\s*:\s*([0-9.]+(?:e[+-]?\d+)?)', window):
            try:
                v = float(pm.group(1))
                if v > 1000:
                    return v
            except ValueError:
                continue
    return None


async def fetch_price_to_beat_from_event_page(slug: str) -> float | None:
    """GET polymarket.com/event/{slug}, parse __NEXT_DATA__ JSON for priceToBeat."""
    if not slug or not slug.strip():
        return None
    slug = slug.strip()
    url = f"{POLYMARKET_EVENT_URL}/{slug}"
    try:
        timeout = aiohttp.ClientTimeout(total=25)
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    log.warning("Polymarket event page %s: HTTP %s", slug, resp.status)
                    return None
                html = await resp.text()
    except Exception as e:
        log.warning("Polymarket event fetch %s failed: %s", slug, e)
        return None

    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>([^<]+)</script>', html)
    if m:
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            data = None
        if data is not None:
            p = _find_price_to_beat(data, slug)
            if p is not None:
                return p

    p = _price_to_beat_regex_fallback(html, slug)
    if p is not None:
        return p

    log.warning("Could not parse priceToBeat for slug=%s", slug)
    return None
