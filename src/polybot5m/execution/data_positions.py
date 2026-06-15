"""Polymarket Data API — user positions for conditional balance fallback."""

from __future__ import annotations

from typing import Any

import aiohttp

DATA_API_URL = "https://data-api.polymarket.com"


async def fetch_outcome_positions_shares(
    session: aiohttp.ClientSession,
    *,
    user_address: str,
    condition_id: str,
    yes_token_id: str,
    no_token_id: str,
    size_threshold: float = 0.0,
) -> tuple[float, float] | None:
    """
    Return (yes_shares, no_shares) for one market from GET /positions, or None on error / no match.
    """
    user = (user_address or "").strip()
    if not user:
        return None

    yes_key = str(yes_token_id).strip()
    no_key = str(no_token_id).strip()
    params: dict[str, Any] = {
        "user": user,
        "sizeThreshold": max(0.0, float(size_threshold)),
        "limit": 100,
    }
    market = (condition_id or "").strip()
    if market:
        params["market"] = market

    url = f"{DATA_API_URL.rstrip('/')}/positions"
    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    if not isinstance(data, list):
        return None

    yes_sz = 0.0
    no_sz = 0.0
    for row in data:
        if not isinstance(row, dict):
            continue
        asset = str(row.get("asset") or "").strip()
        try:
            size = float(row.get("size") or 0)
        except (TypeError, ValueError):
            size = 0.0
        if asset == yes_key:
            yes_sz = size
        elif asset == no_key:
            no_sz = size

    if yes_sz <= 0 and no_sz <= 0:
        return None
    return yes_sz, no_sz
