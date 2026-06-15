"""Epoch strike (target) vs spot — used for end-sniper oracle gate (spot from Chainlink per executor)."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def _asset_product_id(asset: str) -> str:
    return f"{asset.upper()}-USD"


async def fetch_epoch_strike(
    asset: str,
    epoch_unix: int,
    price_store: dict[str, float],
    provider: str,
    interval_secs: int,
    chainlink_user_id: str,
    chainlink_secret: str,
    chainlink_feed_ids: dict[str, str],
    market_slug: str = "",
) -> float:
    """Strike at epoch: chainlink (Data Streams) when configured; else polymarket UI scrape."""
    product_id = _asset_product_id(asset)
    a = asset.lower()
    p = provider.lower().strip()
    feed_ids_clean = {
        k: v.strip()
        for k, v in chainlink_feed_ids.items()
        if v and str(v).strip()
    }

    # --- Chainlink Data Streams (same family as Polymarket resolution) ---
    if p == "chainlink" and chainlink_user_id and chainlink_secret and feed_ids_clean:
        from polybot5m.data.chainlink_feed import fetch_strikes_at_timestamp

        strikes = await fetch_strikes_at_timestamp(
            chainlink_user_id,
            chainlink_secret,
            feed_ids_clean,
            epoch_unix,
        )
        if a in strikes and strikes[a] > 0:
            return strikes[a]
        log.warning(
            "Chainlink strike empty for %s at epoch=%s — check feed_ids and API creds; trying fallbacks",
            a,
            epoch_unix,
        )

    if p == "polymarket" and market_slug:
        from polybot5m.data.polymarket_strike import fetch_price_to_beat_from_event_page

        pm = await fetch_price_to_beat_from_event_page(market_slug)
        if pm is not None and pm > 0:
            return pm

    # chainlink requested but polymarket fallback (UI price to beat) when slug present
    if p == "chainlink" and market_slug:
        from polybot5m.data.polymarket_strike import fetch_price_to_beat_from_event_page

        pm = await fetch_price_to_beat_from_event_page(market_slug)
        if pm is not None and pm > 0:
            log.info("Using Polymarket page priceToBeat as strike fallback for %s", a)
            return pm

    fallback = price_store.get(product_id, 0.0)
    return fallback if fallback > 0 else 0.0
