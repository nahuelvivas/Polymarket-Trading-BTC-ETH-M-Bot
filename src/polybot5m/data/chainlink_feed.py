"""Chainlink Data Streams — REST for strikes (HMAC auth, V3 benchmark decode)."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time

import aiohttp

from polybot5m.constants import CHAINLINK_PRICE_DECIMALS, CHAINLINK_REST_URL

log = logging.getLogger(__name__)


def _generate_auth_headers(
    method: str,
    path: str,
    body: bytes,
    user_id: str,
    secret: str,
) -> dict[str, str]:
    ts = str(int(time.time() * 1000))
    body_hash = hashlib.sha256(body).hexdigest()
    sig_data = f"{method} {path} {body_hash} {user_id} {ts}"
    signature = hmac.new(secret.encode(), sig_data.encode(), hashlib.sha256).hexdigest()
    return {
        "Authorization": user_id,
        "X-Authorization-Timestamp": ts,
        "X-Authorization-Signature-SHA256": signature,
    }


def _decode_v3_benchmark_price(report_hex: str) -> float | None:
    try:
        raw = bytes.fromhex(report_hex.removeprefix("0x"))
    except ValueError:
        return None

    if len(raw) < 224:
        return None

    try:
        blob_offset = int.from_bytes(raw[96:128], "big")
        blob_len = int.from_bytes(raw[blob_offset : blob_offset + 32], "big")
        blob = raw[blob_offset + 32 : blob_offset + 32 + blob_len]
    except Exception:
        return None

    if len(blob) < 224:
        return None

    bp_int = int.from_bytes(blob[192:224], "big", signed=True)
    return bp_int / CHAINLINK_PRICE_DECIMALS


def _price_from_report_payload(data: object) -> float | None:
    if not isinstance(data, dict):
        return None
    report = data.get("report", {})
    full_report_hex = ""
    if isinstance(report, dict):
        full_report_hex = str(report.get("fullReport", "") or "")
    if not full_report_hex and isinstance(data.get("reports"), list) and data["reports"]:
        first = data["reports"][0]
        if isinstance(first, dict):
            full_report_hex = str(first.get("fullReport", "") or "")
    if not full_report_hex:
        return None
    price = _decode_v3_benchmark_price(full_report_hex)
    if price and price > 0:
        return float(price)
    return None


async def _fetch_chainlink_report(
    session: aiohttp.ClientSession,
    path: str,
    user_id: str,
    secret: str,
) -> tuple[object | None, int]:
    """GET a Chainlink report; returns (json body or None, http status)."""
    url = f"{CHAINLINK_REST_URL}{path}"
    headers = _generate_auth_headers("GET", path, b"", user_id, secret)
    try:
        async with session.get(url, headers=headers) as resp:
            status = resp.status
            if status != 200:
                return None, status
            return await resp.json(), status
    except Exception as e:
        log.warning("Chainlink report fetch failed: %s", e)
        return None, 0


async def fetch_latest_spot_price(
    user_id: str,
    secret: str,
    feed_id_hex: str,
    *,
    session: aiohttp.ClientSession | None = None,
) -> float | None:
    """Latest benchmark price for live spot (use /reports/latest, not timestamp lookup)."""
    if not feed_id_hex or not user_id or not secret:
        return None
    path = f"/api/v1/reports/latest?feedID={feed_id_hex}"
    if session is None:
        async with aiohttp.ClientSession() as owned:
            data, status = await _fetch_chainlink_report(owned, path, user_id, secret)
    else:
        data, status = await _fetch_chainlink_report(session, path, user_id, secret)
    if status != 200:
        if status:
            log.warning("Chainlink spot fetch latest: HTTP %s", status)
        return None
    return _price_from_report_payload(data)


async def fetch_strikes_at_timestamp(
    user_id: str,
    secret: str,
    feed_ids: dict[str, str],
    epoch_start_unix: int,
    *,
    lead_delay_s: float = 1.0,
) -> dict[str, float]:
    """Fetch benchmark prices from Chainlink REST at epoch_start_unix. Keys = asset (e.g. btc)."""
    if not feed_ids or not user_id or not secret:
        return {}

    if lead_delay_s > 0:
        await asyncio.sleep(lead_delay_s)

    result: dict[str, float] = {}
    async with aiohttp.ClientSession() as session:
        for asset, hex_id in feed_ids.items():
            path = f"/api/v1/reports?feedID={hex_id}&timestamp={epoch_start_unix}"
            try:
                data, status = await _fetch_chainlink_report(session, path, user_id, secret)
                if status != 200:
                    log.warning("Chainlink strike fetch %s @ %s: HTTP %s", asset, epoch_start_unix, status)
                    continue
                price = _price_from_report_payload(data)
                if price is not None:
                    result[asset] = price
                    log.info("Chainlink strike %s: $%.10f", asset, price)
            except Exception as e:
                log.warning("Chainlink strike fetch %s failed: %s", asset, e)

    return result


async def run_chainlink_spot_loop(
    asset: str,
    feed_id_hex: str,
    user_id: str,
    secret: str,
    product_id: str,
    price_store: dict[str, float],
    stop_event: asyncio.Event,
    poll_interval_s: float,
) -> None:
    """Poll Chainlink reports and mirror spot into price_store[product_id] (e.g. BTC-USD)."""
    interval = max(0.2, float(poll_interval_s))
    async with aiohttp.ClientSession() as session:
        while not stop_event.is_set():
            p = await fetch_latest_spot_price(
                user_id,
                secret,
                feed_id_hex,
                session=session,
            )
            if p is not None and p > 0:
                price_store[product_id] = float(p)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
