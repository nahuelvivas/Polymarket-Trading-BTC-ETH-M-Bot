"""Public CLOB REST API — fetch order book without authentication."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from polybot5m.constants import CLOB_API_URL


async def fetch_book_as_ws_shape(
    session: aiohttp.ClientSession,
    token_id: str,
    *,
    base_url: str = CLOB_API_URL,
    timeout_s: float = 3.0,
) -> dict[str, Any]:
    """REST book in the same shape as CLOB market WebSocket `book` messages."""
    url = f"{base_url.rstrip('/')}/book"
    params = {"token_id": token_id}
    req_timeout = aiohttp.ClientTimeout(total=max(0.5, float(timeout_s)))
    async with session.get(url, params=params, timeout=req_timeout) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return {
        "asset_id": token_id,
        "bids": data.get("bids", []),
        "asks": data.get("asks", []),
    }


async def poll_books_into_store(
    session: aiohttp.ClientSession,
    store: Any,
    yes_token_id: str,
    no_token_id: str,
    *,
    base_url: str = CLOB_API_URL,
    timeout_s: float = 3.0,
) -> bool:
    """REST poll YES/NO books into an InMemoryOrderbookStore. True if both sides have levels."""
    try:
        yes_msg, no_msg = await asyncio.gather(
            fetch_book_as_ws_shape(
                session, yes_token_id, base_url=base_url, timeout_s=timeout_s
            ),
            fetch_book_as_ws_shape(
                session, no_token_id, base_url=base_url, timeout_s=timeout_s
            ),
        )
    except Exception:
        return False
    store.apply_book_msg(yes_msg)
    store.apply_book_msg(no_msg)
    book_yes = store.book_as_executor_view(yes_token_id)
    book_no = store.book_as_executor_view(no_token_id)
    bids_yes = getattr(book_yes, "bids", None) or []
    asks_yes = getattr(book_yes, "asks", None) or []
    bids_no = getattr(book_no, "bids", None) or []
    asks_no = getattr(book_no, "asks", None) or []
    return bool((bids_yes or asks_yes) and (bids_no or asks_no))


async def bootstrap_books(
    session: aiohttp.ClientSession,
    token_ids: list[str],
    *,
    base_url: str = CLOB_API_URL,
) -> list[dict[str, Any]]:
    """One-shot full books for a token list (optional bootstrap; monitor uses poll_books_into_store)."""
    out: list[dict[str, Any]] = []
    for tid in token_ids:
        try:
            msg = await fetch_book_as_ws_shape(session, tid, base_url=base_url)
            out.append(msg)
        except Exception:
            continue
    return out


async def fetch_order_book(token_id: str, base_url: str = CLOB_API_URL) -> Any:
    """
    Fetch order book for a token via public REST API.
    No credentials required.

    Returns object with .bids and .asks (list of {price, size}).
    Compatible with executor._best_bid_from_book.
    """
    url = f"{base_url.rstrip('/')}/book"
    params = {"token_id": token_id}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

    class _Book:
        def __init__(self, data: dict) -> None:
            bids_raw = data.get("bids", [])
            asks_raw = data.get("asks", [])
            self.bids = [
                {"price": float(b.get("price", 0)), "size": float(b.get("size", 0))}
                if isinstance(b, dict)
                else b
                for b in bids_raw
            ]
            self.asks = [
                {"price": float(a.get("price", 0)), "size": float(a.get("size", 0))}
                if isinstance(a, dict)
                else a
                for a in asks_raw
            ]

    return _Book(data)
