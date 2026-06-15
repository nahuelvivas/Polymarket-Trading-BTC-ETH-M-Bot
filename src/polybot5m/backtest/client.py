"""Async PolyBackTest REST client (https://docs.polybacktest.com/)."""

from __future__ import annotations

import asyncio
import random
from typing import Any

import aiohttp


class PolyBacktestClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.polybacktest.com",
        *,
        timeout_s: float = 120.0,
        max_retries: int = 10,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_retries = max(1, max_retries)

    async def _get_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: list[tuple[str, str]] | dict[str, Any],
    ) -> dict[str, Any]:
        """GET JSON with 429 retry (Retry-After or exponential backoff)."""
        for attempt in range(self._max_retries):
            async with session.get(
                url, headers=self._headers, params=params, timeout=self._timeout
            ) as r:
                if r.status == 429:
                    await r.read()
                    ra = r.headers.get("Retry-After")
                    try:
                        wait = float(ra) if ra is not None else 0.0
                        if wait <= 0:
                            raise ValueError
                    except (TypeError, ValueError):
                        wait = min(120.0, 2.0**attempt + random.uniform(0.25, 1.5))
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                return await r.json()
        raise RuntimeError(
            f"PolyBackTest rate limited after {self._max_retries} attempts: {url}"
        )

    async def list_markets(
        self,
        session: aiohttp.ClientSession,
        *,
        coin: str,
        limit: int = 50,
        offset: int = 0,
        market_type: str | None = None,
        resolved: bool | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[str, Any]:
        params: list[tuple[str, str]] = [("coin", coin), ("limit", str(limit)), ("offset", str(offset))]
        if market_type:
            params.append(("market_type", market_type))
        if resolved is not None:
            params.append(("resolved", "true" if resolved else "false"))
        if start_time:
            params.append(("start_time", start_time))
        if end_time:
            params.append(("end_time", end_time))
        url = f"{self._base}/v2/markets"
        return await self._get_json(session, url, params)

    async def get_market(
        self, session: aiohttp.ClientSession, *, coin: str, market_id: str
    ) -> dict[str, Any]:
        url = f"{self._base}/v2/markets/{market_id}"
        return await self._get_json(session, url, [("coin", coin)])

    async def get_snapshots_page(
        self,
        session: aiohttp.ClientSession,
        *,
        coin: str,
        market_id: str,
        limit: int = 1000,
        offset: int = 0,
        include_orderbook: bool = True,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[str, Any]:
        params: list[tuple[str, str]] = [
            ("coin", coin),
            ("limit", str(limit)),
            ("offset", str(offset)),
            ("include_orderbook", "true" if include_orderbook else "false"),
        ]
        if start_time:
            params.append(("start_time", start_time))
        if end_time:
            params.append(("end_time", end_time))
        url = f"{self._base}/v2/markets/{market_id}/snapshots"
        return await self._get_json(session, url, params)

    async def fetch_all_snapshots(
        self,
        session: aiohttp.ClientSession,
        *,
        coin: str,
        market_id: str,
        include_orderbook: bool = True,
        page_delay_s: float = 1.0,
    ) -> dict[str, Any]:
        """Return same shape as one snapshots response, with all pages merged (snapshots ascending by time)."""
        page_limit = 1000
        offset = 0
        merged_snapshots: list[dict[str, Any]] = []
        market: dict[str, Any] | None = None
        total = 0
        page_idx = 0
        while True:
            if page_idx > 0 and page_delay_s > 0:
                await asyncio.sleep(page_delay_s)
            data = await self.get_snapshots_page(
                session,
                coin=coin,
                market_id=market_id,
                limit=page_limit,
                offset=offset,
                include_orderbook=include_orderbook,
            )
            page_idx += 1
            if market is None:
                market = data.get("market") or {}
            total = int(data.get("total", 0))
            chunk = data.get("snapshots") or []
            if not chunk:
                break
            merged_snapshots.extend(chunk)
            offset += len(chunk)
            if len(chunk) < page_limit:
                break
        return {
            "market": market or {},
            "snapshots": merged_snapshots,
            "total": total,
            "limit": len(merged_snapshots),
            "offset": 0,
        }
