"""WebSocket client for Polymarket CLOB market channel — book snapshots."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
import websockets
from websockets import State

from polybot5m.constants import WS_MSG_BOOK, WS_MSG_PRICE_CHANGE, WS_URL

log = structlog.get_logger(__name__)

MsgCallback = Callable[[dict[str, Any]], Awaitable[None]]
_BACKOFF_BASE = 1.0
_BACKOFF_MAX = 60.0
_BACKOFF_JITTER = 0.5


def _ws_is_open(ws: Any) -> bool:
    if ws is None:
        return False
    return ws.state == State.OPEN


class ClobWebSocket:
    def __init__(
        self,
        ws_url: str = WS_URL,
        *,
        on_book: MsgCallback | None = None,
        on_price_change: MsgCallback | None = None,
        on_connection_lost: MsgCallback | None = None,
    ) -> None:
        self._ws_url = ws_url.rstrip("/")
        self._on_book = on_book
        self._on_price_change = on_price_change
        self._on_connection_lost = on_connection_lost
        self._subscribed_assets: set[str] = set()
        self._ws: Any = None
        self._listen_task: asyncio.Task[None] | None = None
        self._running = False
        self._reconnect_attempt = 0
        self.messages_received = 0

    @property
    def connected(self) -> bool:
        return _ws_is_open(self._ws)

    async def connect(self, asset_ids: list[str]) -> None:
        self._running = True
        self._subscribed_assets = set(asset_ids)
        await self._connect_and_subscribe()
        self._listen_task = asyncio.create_task(self._listen_loop(), name="clob-ws-listen")
        log.info("ws_connected", assets=len(asset_ids), url=self._ws_url)

    async def disconnect(self) -> None:
        self._running = False
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
        if _ws_is_open(self._ws):
            await self._ws.close()
        self._ws = None
        log.info("ws_disconnected", total_messages=self.messages_received)

    async def _connect_and_subscribe(self) -> None:
        ws_endpoint = f"{self._ws_url}/ws/market"
        self._ws = await websockets.connect(
            ws_endpoint,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        init_msg = {
            "assets_ids": list(self._subscribed_assets),
            "type": "market",
            "custom_feature_enabled": True,
        }
        await self._send(init_msg)
        self._reconnect_attempt = 0

    async def _send(self, msg: dict[str, Any]) -> None:
        if _ws_is_open(self._ws):
            await self._ws.send(json.dumps(msg))

    async def _listen_loop(self) -> None:
        while self._running:
            try:
                await self._listen()
            except asyncio.CancelledError:
                break
            except (websockets.ConnectionClosed, OSError) as e:
                if not self._running:
                    break
                log.warning("ws_connection_lost", error=str(e), attempt=self._reconnect_attempt)
                if self._on_connection_lost is not None:
                    try:
                        await self._on_connection_lost()
                    except Exception as cb_err:
                        log.error("ws_connection_lost_callback_error", error=str(cb_err))
                await self._reconnect()
            except Exception as e:
                if not self._running:
                    break
                log.error("ws_unexpected_error", error=str(e), attempt=self._reconnect_attempt)
                if self._on_connection_lost is not None:
                    try:
                        await self._on_connection_lost()
                    except Exception as cb_err:
                        log.error("ws_connection_lost_callback_error", error=str(cb_err))
                await self._reconnect()

    async def _listen(self) -> None:
        if not self._ws:
            return
        async for raw in self._ws:
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(data, dict):
                continue
            msg_type = data.get("event_type", data.get("type", ""))
            self.messages_received += 1
            if msg_type == WS_MSG_BOOK and self._on_book:
                try:
                    await self._on_book(data)
                except Exception as e:
                    log.error("ws_callback_error", msg_type=msg_type, error=str(e))
            elif msg_type == WS_MSG_PRICE_CHANGE and self._on_price_change:
                try:
                    await self._on_price_change(data)
                except Exception as e:
                    log.error("ws_callback_error", msg_type=msg_type, error=str(e))

    async def _reconnect(self) -> None:
        self._reconnect_attempt += 1
        delay = min(
            _BACKOFF_BASE * (2 ** (self._reconnect_attempt - 1)),
            _BACKOFF_MAX,
        )
        wait = max(0.1, delay + random.uniform(-_BACKOFF_JITTER, _BACKOFF_JITTER))
        log.info("ws_reconnecting", delay_s=round(wait, 2), attempt=self._reconnect_attempt)
        await asyncio.sleep(wait)
        try:
            if _ws_is_open(self._ws):
                await self._ws.close()
        except Exception:
            pass
        try:
            await self._connect_and_subscribe()
            log.info("ws_reconnected", attempt=self._reconnect_attempt)
        except Exception as e:
            log.error("ws_reconnect_failed", error=str(e), attempt=self._reconnect_attempt)
