"""WebSocket client for Polymarket CLOB user channel — authenticated trade events."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
import websockets
from websockets import State

from polybot5m.constants import WS_MSG_TRADE, WS_URL, WS_USER_PING_INTERVAL_S

log = structlog.get_logger(__name__)

MsgCallback = Callable[[dict[str, Any]], Awaitable[None]]
_BACKOFF_BASE = 1.0
_BACKOFF_MAX = 60.0
_BACKOFF_JITTER = 0.5
_MAX_AUTH_FAILURES = 3
_SESSION_READY_TIMEOUT_S = 5.0


class UserWsAuthError(Exception):
    """User channel rejected credentials or closed before session was ready."""


@dataclass(frozen=True)
class ClobWsCredentials:
    api_key: str
    api_secret: str
    api_passphrase: str

    def valid(self) -> bool:
        return bool(
            (self.api_key or "").strip()
            and (self.api_secret or "").strip()
            and (self.api_passphrase or "").strip()
        )


def normalize_condition_id(condition_id: str) -> str:
    cid = (condition_id or "").strip()
    if cid.startswith("0x") or cid.startswith("0X"):
        return "0x" + cid[2:].lower()
    return cid


def _ws_is_open(ws: Any) -> bool:
    if ws is None:
        return False
    return ws.state == State.OPEN


def credentials_from_clob_client(clob_client: Any | None) -> ClobWsCredentials | None:
    """Prefer API creds loaded on ClobClient (matches signing wallet), not stale .env alone."""
    if clob_client is None:
        return None
    key = (getattr(clob_client, "api_key", "") or "").strip()
    secret = (getattr(clob_client, "api_secret", "") or "").strip()
    passphrase = (getattr(clob_client, "api_passphrase", "") or "").strip()
    if key and secret and passphrase:
        return ClobWsCredentials(api_key=key, api_secret=secret, api_passphrase=passphrase)
    inner = getattr(clob_client, "_client", None)
    creds = getattr(inner, "creds", None) if inner is not None else None
    if creds is None:
        return None
    key = (getattr(creds, "api_key", "") or "").strip()
    secret = (getattr(creds, "api_secret", "") or "").strip()
    passphrase = (getattr(creds, "api_passphrase", "") or "").strip()
    auth = ClobWsCredentials(api_key=key, api_secret=secret, api_passphrase=passphrase)
    return auth if auth.valid() else None


async def probe_user_ws_auth(
    auth: ClobWsCredentials,
    *,
    ws_url: str = WS_URL,
    condition_id: str = "",
) -> tuple[bool, str]:
    """One-shot connect test; returns (ok, detail)."""
    if not auth.valid():
        return False, "missing api_key/secret/passphrase"
    cid = normalize_condition_id(condition_id) if condition_id else ""
    markets = [cid] if cid else []
    endpoint = f"{ws_url.rstrip('/')}/ws/user"
    try:
        async with websockets.connect(
            endpoint,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            init_msg: dict[str, Any] = {
                "type": "user",
                "auth": {
                    "apiKey": auth.api_key,
                    "secret": auth.api_secret,
                    "passphrase": auth.api_passphrase,
                },
            }
            if markets:
                init_msg["markets"] = markets
            await ws.send(json.dumps(init_msg))
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=_SESSION_READY_TIMEOUT_S)
            except asyncio.TimeoutError:
                return True, "connected (no immediate server message)"
            if isinstance(raw, str) and raw.strip() in ("PONG", "PING"):
                return True, "connected (heartbeat)"
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                return True, f"connected (non-json ack: {str(raw)[:120]})"
            if isinstance(data, dict):
                err = data.get("error") or data.get("message") or data.get("errorMsg")
                if err:
                    return False, str(err)
            return True, "connected"
    except websockets.ConnectionClosed as e:
        return False, f"closed code={e.code} reason={e.reason or 'none'} ({e})"
    except Exception as e:
        return False, str(e)


class ClobUserWebSocket:
    """Authenticated user channel: trade events for subscribed condition IDs."""

    def __init__(
        self,
        ws_url: str = WS_URL,
        *,
        auth: ClobWsCredentials,
        on_trade: MsgCallback | None = None,
        ping_interval_s: float = WS_USER_PING_INTERVAL_S,
    ) -> None:
        self._ws_url = ws_url.rstrip("/")
        self._auth = auth
        self._on_trade = on_trade
        self._ping_interval_s = max(1.0, float(ping_interval_s))
        self._markets: list[str] = []
        self._ws: Any = None
        self._listen_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._running = False
        self._reconnect_attempt = 0
        self._auth_failures = 0
        self._fatal_auth_error: str | None = None
        self.trade_messages_received = 0

    @property
    def connected(self) -> bool:
        return _ws_is_open(self._ws)

    @property
    def auth_failed(self) -> bool:
        return self._fatal_auth_error is not None

    async def connect(self, condition_ids: list[str]) -> None:
        self._running = True
        self._markets = [
            normalize_condition_id(c) for c in condition_ids if str(c).strip()
        ]
        await self._connect_and_subscribe()
        self._listen_task = asyncio.create_task(self._listen_loop(), name="clob-user-ws-listen")
        self._ping_task = asyncio.create_task(self._ping_loop(), name="clob-user-ws-ping")
        log.info("user_ws_connected", markets=len(self._markets), url=self._ws_url)

    async def disconnect(self) -> None:
        self._running = False
        for task in (self._ping_task, self._listen_task):
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._ping_task = None
        self._listen_task = None
        if _ws_is_open(self._ws):
            await self._ws.close()
        self._ws = None
        log.info("user_ws_disconnected", trade_messages=self.trade_messages_received)

    async def _connect_and_subscribe(self) -> None:
        ws_endpoint = f"{self._ws_url}/ws/user"
        self._ws = await websockets.connect(
            ws_endpoint,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        init_msg: dict[str, Any] = {
            "type": "user",
            "auth": {
                "apiKey": self._auth.api_key.strip(),
                "secret": self._auth.api_secret.strip(),
                "passphrase": self._auth.api_passphrase.strip(),
            },
        }
        if self._markets:
            init_msg["markets"] = self._markets
        await self._send(init_msg)
        await self._wait_session_ready()
        self._reconnect_attempt = 0

    async def _wait_session_ready(self) -> None:
        """Read first server message or confirm socket stays open (auth ok)."""
        if not self._ws:
            raise UserWsAuthError("websocket not connected")
        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=_SESSION_READY_TIMEOUT_S)
        except asyncio.TimeoutError:
            if _ws_is_open(self._ws):
                return
            raise UserWsAuthError("socket closed during auth handshake") from None
        except websockets.ConnectionClosed as e:
            raise UserWsAuthError(
                f"server closed during auth (code={e.code} reason={e.reason or 'none'})"
            ) from e
        if isinstance(raw, str) and raw.strip() in ("PONG", "PING"):
            return
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(data, dict):
            return
        err = data.get("error") or data.get("message") or data.get("errorMsg")
        if err:
            raise UserWsAuthError(str(err))
        await self._handle_payload(data)

    async def _send(self, msg: dict[str, Any]) -> None:
        if _ws_is_open(self._ws):
            await self._ws.send(json.dumps(msg))

    async def _ping_loop(self) -> None:
        while self._running and not self._fatal_auth_error:
            try:
                await asyncio.sleep(self._ping_interval_s)
                if _ws_is_open(self._ws):
                    await self._ws.send("PING")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("user_ws_ping_error", error=str(e))

    async def _listen_loop(self) -> None:
        while self._running and not self._fatal_auth_error:
            try:
                await self._listen()
            except asyncio.CancelledError:
                break
            except websockets.ConnectionClosed as e:
                if not self._running:
                    break
                await self._on_connection_lost(e)
            except OSError as e:
                if not self._running:
                    break
                log.warning("user_ws_connection_lost", error=str(e), attempt=self._reconnect_attempt)
                await self._reconnect()
            except Exception as e:
                if not self._running:
                    break
                log.error("user_ws_unexpected_error", error=str(e), attempt=self._reconnect_attempt)
                await self._reconnect()

    async def _on_connection_lost(self, e: websockets.ConnectionClosed) -> None:
        log.warning(
            "user_ws_connection_lost",
            error=str(e),
            close_code=e.code,
            close_reason=e.reason,
            attempt=self._reconnect_attempt,
        )
        if e.code in (1006, 1008, 1002, 1003) and self.trade_messages_received == 0:
            self._auth_failures += 1
        if self._auth_failures >= _MAX_AUTH_FAILURES:
            self._fatal_auth_error = (
                "user WebSocket auth failed repeatedly (check CLOB API_KEY/SECRET/PASSPHRASE "
                "match PRIVATE_KEY wallet; use derive_clob_api_creds or refresh API creds)"
            )
            log.error("user_ws_auth_giving_up", failures=self._auth_failures)
            self._running = False
            return
        await self._reconnect()

    async def _handle_payload(self, data: dict[str, Any]) -> None:
        event_type = str(data.get("event_type") or "").lower()
        if event_type == WS_MSG_TRADE and self._on_trade is not None:
            self.trade_messages_received += 1
            try:
                await self._on_trade(data)
            except Exception as e:
                log.error("user_ws_callback_error", msg_type=event_type, error=str(e))
            return
        err = data.get("error") or data.get("message") or data.get("errorMsg")
        if err:
            log.error("user_ws_server_error", payload=data)

    async def _listen(self) -> None:
        if not self._ws:
            return
        async for raw in self._ws:
            if isinstance(raw, str):
                stripped = raw.strip()
                if stripped in ("PONG", "PING"):
                    continue
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                if isinstance(raw, str) and raw.strip():
                    log.debug("user_ws_non_json", sample=raw.strip()[:200])
                continue
            if not isinstance(data, dict):
                continue
            await self._handle_payload(data)

    async def _reconnect(self) -> None:
        if self._fatal_auth_error:
            return
        self._reconnect_attempt += 1
        delay = min(
            _BACKOFF_BASE * (2 ** (self._reconnect_attempt - 1)),
            _BACKOFF_MAX,
        )
        wait = max(0.1, delay + random.uniform(-_BACKOFF_JITTER, _BACKOFF_JITTER))
        log.info("user_ws_reconnecting", delay_s=round(wait, 2), attempt=self._reconnect_attempt)
        await asyncio.sleep(wait)
        try:
            if _ws_is_open(self._ws):
                await self._ws.close()
        except Exception:
            pass
        try:
            await self._connect_and_subscribe()
            self._auth_failures = 0
            log.info("user_ws_reconnected", attempt=self._reconnect_attempt)
        except UserWsAuthError as e:
            self._auth_failures += 1
            log.error("user_ws_auth_failed", error=str(e), failures=self._auth_failures)
            if self._auth_failures >= _MAX_AUTH_FAILURES:
                self._fatal_auth_error = str(e)
                self._running = False
        except Exception as e:
            log.error("user_ws_reconnect_failed", error=str(e), attempt=self._reconnect_attempt)
