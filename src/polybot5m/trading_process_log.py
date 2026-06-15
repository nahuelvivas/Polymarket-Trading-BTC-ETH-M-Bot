"""Append-only NDJSON for trading: `trades` mode = TRADE rows only; `full` = ticks + cycle events."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_iso_z() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def round_t_minus_s(remaining_s: float | None) -> float | None:
    """Seconds until epoch end, millisecond precision (3 decimals); None if unknown."""
    if remaining_s is None:
        return None
    try:
        t = float(remaining_s)
    except (TypeError, ValueError):
        return None
    if t < 0:
        t = 0.0
    return round(t, 3)


def format_t_minus_suffix(remaining_s: float | None) -> str:
    """Console suffix aligned with wave logs, e.g. `` ⏰t_minus=41.345s``."""
    t = round_t_minus_s(remaining_s)
    if t is None:
        return ""
    return f" ⏰t_minus={t:.3f}s"


def enrich_strategy_row_t_minus(
    row: dict[str, Any],
    *,
    remaining_s: float | None = None,
) -> dict[str, Any]:
    """Ensure JSONL strategy rows include ``t_minus_s`` (3 decimal places)."""
    out = dict(row)
    if "t_minus_s" in out:
        t = round_t_minus_s(float(out["t_minus_s"]))
        if t is not None:
            out["t_minus_s"] = t
        return out
    t = round_t_minus_s(remaining_s)
    if t is None and "remaining_s" in out:
        t = round_t_minus_s(float(out["remaining_s"]))
    if t is not None:
        out["t_minus_s"] = t
    return out


def spot_minus_strike_usd(
    strike: float | None,
    spot: float | None,
) -> float | None:
    """Spot (chain/index) minus epoch strike in USD; None if either value is missing or non-positive."""
    if strike is None or spot is None:
        return None
    st, sp = float(strike), float(spot)
    if st <= 0 or sp <= 0:
        return None
    return sp - st


def format_spot_minus_strike_for_log(strike: float | None, spot: float | None) -> str:
    """Suffix fragment like ` spot_minus_strike=+12.345678` for monitor stdout lines; empty if unknown."""
    d = spot_minus_strike_usd(strike, spot)
    if d is None:
        return ""
    s = "+" if d >= 0 else ""
    return f" spot_minus_strike={s}{d:.6f}"


class SpotMinusStrikeDifferenceRate:
    """Rolling ratio: current spot_minus_strike / mean over the last `lookback_s` seconds."""

    __slots__ = ("_hist", "_lookback")

    def __init__(self, lookback_s: float) -> None:
        self._lookback = max(0.0, float(lookback_s))
        self._hist: deque[tuple[float, float]] = deque()

    def record(self, now_mono: float, sms: float | None) -> float | None:
        if self._lookback <= 0 or sms is None:
            return None
        now = float(now_mono)
        self._hist.append((now, float(sms)))
        cutoff = now - self._lookback
        while self._hist and self._hist[0][0] < cutoff:
            self._hist.popleft()
        if not self._hist:
            return None
        avg = sum(v for _, v in self._hist) / len(self._hist)
        if abs(avg) < 1e-12:
            return None
        return float(sms) / avg


def format_difference_rate_for_log(symbol: str, rate: float | None) -> str:
    """Suffix like ` difference_rate_btc = 1.03` for [STRIKE_SPOT] lines; empty when unavailable."""
    if rate is None:
        return ""
    sym = str(symbol).lower().strip()
    if not sym:
        return ""
    return f" difference_rate_{sym} = {float(rate):.2f}"


class SpotMinusStrikeEpochAverage:
    """Running mean of spot_minus_strike since monitor / epoch start."""

    __slots__ = ("_count", "_sum")

    def __init__(self) -> None:
        self._sum = 0.0
        self._count = 0

    def record(self, sms: float | None) -> float | None:
        if sms is not None:
            self._sum += float(sms)
            self._count += 1
        return self.average()

    def average(self) -> float | None:
        if self._count <= 0:
            return None
        return self._sum / self._count


class SpotMinusStrikeEpochExtrema:
    """Running min/max of spot_minus_strike since monitor / epoch start."""

    __slots__ = ("_min", "_max", "_has")

    def __init__(self) -> None:
        self._min: float | None = None
        self._max: float | None = None
        self._has = False

    def record(self, sms: float | None) -> tuple[float | None, float | None]:
        """Update extrema when ``sms`` is valid; return (min, max) after this sample."""
        if sms is None:
            return self.minimum(), self.maximum()
        v = float(sms)
        if not self._has:
            self._min = v
            self._max = v
            self._has = True
        else:
            if v > float(self._max):
                self._max = v
            if v < float(self._min):
                self._min = v
        return self.minimum(), self.maximum()

    def minimum(self) -> float | None:
        return self._min if self._has else None

    def maximum(self) -> float | None:
        return self._max if self._has else None


def format_average_spot_minus_for_log(symbol: str, avg: float | None) -> str:
    """Suffix like ` averge_spot_minus_btc=-50.30974` for [STRIKE_SPOT] lines."""
    if avg is None:
        return ""
    sym = str(symbol).lower().strip()
    if not sym:
        return ""
    return f" averge_spot_minus_{sym}={float(avg):.5f}"


def format_btc_wave_header(
    t_minus_s: float,
    *,
    max_spot_minus_strike_btc: float | None = None,
    min_spot_minus_strike_btc: float | None = None,
) -> str:
    """Merged-wave header: t_minus plus BTC spot-minus-strike epoch extrema."""
    t = round_t_minus_s(t_minus_s)
    base = f"⏰t_minus={t:.3f}s" if t is not None else "⏰t_minus=—"
    max_txt = f"{float(max_spot_minus_strike_btc):.4f}" if max_spot_minus_strike_btc is not None else "—"
    min_txt = f"{float(min_spot_minus_strike_btc):.4f}" if min_spot_minus_strike_btc is not None else "—"
    return (
        f"{base}    📌max_spot_minus_strike_btc = {max_txt}   "
        f"📌min_spot_minus_strike_btc = {min_txt}"
    )


@dataclass
class TradingCycleKey:
    """Identifies one symbol's 5m (or other) epoch window within a bot run."""

    run_cycle: int
    symbol: str
    epoch: str
    slug: str
    epoch_start_unix: int
    epoch_end_unix: int
    condition_id: str = ""
    paper_trading: bool = False
    yes_token_id: str = ""
    no_token_id: str = ""

    @property
    def cycle_id(self) -> str:
        sym = str(self.symbol).lower().strip()
        ep = str(self.epoch).strip()
        return f"{sym}/{ep}/{int(self.epoch_start_unix)}"


class TradingCycleJournal:
    """
    Append-only NDJSON writer scoped to one market cycle.

    Every row includes ``cycle_id``, ``run_cycle``, ``slug``, ``seq``, etc. so
    trades can be grouped when reading ``trading_process.jsonl``.
    """

    def __init__(self, path: Path | None, key: TradingCycleKey, *, tag: str = "") -> None:
        self.path = path
        self.key = key
        self.tag = tag
        self._seq = 0

    def update_key(self, **fields: Any) -> None:
        for name, value in fields.items():
            if hasattr(self.key, name):
                setattr(self.key, name, value)

    def base_fields(self, *, compact: bool = False) -> dict[str, Any]:
        k = self.key
        base: dict[str, Any] = {
            "run_cycle": int(k.run_cycle),
            "symbol": str(k.symbol).lower().strip(),
            "slug": k.slug,
            "paper_trading": bool(k.paper_trading),
            "tag": self.tag,
        }
        if compact:
            return base
        return {
            "cycle_id": k.cycle_id,
            **base,
            "epoch": str(k.epoch).strip(),
            "epoch_start_unix": int(k.epoch_start_unix),
            "epoch_end_unix": int(k.epoch_end_unix),
            "epoch_start_utc": datetime.fromtimestamp(
                int(k.epoch_start_unix), tz=UTC
            ).isoformat().replace("+00:00", "Z"),
            "epoch_end_utc": datetime.fromtimestamp(
                int(k.epoch_end_unix), tz=UTC
            ).isoformat().replace("+00:00", "Z"),
            "condition_id": k.condition_id,
        }

    def append(self, row: dict[str, Any], *, compact: bool = False) -> int:
        """Write one NDJSON line; return monotonic ``seq`` within this cycle."""
        if self.path is None:
            return 0
        self._seq += 1
        out = {
            **self.base_fields(compact=compact),
            "seq": self._seq,
            "ts_utc": utc_iso_z(),
            **row,
        }
        append_trading_jsonl(self.path, out)
        return self._seq

    def append_strategy(self, row: dict[str, Any]) -> int:
        """Compact cycle header + row (reserved for strategy-specific events)."""
        return self.append(row, compact=True)

    def log_cycle_start(self, **extra: Any) -> None:
        self.append({"event": "CYCLE_START", **extra}, compact=True)

    def log_cycle_end(self, **extra: Any) -> None:
        self.append({"event": "CYCLE_END", **extra}, compact=True)

    def log_user_trade(self, row: dict[str, Any]) -> None:
        """USER_TRADE from CLOB websocket during this cycle."""
        self.append(enrich_strategy_row_t_minus({"event": "USER_TRADE", **row}))


def build_btc_strategy_fields(
    *,
    spot_minus_strike_btc: float | None = None,
    averge_spot_minus_btc: float | None = None,
    difference_rate_btc: float | None = None,
    max_spot_minus_strike_btc: float | None = None,
    min_spot_minus_strike_btc: float | None = None,
    btc_yes_best_ask: float = 0.0,
    btc_yes_best_bid: float = 0.0,
    btc_no_best_ask: float = 0.0,
    btc_no_best_bid: float = 0.0,
) -> dict[str, Any]:
    """BTC strike/spot metrics and top-of-book for strategy JSONL rows."""
    out: dict[str, Any] = {}
    if spot_minus_strike_btc is not None:
        out["spot_minus_strike_btc"] = round(float(spot_minus_strike_btc), 6)
    if averge_spot_minus_btc is not None:
        out["averge_spot_minus_btc"] = round(float(averge_spot_minus_btc), 5)
    if difference_rate_btc is not None:
        out["difference_rate_btc"] = round(float(difference_rate_btc), 2)
    if max_spot_minus_strike_btc is not None:
        out["max_spot_minus_strike_btc"] = round(float(max_spot_minus_strike_btc), 4)
    if min_spot_minus_strike_btc is not None:
        out["min_spot_minus_strike_btc"] = round(float(min_spot_minus_strike_btc), 4)
    if btc_yes_best_ask > 0:
        out["btc_yes_best_ask"] = round(float(btc_yes_best_ask), 4)
    if btc_yes_best_bid > 0:
        out["btc_yes_best_bid"] = round(float(btc_yes_best_bid), 4)
    if btc_no_best_ask > 0:
        out["btc_no_best_ask"] = round(float(btc_no_best_ask), 4)
    if btc_no_best_bid > 0:
        out["btc_no_best_bid"] = round(float(btc_no_best_bid), 4)
    return out


@dataclass
class BtcStrategySnapshot:
    """Latest BTC strike/spot metrics and BTC market top-of-book for strategy JSONL."""

    spot_minus_strike_btc: float | None = None
    averge_spot_minus_btc: float | None = None
    difference_rate_btc: float | None = None
    max_spot_minus_strike_btc: float | None = None
    min_spot_minus_strike_btc: float | None = None
    yes_best_ask: float = 0.0
    yes_best_bid: float = 0.0
    no_best_ask: float = 0.0
    no_best_bid: float = 0.0
    _sms_extrema: SpotMinusStrikeEpochExtrema = field(
        default_factory=SpotMinusStrikeEpochExtrema,
        repr=False,
    )

    def record_spot_minus_strike_btc(self, sms: float | None) -> None:
        """Update epoch running min/max from a new spot_minus_strike sample."""
        lo, hi = self._sms_extrema.record(sms)
        self.min_spot_minus_strike_btc = lo
        self.max_spot_minus_strike_btc = hi

    def reset_epoch(self) -> None:
        """Clear per-epoch strike/spot accumulators (new 5m window)."""
        self.spot_minus_strike_btc = None
        self.averge_spot_minus_btc = None
        self.difference_rate_btc = None
        self.max_spot_minus_strike_btc = None
        self.min_spot_minus_strike_btc = None
        self._sms_extrema = SpotMinusStrikeEpochExtrema()

    def as_log_fields(self) -> dict[str, Any]:
        return build_btc_strategy_fields(
            spot_minus_strike_btc=self.spot_minus_strike_btc,
            averge_spot_minus_btc=self.averge_spot_minus_btc,
            difference_rate_btc=self.difference_rate_btc,
            max_spot_minus_strike_btc=self.max_spot_minus_strike_btc,
            min_spot_minus_strike_btc=self.min_spot_minus_strike_btc,
            btc_yes_best_ask=self.yes_best_ask,
            btc_yes_best_bid=self.yes_best_bid,
            btc_no_best_ask=self.no_best_ask,
            btc_no_best_bid=self.no_best_bid,
        )


def strategy_phase_from_event(event: str) -> str:
    """Map strategy event name prefix to a normalized phase label."""
    ev = str(event or "").upper()
    for prefix in ("BUY1", "BUY2", "RISK1", "RISK2", "RISK3", "SELL1"):
        if ev == prefix or ev.startswith(prefix + "_"):
            return prefix.lower()
    return ""


def resolve_trading_process_path(settings: Any) -> Path | None:
    """Read liquidity_maker.trading_process_jsonl."""
    lm = getattr(settings, "liquidity_maker", None)
    if lm is None:
        return None
    raw = getattr(lm, "trading_process_jsonl", "") or ""
    raw = str(raw).strip()
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


def append_trading_jsonl(path: str | Path | None, row: dict[str, Any]) -> None:
    if path is None:
        return
    raw = str(path).strip()
    if not raw:
        return
    p = Path(raw).expanduser()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(row, default=str) + "\n"
        with open(p, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        print(f"  trading_process_jsonl write error: {e}")
