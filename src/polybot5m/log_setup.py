"""Tee stdout/stderr to a log file so all prints and Rich output are recorded."""

from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TextIO


class _TeeTextIO:
    """Write to primary (terminal) and secondary (log file)."""

    def __init__(self, primary: TextIO, secondary: TextIO) -> None:
        self._primary = primary
        self._secondary = secondary

    def write(self, data: str) -> int:
        n = self._primary.write(data)
        self._secondary.write(data)
        self._primary.flush()
        self._secondary.flush()
        return n

    def flush(self) -> None:
        self._primary.flush()
        self._secondary.flush()

    def isatty(self) -> bool:
        return getattr(self._primary, "isatty", lambda: False)()

    def fileno(self) -> int:
        return self._primary.fileno()

    @property
    def encoding(self) -> str | None:
        return getattr(self._primary, "encoding", "utf-8")


def _resolve_log_path(log_file: str, use_timestamp: bool) -> Path:
    raw = log_file.strip()
    p = Path(raw).expanduser()
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    if use_timestamp:
        if p.suffix.lower() == ".log":
            return p.parent / f"polybot5m_{ts}.log"
        return p / f"polybot5m_{ts}.log"
    if raw.endswith("/") or raw.endswith("\\"):
        return Path(raw.rstrip("/\\")) / f"polybot5m_{ts}.log"
    return p


def install_run_logging(
    log_file: str,
    *,
    log_append: bool = False,
    log_timestamp_name: bool = False,
    run_kind: str = "run",
) -> Callable[[], None]:
    """
    Tee stdout/stderr to log_file. Call the returned cleanup() in finally to restore streams.

    log_timestamp_name: write logs/polybot5m_YYYYMMDD_HHMMSS.log under given path (dir or .log parent).
    Trailing slash on log_file implies timestamped filename in that directory.
    log_append: when using a fixed path (not timestamp), use append mode; otherwise truncate each run.
    run_kind: banner label (e.g. "backtest" vs default "run").
    """
    if not log_file.strip():
        return lambda: None

    path = _resolve_log_path(log_file, log_timestamp_name or log_file.strip().endswith(("/", "\\")))
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamped = log_timestamp_name or log_file.strip().endswith(("/", "\\"))
    mode = "a" if log_append and not timestamped else "w"
    log_fp: IO[str] = open(path, mode, encoding="utf-8", buffering=1)

    old_out = sys.__stdout__
    old_err = sys.__stderr__
    sys.stdout = _TeeTextIO(old_out, log_fp)
    sys.stderr = _TeeTextIO(old_err, log_fp)

    header = (
        f"\n{'='*60}\n polybot5m {run_kind} started {datetime.now(UTC).isoformat()}Z\n"
        f" log: {path.resolve()}\n{'='*60}\n"
    )
    log_fp.write(header)
    log_fp.flush()
    old_out.write(header)
    old_out.flush()

    def cleanup() -> None:
        try:
            if isinstance(sys.stdout, _TeeTextIO):
                sys.stdout = old_out
            if isinstance(sys.stderr, _TeeTextIO):
                sys.stderr = old_err
            log_fp.close()
        except Exception:
            pass

    return cleanup
