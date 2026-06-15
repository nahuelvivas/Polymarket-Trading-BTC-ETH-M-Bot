"""CLI: replay sell strategy on PolyBackTest historical snapshots."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import click
from dotenv import load_dotenv

from polybot5m.config import load_config
from polybot5m.backtest.replay import run_backtest_batch
from polybot5m.log_setup import install_run_logging


@click.command()
@click.option("--config", "-c", default="config/default.yaml", help="YAML config (monitor/redeem + allocation)")
@click.option("--coin", default="btc", type=str, help="PolyBackTest coin (btc, eth, sol)")
@click.option("--market-type", default="5m", type=str, help="Filter: 5m, 15m, 1hr, 4hr, 24hr")
@click.option("--last", "last_n", type=int, default=20, help="Number of newest markets to replay")
@click.option("--base-url", default="https://api.polybacktest.com", show_default=True)
@click.option(
    "--api-key",
    default="",
    help="PolyBackTest API key (default: env POLYBACKTEST_API_KEY)",
)
@click.option("--include-orderbook/--no-orderbook", default=True, help="Use order book bids when present")
@click.option(
    "--resolved-only/--include-unresolved",
    default=True,
    help="List only resolved markets (needed for realized PnL)",
)
@click.option("--out-dir", default="exports", type=click.Path(), help="JSON + paper_summary CSV/JSON")
@click.option(
    "--market-delay",
    type=float,
    default=10.0,
    show_default=True,
    help="Seconds to wait between each market_id (reduces 429 rate limits)",
)
@click.option(
    "--page-delay",
    type=float,
    default=1.0,
    show_default=True,
    help="Seconds between snapshot pagination requests for the same market",
)
@click.option(
    "--realistic-sim/--no-realistic-sim",
    default=None,
    help="Enable realistic execution sim (latency, VWAP, fees, risk). Overrides config backtest.simulation.enabled.",
)
@click.option(
    "--log-file",
    type=str,
    default=None,
    help="Tee stdout/stderr to this path (default: backtest.log_file from YAML; empty string disables)",
)
@click.option("--log-append", is_flag=True, help="Append to log file (fixed path only)")
@click.option(
    "--log-timestamp-name",
    is_flag=True,
    help="Use polybot5m_YYYYMMDD_HHMMSS.log under log path (see log_setup)",
)
@click.option("--quiet", "-q", is_flag=True, help="Less stdout")
def main(
    config: str,
    coin: str,
    market_type: str,
    last_n: int,
    base_url: str,
    api_key: str,
    include_orderbook: bool,
    resolved_only: bool,
    out_dir: str,
    market_delay: float,
    page_delay: float,
    realistic_sim: bool | None,
    log_file: str | None,
    log_append: bool,
    log_timestamp_name: bool,
    quiet: bool,
) -> None:
    """Backtest: fetch latest N markets and replay market snapshots."""
    # Load .env before reading POLYBACKTEST_API_KEY (same search order as load_config)
    cfg_path = Path(config).resolve()
    for base in (cfg_path.parent.parent, cfg_path.parent, Path.cwd()):
        env_file = base / ".env"
        if env_file.is_file():
            load_dotenv(dotenv_path=str(env_file), override=True)
            break
    load_dotenv(override=True)

    key = (api_key or os.environ.get("POLYBACKTEST_API_KEY", "")).strip()
    if not key:
        raise click.ClickException(
            "Missing API key: set POLYBACKTEST_API_KEY or pass --api-key (see https://docs.polybacktest.com/api-keys )"
        )

    settings = load_config(config)
    if realistic_sim is not None:
        sim = settings.backtest.simulation.model_copy(update={"enabled": realistic_sim})
        bt = settings.backtest.model_copy(update={"simulation": sim})
        settings = settings.model_copy(update={"backtest": bt})

    bt = settings.backtest
    eff_log = (log_file if log_file is not None else bt.log_file).strip()
    eff_append = bool(bt.log_append or log_append)
    eff_ts_name = bool(bt.log_timestamp_name or log_timestamp_name)

    cleanup_log = install_run_logging(
        eff_log,
        log_append=eff_append,
        log_timestamp_name=eff_ts_name,
        run_kind="backtest",
    )
    try:
        out = Path(out_dir).resolve()
        asyncio.run(
            run_backtest_batch(
                api_key=key,
                base_url=base_url.rstrip("/"),
                coin=coin.strip().lower(),
                market_type=market_type.strip(),
                last_n=max(1, last_n),
                settings=settings,
                include_orderbook=include_orderbook,
                resolved_only=resolved_only,
                verbose=not quiet,
                out_dir=out,
                market_delay_s=max(0.0, market_delay),
                page_delay_s=max(0.0, page_delay),
            )
        )
    finally:
        cleanup_log()
