"""CLI for Polymarket monitor → redeem bot."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click
from rich.console import Console

from polybot5m.config import load_config
from polybot5m.constants import INTERVAL_SECONDS
from polybot5m.log_setup import install_run_logging

console = Console()


def _utc_now() -> datetime:
    return datetime.now(UTC)


@click.group()
@click.option("--config", "-c", default="config/default.yaml", help="Config file path")
@click.pass_context
def cli(ctx: click.Context, config: str) -> None:
    """Polymarket monitor order book → redeem."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


@cli.command()
@click.option("--dry-run", is_flag=True, help="Simulate without redeeming on chain")
@click.option(
    "--paper",
    "paper_trading",
    is_flag=True,
    help="Paper mode: skip on-chain redeem (monitor + export only)",
)
@click.option("--cycles", type=int, default=None, help="Max cycles (0 = run forever)")
@click.option("--log-file", type=str, default=None, help="Tee stdout/stderr to this path (empty disables)")
@click.option("--log-append", is_flag=True, help="Append to log file instead of truncating")
@click.option("--log-timestamp-name", is_flag=True, help="Use polybot5m_YYYYMMDD_HHMMSS.log under log path")
@click.pass_context
def run(
    ctx: click.Context,
    dry_run: bool,
    paper_trading: bool,
    cycles: int | None,
    log_file: str | None,
    log_append: bool,
    log_timestamp_name: bool,
) -> None:
    """Run: monitor order book → redeem, repeat."""
    settings = load_config(ctx.obj["config_path"])

    if dry_run:
        settings.bot.dry_run = True
    if paper_trading:
        settings.bot.paper_trading = True
    if settings.bot.paper_trading:
        settings.bot.dry_run = False
    if cycles is not None:
        settings.liquidity_maker.cycles = cycles
    if log_file is not None:
        settings.bot.log_file = log_file
    if log_append:
        settings.bot.log_append = True
    if log_timestamp_name:
        settings.bot.log_timestamp_name = True

    cleanup_log = install_run_logging(
        settings.bot.log_file,
        log_append=settings.bot.log_append,
        log_timestamp_name=settings.bot.log_timestamp_name,
    )
    try:
        _run_with_logging(settings)
    finally:
        cleanup_log()


def _run_with_logging(settings) -> None:
    lm = settings.liquidity_maker
    console.print("[bold green]Polymarket monitor → redeem[/bold green]")
    from polybot5m.schedule import describe_schedule

    console.print(f"  dry_run={settings.bot.dry_run}")
    console.print(f"  paper_trading={settings.bot.paper_trading}")
    console.print(f"  {describe_schedule(settings.schedule)}")
    console.print(
        f"  monitor_ws_eval={lm.monitor_poll_interval_s}s "
        f"user_ws={getattr(lm, 'monitor_user_ws_enabled', True)} "
        f"balance_poll={getattr(lm, 'monitor_balance_poll_interval_s', 0.3)}s "
        f"log={lm.monitor_log_interval_s}s "
        f"verbose_last={lm.monitor_verbose_seconds_before_end}s "
        f"strike_spot_log={lm.log_strike_spot_interval_s}s "
        f"redeem_enabled={lm.redeem_enabled} "
        f"post_redeem_monitor={lm.post_redeem_monitor_seconds:g}s",
    )
    if settings.bot.log_file.strip():
        console.print(
            f"  log_file={settings.bot.log_file!r} append={settings.bot.log_append} "
            f"timestamp_name={settings.bot.log_timestamp_name}"
        )
    tpj = (lm.trading_process_jsonl or "").strip()
    if tpj:
        console.print(
            f"  trading_process_jsonl={tpj!r} mode={getattr(lm, 'trading_process_log_mode', 'trades')} "
            f"interval_s={lm.trading_process_log_interval_s} stdout={lm.trading_process_log_stdout} "
            f"(strategy logging only)",
        )
    asyncio.run(_run_loop(settings))


async def _run_loop(settings) -> None:
    import os

    from polybot5m.constants import RELAYER_URL
    from polybot5m.engine import run_all_markets
    from polybot5m.execution.deposit_wallet import (
        ensure_deposit_wallet_deployed,
        resolve_deposit_wallet_address,
    )

    exe = settings.execution
    lm = settings.liquidity_maker
    from polybot5m.constants import (
        CTF_COLLATERAL_ADAPTER_ADDRESS,
        NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS,
        collateral_adapter_address,
    )

    ctf_ad = collateral_adapter_address(
        neg_risk=False,
        override=getattr(exe, "ctf_collateral_adapter", "") or "",
    )
    neg_ad = collateral_adapter_address(
        neg_risk=True,
        override=getattr(exe, "neg_risk_ctf_collateral_adapter", "") or "",
    )
    print(f"  CtfCollateralAdapter: {ctf_ad}")
    print(f"  NegRiskCtfCollateralAdapter: {neg_ad}")
    if (
        ctf_ad.lower() != CTF_COLLATERAL_ADAPTER_ADDRESS.lower()
        or neg_ad.lower() != NEG_RISK_CTF_COLLATERAL_ADAPTER_ADDRESS.lower()
    ):
        print("  NOTE: custom adapter override(s) in effect")

    deposit_wallet_address = ""
    if (exe.private_key or "").strip():
        deposit_wallet_address = resolve_deposit_wallet_address(
            private_key=exe.private_key,
            chain_id=exe.chain_id,
            configured_funder=exe.funder or "",
        )
        print(f"  Deposit wallet: {deposit_wallet_address}")
        if (
            not settings.bot.paper_trading
            and not settings.bot.dry_run
            and bool(getattr(exe, "auto_deploy_deposit_wallet", True))
        ):
            builder_key = os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_KEY", "")
            builder_secret = os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_SECRET", "")
            builder_passphrase = os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_API_PASSPHRASE", "")
            if builder_key and builder_secret and builder_passphrase:
                try:
                    deploy_tx = await asyncio.to_thread(
                        ensure_deposit_wallet_deployed,
                        relayer_url=RELAYER_URL,
                        chain_id=exe.chain_id,
                        private_key=exe.private_key,
                        builder_api_key=builder_key,
                        builder_api_secret=builder_secret,
                        builder_api_passphrase=builder_passphrase,
                        wallet_address=deposit_wallet_address,
                    )
                    if deploy_tx:
                        print(f"  Deployed deposit wallet tx={deploy_tx}")
                except Exception as e:
                    print(f"  WARNING: deposit wallet deploy check failed ({e})")
            else:
                print("  WARNING: builder relayer creds missing; skipping deposit wallet deploy check")

    clob_client = None
    paper_session = None
    if settings.bot.paper_trading:
        from polybot5m.execution.paper_exchange import paper_session_from_bot_config

        paper_session = paper_session_from_bot_config(settings.bot)
        print(
            f"  PAPER SESSION: bankroll={paper_session.starting_usdc:.2f} USDC "
            f"fee_bps={paper_session.fee_bps:g}",
        )
    if (exe.private_key or "").strip() and not settings.bot.paper_trading:
        try:
            from polybot5m.execution.clob_client import ClobClient

            clob_client = ClobClient(
                private_key=exe.private_key,
                api_key=exe.api_key or os.getenv("POLYBOT5MBES_EXECUTION__API_KEY", ""),
                api_secret=exe.api_secret or os.getenv("POLYBOT5MBES_EXECUTION__API_SECRET", ""),
                api_passphrase=exe.api_passphrase
                or os.getenv("POLYBOT5MBES_EXECUTION__API_PASSPHRASE", ""),
                host=settings.api.clob_url,
                chain_id=exe.chain_id,
                signature_type=int(getattr(exe, "signature_type", 3) or 3),
                funder=deposit_wallet_address or exe.funder or "",
                derive_api_creds=bool(getattr(exe, "derive_clob_api_creds", True)),
                rpc_url=exe.rpc_url or "",
                builder_code=(
                    getattr(exe, "builder_code", "")
                    or os.getenv("POLYBOT5MBES_EXECUTION__BUILDER_CODE", "")
                ).strip(),
            )
        except Exception as e:
            print(f"  WARNING: CLOB client init failed ({e}); neg_risk/allowance sync disabled")
        if clob_client is not None and not (
            getattr(clob_client, "api_key", "")
            and getattr(clob_client, "api_secret", "")
            and getattr(clob_client, "api_passphrase", "")
        ):
            print(
                "  WARNING: CLOB API creds missing after init — user WebSocket will fail. "
                "Set POLYBOT5MBES_EXECUTION__API_KEY/SECRET/PASSPHRASE in .env or fix "
                "derive_clob_api_creds (see startup py_clob 400 errors)."
            )
        elif (
            clob_client is not None
            and bool(getattr(lm, "monitor_user_ws_enabled", True))
            and not settings.bot.paper_trading
        ):
            from polybot5m.data.clob_user_ws import credentials_from_clob_client, probe_user_ws_auth

            ws_auth = credentials_from_clob_client(clob_client)
            if ws_auth is None:
                print("  WARNING: cannot probe user WebSocket — no CLOB API creds on client")
            else:
                ok, detail = await probe_user_ws_auth(ws_auth, ws_url=settings.api.ws_url)
                if ok:
                    print(f"  user WebSocket probe OK ({detail})")
                else:
                    print(
                        f"  WARNING: user WebSocket probe FAILED: {detail}. "
                        "Refresh POLYBOT5MBES_EXECUTION__API_* from the same wallet as PRIVATE_KEY "
                        "(Polymarket → Settings → API Keys), or set derive_clob_api_creds: true "
                        "and remove stale API_* from .env."
                    )

    if not settings.bot.paper_trading and bool(getattr(lm, "redeem_enabled", True)):
        from polybot5m.execution.redeem import _load_builder_creds_pool
        from polybot5m.execution.relayer_creds import validated_builder_cred_pool

        pool = _load_builder_creds_pool()
        if not pool:
            print(
                "  WARNING: no builder relayer creds — redeem will fail. "
                "Set POLYBOT5MBES_EXECUTION__BUILDER_API_KEY/SECRET/PASSPHRASE "
                "(Builder keys from Polymarket, not CLOB trading keys)."
            )
        elif (exe.private_key or "").strip():
            wallet = resolve_deposit_wallet_address(
                private_key=exe.private_key,
                chain_id=int(exe.chain_id),
                configured_funder=deposit_wallet_address or exe.funder or "",
            )
            valid = validated_builder_cred_pool(
                pool,
                private_key=exe.private_key,
                chain_id=int(exe.chain_id),
                relayer_url=RELAYER_URL,
                deposit_wallet_address=wallet,
            )
            print(
                f"  builder relayer creds: {len(valid)}/{len(pool)} authorized for redeem "
                f"(wallet={wallet[:10]}…)",
            )
        else:
            k0 = pool[0][0]
            print(f"  builder relayer creds loaded: {len(pool)} key(s), first={k0[:8]}...")

    targets = lm.markets
    if not targets:
        print("ERROR: No markets configured in liquidity_maker.markets")
        return

    print(f"Markets: {[(t.symbol, t.epoch) for t in targets]}")

    export_path: Path | None = None
    if lm.export_dir:
        export_path = Path(lm.export_dir).resolve() / "liquidity_maker_activity.json"

    max_cycles = lm.cycles
    cycle_count = 0
    redeem_scheduler = None
    if bool(getattr(lm, "redeem_enabled", True)):
        from polybot5m.execution.redeem_scheduler import RedeemScheduler

        redeem_scheduler = RedeemScheduler(settings)
        redeem_scheduler.start()
        gap = float(getattr(lm, "redeem_per_symbol_gap_seconds", 10.0) or 10.0)
        print(
            f"  redeem queue: serial relayer, delay={lm.redeem_delay_seconds}s "
            f"+ per-symbol gap={gap:g}s (btc, eth)",
        )

    from polybot5m.schedule import wait_for_trading_window

    try:
        while max_cycles == 0 or cycle_count < max_cycles:
            await wait_for_trading_window(settings.schedule)

            min_interval = min(INTERVAL_SECONDS.get(t.epoch, 300) for t in targets)
            now = _utc_now()
            epoch_ts = (int(now.timestamp()) // min_interval) * min_interval
            epoch_end = datetime.fromtimestamp(epoch_ts + min_interval, tz=UTC)

            print(f"\n{'='*50}")
            print(f"CYCLE {cycle_count + 1}  epoch_end={epoch_end.isoformat()}")
            print(f"{'='*50}")
            summaries = await run_all_markets(
                targets,
                settings,
                export_path,
                redeem_scheduler=redeem_scheduler,
                clob_client=clob_client,
                paper_session=paper_session,
                run_cycle=cycle_count + 1,
            )

            for s in summaries:
                symbol = s.get("symbol", "?")
                epoch = s.get("epoch", "?")
                err = s.get("error")
                if err:
                    print(f"  [{symbol}/{epoch}] ERROR: {err}")
                else:
                    redeem_tx = s.get("redeem_tx", "")
                    if redeem_tx:
                        print(f"  [{symbol}/{epoch}] OK  redeem={str(redeem_tx)[:16]}...")
                    else:
                        print(f"  [{symbol}/{epoch}] OK  monitor_complete")

            cycle_count += 1
            if max_cycles > 0 and cycle_count >= max_cycles:
                break

            while _utc_now() < epoch_end + timedelta(seconds=2):
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("\nInterrupted by user.")
    finally:
        if redeem_scheduler is not None:
            pending = redeem_scheduler.pending_count
            if pending > 0:
                print(f"Waiting for {pending} queued redeem job(s)...")
            await redeem_scheduler.drain()
            await redeem_scheduler.shutdown()
        if clob_client is not None:
            await clob_client.close()
        print(f"Shutdown. Cycles completed: {cycle_count}")
