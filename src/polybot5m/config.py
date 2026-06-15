"""Configuration — YAML + env (POLYBOT5MBES_ prefix)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from dotenv import load_dotenv

from polybot5m.constants import PUSD_ADDRESS, SIGNATURE_TYPE_POLY_1271


class ScheduleWindowConfig(BaseModel):
    start_hour: int = 4
    start_minute: int = 0
    end_hour: int = 11
    end_minute: int = 0


class ScheduleConfig(BaseModel):
    """UTC trading hours. When enabled, bot sleeps outside configured windows."""

    enabled: bool = False
    weekdays: list[str] = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
    ]
    windows: list[ScheduleWindowConfig] = []
    sleep_log_interval_min: int = 1

    @model_validator(mode="after")
    def _default_windows(self) -> ScheduleConfig:
        if not self.windows:
            self.windows = [
                ScheduleWindowConfig(
                    start_hour=4,
                    start_minute=0,
                    end_hour=11,
                    end_minute=0,
                ),
                ScheduleWindowConfig(
                    start_hour=18,
                    start_minute=0,
                    end_hour=22,
                    end_minute=0,
                ),
            ]
        return self


class BotConfig(BaseModel):
    dry_run: bool = True
    paper_trading: bool = False  # Skip redeem (no on-chain redeem).
    paper_starting_usdc: float = 3000.0
    paper_fee_bps: float = 0.0
    # Paper: delay before settled balance reflects a fill (like chain/CLOB lag).
    paper_settlement_delay_min_s: float = 0.5
    paper_settlement_delay_max_s: float = 2.0
    # Paper: fraction of order size that settles (FAK uses fak_* if set).
    paper_partial_fill_fraction_min: float = 0.5
    paper_partial_fill_fraction_max: float = 1.0
    paper_fak_partial_fill_fraction_min: float | None = None
    paper_fak_partial_fill_fraction_max: float | None = None
    # Paper: SELL limit orders settle after this many monitor poll ticks (shared session).
    paper_sell_limit_settle_ticks: int = 2
    log_level: str = "INFO"
    log_file: str = ""
    log_append: bool = False
    log_timestamp_name: bool = False


class ApiConfig(BaseModel):
    gamma_url: str = "https://gamma-api.polymarket.com"
    clob_url: str = "https://clob.polymarket.com"
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com"


class Buy1Config(BaseModel):
    enabled: bool = True
    trigger_time_start_sec: float = 35.0
    trigger_time_end_sec: float = 3.0
    max_spread: float = 0.07
    min_best_ask: float = 0.70
    spot_minus_strike_btc_abs_min: float = 40.0
    averge_spot_minus_btc_abs_min: float = 18.0
    spot_minus_strike_eth_abs_min: float = 1.0
    other_symbol_min_best_bid: float = 0.90
    monitoring_cycles: int = 2
    shares: float = 5.0
    buy_limit_price: float = 0.99
    order_type: str = "FAK"


class Buy2Config(BaseModel):
    enabled: bool = True
    trigger_time_start_sec: float = 36.0
    trigger_time_end_sec: float = 4.0
    max_spread: float = 0.07
    min_best_ask: float = 0.70
    spot_minus_strike_btc_abs_min: float = 50.0
    averge_spot_minus_btc_abs_min: float = 18.0
    spot_minus_strike_eth_abs_min: float = 0.9
    other_symbol_min_best_bid: float = 0.90
    monitoring_cycles: int = 2
    shares: float = 5.0
    buy_limit_price: float = 0.99
    order_type: str = "FAK"


class Risk1Config(BaseModel):
    """Stop loss vs entry price: sell when best_bid < fill_price - loss_offset for N ticks."""

    enabled: bool = True
    loss_offset: float = 0.20
    monitoring_cycles: int = 3
    sell_offset: float = 0.05
    max_sell_attempts: int = 5


class Risk2Config(BaseModel):
    """Time stop from first confirmed buy fill."""

    enabled: bool = True
    hold_timeout_sec: float = 10.0
    sell_offset: float = 0.10
    max_sell_attempts: int = 5


class Risk3Config(BaseModel):
    """Absolute low bid: sell when best_bid < bid_below for N ticks."""

    enabled: bool = True
    bid_below: float = 0.03
    monitoring_cycles: int = 3
    sell_offset: float = 0.02
    max_sell_attempts: int = 5


class ExecutionConfig(BaseModel):
    """Builder-relayer config for redeem operations."""
    balance_epsilon: float = 0.01
    enabled: bool = False
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    private_key: str = ""
    funder: str = ""
    chain_id: int = 137
    signature_type: int = SIGNATURE_TYPE_POLY_1271
    auto_deploy_deposit_wallet: bool = True
    rpc_url: str = "https://polygon-mainnet.g.alchemy.com/v2/XwoKGTuXJtL-R8bVNwO3N"
    # When true, derive L2 API creds from private key (requires network). Set false and set API_* if offline.
    derive_clob_api_creds: bool = True
    # Builder relayer: when >0, advance first-try cred every N seconds (UTC) over the pool (split + redeem). 0 = off.
    builder_cred_rotation_seconds: float = 0.0
    # If true, add market_index to the rotation offset so parallel markets prefer different keys in the same window.
    builder_cred_rotation_stagger_markets: bool = False
    # V2 collateral (pUSD); used for splitPosition + redeemPositions.
    collateral_token: str = PUSD_ADDRESS
    builder_code: str = ""
    # Optional override; empty = constants.CTF_COLLATERAL_ADAPTER_ADDRESS (May 2026 redeploy)
    ctf_collateral_adapter: str = ""
    neg_risk_ctf_collateral_adapter: str = ""


class MarketTarget(BaseModel):
    """A single market+epoch to run."""
    symbol: str
    epoch: str


class ChainlinkConfig(BaseModel):
    streams_user_id: str = ""
    streams_secret: str = ""
    feed_ids: dict[str, str] = {}


class PriceFeedConfig(BaseModel):
    """Strike: chainlink = Data Streams API (Polymarket resolution); polymarket = UI scrape."""
    provider: str = "chainlink"
    chainlink: ChainlinkConfig = ChainlinkConfig()
    # Spot for [STRIKE_SPOT] logging: chainlink = poll Data Streams (same feed_ids as strike).
    spot_provider: str = "chainlink"
    chainlink_spot_poll_interval_s: float = 1.0


@dataclass(frozen=True)
class StrikeSpotContext:
    """Strike + spot feeds for optional [STRIKE_SPOT] logging during monitor."""

    symbol: str
    epoch_start_unix: int
    interval_secs: int
    strike_provider: str
    chainlink_user_id: str
    chainlink_secret: str
    chainlink_feed_ids: dict[str, str]
    market_slug: str = ""
    spot_provider: str = "chainlink"
    chainlink_spot_poll_interval_s: float = 1.0


class BacktestRiskConfig(BaseModel):
    """Optional stress on execution-time books (near resolution)."""

    spread_widen_seconds_before_end: float = 30.0
    # Effective bid shrink: multiply top-of-book sizes by this factor when inside window (simulates widening).
    spread_widen_depth_mult: float = 0.55
    liquidity_eviction_probability: float = 0.05
    # Fraction of top-level size removed when eviction fires.
    eviction_size_fraction: float = 0.35
    # Rare: no bid liquidity at execution (failed exit attempt).
    no_exit_liquidity_probability: float = 0.02


class BacktestSimulationConfig(BaseModel):
    """Realistic execution simulation for historical replay (nearest to live trading)."""

    enabled: bool = False
    latency_ms_min: float = 50.0
    latency_ms_max: float = 500.0
    # Taker-style fee on notional proceeds (Polymarket varies; 0 = off).
    fee_bps: float = 0.0
    # Bernoulli: limit order reaches matching engine and fills (rest = no fill this attempt).
    limit_order_fill_probability: float = 0.92
    # If True, treat each sell as aggressive (walk book); if False, still walk book but limit fill prob applies.
    market_style_sell: bool = True
    random_seed: int | None = 42
    risk: BacktestRiskConfig = BacktestRiskConfig()


class BacktestRootConfig(BaseModel):
    """Nested under `backtest:` in YAML."""

    simulation: BacktestSimulationConfig = BacktestSimulationConfig()
    # Hedge in replay: use bid/ask gates only (no strike vs chain spot cycles). Live trading ignores this.
    hedge_without_chain_compare: bool = True
    # Replay: when strike/spot are absent, do not apply sell_min_strike_spot_diff_usd (book-only sells).
    sell_without_strike_spot_margin: bool = True
    # Tee backtest stdout/stderr to file (same behavior as bot.log_file; empty = off).
    log_file: str = ""
    log_append: bool = False
    log_timestamp_name: bool = True


class LiquidityMakerConfig(BaseModel):
    """Monitor order book -> redeem."""

    # When false, skip on-chain redeem after the monitor phase.
    redeem_enabled: bool = True
    # When true, each market schedules redeem as an async task after monitor phase.
    # run_all_markets awaits all scheduled redeems before returning summaries.
    redeem_async_enabled: bool = True
    redeem_delay_seconds: int = 120
    # After epoch_end + redeem_delay: btc=0, eth=+gap, sol=+2*gap, xrp=+3*gap (serial relayer queue).
    redeem_per_symbol_gap_seconds: float = 10.0
    redeem_retry_delay_seconds: float = 10.0
    redeem_max_retries: int = 5
    stagger_delay_seconds: int = 5
    export_dir: str = "exports"
    cycles: int = 0
    # Seconds between monitor ticks; each tick REST-polls YES/NO CLOB /book.
    monitor_poll_interval_s: float = 0.2
    # REST /book timeout per poll.
    monitor_rest_book_timeout_s: float = 3.0
    # Max seconds for on-chain/CLOB balance refresh during monitor (exceed → skip, use last rem_*).
    monitor_balance_refresh_timeout_s: float = 3.0
    # Min seconds between user-trade-triggered balance refreshes (avoids RPC spam every tick).
    monitor_balance_force_refresh_min_s: float = 1.0
    # Merged wave: wait this long per symbol; print partial wave if some symbols are slow.
    monitor_wave_collect_timeout_s: float = 3.0
    # Authenticated CLOB user channel (orders/trades) per market condition_id.
    monitor_user_ws_enabled: bool = True
    # Poll YES/NO wallet balances via CLOB SDK during monitor (0 = off).
    monitor_balance_poll_interval_s: float = 2.0
    monitor_log_interval_s: float = 1.0
    # In the last N seconds before epoch end, log every monitor tick ([MARKET_TICK]).
    monitor_verbose_seconds_before_end: float = 5.0
    # Log market target (strike) and spot every N seconds (0 = off). Implies spot feed + strike fetch for this market.
    log_strike_spot_interval_s: float = 0.0
    # STRIKE_SPOT: difference_rate = sms / mean(sms) over this many seconds (0 = off).
    spot_minus_strike_difference_rate_lookback_s: float = 3.0
    # After redeem, poll public order books for this many seconds (0 = off).
    post_redeem_monitor_seconds: float = 0.0

    # NDJSON log of phases + monitor ticks. Empty = off.
    trading_process_jsonl: str = ""
    # full = INFLUENCE/MONITOR ticks + cycle events; trades = BUY/SELL fills only (compact).
    trading_process_log_mode: str = "trades"
    # 0 = log every poll; >0 = min seconds between MONITOR_TICK lines (reduces file size). Used when mode=full.
    trading_process_log_interval_s: float = 0.0
    # Echo compact [TRADING_PROCESS] lines to stdout when trading_process_jsonl is set.
    trading_process_log_stdout: bool = False
    # Track opposite best_bid min/max across the cycle.
    opposite_bid_history_enabled: bool = False
    epoch: str = "5m"
    symbols: list[str] = []
    markets: list[MarketTarget] = []

    @model_validator(mode="after")
    def _symbols_to_markets(self) -> LiquidityMakerConfig:
        if self.symbols:
            self.markets = [
                MarketTarget(symbol=str(s).strip().lower(), epoch=self.epoch)
                for s in self.symbols
                if str(s).strip()
            ]
        return self


class Settings(BaseSettings):
    bot: BotConfig = BotConfig()
    schedule: ScheduleConfig = ScheduleConfig()
    api: ApiConfig = ApiConfig()
    execution: ExecutionConfig = ExecutionConfig()
    liquidity_maker: LiquidityMakerConfig = LiquidityMakerConfig()
    price_feed: PriceFeedConfig = PriceFeedConfig()
    backtest: BacktestRootConfig = BacktestRootConfig()
    buy1: Buy1Config = Buy1Config()
    buy2: Buy2Config = Buy2Config()
    risk1: Risk1Config = Risk1Config()
    risk2: Risk2Config = Risk2Config()
    risk3: Risk3Config = Risk3Config()

    model_config = SettingsConfigDict(
        env_prefix="POLYBOT5MBES_",
        env_nested_delimiter="__",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # YAML is passed via init_settings; .env / process env must win for secrets and wallet flags.
        return env_settings, dotenv_settings, init_settings, file_secret_settings


def load_config(path: str = "config/default.yaml") -> Settings:
    config_path = Path(path).resolve()
    for base in (config_path.parent.parent, config_path.parent, Path.cwd()):
        env_file = base / ".env"
        if env_file.is_file():
            load_dotenv(dotenv_path=str(env_file), override=True)
            break
    load_dotenv(override=True)
    data: dict = {}
    if config_path.exists():
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
    return Settings(**data)
