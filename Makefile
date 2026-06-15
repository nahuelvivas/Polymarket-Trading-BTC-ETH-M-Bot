# PolyBackTest API helpers — mirrors https://docs.polybacktest.com/ (REST only; no official CLI).
# Set POLYBACKTEST_API_KEY (dashboard key, prefix pdm_...) or add it to .env

.DEFAULT_GOAL := help

PYTHON ?= python3
BACKTEST_OPTS ?= --config config/default.yaml --coin btc --market-type 5m --last 20 --market-delay 10 --page-delay 1

PB_BASE ?= https://api.polybacktest.com
COIN ?= btc
MARKET_ID ?=
SLUG ?=
PB_TIMESTAMP ?=

PB_LIMIT ?=
PB_OFFSET ?=
PB_MARKET_TYPE ?=
PB_RESOLVED ?=
PB_START_TIME ?=
PB_END_TIME ?=
PB_INCLUDE_ORDERBOOK ?=

CURL ?= curl -sS -H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)"

-include .env
export POLYBACKTEST_API_KEY

.PHONY: help backtest
help: ## List PolyBackTest make targets (see comments for variables)
	@echo "PolyBackTest — set POLYBACKTEST_API_KEY (see https://docs.polybacktest.com/api-keys )"
	@echo ""
	@grep -hE '^[a-zA-Z0-9_.-]+:.*##' $(MAKEFILE_LIST) | sort -u | awk -F ':.*## ' '{printf "  %-28s %s\n", $$1, $$2}'

backtest: ## Replay paper sell strategy on latest N markets (BACKTEST_OPTS)
	$(PYTHON) -m polybot5m.backtest $(BACKTEST_OPTS)

.PHONY: pb-require-key
pb-require-key:
	@test -n "$(POLYBACKTEST_API_KEY)" || { echo >&2 "POLYBACKTEST_API_KEY is not set"; exit 1; }

# --- Docs / spec -------------------------------------------------------------

.PHONY: pb-open-docs pb-fetch-openapi
pb-open-docs: ## Open documentation index in browser (uses xdg-open)
	@xdg-open "https://docs.polybacktest.com/" 2>/dev/null || open "https://docs.polybacktest.com/" 2>/dev/null || echo "Open https://docs.polybacktest.com/"

pb-fetch-openapi: ## Download OpenAPI JSON to exports/polybacktest-openapi.json (docs URL first; no key required if it succeeds)
	@mkdir -p exports
	@curl -fsSL -o exports/polybacktest-openapi.json "https://docs.polybacktest.com/api-reference/openapi.json" 2>/dev/null || \
		( test -n "$(POLYBACKTEST_API_KEY)" && $(CURL) -fsSL -o exports/polybacktest-openapi.json "$(PB_BASE)/openapi.json" ) || \
		{ echo >&2 "Fetch failed (docs 5xx or missing POLYBACKTEST_API_KEY for $(PB_BASE)/openapi.json)"; exit 1; }
	@echo "Wrote exports/polybacktest-openapi.json"

# --- v2 — markets & prediction snapshots -----------------------------------

.PHONY: pb-v2-list-markets
pb-v2-list-markets: pb-require-key ## GET /v2/markets — COIN; optional PB_LIMIT PB_OFFSET PB_MARKET_TYPE PB_RESOLVED PB_START_TIME PB_END_TIME
	@curl -sS -G "$(PB_BASE)/v2/markets" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		--data-urlencode "coin=$(COIN)" \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)") \
		$(if $(strip $(PB_MARKET_TYPE)),--data-urlencode "market_type=$(PB_MARKET_TYPE)") \
		$(if $(strip $(PB_RESOLVED)),--data-urlencode "resolved=$(PB_RESOLVED)") \
		$(if $(strip $(PB_START_TIME)),--data-urlencode "start_time=$(PB_START_TIME)") \
		$(if $(strip $(PB_END_TIME)),--data-urlencode "end_time=$(PB_END_TIME)")

.PHONY: pb-v2-get-market
pb-v2-get-market: pb-require-key ## GET /v2/markets/{id} — MARKET_ID COIN
	@test -n "$(MARKET_ID)" || { echo >&2 "MARKET_ID required"; exit 1; }
	@$(CURL) "$(PB_BASE)/v2/markets/$(MARKET_ID)?coin=$(COIN)"

.PHONY: pb-v2-get-market-by-slug
pb-v2-get-market-by-slug: pb-require-key ## GET /v2/markets/by-slug/{slug} — SLUG COIN
	@test -n "$(SLUG)" || { echo >&2 "SLUG required"; exit 1; }
	@$(CURL) "$(PB_BASE)/v2/markets/by-slug/$(SLUG)?coin=$(COIN)"

.PHONY: pb-v2-market-snapshots
pb-v2-market-snapshots: pb-require-key ## GET /v2/markets/{id}/snapshots — MARKET_ID COIN; optional PB_INCLUDE_ORDERBOOK PB_LIMIT PB_OFFSET PB_START_TIME PB_END_TIME
	@test -n "$(MARKET_ID)" || { echo >&2 "MARKET_ID required"; exit 1; }
	@curl -sS -G "$(PB_BASE)/v2/markets/$(MARKET_ID)/snapshots" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		--data-urlencode "coin=$(COIN)" \
		$(if $(strip $(PB_INCLUDE_ORDERBOOK)),--data-urlencode "include_orderbook=$(PB_INCLUDE_ORDERBOOK)") \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)") \
		$(if $(strip $(PB_START_TIME)),--data-urlencode "start_time=$(PB_START_TIME)") \
		$(if $(strip $(PB_END_TIME)),--data-urlencode "end_time=$(PB_END_TIME)")

.PHONY: pb-v2-snapshot-at
pb-v2-snapshot-at: pb-require-key ## GET /v2/markets/{id}/snapshot-at/{ts} — MARKET_ID COIN PB_TIMESTAMP (URL-encoded if ISO8601, e.g. 2026-01-01T12%3A30%3A00Z)
	@test -n "$(MARKET_ID)" || { echo >&2 "MARKET_ID required"; exit 1; }
	@test -n "$(PB_TIMESTAMP)" || { echo >&2 "PB_TIMESTAMP required (path segment; encode : as %3A for ISO8601)"; exit 1; }
	@$(CURL) "$(PB_BASE)/v2/markets/$(MARKET_ID)/snapshot-at/$(PB_TIMESTAMP)?coin=$(COIN)"

# --- v2 — Binance spot & futures ---------------------------------------------

.PHONY: pb-v2-spot-latest pb-v2-spot-snapshots pb-v2-spot-trades
pb-v2-spot-latest: pb-require-key ## GET /v2/spot/latest — COIN
	@$(CURL) "$(PB_BASE)/v2/spot/latest?coin=$(COIN)"

pb-v2-spot-snapshots: pb-require-key ## GET /v2/spot/snapshots — COIN; optional PB_START_TIME PB_END_TIME PB_LIMIT PB_OFFSET
	@curl -sS -G "$(PB_BASE)/v2/spot/snapshots" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		--data-urlencode "coin=$(COIN)" \
		$(if $(strip $(PB_START_TIME)),--data-urlencode "start_time=$(PB_START_TIME)") \
		$(if $(strip $(PB_END_TIME)),--data-urlencode "end_time=$(PB_END_TIME)") \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)")

pb-v2-spot-trades: pb-require-key ## GET /v2/spot/trades — COIN; optional PB_START_TIME PB_END_TIME PB_LIMIT PB_OFFSET
	@curl -sS -G "$(PB_BASE)/v2/spot/trades" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		--data-urlencode "coin=$(COIN)" \
		$(if $(strip $(PB_START_TIME)),--data-urlencode "start_time=$(PB_START_TIME)") \
		$(if $(strip $(PB_END_TIME)),--data-urlencode "end_time=$(PB_END_TIME)") \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)")

.PHONY: pb-v2-futures-latest pb-v2-futures-snapshots pb-v2-futures-trades
pb-v2-futures-latest: pb-require-key ## GET /v2/futures/latest — COIN
	@$(CURL) "$(PB_BASE)/v2/futures/latest?coin=$(COIN)"

pb-v2-futures-snapshots: pb-require-key ## GET /v2/futures/snapshots — COIN; optional PB_START_TIME PB_END_TIME PB_LIMIT PB_OFFSET
	@curl -sS -G "$(PB_BASE)/v2/futures/snapshots" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		--data-urlencode "coin=$(COIN)" \
		$(if $(strip $(PB_START_TIME)),--data-urlencode "start_time=$(PB_START_TIME)") \
		$(if $(strip $(PB_END_TIME)),--data-urlencode "end_time=$(PB_END_TIME)") \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)")

pb-v2-futures-trades: pb-require-key ## GET /v2/futures/trades — COIN; optional PB_START_TIME PB_END_TIME PB_LIMIT PB_OFFSET
	@curl -sS -G "$(PB_BASE)/v2/futures/trades" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		--data-urlencode "coin=$(COIN)" \
		$(if $(strip $(PB_START_TIME)),--data-urlencode "start_time=$(PB_START_TIME)") \
		$(if $(strip $(PB_END_TIME)),--data-urlencode "end_time=$(PB_END_TIME)") \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)")

# --- v1 — legacy (deprecated; BTC-oriented) ----------------------------------

.PHONY: pb-v1-list-markets
pb-v1-list-markets: pb-require-key ## GET /v1/markets — optional PB_LIMIT PB_OFFSET PB_MARKET_TYPE PB_RESOLVED
	@curl -sS -G "$(PB_BASE)/v1/markets" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)") \
		$(if $(strip $(PB_MARKET_TYPE)),--data-urlencode "market_type=$(PB_MARKET_TYPE)") \
		$(if $(strip $(PB_RESOLVED)),--data-urlencode "resolved=$(PB_RESOLVED)")

.PHONY: pb-v1-get-market
pb-v1-get-market: pb-require-key ## GET /v1/markets/{id} — MARKET_ID
	@test -n "$(MARKET_ID)" || { echo >&2 "MARKET_ID required"; exit 1; }
	@$(CURL) "$(PB_BASE)/v1/markets/$(MARKET_ID)"

.PHONY: pb-v1-get-market-by-slug
pb-v1-get-market-by-slug: pb-require-key ## GET /v1/markets/by-slug/{slug} — SLUG
	@test -n "$(SLUG)" || { echo >&2 "SLUG required"; exit 1; }
	@$(CURL) "$(PB_BASE)/v1/markets/by-slug/$(SLUG)"

.PHONY: pb-v1-market-snapshots
pb-v1-market-snapshots: pb-require-key ## GET /v1/markets/{id}/snapshots — MARKET_ID; optional PB_INCLUDE_ORDERBOOK PB_LIMIT PB_OFFSET PB_START_TIME PB_END_TIME
	@test -n "$(MARKET_ID)" || { echo >&2 "MARKET_ID required"; exit 1; }
	@curl -sS -G "$(PB_BASE)/v1/markets/$(MARKET_ID)/snapshots" \
		-H "Accept: application/json" -H "Authorization: Bearer $(POLYBACKTEST_API_KEY)" \
		$(if $(strip $(PB_INCLUDE_ORDERBOOK)),--data-urlencode "include_orderbook=$(PB_INCLUDE_ORDERBOOK)") \
		$(if $(strip $(PB_LIMIT)),--data-urlencode "limit=$(PB_LIMIT)") \
		$(if $(strip $(PB_OFFSET)),--data-urlencode "offset=$(PB_OFFSET)") \
		$(if $(strip $(PB_START_TIME)),--data-urlencode "start_time=$(PB_START_TIME)") \
		$(if $(strip $(PB_END_TIME)),--data-urlencode "end_time=$(PB_END_TIME)")

.PHONY: pb-v1-snapshot-at
pb-v1-snapshot-at: pb-require-key ## GET /v1/markets/{id}/snapshot-at/{ts} — MARKET_ID PB_TIMESTAMP (URL-encoded if needed)
	@test -n "$(MARKET_ID)" || { echo >&2 "MARKET_ID required"; exit 1; }
	@test -n "$(PB_TIMESTAMP)" || { echo >&2 "PB_TIMESTAMP required"; exit 1; }
	@$(CURL) "$(PB_BASE)/v1/markets/$(MARKET_ID)/snapshot-at/$(PB_TIMESTAMP)"

.PHONY: pb-v2-all-endpoints
pb-v2-all-endpoints: ## Print list of v2 routes implemented in this Makefile
	@echo "v2/markets"
	@echo "v2/markets/{market_id}"
	@echo "v2/markets/by-slug/{slug}"
	@echo "v2/markets/{market_id}/snapshots"
	@echo "v2/markets/{market_id}/snapshot-at/{timestamp}"
	@echo "v2/spot/latest"
	@echo "v2/spot/snapshots"
	@echo "v2/spot/trades"
	@echo "v2/futures/latest"
	@echo "v2/futures/snapshots"
	@echo "v2/futures/trades"