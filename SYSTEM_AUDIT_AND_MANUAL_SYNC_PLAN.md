# System Audit and Manual Sync Plan

> Created: 2026-04-20
> Goal: make the user manual, current implementation, FinMind Sponsor data usage, and fetcher behavior consistent and verifiable.

## Objectives

1. Verify that `使用說明手冊.md` matches the current codebase, especially:
   - strategy behavior and scoring
   - data sources and cache layers
   - available data categories
   - built-in fetcher behavior, quota policy, and manual controls
2. Build a FinMind Sponsor data inventory:
   - what Sponsor-level datasets are available
   - what this app currently fetches or caches
   - where each dataset is used in scanner, portfolio monitor, stock analysis, data management, alerts, and backtests
   - what remains available but unused
3. Audit recent changes for hidden risks:
   - stale text or mismatched labels
   - leftover helper functions or unused scripts
   - blocking UI calls that should be background worker tasks
   - wrong `if/else` branches or fallback behavior
   - data-source labels that say one thing but read another cache/API
   - direct FinMind calls inside pages where cache-only behavior is expected

## Authorization Needed Up Front

The audit itself can mostly run locally. The following actions may need explicit permission because they either write git metadata, start long-running services, or access the network:

1. Git write operations:
   - `git add ...`
   - `git commit -m "..."`
   - optional `git diff --cached`
2. Service operations:
   - `srock status`
   - `srock start streamlit`
   - `srock start caddy`
   - `srock start funnel`
   - optional `srock restart`
3. Network verification:
   - official FinMind documentation lookup for Sponsor-level dataset availability
   - optional direct API probes for dataset availability using the configured token
4. Long-running local validation:
   - background worker observation for 5-15 minutes
   - database summary queries while the worker is running

No destructive git reset/checkout/delete operation should be used without a separate explicit request.

## Deliverables

1. Updated `使用說明手冊.md`.
2. A dataset inventory table, either inside the manual or a companion appendix, covering:
   - dataset name
   - Free/Sponsor availability
   - current cache table
   - client function
   - worker/backfill path
   - UI/strategy usage
   - current status: used, partially used, display-only, cache-only, unused, blocked, or needs confirmation
3. A fetcher behavior map:
   - app startup path
   - `PrefetchWorker` loop priority
   - normal OHLCV fetch
   - fundamentals fetch
   - Supplementary institutional/margin fetch
   - Yahoo bridge
   - Sponsor Premium market backfill
   - Sponsor broker-main-force built-in backfill
   - rate limit and 429 recovery
   - manual controls in Data Management
4. A risk/cleanup report:
   - bugs found
   - text mismatches found
   - unused or suspicious functions
   - stale external scripts
   - direct API calls that should be cache-only
   - recommended fixes
5. Verification notes:
   - commands run
   - tests/compile checks
   - database query snapshots
   - remaining gaps.

## Phase 0 - Baseline and Safety

1. Record current git state:
   - `git status --short`
   - `git log --oneline -5`
   - `git diff --stat`
2. Record active services:
   - `srock status`
   - process list for Streamlit/Caddy/cloudflared/Python
3. Record current database cache state:
   - row counts by cache table
   - latest fetched timestamps
   - `premium_fetch_status` grouped by dataset/date/status
   - app settings related to worker modes and FinMind config
4. Confirm no external one-shot backfill process is running:
   - no `scripts\premium_broker_backfill.py` process
   - built-in Streamlit worker should be the source of ongoing writes.

## Phase 1 - Manual vs Code Consistency

Review `使用說明手冊.md` against the code paths below:

1. App entry and auth:
   - `app.py`
   - `modules/auth.py`
   - `srock/services.py`
2. Scanner and strategy behavior:
   - `pages/1_選股雷達.py`
   - `modules/scanner.py`
   - scan history persistence
3. Portfolio monitor:
   - `pages/2_持股監控.py`
   - `modules/portfolio.py`
   - `modules/intraday_monitor.py`
   - alert levels and LINE notification paths
4. Stock analysis:
   - `pages/7_個股分析.py`
   - displayed indicators, Premium summaries, fundamentals, broker, holding-share, risk flags
5. Data management and fetcher controls:
   - `pages/6_資料管理.py`
   - `scheduler/prefetch.py`
   - `scheduler/jobs.py`
6. Configuration:
   - `config.toml`
   - `.streamlit/secrets.example.toml`
   - `.env` expectations
   - `db/settings.py`

Manual sections must clearly state whether a feature:

- uses live API
- uses local cache only
- uses cache first with API fallback
- requires FinMind Sponsor
- is disabled/degraded when quota is low
- is historical-mode only.

## Phase 2 - FinMind Sponsor Dataset Inventory

Build a matrix for all FinMind datasets referenced or relevant to current features.

Initial categories to verify:

1. Prices and market structure:
   - `TaiwanStockPrice`
   - `TaiwanStockInfo`
   - `TaiwanStockKBar`
2. Basic market/fundamental data:
   - revenue
   - financial statements
   - balance sheet
   - cash flow
   - dividend or valuation inputs if currently referenced
3. Chips and flow:
   - institutional investors
   - margin trading
   - broker branch / broker-main-force data
   - holding shares distribution
4. Official risk/market warnings:
   - disposition securities
   - suspended securities
   - price limit
   - attention securities
5. Corporate actions / monitoring:
   - treasury shares
   - shareholding transfer
   - any dataset currently blocked or returning API errors

For each dataset, capture:

| Dataset | Free/Sponsor | Client Function | Cache Table | Worker Path | UI Usage | Strategy Usage | Current Status | Notes |
|---|---|---|---|---|---|---|---|---|

Status values:

- `used`
- `partially used`
- `cache-only`
- `display-only`
- `blocked`
- `unused`
- `needs confirmation`

Known issue to carry forward:

- `TaiwanStockShareholdingTransfer` needs source confirmation before production backfill. The app can display cached insider-transfer alerts, but the automated FinMind source is not confirmed.

## Phase 3 - Fetcher Logic Trace

Trace the built-in fetcher end to end:

1. Startup:
   - `app.py` calls cached `_start_prefetch_worker()`
   - `get_worker()` singleton lifecycle
2. Normal worker loop:
   - trading-hour throttle
   - off-peak throttle
   - stale price queue
   - fundamentals queue
   - Supplementary phase
   - Yahoo bridge phase
3. Sponsor Premium backfill:
   - portfolio Premium
   - candidate Premium
   - full-market Premium basics
   - built-in broker-main-force date-first backfill
4. Rate limiting:
   - client-side per-minute limiter in `data/finmind_client.py`
   - worker-side hourly limiter in `scheduler/prefetch.py`
   - quota state from FinMind user info
   - 429 handling and hourly resume behavior
5. UI controls:
   - sync vs background buttons
   - status display
   - settings persistence
   - whether closing the browser affects work

Outputs:

- a concise fetcher priority diagram
- a table of all worker modes and whether each is persistent
- a list of modes that still block Streamlit UI and should be converted to background tasks.

## Phase 4 - Hidden Risk and Cleanup Audit

Search patterns:

- `TODO`, `FIXME`, `deprecated`, `premium_broker_backfill.py`
- direct calls to FinMind APIs inside `pages/`
- duplicate or stale helper functions
- `if ... else` branches around missing data, cache fallback, and PremiumUnavailableError
- labels containing "Premium" but reading fundamental or non-Premium fields
- text showing "no data" when the actual state is "not selected", "not cached", or "not applicable"
- scripts that still imply external one-shot backfill is the recommended path
- unused imports/functions introduced during recent fixes

Specific checks:

1. `data/finmind_client.py`
   - ensure `_requests_per_minute()` matches documented Free/Backer/Sponsor behavior
   - ensure no Premium-only dataset bypasses `_premium_gate()`
   - review `get_kbar_latest()` usage and whether it belongs with intraday monitor work
2. `scheduler/prefetch.py`
   - ensure built-in broker backfill cannot starve normal critical work during trading hours
   - ensure 429 resume does not skip target rows incorrectly
   - ensure status fields are stable for UI
3. `pages/6_資料管理.py`
   - ensure manual controls do not block UI for long jobs
   - ensure labels match actual behavior
4. Portfolio monitor:
   - verify official risk flags, attention/disposition/suspended/treasury/share-transfer display semantics
   - verify stop-loss trigger wording and source
5. Scanner:
   - verify mandatory condition display after scan
   - verify overheat distance fields in scanner/history/stock analysis
   - verify Premium/fundamental flag grouping labels.

## Phase 5 - Manual Update Plan

Update `使用說明手冊.md` with:

1. Architecture:
   - local access
   - Cloudflare access
   - Streamlit auth behavior
2. Data source table:
   - Free vs Sponsor
   - cache tables
   - refresh/backfill behavior
3. Strategy section:
   - mandatory filters
   - selected filters
   - Premium score/risk penalty
   - overheat distance
4. Portfolio monitor section:
   - checked risks
   - warning levels
   - cache-only vs live data
5. Data management section:
   - built-in worker status
   - background Sponsor broker backfill
   - when to use manual buttons
   - quota behavior and expected speed
6. Known issues:
   - shareholding transfer source pending
   - any confirmed dataset/API limitations
7. Operational guidance:
   - what can run during non-trading hours
   - what should not run during trading hours
   - how to verify cache status.

## Acceptance Criteria

The audit is complete only when:

1. `使用說明手冊.md` and code behavior agree on strategy, data sources, cache behavior, and fetcher behavior.
2. Every FinMind dataset used by the app is listed with cache/use/status.
3. Every Sponsor-only feature states whether it reads cache, calls API, or does both.
4. Built-in background fetcher behavior is documented and verified from the app process.
5. Any leftover external script is either documented as diagnostic-only or deprecated/removed.
6. `py_compile` and relevant local checks pass.
7. A final findings section lists unresolved issues with exact file references.
