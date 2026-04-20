# Project File Audit - 2026-04-20

## Scope

This audit covers tracked project source, configuration, scripts, and documentation, plus the new untracked `scheduler/intraday_service.py`. It intentionally excludes generated/runtime artifacts:

- `srock.db`, `srock.db-shm`, `srock.db-wal`
- `runtime/`
- `__pycache__/`, `*.pyc`
- `.env`, `.streamlit/secrets.toml`, `secrets/`
- local tools under `tools/`

## Verification Commands

| Check | Result |
|---|---|
| Python compile for non-deprecated `.py` files | Passed |
| `git diff --check` | Passed |
| Secret scan in tracked files | No live token/password pattern found |
| Manual keyword coverage scan | Main manual covers current Sponsor realtime, broker repair, quota policy, and built-in intraday monitor |

## Current Uncommitted Work

| File | Purpose of Change | Status |
|---|---|---|
| `app.py` | Sync built-in intraday portfolio monitor scheduler at Streamlit startup | Needs commit |
| `db/settings.py` | Persist `intraday_monitor_scheduler_enabled` | Needs commit |
| `pages/2_持股監控.py` | Add UI controls for built-in intraday monitor scheduler | Needs commit |
| `pages/4_市場環境.py` | Clarify external scheduler vs built-in intraday scheduler | Needs commit |
| `scheduler/intraday_service.py` | New in-process APScheduler service for per-minute intraday portfolio checks | Needs commit |
| `scheduler/prefetch.py` | Free/Sponsor trading-hour quota split: 100 vs 3000 per hour | Needs commit |
| `使用說明手冊.md` | Updated for database snapshot, Sponsor realtime, intraday scheduler, broker repair, quota policy | Needs commit |

## High-Risk Findings

| Severity | Finding | Evidence | Recommendation |
|---|---|---|---|
| Resolved | `ref/manual_sections/` and `ref/manual_index.*` were stale after the manual was updated. | Regenerated with `python scripts/build_manual_index.py` during this audit. | Keep regenerating these artifacts after final manual edits, or mark them as generated if they should not be reviewed manually. |
| Medium | `scheduler/jobs.py` still contains the old per-minute `job_intraday_monitor`. | The new runtime path is `scheduler/intraday_service.py`; external jobs remain available and could duplicate intraday alerts if someone also runs `python scheduler/jobs.py`. | Either remove/disable the intraday job from `scheduler/jobs.py`, or clearly label it as legacy/optional and prevent duplicate scheduling. |
| Medium | Broad `except Exception` usage is common in data fetchers and pages. | Found across `scheduler/prefetch.py`, `data/finmind_client.py`, Streamlit pages, and notification helpers. | Acceptable for UI resilience, but core fetch paths should log enough detail and avoid silently swallowing data corruption. Prioritize `scheduler/prefetch.py` and `data/finmind_client.py`. |
| Medium | `requirements.txt` and `pyproject.toml` dependency sets diverge. | Runtime app depends on Streamlit, Plotly, SQLAlchemy, APScheduler, yfinance; package metadata only covers CLI deps. | If installing via `pip install -e .`, document that `requirements.txt` is still required, or merge dependencies into optional extras. |
| Low | Deprecated scripts remain tracked. | `scripts/_deprecated/*` are clearly named but still easy to run accidentally. | Keep only if needed for archaeology; otherwise archive or add README warning inside the directory. |

## Manual Coverage Check

| Area | Main Manual Coverage | Status |
|---|---|---|
| Startup / Cloudflare / Streamlit auth | Covered | OK |
| FinMind Free vs Sponsor quota | Covered: Free 600/h, Sponsor 6000/h, trading-hour Free 100 vs Sponsor 3000 | OK |
| Sponsor realtime stock snapshot | Covered: `taiwan_stock_tick_snapshot`, 30s session cache | OK |
| Intraday portfolio monitor | Covered: built-in scheduler, no external `scheduler/jobs.py` required for per-minute monitor | OK |
| External scheduler | Covered as still needed for old-style daily/weekly push jobs | OK |
| Sponsor broker backfill | Covered: built-in PrefetchWorker, date-first backfill, repair note | OK |
| DB active path | Covered: `srock.db`, not `data/stock_data.db` | OK |
| Known issues | Covered: shareholding transfer, price_limit wording, holding level numeric default, broker bad-cache repair | OK |
| Generated manual references | Regenerated after latest manual changes | OK |

## File Inventory

### Application Entry And CLI

| File | Purpose | Robustness Notes |
|---|---|---|
| `app.py` | Streamlit home page, auth gate, starts prefetch worker and built-in intraday scheduler sync | Good separation; `@st.cache_resource` means setting changes need page rerun or explicit page control to sync. |
| `version.py` | App version constant | Simple. |
| `srock-up.bat` | Windows launcher for CLI | Simple; points users toward CLI flow. |
| `srock/__main__.py` | `python -m srock` entry | Simple. |
| `srock/cli.py` | Typer CLI for service start/stop/restart/status/auth and startup notification | Strong operational surface; confirm startup notification failures remain visible enough. |
| `srock/config.py` | TOML config loader and runtime path config | Good. Depends on local `config.toml`. |
| `srock/services.py` | Service orchestration for Streamlit, Caddy auth proxy, Cloudflare/Tailscale tunnel | Important operational code; handles processes, URLs, and restart. |
| `srock/process.py` | Process start/stop helpers | Uses `subprocess`; acceptable CLI boundary. |
| `srock/display.py` | Rich status/watch UI | Presentation only. |
| `srock/auth.py` | Basic Auth user/password management | Uses bcrypt; secrets stored under ignored `secrets/`. |

### Streamlit Pages

| File | Purpose | Robustness Notes |
|---|---|---|
| `pages/1_選股雷達.py` | Main scanner UI, custom presets, scan history, Premium trial evaluation | Large file; behavior is rich but high coupling. Future refactor into UI, scan execution, and history/report helpers would reduce risk. |
| `pages/2_持股監控.py` | Portfolio monitor UI, holdings CRUD/import/export, Premium alerts, built-in intraday scheduler controls | New scheduler controls align with user goal. Watch for Streamlit rerun/state edge cases. |
| `pages/3_風險控制.py` | Position sizing, risk, exposure, pyramiding, drawdown tools | Mostly deterministic calculations; good candidate for more unit tests. |
| `pages/4_市場環境.py` | Market condition dashboard and manual push-task triggers | Text now distinguishes built-in intraday monitor from external daily/weekly scheduler. |
| `pages/5_交易日誌.py` | Trade journal CRUD and performance summaries | Tightly coupled to DB models; acceptable. |
| `pages/6_回測模組.py` | Backtest UI and AI report trigger | Heavy operation in UI; acceptable if data already cached, but long runs can block. |
| `pages/6_資料管理.py` | Cache status, worker controls, Sponsor backfill, line subscriber management | Large operational control center; high value but needs cautious changes. |
| `pages/7_個股分析.py` | Single-stock analysis, realtime Sponsor snapshot, scorecards, charts, Premium summary | Large file; recently updated realtime snapshot with TTL. Consider extracting rendering helpers later. |

### Data Clients

| File | Purpose | Robustness Notes |
|---|---|---|
| `data/finmind_client.py` | FinMind API client, Premium gate, rate limiter, stock prices, institutional, margin, broker, fundamentals, risk flags, holding shares, realtime snapshot | Critical file. Current mapping fix for broker `buy`/`sell` is important. Broad exceptions should continue to log. |
| `data/data_source.py` | DataSourceManager fallback between FinMind and Yahoo | Useful abstraction; manual should keep clear what is realtime vs daily. |
| `data/yahoo_client.py` | Yahoo Finance fallback/batch fetching | External dependency risk; used as bridge/fallback. |
| `data/llm_client.py` | Gemini client factory | Optional feature; gracefully returns None when unavailable. |
| `data/__init__.py` | Package marker | Empty. |

### Database Layer

| File | Purpose | Robustness Notes |
|---|---|---|
| `db/database.py` | SQLAlchemy engine/session, SQLite pragmas, migrations, vacuum | Central DB path is `srock.db`; migrations are pragmatic and important. |
| `db/models.py` | SQLAlchemy models | Schema source; verify with migrations when adding columns. |
| `db/settings.py` | SQLite key-value app settings | Now includes intraday scheduler switch; simple and good. |
| `db/price_cache.py` | OHLCV cache, status, cleanup, diagnostics | Core cache. Uses suspend/delisted heuristics. |
| `db/inst_cache.py` | Institutional investor cache | OK. |
| `db/margin_cache.py` | Margin cache and stats | OK. |
| `db/fundamental_cache.py` | Fundamental metrics cache | Effective data coverage currently weaker than row count. |
| `db/broker_cache.py` | Broker main-force derived cache | Critical for v3/v4主力連3日. Repair script exists for old bad rows. |
| `db/risk_flags_cache.py` | Official risk flag cache | Good; keep user-facing wording clear for `price_limit`. |
| `db/holding_shares_cache.py` | Holding distribution cache | Numeric level safe-default is documented. |
| `db/scan_history.py` | Scan session/results JSON persistence | `scan_result` legacy table is unused; current path uses `scan_session.results_json`. |
| `db/__init__.py` | Package marker | Empty. |

### Domain Modules

| File | Purpose | Robustness Notes |
|---|---|---|
| `modules/scanner.py` | Indicator computation, v3/v4 scoring, sector analysis, Premium scoring fields | Critical strategy logic; needs tests around scorecard edge cases. |
| `modules/indicators.py` | SMA, RSI, MACD, Bollinger, ATR, weekly MA, RS | Deterministic and testable. |
| `modules/portfolio.py` | Portfolio alert calculations | Good base logic; UI adds Premium alerts. |
| `modules/intraday_monitor.py` | Per-minute Sponsor realtime snapshot checks for monitored holdings, with KBar fallback | Works only for `intraday_monitor=True`; cooldown is in-memory and resets on restart. |
| `modules/risk.py` | Position sizing, trailing stop, Kelly, exposure, drawdown | Deterministic and testable. |
| `modules/backtester.py` | Backtesting engine and markdown report | Large but cohesive. More tests would be valuable. |
| `modules/journal.py` | Trade journal, performance, sync to portfolio | DB coupled; OK. |
| `modules/portfolio_io.py` | CSV import/export and validation | Useful hardening layer. |
| `modules/auth.py` | Streamlit auth with local auto-login | Good for Cloudflare direct tunnel; local host bypass is intentional. |
| `modules/__init__.py` | Package marker | Empty. |

### Scheduler And Background Jobs

| File | Purpose | Robustness Notes |
|---|---|---|
| `scheduler/prefetch.py` | Built-in background worker: OHLCV, Yahoo bridge, Supplementary, fundamentals, Sponsor backfills, rate limits | Most complex operational file. Recent quota change is correct but should be committed and observed. |
| `scheduler/intraday_service.py` | New Streamlit-process BackgroundScheduler for per-minute intraday portfolio monitor | Satisfies no-external-scheduler requirement. Potential duplication if external `scheduler/jobs.py` also runs. |
| `scheduler/jobs.py` | Legacy/optional external APScheduler for daily scan, portfolio snapshots, weekly report, and old intraday monitor | Keep for daily/weekly jobs; consider removing old intraday job or documenting duplicate risk. |
| `scheduler/__init__.py` | Package marker | Empty. |

### Notifications, Webhook, AI

| File | Purpose | Robustness Notes |
|---|---|---|
| `notifications/line_notify.py` | LINE multicast/subscriber management | Uses env secrets; DB subscriber list. Good. |
| `notifications/__init__.py` | Package marker | Empty. |
| `webhook.py` | LINE webhook subscriber management | Optional server. Ensure deployment uses env secrets only. |
| `agents/reviewer.py` | Gemini-based backtest report analysis | Optional; graceful failures. |
| `agents/__init__.py` | Package marker | Empty. |
| `sync_to_notion.py` | GitHub Action/manual sync of manual to Notion | Requires Notion secrets; good separation. |

### Scripts

| File | Purpose | Robustness Notes |
|---|---|---|
| `scripts/build_manual_index.py` | Builds manual index and section files | Should be rerun after current manual changes if `ref/manual_sections` are used. |
| `scripts/read_manual_section.py` | Reads indexed manual sections | Depends on generated index freshness. |
| `scripts/clear_stale_no_update.py` | Maintenance cleanup for stale no-update statuses | Direct DB maintenance; use intentionally. |
| `scripts/premium_broker_backfill.py` | Diagnostic/maintenance wrapper for broker backfill | Manual says daily use should prefer built-in Data Management control. |
| `scripts/repair_broker_main_force_cache.py` | Removes bad broker cache rows from old mapping bug | Purpose is clear; keep as maintenance tool. |
| `scripts/_deprecated/*` | Old PowerShell/batch service tools | Deprecated; not part of normal operation. |

### Configuration And Packaging

| File | Purpose | Robustness Notes |
|---|---|---|
| `config.toml` | Runtime ports, paths, startup profile, FinMind tier/features | Current tier is Sponsor and Premium enabled. |
| `requirements.txt` | Runtime Python dependencies | Primary app dependency list. |
| `pyproject.toml` | CLI package metadata for `srock` | Does not include full app dependencies. |
| `.env.example` | Environment variable template | OK. |
| `.streamlit/secrets.example.toml` | Auth secrets template | OK. Real secrets ignored. |
| `.gitignore` | Ignores DB, runtime, local secrets, tools, pycache | Good. |
| `.github/workflows/sync.yml` | Sync manual to Notion on manual changes | Depends on `sync_to_notion.py` and GitHub secrets. |

### Documentation And Reference

| File | Purpose | Robustness Notes |
|---|---|---|
| `使用說明手冊.md` | Main user manual | Main source is up to date after latest changes. |
| `SYSTEM_AUDIT_AND_MANUAL_SYNC_PLAN.md` | Audit plan and findings from earlier pass | Useful; may need append if this report supersedes parts. |
| `FINMIND_SPONSOR_PREMIUM_PLAN.md` | Sponsor rollout plan and completion notes | Historical plan; not necessarily the current user manual. |
| `FUTURE_FEATURES.md` | Future ideas | Low risk. |
| `ref/finmind_api_spec.md` | Local FinMind notes | Reference only. |
| `ref/design_yahoo_bridge.md` | Yahoo bridge design notes | Reference only. |
| `ref/manual_index.md` | Generated manual index | Stale until regenerated. |
| `ref/manual_index.json` | Generated manual index data | Stale until regenerated. |
| `ref/manual_index_usage.md` | How to use manual index tooling | OK. |
| `ref/manual_sections/*.md` | Generated manual section slices | Stale until regenerated. |
| `ref/suspendList.csv` | Suspended/delisted reference list | Data reference. |
| `ref/strategy_v3.png`, `ref/strategy_v4.png`, strategy PDF | Visual/reference strategy material | Reference only. |
| `logo.svg` | App/logo asset | OK. |

## Design Robustness Summary

Strong areas:

- Clear separation between data clients, cache layer, domain modules, Streamlit pages, and service CLI.
- SQLite cache design is pragmatic and resilient for a single-user desktop/server app.
- Premium gates and degraded behavior prevent hard failures when Sponsor data is unavailable.
- App now has built-in intraday monitor control, matching the requested architecture.

Areas to harden next:

1. Regenerate manual index artifacts after final manual edits.
2. Decide whether `scheduler/jobs.py` should keep or remove the old intraday monitor job.
3. Add focused tests for:
   - broker buy/sell mapping and cache repair detection
   - v3/v4 scorecard edge cases
   - intraday scheduler start/stop state
   - Premium gate behavior under Free/Sponsor/degraded/quota-low states
4. Reduce broad `except Exception` in core data paths where silent data corruption is possible.
5. Split the largest Streamlit pages and `scheduler/prefetch.py` into smaller helpers over time.

## Manual Consistency Verdict

The main manual currently covers the implemented behavior sufficiently for users:

- It explains Sponsor realtime snapshot in individual stock analysis.
- It explains built-in intraday portfolio monitor controls.
- It distinguishes per-minute intraday monitor from the optional external scheduler.
- It documents Sponsor trading-hour quota adjustment and broker cache repair.
- It documents current DB state and known Premium issues.

The generated reference files were refreshed during this audit: `ref/manual_index.*` and `ref/manual_sections/*.md` now include the latest manual changes.
