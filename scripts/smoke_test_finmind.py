"""
FinMind smoke test -- 5 groups matching final verification checklist.

Usage:
    python scripts/smoke_test_finmind.py
"""
import sys
import traceback
from datetime import date, timedelta

# ── helpers ───────────────────────────────────────────────────────────────────

results: list[tuple[str, str, str]] = []   # (group, label, status)


def ok(group: str, label: str, note: str = "") -> None:
    msg = f"PASS -- {note}" if note else "PASS"
    results.append((group, label, msg))
    print(f"[{group}] {label}: {msg}")


def fail(group: str, label: str, note: str = "") -> None:
    msg = f"FAIL -- {note}" if note else "FAIL"
    results.append((group, label, msg))
    print(f"[{group}] {label}: {msg}", file=sys.stderr)


def skip(group: str, label: str, note: str = "") -> None:
    msg = f"SKIP -- {note}" if note else "SKIP"
    results.append((group, label, msg))
    print(f"[{group}] {label}: {msg}")


def last_friday() -> str:
    today = date.today()
    offset = (today.weekday() - 4) % 7
    return (today - timedelta(days=offset)).strftime("%Y-%m-%d")


TRADE_DATE = last_friday()
STOCK_ID   = "2330"
BROKER_ID  = "1160"   # Yuan-Da Taipei

print(f"\n{'='*60}")
print(f"FinMind smoke test  |  trade_date={TRADE_DATE}")
print(f"{'='*60}\n")

# ──────────────────────────────────────────────────────────────────────────────
# Group 1: TaiwanStockTradingDailyReport (special endpoint, sponsor only)
# ──────────────────────────────────────────────────────────────────────────────
G = "1-BrokerDaily"
print(f"\n-- {G}: TaiwanStockTradingDailyReport --")
from data.finmind_client import (
    get_broker_trading_daily_report,
    PremiumUnavailableError,
)

try:
    df1 = get_broker_trading_daily_report(STOCK_ID, TRADE_DATE)
    if df1 is None:
        fail(G, "fetch", "returned None")
    elif df1.empty:
        ok(G, "fetch", "empty DataFrame (no data or non-trading day)")
    else:
        required = {"date", "securities_trader_id"}
        missing = required - set(df1.columns)
        if missing:
            fail(G, "schema", f"missing columns: {missing}")
        else:
            ok(G, "fetch", f"{len(df1)} rows, cols={list(df1.columns[:6])}")
except PremiumUnavailableError as e:
    skip(G, "fetch", f"Premium unavailable ({e.reason}) -- gate correctly blocked")
except Exception as e:
    fail(G, "fetch", str(e))
    traceback.print_exc()

# ──────────────────────────────────────────────────────────────────────────────
# Group 2: TaiwanStockTradingDailyReportSecIdAgg (special endpoint)
#   2a. without securities_trader_id -- should raise ValueError (API requires it)
#   2b. with securities_trader_id    -- should succeed
# ──────────────────────────────────────────────────────────────────────────────
G = "2-SecIdAgg"
print(f"\n-- {G}: TaiwanStockTradingDailyReportSecIdAgg --")
from data.finmind_client import get_broker_trading_daily_report_secid_agg

start_d = (date.today() - timedelta(days=14)).strftime("%Y-%m-%d")

# 2a: no trader_id must raise ValueError (API-mandated requirement)
try:
    get_broker_trading_daily_report_secid_agg(STOCK_ID, start_date=start_d, end_date=TRADE_DATE)
    fail(G, "no-trader-id raises ValueError", "should have raised but did not")
except ValueError as e:
    ok(G, "no-trader-id raises ValueError", str(e)[:80])
except PremiumUnavailableError as e:
    skip(G, "no-trader-id raises ValueError", f"Premium unavailable ({e.reason})")
except Exception as e:
    fail(G, "no-trader-id raises ValueError", f"wrong exception {type(e).__name__}: {e}")

# 2b: with trader_id
try:
    df2 = get_broker_trading_daily_report_secid_agg(
        STOCK_ID,
        start_date=start_d,
        end_date=TRADE_DATE,
        securities_trader_id=BROKER_ID,
    )
    if df2 is None:
        fail(G, f"with trader_id={BROKER_ID}", "returned None")
    elif df2.empty:
        ok(G, f"with trader_id={BROKER_ID}", "empty DataFrame (OK -- no data for this broker/range)")
    else:
        ok(G, f"with trader_id={BROKER_ID}", f"{len(df2)} rows")
except PremiumUnavailableError as e:
    skip(G, f"with trader_id={BROKER_ID}", f"Premium unavailable ({e.reason})")
except Exception as e:
    fail(G, f"with trader_id={BROKER_ID}", str(e))
    traceback.print_exc()

# ──────────────────────────────────────────────────────────────────────────────
# Group 3: get_all_institutional_by_date / get_all_margin_by_date
# ──────────────────────────────────────────────────────────────────────────────
G = "3-AllByDate"
print(f"\n-- {G}: all-market single-day --")
from data.finmind_client import get_all_institutional_by_date, get_all_margin_by_date

for label, fn in [
    ("institutional", get_all_institutional_by_date),
    ("margin",        get_all_margin_by_date),
]:
    try:
        df3 = fn(TRADE_DATE)
        if df3 is None:
            fail(G, label, "returned None")
        elif df3.empty:
            ok(G, label, "empty DataFrame (non-trading day or API no data)")
        else:
            ok(G, label, f"{len(df3)} rows")
    except Exception as e:
        fail(G, label, str(e))
        traceback.print_exc()

# ──────────────────────────────────────────────────────────────────────────────
# Group 4: Premium gate
#   4a. premium_enabled=False -> raise PremiumUnavailableError(reason="disabled")
#   4b. quota_pct < 0.15      -> raise PremiumUnavailableError(reason="quota_low")
#   4c. free dataset           -> must NOT raise
# ──────────────────────────────────────────────────────────────────────────────
G = "4-PremiumGate"
print(f"\n-- {G}: premium gate logic --")
import data.finmind_client as _fc

_orig_load = _fc._load_finmind_settings

# 4a: premium disabled
def _mock_disabled():
    return {"tier": "free", "premium_enabled": False, "features": {}}

_fc._load_finmind_settings = _mock_disabled
try:
    _fc._premium_gate("TaiwanStockTradingDailyReport")
    fail(G, "disabled-gate", "should have raised but did not")
except PremiumUnavailableError as e:
    if e.reason in ("disabled", "free_tier"):
        ok(G, "disabled-gate", f"reason={e.reason}")
    else:
        fail(G, "disabled-gate", f"wrong reason={e.reason}")
except Exception as e:
    fail(G, "disabled-gate", str(e))
finally:
    _fc._load_finmind_settings = _orig_load

# 4b: quota_pct < 0.15
def _mock_sponsor():
    return {"tier": "sponsor", "premium_enabled": True, "features": {}}

_fc._load_finmind_settings = _mock_sponsor
with _fc._premium_state_lock:
    _snap = (_fc._premium_state.user_enabled, _fc._premium_state.tier,
             _fc._premium_state.quota_pct, _fc._premium_state.degraded)
    _fc._premium_state.user_enabled = True
    _fc._premium_state.tier         = "sponsor"
    _fc._premium_state.quota_pct    = 0.05
    _fc._premium_state.degraded     = False
try:
    _fc._premium_gate("TaiwanStockTradingDailyReport")
    fail(G, "quota-low-gate", "should have raised but did not")
except PremiumUnavailableError as e:
    if e.reason == "quota_low":
        ok(G, "quota-low-gate", f"reason={e.reason}")
    else:
        fail(G, "quota-low-gate", f"wrong reason={e.reason}")
except Exception as e:
    fail(G, "quota-low-gate", str(e))
finally:
    _fc._load_finmind_settings = _orig_load
    with _fc._premium_state_lock:
        (_fc._premium_state.user_enabled, _fc._premium_state.tier,
         _fc._premium_state.quota_pct, _fc._premium_state.degraded) = _snap

# 4c: free dataset must not be blocked
try:
    _fc._premium_gate("TaiwanStockPrice")
    ok(G, "free-dataset-pass", "TaiwanStockPrice not blocked by gate")
except PremiumUnavailableError as e:
    fail(G, "free-dataset-pass", f"should not block TaiwanStockPrice: {e}")
except Exception as e:
    fail(G, "free-dataset-pass", str(e))

# ──────────────────────────────────────────────────────────────────────────────
# Group 5: Routing guard
#   special/snapshot datasets via generic _get() must raise RuntimeError
#   free datasets must NOT raise
# ──────────────────────────────────────────────────────────────────────────────
G = "5-RoutingGuard"
print(f"\n-- {G}: routing guard --")
from data.finmind_client import _ensure_dataset_routing

for ds in [
    "TaiwanStockTradingDailyReport",
    "TaiwanStockTradingDailyReportSecIdAgg",
    "taiwan_stock_tick_snapshot",
]:
    try:
        _ensure_dataset_routing(ds)
        fail(G, ds, "should have raised but did not")
    except RuntimeError:
        ok(G, ds, "raise RuntimeError correctly")
    except Exception as e:
        fail(G, ds, f"wrong exception type {type(e).__name__}: {e}")

for ds in ["TaiwanStockPrice", "TaiwanStockInstitutionalInvestorsBuySell"]:
    try:
        _ensure_dataset_routing(ds)
        ok(G, ds, "free dataset not blocked")
    except RuntimeError as e:
        fail(G, ds, f"should not block: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print("SUMMARY")
print(f"{'='*60}")
pass_n = sum(1 for _, _, s in results if s.startswith("PASS"))
fail_n = sum(1 for _, _, s in results if s.startswith("FAIL"))
skip_n = sum(1 for _, _, s in results if s.startswith("SKIP"))
for group, label, status in results:
    print(f"  [{group}] {label:45s} {status}")
print(f"\n  Total: {pass_n} PASS / {fail_n} FAIL / {skip_n} SKIP")
if fail_n:
    print("\n  !! FAIL items found -- see traceback above.")
    sys.exit(1)
else:
    print("\n  All checks passed.")
