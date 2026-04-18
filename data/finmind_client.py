"""
FinMind API 客戶端
文件：https://finmindtrade.com/analysis/#/Guidance/api
免費帳號（註冊會員）每小時限制 600 次請求；遇 429 自動退避重試。
"""
import os
import threading
import logging
import requests
import time
import pandas as pd
import numpy as np
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from dotenv import load_dotenv

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore

load_dotenv()
logger = logging.getLogger(__name__)

FINMIND_API = "https://api.finmindtrade.com/api/v4/data"
FINMIND_USER_INFO_API = "https://api.web.finmindtrade.com/v2/user_info"
TOKEN = os.getenv("FINMIND_TOKEN", "")

ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = ROOT / "config.toml"


@dataclass
class PremiumState:
    user_enabled: bool = False
    tier: str = "free"
    quota_pct: float = 1.0
    degraded: bool = False
    last_error: str = ""
    last_quota_check: datetime | None = None
    user_count: int | None = None
    api_request_limit: int | None = None


class PremiumUnavailableError(RuntimeError):
    """Raised when a premium-only dataset is requested while Premium is unavailable."""


_PREMIUM_DATASETS = {
    "TaiwanStockTradingDailyReport",
    "TaiwanStockTradingDailyReportSecIdAgg",
    "TaiwanStockHoldingSharesPer",
    "TaiwanStockDispositionSecuritiesPeriod",
    "TaiwanStockSuspended",
    "TaiwanStockKBar",
    "TaiwanStockPriceTick",
}

_FUNDAMENTAL_DATASETS = {
    "TaiwanStockFinancialStatements",
    "TaiwanStockBalanceSheet",
    "TaiwanStockCashFlowsStatement",
}

_settings_lock = threading.Lock()
_settings_cache: dict | None = None
_settings_loaded_at: datetime | None = None
_SETTINGS_TTL_SEC = 30

_premium_state_lock = threading.Lock()
_premium_state = PremiumState()

_request_lock = threading.Lock()
_request_times = deque()

# ── 全域最新交易日（執行緒安全）────────────────────────────────────
_trading_day_lock = threading.Lock()
_global_latest_trading_day: date | None = None
_trading_day_resolved_at: datetime | None = None
_TRADING_DAY_TTL_SEC = 3600   # 解析結果快取 1 小時，避免重複打 API


def _load_finmind_settings(force: bool = False) -> dict:
    """Read FinMind feature flags from config.toml with a short in-memory TTL."""
    global _settings_cache, _settings_loaded_at

    now = datetime.now()
    with _settings_lock:
        if (
            not force
            and _settings_cache is not None
            and _settings_loaded_at is not None
            and (now - _settings_loaded_at).total_seconds() < _SETTINGS_TTL_SEC
        ):
            return dict(_settings_cache)

        settings = {
            "tier": "free",
            "premium_enabled": False,
            "features": {
                "risk_flags": True,
                "broker_branch": True,
                "holding_shares": True,
                "fundamentals_mode": "penalty",
            },
        }

        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE, "rb") as f:
                    raw = tomllib.load(f)
                finmind = raw.get("finmind", {}) if isinstance(raw, dict) else {}
                features = finmind.get("features", {}) if isinstance(finmind, dict) else {}
                settings["tier"] = str(finmind.get("tier", settings["tier"])).strip().lower()
                settings["premium_enabled"] = bool(
                    finmind.get("premium_enabled", settings["premium_enabled"])
                )
                settings["features"].update(features)
            except Exception as exc:
                logger.warning(f"Failed to read config.toml finmind settings: {exc}")

        if settings["tier"] not in {"free", "backer", "sponsor", "auto"}:
            settings["tier"] = "free"

        _settings_cache = settings
        _settings_loaded_at = now

        with _premium_state_lock:
            _premium_state.user_enabled = bool(settings["premium_enabled"])
            _premium_state.tier = settings["tier"]

        return dict(settings)


def get_premium_state() -> PremiumState:
    """Return a snapshot of the current Premium runtime state for UI/status use."""
    _load_finmind_settings()
    with _premium_state_lock:
        return PremiumState(
            user_enabled=_premium_state.user_enabled,
            tier=_premium_state.tier,
            quota_pct=_premium_state.quota_pct,
            degraded=_premium_state.degraded,
            last_error=_premium_state.last_error,
            last_quota_check=_premium_state.last_quota_check,
            user_count=_premium_state.user_count,
            api_request_limit=_premium_state.api_request_limit,
        )


def _set_premium_degraded(error: str) -> None:
    with _premium_state_lock:
        _premium_state.degraded = True
        _premium_state.last_error = error


def refresh_finmind_user_info(force: bool = False) -> PremiumState:
    """
    Refresh FinMind API quota state.

    This endpoint uses a different base URL and Authorization Bearer header.
    It intentionally does not go through _get(), which targets /api/v4/data and
    sends the token as a query parameter.
    """
    _load_finmind_settings()
    now = datetime.now()
    with _premium_state_lock:
        last = _premium_state.last_quota_check
        cached_valid = not force and last is not None and (now - last).total_seconds() < 3600
    if cached_valid:
        return get_premium_state()

    if not TOKEN:
        with _premium_state_lock:
            _premium_state.last_quota_check = now
            _premium_state.last_error = "FINMIND_TOKEN is not configured"
        return get_premium_state()

    try:
        resp = requests.get(
            FINMIND_USER_INFO_API,
            headers={"Authorization": f"Bearer {TOKEN}"},
            timeout=10,
        )
        if resp.status_code in (402, 403):
            _set_premium_degraded(f"user_info HTTP {resp.status_code}")
            return get_premium_state()
        resp.raise_for_status()
        data = resp.json()
        user_count = int(data.get("user_count") or 0)
        api_limit = int(data.get("api_request_limit") or 0)
        quota_pct = 1.0
        if api_limit > 0:
            quota_pct = max(0.0, min(1.0, (api_limit - user_count) / api_limit))
        with _premium_state_lock:
            _premium_state.user_count = user_count
            _premium_state.api_request_limit = api_limit
            _premium_state.quota_pct = quota_pct
            _premium_state.last_quota_check = now
            _premium_state.last_error = ""
            if quota_pct >= 0.15:
                _premium_state.degraded = False
    except Exception as exc:
        with _premium_state_lock:
            _premium_state.last_quota_check = now
            _premium_state.last_error = f"user_info failed: {exc}"
    return get_premium_state()


def _is_premium_dataset(dataset: str) -> bool:
    return dataset in _PREMIUM_DATASETS or dataset in _FUNDAMENTAL_DATASETS


def _premium_gate(dataset: str) -> None:
    if not _is_premium_dataset(dataset):
        return

    settings = _load_finmind_settings()
    enabled = bool(settings["premium_enabled"])
    tier = str(settings["tier"])
    state = get_premium_state()

    if not enabled or tier == "free":
        raise PremiumUnavailableError(f"{dataset} requires FinMind Premium; current tier={tier}")
    if state.degraded:
        raise PremiumUnavailableError(f"FinMind Premium runtime degraded: {state.last_error}")
    if state.quota_pct < 0.15:
        raise PremiumUnavailableError("FinMind Premium quota below 15%; premium fetch paused")


def _requests_per_minute() -> int:
    settings = _load_finmind_settings()
    tier = str(settings["tier"])
    enabled = bool(settings["premium_enabled"])
    state = get_premium_state()

    if enabled and tier in {"backer", "sponsor", "auto"} and state.quota_pct >= 0.15:
        return 40
    return 8


def _wait_for_rate_limit() -> None:
    """Small sliding-window limiter shared by all FinMind /api/v4/data requests."""
    while True:
        limit = max(1, int(_requests_per_minute()))
        now = time.monotonic()
        with _request_lock:
            while _request_times and now - _request_times[0] >= 60:
                _request_times.popleft()
            if len(_request_times) < limit:
                _request_times.append(now)
                return
            sleep_for = max(0.05, 60 - (now - _request_times[0]))
        time.sleep(min(sleep_for, 5.0))


def _get(dataset: str, stock_id: str = "", start_date: str = "", **kwargs) -> pd.DataFrame:
    _premium_gate(dataset)
    _wait_for_rate_limit()

    params = {
        "dataset": dataset,
        "token": TOKEN,
    }
    if stock_id:
        params["data_id"] = stock_id
    if start_date:
        params["start_date"] = start_date
    params.update(kwargs)

    resp = requests.get(FINMIND_API, params=params, timeout=30)
    if resp.status_code in (402, 403):
        _set_premium_degraded(f"{dataset} HTTP {resp.status_code}")
    resp.raise_for_status()  # 429 立即拋出，由呼叫端決定如何處理（DSM 備援 / Worker 暫停）

    data = resp.json()

    if data.get("status") != 200:
        status = data.get("status")
        if status in (402, 403):
            _set_premium_degraded(f"{dataset} API status {status}: {data.get('msg', '')}")
        raise RuntimeError(f"FinMind API error: {data.get('msg', 'unknown')}")

    return pd.DataFrame(data.get("data", []))


STOCK_INFO_TTL_DAYS = 30  # 股票清單快取有效期（天）
STOCK_INFO_MIN_VALID_ROWS = 1800  # 上市 + 上櫃 + ETF/權證等，低於此值通常代表舊版只抓上市


def get_stock_list(force_refresh: bool = False) -> pd.DataFrame:
    """
    取得所有上市股票清單

    快取策略：本機快取有效期 30 天，過期自動重抓。
    force_refresh=True 強制重新抓取並更新快取。
    """
    from db.database import get_session, init_db
    from db.models import StockInfoCache
    from sqlalchemy import text

    init_db()

    if not force_refresh:
        # 查快取，同時檢查最新更新時間是否在 TTL 內
        with get_session() as sess:
            result = sess.execute(text(
                "SELECT s.stock_id, s.stock_name, s.industry_category, MAX(s.updated_at) as latest "
                "FROM stock_info_cache s "
                "LEFT JOIN price_fetch_status p ON s.stock_id = p.stock_id "
                "WHERE (p.status IS NULL OR p.status != 'delisted') "
                "  AND s.stock_id GLOB '[0-9][0-9][0-9][0-9]*' "
                "  AND s.stock_id NOT GLOB '*[^0-9]*' "
                "GROUP BY s.stock_id"
            )).fetchall()

        if result:
            # 取最舊的 updated_at 判斷是否過期
            latest_str = max(r[3] for r in result if r[3])
            try:
                latest_dt = datetime.fromisoformat(latest_str)
                age_days = (datetime.now() - latest_dt).days
                if age_days <= STOCK_INFO_TTL_DAYS and len(result) >= STOCK_INFO_MIN_VALID_ROWS:
                    return pd.DataFrame(
                        [(r[0], r[1], r[2]) for r in result],
                        columns=["stock_id", "stock_name", "industry_category"]
                    )
            except Exception:
                pass  # 解析失敗則繼續重抓

    # 快取不存在或強制刷新，呼叫 API
    df = _get("TaiwanStockInfo")
    if df.empty:
        return df
    df = df[df["type"].isin(["twse", "tpex"])].copy()
    df["stock_id"] = df["stock_id"].astype(str).str.strip()
    df = df[df["stock_id"].str.fullmatch(r"\d{4,6}", na=False)].copy()
    df = (
        df[["stock_id", "stock_name", "industry_category"]]
        .drop_duplicates(subset="stock_id", keep="last")
        .sort_values("stock_id")
        .reset_index(drop=True)
    )

    # 更新快取（REPLACE INTO = upsert）
    try:
        from db.database import get_session
        from sqlalchemy import text
        rows = df.to_dict("records")
        sql = text("""
            INSERT OR REPLACE INTO stock_info_cache (stock_id, stock_name, industry_category, updated_at)
            VALUES (:stock_id, :stock_name, :industry_category, :ts)
        """)
        now = datetime.now().isoformat()
        with get_session() as sess:
            sess.execute(text("DELETE FROM stock_info_cache"))
            sess.execute(sql, [{**r, "ts": now} for r in rows])
            sess.commit()
    except Exception:
        pass  # 快取寫入失敗不影響功能

    return df


def get_daily_price(stock_id: str, days: int = 120, start_date: str = None) -> pd.DataFrame:
    """取得個股日K資料（預設近 120 天，可指定 start_date 覆蓋 days）"""
    start = start_date or (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = _get("TaiwanStockPrice", stock_id=stock_id, start_date=start)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    numeric_cols = ["open", "max", "min", "close", "Trading_Volume", "Trading_money", "spread", "Trading_turnover"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_institutional_investors(stock_id: str, days: int = 30) -> pd.DataFrame:
    """取得三大法人買賣超（外資、投信、自營）"""
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = _get("TaiwanStockInstitutionalInvestorsBuySell", stock_id=stock_id, start_date=start)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["buy"] = pd.to_numeric(df.get("buy", 0), errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
    df["net"] = df["buy"] - df["sell"]
    return df


def get_broker_trading_daily_report(stock_id: str, trade_date) -> pd.DataFrame:
    """
    取得單日券商分點買賣資料。

    FinMind TaiwanStockTradingDailyReport 單次只支援一檔股票一天資料，且需
    sponsor 權限。buy_volume / sell_volume 單位為股。
    """
    d = trade_date.date().isoformat() if hasattr(trade_date, "date") else str(trade_date)[:10]
    df = _get(
        "TaiwanStockTradingDailyReport",
        stock_id=stock_id,
        start_date=d,
        end_date=d,
    )
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    for col in ["buy_volume", "sell_volume", "buy_price", "sell_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if "buy_volume" not in df.columns:
        df["buy_volume"] = 0
    if "sell_volume" not in df.columns:
        df["sell_volume"] = 0
    df["net_volume"] = df["buy_volume"] - df["sell_volume"]
    return df


def summarize_broker_main_force(df: pd.DataFrame, top_n: int = 15) -> dict:
    """
    主力買賣超 = 前 N 大買超券商淨買張 - 前 N 大賣超券商淨賣張。

    回傳單位為「張」。FinMind 原始分點量為股，因此除以 1000。
    """
    if df is None or df.empty:
        return {}

    work = df.copy()
    if "net_volume" not in work.columns:
        work["net_volume"] = work.get("buy_volume", 0) - work.get("sell_volume", 0)

    grouped = (
        work.groupby(["date", "securities_trader_id", "securities_trader"], dropna=False)
        ["net_volume"]
        .sum()
        .reset_index()
    )
    if grouped.empty:
        return {}

    latest_date = pd.to_datetime(grouped["date"].iloc[0]).date().isoformat()
    buy_top = grouped[grouped["net_volume"] > 0].nlargest(top_n, "net_volume")
    sell_top = grouped[grouped["net_volume"] < 0].nsmallest(top_n, "net_volume")

    buy_top15 = float(buy_top["net_volume"].sum()) / 1000
    sell_top15 = float((-sell_top["net_volume"]).sum()) / 1000
    return {
        "date": latest_date,
        "buy_top15": buy_top15,
        "sell_top15": sell_top15,
        "net": buy_top15 - sell_top15,
        "broker_count": int(grouped["securities_trader_id"].nunique()),
    }


def get_broker_main_force_series(
    stock_id: str,
    trade_dates,
    *,
    top_n: int = 15,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    取得多日主力買賣超序列，優先使用本機快取，缺少日期才逐日呼叫 FinMind。
    """
    from db.broker_cache import load_broker_main_force, save_broker_main_force

    dates = []
    for d in trade_dates:
        if pd.isna(d):
            continue
        if hasattr(d, "date"):
            dates.append(d.date().isoformat())
        else:
            dates.append(str(d)[:10])
    dates = list(dict.fromkeys(dates))
    if not dates:
        return pd.DataFrame()

    cached = pd.DataFrame() if force_refresh else load_broker_main_force(stock_id, dates)
    cached_dates = set()
    if not cached.empty:
        cached_dates = set(pd.to_datetime(cached["date"]).dt.date.astype(str))

    missing = [d for d in dates if d not in cached_dates]
    fetched_rows = []
    for d in missing:
        try:
            daily = get_broker_trading_daily_report(stock_id, d)
            summary = summarize_broker_main_force(daily, top_n=top_n)
        except Exception as exc:
            logger.warning(f"get_broker_main_force_series {stock_id} {d}: {exc}")
            break
        if summary:
            fetched_rows.append(summary)

    if fetched_rows:
        save_broker_main_force(stock_id, fetched_rows)
        fresh = load_broker_main_force(stock_id, dates)
    else:
        fresh = cached

    if fresh.empty:
        return fresh
    fresh = fresh.sort_values("date").reset_index(drop=True)
    return fresh


def resolve_latest_trading_day() -> date:
    """
    確立全域最新交易日（GLOBAL_LATEST_TRADING_DAY）。

    判斷邏輯（依序）：
    1. 若快取結果在 TTL 內，直接回傳快取值。
    2. 週六 → 週五，週日 → 週五（不打 API）。
    3. 平日且時間 < 15:00 → 回傳昨日（今日收盤資料尚未入庫）。
    4. 平日且時間 >= 15:00 → 查 2330 最新一筆日期：
       - 等於今日 → 確立今日為最新交易日
       - 早於今日 → 可能颱風假/停市，退回該日期
    結果快取 1 小時，避免重複打 API。
    """
    global _global_latest_trading_day, _trading_day_resolved_at

    with _trading_day_lock:
        now = datetime.now()
        # TTL 快取：若已解析且在有效期內直接回傳
        if (_global_latest_trading_day is not None
                and _trading_day_resolved_at is not None
                and (now - _trading_day_resolved_at).total_seconds() < _TRADING_DAY_TTL_SEC):
            return _global_latest_trading_day

        today = now.date()
        weekday = today.weekday()  # 0=Mon … 4=Fri, 5=Sat, 6=Sun

        # 週末直接退回上週五，不打 API
        if weekday == 5:
            result = today - timedelta(days=1)
            logger.debug(f"resolve_latest_trading_day: 週六 → {result}")
        elif weekday == 6:
            result = today - timedelta(days=2)
            logger.debug(f"resolve_latest_trading_day: 週日 → {result}")
        elif now.hour < 15:
            # 平日收盤前：FinMind 今日資料尚未入庫，退回昨日
            result = today - timedelta(days=1)
            # 若昨日是週末，再往前推
            while result.weekday() >= 5:
                result -= timedelta(days=1)
            logger.debug(f"resolve_latest_trading_day: 收盤前 → {result}")
        else:
            # 平日 15:00 後：以 2330 最新資料日為基準
            try:
                yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
                benchmark_df = get_daily_price("2330", start_date=yesterday)
                if not benchmark_df.empty:
                    latest = pd.to_datetime(benchmark_df["date"].max()).date()
                    result = latest
                    logger.info(f"resolve_latest_trading_day: 2330 基準 → {result}")
                else:
                    # API 回空（可能颱風假），退回上個交易日
                    result = today - timedelta(days=1)
                    while result.weekday() >= 5:
                        result -= timedelta(days=1)
                    logger.warning(f"resolve_latest_trading_day: 2330 無資料，退回 {result}")
            except Exception as e:
                result = today - timedelta(days=1)
                while result.weekday() >= 5:
                    result -= timedelta(days=1)
                logger.warning(f"resolve_latest_trading_day: 查詢失敗（{e}），退回 {result}")

        _global_latest_trading_day = result
        _trading_day_resolved_at = now
        return result


def smart_get_price(stock_id: str, required_days: int = 150) -> pd.DataFrame:
    """
    智慧取價：先查本機快取，只補缺少的資料。

    - 快取已是最新交易日 → 視窗查詢快取，0 次 API
    - 快取有舊資料       → 補抓缺失段後，視窗查詢快取
    - 完全無快取         → 全段抓取存入後，視窗查詢快取

    最新交易日由 resolve_latest_trading_day() 確立（含 2330 基準驗證）。
    查詢使用 lookback_days 視窗，避免載入全量歷史造成記憶體瓶頸。
    """
    from db.price_cache import get_cached_dates, save_prices, load_prices

    latest_trading_day = resolve_latest_trading_day()
    today = datetime.now().date()

    min_date, max_date = get_cached_dates(stock_id)

    if max_date is not None:
        max_cache = (max_date if isinstance(max_date, date)
                     else datetime.strptime(str(max_date), "%Y-%m-%d").date())

        if max_cache >= latest_trading_day:
            # 快取已是最新，直接視窗讀取
            logger.debug(f"smart_get_price {stock_id}: 快取命中（{max_cache}）")
            return load_prices(stock_id, lookback_days=required_days)

        # 快取有舊資料，補抓缺失段
        fetch_from = (max_cache + timedelta(days=1)).strftime("%Y-%m-%d")
        logger.debug(f"smart_get_price {stock_id}: 補抓 {fetch_from} 起")
        try:
            new_df = get_daily_price(stock_id, start_date=fetch_from)
            if not new_df.empty:
                save_prices(stock_id, new_df)
        except Exception as e:
            logger.warning(f"smart_get_price {stock_id}: 補抓失敗（{e}），回傳舊快取")
        return load_prices(stock_id, lookback_days=required_days)

    # 完全無快取，全段抓取後存入再視窗讀取
    logger.debug(f"smart_get_price {stock_id}: 無快取，全量抓取")
    df = get_daily_price(stock_id, days=required_days)
    if not df.empty:
        save_prices(stock_id, df)
        return load_prices(stock_id, lookback_days=required_days)
    return df


def check_institutions_buying(
    idf: pd.DataFrame,
    days: int = 2,
    institutions: list = None,
    logic: str = "and",
) -> bool:
    """
    判斷指定法人是否連續 days 個交易日都是淨買超

    FinMind 的 name 欄位對應關係：
      Foreign_Investor / Foreign_Dealer_Self → 外資
      Investment_Trust                        → 投信
      Dealer_self / Dealer_Hedging            → 自營商

    institutions: ["外資", "投信", "自營商"] 的任意子集；
                  None 或空串列視同三者全選。
    logic: "and" = 所選法人都要符合；"or" = 所選法人任一符合即可。
    """
    if idf.empty or "name" not in idf.columns:
        return False

    if not institutions:
        institutions = ["外資", "投信", "自營商"]
    logic = (logic or "and").strip().lower()

    _filters = {
        "外資":   lambda d: d[d["name"].str.contains("Foreign", case=False, na=False)],
        "投信":   lambda d: d[d["name"].str.contains("Investment_Trust", case=False, na=False)],
        "自營商": lambda d: d[d["name"].str.contains("Dealer", case=False, na=False)],
    }

    results = []
    for inst in institutions:
        if inst not in _filters:
            continue
        grp = _filters[inst](idf)
        if grp.empty:
            results.append(False)
            continue
        daily = grp.groupby("date")["net"].sum().sort_index()
        recent = daily.tail(days)
        passed = len(recent) >= days and (recent > 0).all()
        results.append(passed)

    if not results:
        return False

    if logic == "or":
        return any(results)

    return all(results)


def summarize_institutional_signal(
    idf: pd.DataFrame,
    *,
    selected_institutions: list | None = None,
    strict_days: int = 2,
    agg_mode: str = "rolling_sum",
    agg_days: int = 5,
) -> dict:
    """
    彙總三大法人訊號，供選股 UI 與計分邏輯共用。

    參數：
        selected_institutions:
            嚴格模式下要檢查的法人集合，None/空值視同三者全選。
        strict_days:
            嚴格模式要求「各自連續買超」的交易日數。
        agg_mode:
            "rolling_sum"  -> 近 agg_days 日合計淨買超總和 > 0
            "consecutive"  -> 最近 agg_days 個交易日每天合計淨買超都 > 0
        agg_days:
            合計模式判斷視窗大小。

    回傳：
        {
            "strict_pass": bool,
            "aggregate_pass": bool,
            "foreign_trust_pass": bool,
            "aggregate_sum": float,
            "daily_total_net": pd.Series,
            "recent_inst_net": pd.DataFrame,
        }
    """
    if idf.empty or "name" not in idf.columns:
        return {
            "strict_pass": False,
            "aggregate_pass": False,
            "foreign_trust_pass": False,
            "aggregate_sum": 0.0,
            "main_force_buy_3d": False,
            "daily_total_net": pd.Series(dtype=float),
            "recent_inst_net": pd.DataFrame(),
        }

    if not selected_institutions:
        selected_institutions = ["外資", "投信", "自營商"]

    df = idf.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["net"] = pd.to_numeric(df.get("net", 0), errors="coerce").fillna(0.0)

    inst_label = np.select(
        [
            df["name"].str.contains("Foreign", case=False, na=False),
            df["name"].str.contains("Investment_Trust", case=False, na=False),
            df["name"].str.contains("Dealer", case=False, na=False),
        ],
        ["外資", "投信", "自營商"],
        default="其他",
    )
    df = df.assign(inst_label=inst_label)
    df = df[df["inst_label"].isin(["外資", "投信", "自營商"])].copy()
    if df.empty:
        return {
            "strict_pass": False,
            "aggregate_pass": False,
            "foreign_trust_pass": False,
            "aggregate_sum": 0.0,
            "main_force_buy_3d": False,
            "daily_total_net": pd.Series(dtype=float),
            "recent_inst_net": pd.DataFrame(),
        }

    # 先以向量化方式彙總成「日期 x 法人」矩陣，後續所有條件都從這個矩陣判斷。
    daily_inst_net = (
        df.groupby(["date", "inst_label"], as_index=False)["net"]
        .sum()
        .pivot(index="date", columns="inst_label", values="net")
        .fillna(0.0)
        .sort_index()
    )

    # 嚴格模式：所選法人各自都要連續 strict_days 為正。
    strict_pass = True
    for inst in selected_institutions:
        if inst not in daily_inst_net.columns:
            strict_pass = False
            break
        recent = daily_inst_net[inst].tail(strict_days)
        if len(recent) < strict_days or (recent <= 0).any():
            strict_pass = False
            break

    daily_total_net = daily_inst_net.sum(axis=1)
    recent_total = daily_total_net.tail(agg_days)
    aggregate_sum = float(recent_total.sum()) if not recent_total.empty else 0.0

    if agg_mode == "consecutive":
        aggregate_pass = len(recent_total) >= agg_days and (recent_total > 0).all()
    else:
        aggregate_pass = len(recent_total) >= agg_days and aggregate_sum > 0

    main_force_recent = daily_total_net.tail(3)
    main_force_buy_3d = len(main_force_recent) >= 3 and (main_force_recent > 0).all()

    # 土洋合買：外資與投信最近 strict_days 皆為淨買超。
    foreign_trust_pass = True
    for inst in ["外資", "投信"]:
        if inst not in daily_inst_net.columns:
            foreign_trust_pass = False
            break
        recent = daily_inst_net[inst].tail(strict_days)
        if len(recent) < strict_days or (recent <= 0).any():
            foreign_trust_pass = False
            break

    return {
        "strict_pass": strict_pass,
        "aggregate_pass": aggregate_pass,
        "foreign_trust_pass": foreign_trust_pass,
        "aggregate_sum": round(aggregate_sum, 2),
        "main_force_buy_3d": main_force_buy_3d,
        "daily_total_net": daily_total_net,
        "recent_inst_net": daily_inst_net.tail(max(strict_days, agg_days, 3)),
    }


def check_all_three_buying(idf: pd.DataFrame, days: int = 2) -> bool:
    """向下相容包裝：判斷三大法人是否連續 days 日齊買"""
    return check_institutions_buying(idf, days=days, institutions=None)


def smart_get_institutional(stock_id: str, days: int = 10) -> pd.DataFrame:
    """
    智慧取法人資料：先查本機快取，24 小時內不重複 API 請求

    - 快取夠新（24 小時內）→ 直接讀快取（0 次 API）
    - 快取過期或無快取    → 呼叫 FinMind API 並寫入快取
    """
    from db.inst_cache import is_inst_fresh, save_institutional, load_institutional

    if is_inst_fresh(stock_id):
        return load_institutional(stock_id, days=days)

    df = get_institutional_investors(stock_id, days=days)
    if not df.empty:
        save_institutional(stock_id, df)
    return df


def get_margin_trading(stock_id: str, days: int = 10) -> pd.DataFrame:
    """取得融資融券餘額"""
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = _get("TaiwanStockMarginPurchaseShortSale", stock_id=stock_id, start_date=start)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


def compute_margin_trend(margin_df: pd.DataFrame) -> tuple[str, int, int]:
    """
    根據融資餘額趨勢回傳 (trend, latest_balance, prev_balance)

    trend:
      'down'  — 最新融資餘額 < 前一日，散戶去槓桿（籌碼轉乾淨）
      'up'    — 融資餘額上升
      'flat'  — 無資料或持平

    回傳值中 latest_balance / prev_balance 供 UI 顯示用（張數）。
    """
    col = "MarginPurchaseTodayBalance"
    if margin_df.empty or col not in margin_df.columns:
        return "flat", 0, 0
    df = margin_df.sort_values("date").dropna(subset=[col])
    if len(df) < 2:
        return "flat", 0, 0
    latest = int(df[col].iloc[-1])
    prev   = int(df[col].iloc[-2])
    if latest < prev:
        return "down", latest, prev
    if latest > prev:
        return "up", latest, prev
    return "flat", latest, prev


def get_financial_statements(stock_id: str, years: int = 3) -> pd.DataFrame:
    """
    取得綜合損益表、資產負債表、現金流量表（季頻）

    回傳 DataFrame 欄位：date, stock_id, type, value, origin_name
    type 可能值：綜合損益表 / 資產負債表 / 現金流量表
    """
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    df = _get("TaiwanStockFinancialStatements", stock_id=stock_id, start_date=start)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def _extract_series(df: pd.DataFrame, name_pattern: str,
                    stmt_type: str = None) -> pd.Series:
    """從財報 DataFrame 提取特定科目的季度序列（按日期排序）"""
    mask = df["origin_name"].str.contains(name_pattern, case=False, na=False)
    if stmt_type:
        mask &= df["type"].str.contains(stmt_type, na=False)
    sub = df[mask].copy()
    if sub.empty:
        return pd.Series(dtype=float)
    # 同一日期可能有多個子科目，加總
    return sub.groupby("date")["value"].sum().sort_index()


def compute_fundamentals(df: pd.DataFrame) -> dict:
    """
    從 TaiwanStockFinancialStatements 原始資料計算基本面指標

    回傳 dict（有任何指標算不出來則對應欄位為 None）：
        eps_ttm              — 近 4 季 EPS 合計
        roe                  — 近 4 季 ROE (%)
        operating_cf         — 近 4 季營業現金流合計
        debt_ratio           — 最新負債比 (%)
        gross_margin_latest  — 最新季毛利率 (%)
        gross_margin_yoy     — 毛利率 YoY 變化（百分點）
        data_date            — 最新資料日期字串
    """
    if df.empty:
        return {}

    result: dict = {}

    # ── EPS（近 4 季合計）────────────────────────────────────────
    eps_s = _extract_series(df, "每股盈餘", "損益")
    if eps_s.empty:
        eps_s = _extract_series(df, "每股盈餘")
    result["eps_ttm"] = float(eps_s.tail(4).sum()) if len(eps_s) >= 1 else None

    # ── 淨利 & 股東權益 → ROE ───────────────────────────────────
    ni_s  = _extract_series(df, "本期淨利", "損益")
    eq_s  = _extract_series(df, "權益", "資產負債")
    if ni_s.empty:
        ni_s = _extract_series(df, "本期淨利")
    if eq_s.empty:
        eq_s = _extract_series(df, "權益")
    if len(ni_s) >= 1 and len(eq_s) >= 1:
        ni_ttm   = float(ni_s.tail(4).sum())
        eq_avg   = float(eq_s.tail(2).mean())  # 期初期末平均
        result["roe"] = round(ni_ttm / eq_avg * 100, 2) if eq_avg != 0 else None
    else:
        result["roe"] = None

    # ── 營業現金流（近 4 季合計）────────────────────────────────
    ocf_s = _extract_series(df, "營業活動", "現金流")
    if ocf_s.empty:
        ocf_s = _extract_series(df, "營業活動")
    result["operating_cf"] = float(ocf_s.tail(4).sum()) if len(ocf_s) >= 1 else None

    # ── 負債比（最新季）─────────────────────────────────────────
    ast_s = _extract_series(df, "資產總", "資產負債")
    lib_s = _extract_series(df, "負債總", "資產負債")
    if ast_s.empty:
        ast_s = _extract_series(df, "資產總計")
    if lib_s.empty:
        lib_s = _extract_series(df, "負債總計")
    if len(ast_s) >= 1 and len(lib_s) >= 1:
        ast_latest = float(ast_s.iloc[-1])
        lib_latest = float(lib_s.iloc[-1])
        result["debt_ratio"] = round(lib_latest / ast_latest * 100, 2) if ast_latest != 0 else None
    else:
        result["debt_ratio"] = None

    # ── 毛利率（最新季 & YoY）──────────────────────────────────
    rev_s = _extract_series(df, "營業收入", "損益")
    gp_s  = _extract_series(df, "毛利|營業毛利", "損益")
    if rev_s.empty:
        rev_s = _extract_series(df, "營業收入")
    if gp_s.empty:
        gp_s = _extract_series(df, "毛利")
    result["gross_margin_latest"] = None
    result["gross_margin_yoy"]    = None
    common = sorted(set(rev_s.index) & set(gp_s.index))
    if len(common) >= 1:
        latest = common[-1]
        rev_v = float(rev_s[latest])
        gp_v  = float(gp_s[latest])
        if rev_v != 0:
            gm_now = round(gp_v / rev_v * 100, 2)
            result["gross_margin_latest"] = gm_now
            # YoY：找同一季去年（4 季前）
            if len(common) >= 5:
                yoy_date = common[-5]
                rev_y = float(rev_s[yoy_date])
                gp_y  = float(gp_s[yoy_date])
                if rev_y != 0:
                    gm_yoy = round(gp_v / rev_v * 100 - gp_y / rev_y * 100, 2)
                    result["gross_margin_yoy"] = gm_yoy

    # 最新資料日期
    all_dates = df["date"].dropna()
    result["data_date"] = str(all_dates.max().date()) if not all_dates.empty else ""

    return result


def smart_get_fundamentals(stock_id: str) -> dict:
    """
    智慧取基本面指標：先查本機快取（90 天 TTL），過期才呼叫 API

    回傳 dict（空 dict 表示無資料，跳過基本面過濾）

    快取行為：
    - HTTP 402 / API 回空資料：存空快取（資料本身不可用，90 天內不重試）
    - PremiumUnavailableError：不存快取（Premium 關閉或 quota 不足，屬暫時狀態）
    - 其他 HTTP / 網路錯誤：不存快取（暫時性問題，下次重試）
    """
    import requests as _req
    from db.fundamental_cache import is_fundamental_fresh, load_fundamental, save_fundamental

    if is_fundamental_fresh(stock_id):
        return load_fundamental(stock_id)

    try:
        df = get_financial_statements(stock_id, years=3)
    except PremiumUnavailableError:
        # Premium 未啟用 / quota 不足 / runtime degraded：暫時狀態，不快取
        logger.debug(f"smart_get_fundamentals {stock_id}: Premium unavailable, skipping cache")
        return {}
    except _req.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        if status == 402:
            # API 端確認此資料集需要付費方案，存空快取避免 90 天內重複嘗試
            save_fundamental(stock_id, {})
            logger.warning(
                f"FinMind 財報端點需要付費方案（402），基本面過濾已停用。"
                f"（首次觸發於 {stock_id}）"
            )
        else:
            # 其他 HTTP 錯誤（429、5xx 等）屬暫時問題，不快取
            logger.debug(f"get_financial_statements {stock_id} HTTP {status}: {e}")
        return {}
    except Exception as e:
        # 網路錯誤、timeout 等暫時性問題，不快取
        logger.debug(f"get_financial_statements {stock_id} failed: {e}")
        return {}

    if df.empty:
        # 儲存空指標作為「已嘗試」記錄，避免每次都打 API
        save_fundamental(stock_id, {})
        return {}

    metrics = compute_fundamentals(df)
    if metrics:
        save_fundamental(stock_id, metrics)
    return metrics


def get_batch_prices(stock_ids: list, days: int = 120) -> dict[str, pd.DataFrame]:
    """批次取得多檔股票日K（逐一呼叫，注意 API 限制）"""
    result = {}
    for sid in stock_ids:
        try:
            df = get_daily_price(sid, days=days)
            if not df.empty:
                result[sid] = df
        except Exception:
            pass
    return result
