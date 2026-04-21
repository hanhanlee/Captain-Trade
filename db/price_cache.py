"""
本機歷史價格快取管理

效能設計：
  - 批次 INSERT OR IGNORE（一次 SQL 取代逐行）
  - 複合索引 (stock_id, date) 加速查詢
  - WAL mode + 32MB cache（在 database.py 設定）
"""
import pandas as pd
from datetime import date, datetime, timedelta

from sqlalchemy import text
from .database import get_session, ENGINE, init_db
from .models import Base


STATUS_NORMAL = "normal"
STATUS_SUSPENDED = "suspended"
STATUS_DELISTED = "delisted"
STATUS_LEGACY_NO_UPDATE = "no_update"


def init_cache_table():
    Base.metadata.create_all(ENGINE)


def get_cached_dates(stock_id: str) -> tuple:
    """回傳該股在快取中的最早/最新日期，若無則回傳 (None, None)"""
    with get_session() as sess:
        result = sess.execute(
            text("SELECT MIN(date), MAX(date) FROM price_cache WHERE stock_id = :sid"),
            {"sid": stock_id}
        ).fetchone()
        return result[0], result[1]


def save_prices(stock_id: str, df: pd.DataFrame, replace: bool = True) -> int:
    """
    批次寫入日K資料。

    replace=True（預設）: INSERT OR REPLACE — FinMind 使用，覆蓋舊資料確保資料正確性
    replace=False:        INSERT OR IGNORE  — Yahoo Bridge 使用，不覆蓋已有的 FinMind 資料

    df 欄位：date, open, max(=high), min(=low), close, Trading_Volume
    """
    if df.empty:
        return 0

    rows = []
    for _, row in df.iterrows():
        d = row["date"].date() if hasattr(row["date"], "date") else row["date"]
        rows.append({
            "stock_id": stock_id,
            "date":   str(d),
            "open":   float(row["open"])                                          if pd.notna(row.get("open"))                            else None,
            "high":   float(row.get("max", row.get("high", None)))                if pd.notna(row.get("max", row.get("high")))            else None,
            "low":    float(row.get("min", row.get("low",  None)))                if pd.notna(row.get("min", row.get("low")))             else None,
            "close":  float(row["close"])                                         if pd.notna(row.get("close"))                           else None,
            "volume": float(row.get("Trading_Volume", row.get("volume", None)))   if pd.notna(row.get("Trading_Volume", row.get("volume"))) else None,
            "ts":     datetime.now().isoformat(),
        })

    if not rows:
        return 0

    keyword = "REPLACE" if replace else "IGNORE"
    sql = text(f"""
        INSERT OR {keyword} INTO price_cache
            (stock_id, date, open, high, low, close, volume, updated_at)
        VALUES
            (:stock_id, :date, :open, :high, :low, :close, :volume, :ts)
    """)

    with get_session() as sess:
        sess.execute(sql, rows)
        sess.commit()

    return len(rows)


def save_prices_batch(df: pd.DataFrame) -> int:
    """
    多股批次寫入日K資料（一次 DB transaction）。

    df 須含 stock_id 欄位，其餘欄位同 save_prices。
    使用 INSERT OR REPLACE 確保覆蓋舊資料。
    """
    if df.empty or "stock_id" not in df.columns:
        return 0

    now_str = datetime.now().isoformat()
    rows = []
    for _, row in df.iterrows():
        d = row["date"].date() if hasattr(row["date"], "date") else row["date"]
        rows.append({
            "stock_id": str(row["stock_id"]),
            "date":   str(d),
            "open":   float(row["open"])                                          if pd.notna(row.get("open"))                            else None,
            "high":   float(row.get("max", row.get("high", None)))                if pd.notna(row.get("max", row.get("high")))            else None,
            "low":    float(row.get("min", row.get("low",  None)))                if pd.notna(row.get("min", row.get("low")))             else None,
            "close":  float(row["close"])                                         if pd.notna(row.get("close"))                           else None,
            "volume": float(row.get("Trading_Volume", row.get("volume", None)))   if pd.notna(row.get("Trading_Volume", row.get("volume"))) else None,
            "ts":     now_str,
        })

    if not rows:
        return 0

    sql = text("""
        INSERT OR REPLACE INTO price_cache
            (stock_id, date, open, high, low, close, volume, updated_at)
        VALUES
            (:stock_id, :date, :open, :high, :low, :close, :volume, :ts)
    """)
    with get_session() as sess:
        sess.execute(sql, rows)
        sess.commit()
    return len(rows)


def load_prices(stock_id: str, start_date: str = None, end_date: str = None,
                lookback_days: int = None) -> pd.DataFrame:
    """
    從本機快取讀取日K資料。

    優先使用 lookback_days 視窗查詢（SQL 層截斷，避免載入大量歷史資料）：
      - lookback_days=150 → 取最近 150 筆，記憶體使用量固定
      - lookback_days=None → 全量讀取（回測專用）
    start_date / end_date 可與 lookback_days 並用做二次過濾。
    """
    params: dict = {"sid": stock_id}

    if lookback_days is not None:
        # DESC LIMIT 取最新 N 筆，再用子查詢翻回 ASC 順序給 pandas
        inner = (
            "SELECT date, open, high, low, close, volume "
            "FROM price_cache WHERE stock_id = :sid"
        )
        if end_date:
            inner += " AND date <= :end"
            params["end"] = end_date
        inner += f" ORDER BY date DESC LIMIT {int(lookback_days)}"
        query = f"SELECT * FROM ({inner}) ORDER BY date ASC"
    else:
        query = "SELECT date, open, high, low, close, volume FROM price_cache WHERE stock_id = :sid"
        if start_date:
            query += " AND date >= :start"
            params["start"] = start_date
        if end_date:
            query += " AND date <= :end"
            params["end"] = end_date
        query += " ORDER BY date ASC"

    with get_session() as sess:
        result = sess.execute(text(query), params).fetchall()

    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(result, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.rename(columns={"high": "max", "low": "min", "volume": "Trading_Volume"})


def get_fetch_status(stock_id: str) -> dict | None:
    """回傳單股的抓取狀態，無記錄時回傳 None"""
    with get_session() as sess:
        row = sess.execute(
            text("SELECT status, last_attempt_at FROM price_fetch_status WHERE stock_id = :sid"),
            {"sid": stock_id}
        ).fetchone()
    if not row:
        return None
    return {"status": row[0], "last_attempt_at": row[1]}


def set_fetch_status(stock_id: str, status: str):
    """寫入或更新單股的抓取狀態（ok / no_update / error）"""
    now = datetime.now().isoformat()
    with get_session() as sess:
        sess.execute(text("""
            INSERT OR REPLACE INTO price_fetch_status
                (stock_id, status, last_attempt_at, updated_at)
            VALUES (:sid, :status, :now, :now)
        """), {"sid": stock_id, "status": status, "now": now})
        sess.commit()


def get_status_stock_ids(statuses: list[str] | tuple[str, ...], *,
                         stale_days: int | None = None,
                         today_only: bool = False,
                         recent_hours: float | None = None) -> list[str]:
    """
    依 status 取回股票代碼。

    recent_hours: 只回傳 last_attempt_at 在最近 N 小時內的紀錄。
                  與 today_only 合用時兩者都要滿足。
                  用途：suspended 判斷只封鎖「近期才失敗」的股票，
                  避免同一天早些時候的失敗封鎖整天的重試。
    """
    if not statuses:
        return []

    params = {f"s{i}": status for i, status in enumerate(statuses)}
    placeholders = ",".join(f":s{i}" for i in range(len(statuses)))
    clauses = [f"status IN ({placeholders})"]

    if stale_days is not None:
        params["cutoff"] = (datetime.now() - timedelta(days=stale_days)).isoformat()
        clauses.append("(last_attempt_at IS NULL OR last_attempt_at < :cutoff)")

    if today_only:
        params["today"] = datetime.now().date().isoformat()
        clauses.append("substr(COALESCE(last_attempt_at, ''), 1, 10) = :today")

    if recent_hours is not None:
        params["recent_cutoff"] = (datetime.now() - timedelta(hours=recent_hours)).isoformat()
        clauses.append("last_attempt_at >= :recent_cutoff")

    where_sql = " AND ".join(clauses)
    with get_session() as sess:
        rows = sess.execute(text(f"""
            SELECT stock_id
            FROM price_fetch_status
            WHERE {where_sql}
            ORDER BY last_attempt_at ASC, stock_id ASC
        """), params).fetchall()
    return [r[0] for r in rows]


def get_delisted_stocks(*, stale_days: int | None = None,
                        include_legacy_no_update: bool = False) -> list[str]:
    """回傳已標記為 delisted 的股票；必要時可暫時含舊版 no_update。"""
    statuses = [STATUS_DELISTED]
    if include_legacy_no_update:
        statuses.append(STATUS_LEGACY_NO_UPDATE)
    return get_status_stock_ids(tuple(statuses), stale_days=stale_days)


def get_suspended_stocks(*, today_only: bool = False,
                         recent_hours: float | None = None) -> list[str]:
    """回傳已標記為 suspended 的股票。"""
    return get_status_stock_ids((STATUS_SUSPENDED,), today_only=today_only,
                                recent_hours=recent_hours)


def get_failed_today_detail() -> pd.DataFrame:
    """
    回傳今日抓取失敗（suspended）的股票詳情，含股票名稱。

    回傳欄位：stock_id, stock_name, industry, failed_at
    """
    today = datetime.now().date().isoformat()
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT
                f.stock_id,
                COALESCE(i.stock_name, '') AS stock_name,
                COALESCE(i.industry_category, '') AS industry,
                f.last_attempt_at AS failed_at
            FROM price_fetch_status f
            LEFT JOIN stock_info_cache i USING (stock_id)
            WHERE f.status = 'suspended'
              AND substr(COALESCE(f.last_attempt_at, ''), 1, 10) = :today
            ORDER BY f.last_attempt_at ASC
        """), {"today": today}).fetchall()
    if not rows:
        return pd.DataFrame(columns=["stock_id", "stock_name", "industry", "failed_at"])
    df = pd.DataFrame(rows, columns=["stock_id", "stock_name", "industry", "failed_at"])
    # 只保留時間部分（HH:MM:SS）
    df["failed_at"] = pd.to_datetime(df["failed_at"], errors="coerce").dt.strftime("%H:%M:%S")
    return df


def get_no_update_stocks(stale_days: int = 7) -> list[str]:
    """
    回傳 no_update 且 last_attempt_at 超過 stale_days 天的股票清單
    （供死股回收邏輯使用）
    """
    return get_status_stock_ids((STATUS_LEGACY_NO_UPDATE,), stale_days=stale_days)


def load_prices_multi(stock_ids: list, start_date: str = None) -> dict:
    """
    批次讀取多檔股票的快取資料（一次 SQL，比逐檔查詢快）

    回傳：{stock_id: DataFrame}
    """
    if not stock_ids:
        return {}

    placeholders = ",".join(f":id{i}" for i in range(len(stock_ids)))
    params: dict = {f"id{i}": sid for i, sid in enumerate(stock_ids)}

    query = f"SELECT stock_id, date, open, high, low, close, volume FROM price_cache WHERE stock_id IN ({placeholders})"
    if start_date:
        query += " AND date >= :start"
        params["start"] = start_date
    query += " ORDER BY stock_id, date ASC"

    with get_session() as sess:
        rows = sess.execute(text(query), params).fetchall()

    if not rows:
        return {}

    df_all = pd.DataFrame(rows, columns=["stock_id", "date", "open", "high", "low", "close", "volume"])
    df_all["date"] = pd.to_datetime(df_all["date"])
    for col in ["open", "high", "low", "close", "volume"]:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce")
    df_all = df_all.rename(columns={"high": "max", "low": "min", "volume": "Trading_Volume"})

    return {sid: grp.drop(columns="stock_id").reset_index(drop=True)
            for sid, grp in df_all.groupby("stock_id")}


def get_all_cached_stocks() -> list:
    """回傳快取中有資料的股票代碼列表"""
    with get_session() as sess:
        rows = sess.execute(
            text("SELECT DISTINCT stock_id FROM price_cache ORDER BY stock_id")
        ).fetchall()
    return [r[0] for r in rows]


def get_cache_summary() -> pd.DataFrame:
    """回傳快取狀態摘要表"""
    with get_session() as sess:
        rows = sess.execute(text("""
            SELECT stock_id,
                   MIN(date) as earliest,
                   MAX(date) as latest,
                   COUNT(*) as days
            FROM price_cache
            GROUP BY stock_id
            ORDER BY stock_id
        """)).fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["stock_id", "earliest", "latest", "days"])


def get_known_stock_ids() -> list[str]:
    """回傳 stock_info_cache 中已知的所有股票代碼"""
    with get_session() as sess:
        rows = sess.execute(
            text("SELECT stock_id FROM stock_info_cache ORDER BY stock_id")
        ).fetchall()
    return [r[0] for r in rows]


_SUSPEND_LIST_PATH = None  # 由外部設定，或自動偵測


def load_suspend_ids() -> set[str]:
    """讀取 ref/suspendList.csv，回傳已下市股票代碼集合。"""
    import pathlib
    candidates = [
        pathlib.Path(__file__).parent.parent / "ref" / "suspendList.csv",
    ]
    for p in candidates:
        if p.exists():
            try:
                df = pd.read_csv(str(p), encoding="cp950", skiprows=1, header=None)
                ids = df.iloc[:, 2].dropna().astype(str).str.strip()
                return set(ids[ids.str.match(r"^\d{4,6}$")].tolist())
            except Exception:
                pass
    return set()


def diagnose_cache(min_days: int = 100) -> dict:
    """
    診斷快取品質，找出問題股票。已下市股票（ref/suspendList.csv）單獨分類，不列入問題清單。
    「過舊」判斷以 resolve_latest_trading_day() 為基準，而非固定天數。

    回傳 dict：
      summary          — DataFrame：每支有快取的股票完整摘要（含 status 欄）
      missing          — 在 stock_info_cache 但完全沒有快取的股票代碼列表（排除下市）
      thin             — 快取筆數 < min_days 的股票 DataFrame（排除下市）
      stale            — 最新日期 < 最新交易日 的股票 DataFrame（排除下市）
      delisted         — 快取中屬於下市股票的 DataFrame
      problem_ids      — thin + stale + missing 的聯集，供批次補抓用
      latest_trading_day — 本次使用的基準交易日
    """
    try:
        from data.finmind_client import resolve_latest_trading_day
        latest_trading_day = resolve_latest_trading_day()
    except Exception:
        from datetime import date, timedelta
        today = date.today()
        wd = today.weekday()
        latest_trading_day = (today - timedelta(days=1) if wd == 5 else
                              today - timedelta(days=2) if wd == 6 else today)

    suspend_ids = load_suspend_ids()
    summary = get_cache_summary()
    known_ids = get_known_stock_ids()
    cached_ids = set(summary["stock_id"].tolist()) if not summary.empty else set()

    # 完全缺失（排除下市股票）
    missing_ids = [sid for sid in known_ids if sid not in cached_ids and sid not in suspend_ids]

    if summary.empty:
        return {
            "summary": summary,
            "missing": missing_ids,
            "thin": pd.DataFrame(),
            "stale": pd.DataFrame(),
            "delisted": pd.DataFrame(),
            "problem_ids": missing_ids,
            "suspend_ids": suspend_ids,
            "latest_trading_day": latest_trading_day,
        }

    stale_cutoff = latest_trading_day.isoformat()
    summary["latest"] = summary["latest"].astype(str)
    summary["delisted"] = summary["stock_id"].isin(suspend_ids)

    active = summary[~summary["delisted"]].copy()
    delisted_df = summary[summary["delisted"]].copy()

    thin_df = active[active["days"] < min_days].copy()
    stale_df = active[active["latest"] < stale_cutoff].copy()

    def _status(row):
        if row["delisted"]:
            return "已下市"
        flags = []
        if row["days"] < min_days:
            flags.append(f"資料不足({row['days']}天)")
        if row["latest"] < stale_cutoff:
            flags.append(f"資料過舊（基準：{stale_cutoff}）")
        return "、".join(flags) if flags else "正常"

    summary["status"] = summary.apply(_status, axis=1)

    problem_ids = list(
        set(thin_df["stock_id"].tolist())
        | set(stale_df["stock_id"].tolist())
        | set(missing_ids)
    )

    return {
        "summary": summary,
        "missing": missing_ids,
        "thin": thin_df,
        "stale": stale_df,
        "delisted": delisted_df,
        "problem_ids": problem_ids,
        "suspend_ids": suspend_ids,
        "latest_trading_day": latest_trading_day,
    }


def delete_old_prices(keep_days: int = 400):
    """
    刪除超過 keep_days 天的舊資料，控制資料庫大小

    建議定期執行（例如每月一次），之後搭配 vacuum_db() 回收空間
    """
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=keep_days)).isoformat()
    with get_session() as sess:
        result = sess.execute(
            text("DELETE FROM price_cache WHERE date < :cutoff"),
            {"cutoff": cutoff}
        )
        sess.commit()
        return result.rowcount
