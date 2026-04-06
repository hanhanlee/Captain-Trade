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


def save_prices(stock_id: str, df: pd.DataFrame) -> int:
    """
    批次寫入日K資料，已存在的 (stock_id, date) 自動跳過

    df 欄位：date, open, max(=high), min(=low), close, Trading_Volume
    """
    if df.empty:
        return 0

    rows = []
    for _, row in df.iterrows():
        d = row["date"].date() if hasattr(row["date"], "date") else row["date"]
        rows.append({
            "stock_id": stock_id,
            "date": str(d),
            "open":   float(row["open"])            if pd.notna(row.get("open"))            else None,
            "high":   float(row.get("max", row.get("high", None))) if pd.notna(row.get("max", row.get("high"))) else None,
            "low":    float(row.get("min", row.get("low",  None))) if pd.notna(row.get("min", row.get("low")))  else None,
            "close":  float(row["close"])           if pd.notna(row.get("close"))           else None,
            "volume": float(row.get("Trading_Volume", row.get("volume", None)))
                      if pd.notna(row.get("Trading_Volume", row.get("volume"))) else None,
            "ts": datetime.now().isoformat(),
        })

    if not rows:
        return 0

    sql = text("""
        INSERT OR IGNORE INTO price_cache
            (stock_id, date, open, high, low, close, volume, updated_at)
        VALUES
            (:stock_id, :date, :open, :high, :low, :close, :volume, :ts)
    """)

    with get_session() as sess:
        sess.execute(sql, rows)   # 批次執行，一次送出所有 rows
        sess.commit()

    return len(rows)


def load_prices(stock_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """從本機快取讀取日K資料（利用複合索引快速查詢）"""
    query = "SELECT date, open, high, low, close, volume FROM price_cache WHERE stock_id = :sid"
    params: dict = {"sid": stock_id}

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

    # 對齊 scanner 期望的欄位名稱
    return df.rename(columns={"high": "max", "low": "min", "volume": "Trading_Volume"})


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


def diagnose_cache(min_days: int = 100, stale_days: int = 10) -> dict:
    """
    診斷快取品質，找出問題股票。已下市股票（ref/suspendList.csv）單獨分類，不列入問題清單。

    回傳 dict：
      summary       — DataFrame：每支有快取的股票完整摘要（含 status 欄）
      missing       — 在 stock_info_cache 但完全沒有快取的股票代碼列表（排除下市）
      thin          — 快取筆數 < min_days 的股票 DataFrame（排除下市）
      stale         — 最新日期超過 stale_days 天前的股票 DataFrame（排除下市）
      delisted      — 快取中屬於下市股票的 DataFrame
      problem_ids   — thin + stale + missing 的聯集，供批次補抓用
    """
    from datetime import date, timedelta

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
        }

    stale_cutoff = (date.today() - timedelta(days=stale_days)).isoformat()
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
            flags.append("資料過舊")
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
