"""
FinMind API 客戶端
文件：https://finmindtrade.com/analysis/#/Guidance/api
免費帳號每日限制 600 次請求
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

FINMIND_API = "https://api.finmindtrade.com/api/v4/data"
TOKEN = os.getenv("FINMIND_TOKEN", "")


def _get(dataset: str, stock_id: str = "", start_date: str = "", **kwargs) -> pd.DataFrame:
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
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != 200:
        raise RuntimeError(f"FinMind API error: {data.get('msg', 'unknown')}")

    return pd.DataFrame(data.get("data", []))


def get_stock_list(force_refresh: bool = False) -> pd.DataFrame:
    """
    取得所有上市股票清單

    優先讀本機快取（每日更新一次），避免每次掃描都呼叫 API。
    force_refresh=True 強制重新抓取並更新快取。
    """
    from db.database import get_session, init_db
    from db.models import StockInfoCache
    from sqlalchemy import text

    init_db()

    if not force_refresh:
        # 先查快取，若今天已更新過就直接用
        with get_session() as sess:
            rows = sess.execute(text(
                "SELECT stock_id, stock_name, industry_category FROM stock_info_cache"
            )).fetchall()
        if rows:
            return pd.DataFrame(rows, columns=["stock_id", "stock_name", "industry_category"])

    # 快取不存在或強制刷新，呼叫 API
    df = _get("TaiwanStockInfo")
    if df.empty:
        return df
    df = df[df["type"] == "twse"].copy()
    df = df[["stock_id", "stock_name", "industry_category"]].reset_index(drop=True)

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
    df = _get("TaiwanStockInstitutionalInvestors", stock_id=stock_id, start_date=start)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["buy"] = pd.to_numeric(df.get("buy", 0), errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df.get("sell", 0), errors="coerce").fillna(0)
    df["net"] = df["buy"] - df["sell"]
    return df


def smart_get_price(stock_id: str, required_days: int = 150) -> pd.DataFrame:
    """
    智慧取價：先查本機快取，只補缺少的資料

    - 快取有 5 日內資料 → 直接讀快取（0 次 API）
    - 快取有舊資料 → 只抓缺失的新資料後合併（省 90%+ API）
    - 完全無快取 → 全部抓並存入快取
    """
    from db.price_cache import get_cached_dates, save_prices, load_prices

    today = datetime.now().date()
    fresh_threshold = today - timedelta(days=5)  # 涵蓋週末與假日
    start_str = (today - timedelta(days=required_days)).strftime("%Y-%m-%d")

    min_date, max_date = get_cached_dates(stock_id)

    if max_date is not None:
        max_cache = max_date if isinstance(max_date, type(today)) else \
            datetime.strptime(str(max_date), "%Y-%m-%d").date()

        # 快取夠新，直接讀
        if max_cache >= fresh_threshold:
            return load_prices(stock_id, start_date=start_str)

        # 快取有舊資料，只抓缺失的部分
        fetch_from = (max_cache + timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            new_df = get_daily_price(stock_id, start_date=fetch_from)
            if not new_df.empty:
                save_prices(stock_id, new_df)
        except Exception:
            pass  # 補資料失敗時，仍使用舊快取
        return load_prices(stock_id, start_date=start_str)

    # 完全無快取，全部抓
    df = get_daily_price(stock_id, days=required_days)
    if not df.empty:
        save_prices(stock_id, df)
    return df


def check_all_three_buying(idf: pd.DataFrame, days: int = 2) -> bool:
    """
    判斷三大法人是否連續 days 個交易日齊買

    FinMind 的 name 欄位包含：
      Foreign_Investor / Foreign_Dealer_Self → 外資
      Investment_Trust                        → 投信
      Dealer_self / Dealer_Hedging            → 自營商

    回傳 True 代表三方在最近 days 個交易日每日都是淨買超
    """
    if idf.empty or "name" not in idf.columns:
        return False

    groups = {
        "外資":   idf[idf["name"].str.contains("Foreign", case=False, na=False)],
        "投信":   idf[idf["name"].str.contains("Investment_Trust", case=False, na=False)],
        "自營商": idf[idf["name"].str.contains("Dealer", case=False, na=False)],
    }

    for name, grp in groups.items():
        if grp.empty:
            return False
        daily = grp.groupby("date")["net"].sum().sort_index()
        recent = daily.tail(days)
        if len(recent) < days or (recent <= 0).any():
            return False

    return True


def get_margin_trading(stock_id: str, days: int = 10) -> pd.DataFrame:
    """取得融資融券餘額"""
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    df = _get("TaiwanStockMarginPurchaseShortSale", stock_id=stock_id, start_date=start)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


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
