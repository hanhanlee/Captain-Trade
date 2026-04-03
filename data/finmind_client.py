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


def get_stock_list() -> pd.DataFrame:
    """取得所有上市股票清單"""
    df = _get("TaiwanStockInfo")
    if df.empty:
        return df
    # 過濾普通股（排除 ETF、特別股等）
    df = df[df["type"] == "twse"].copy()
    return df[["stock_id", "stock_name", "industry_category"]].reset_index(drop=True)


def get_daily_price(stock_id: str, days: int = 120) -> pd.DataFrame:
    """取得個股日K資料（預設近 120 天）"""
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
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
