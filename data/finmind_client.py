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
