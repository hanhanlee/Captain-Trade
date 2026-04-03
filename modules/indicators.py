"""
技術指標計算（純 pandas / numpy 實作，無需第三方指標套件）
"""
import pandas as pd
import numpy as np


def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(window=length).mean()


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=length - 1, min_periods=length).mean()
    avg_loss = loss.ewm(com=length - 1, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """回傳 (dif, dea, hist)"""
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    dif = ema_fast - ema_slow
    dea = ema(dif, signal)
    hist = (dif - dea) * 2
    return dif, dea, hist


def bollinger_bands(series: pd.Series, length: int = 20, std: float = 2.0):
    """回傳 (upper, mid, lower)"""
    mid = sma(series, length)
    sigma = series.rolling(length).std()
    upper = mid + std * sigma
    lower = mid - std * sigma
    return upper, mid, lower


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    平均真實振幅 (Average True Range)

    TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    ATR = EMA(TR, period)
    """
    high_col = "max" if "max" in df.columns else "high"
    low_col = "min" if "min" in df.columns else "low"
    high = df[high_col]
    low = df[low_col]
    prev_close = df["close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    return tr.ewm(com=period - 1, min_periods=period).mean()


# ── 週線指標 ──────────────────────────────────────────────────

def to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """
    將日K轉換成週K（以每週最後一個交易日為基準）

    輸入 df 需含：date, open, close, max/high, min/low, Trading_Volume
    """
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    df = df.set_index("date").sort_index()

    high_col = "max" if "max" in df.columns else "high"
    low_col = "min" if "min" in df.columns else "low"
    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else "volume"

    agg = {
        "open": "first",
        "close": "last",
    }
    if high_col in df.columns:
        agg[high_col] = "max"
    if low_col in df.columns:
        agg[low_col] = "min"
    if vol_col in df.columns:
        agg[vol_col] = "sum"

    weekly = df.resample("W").agg(agg).dropna(subset=["close"])
    weekly = weekly.reset_index().rename(columns={"date": "week_end"})
    return weekly


def weekly_ma_trend(df: pd.DataFrame, ma_period: int = 10) -> dict:
    """
    計算週線趨勢指標

    回傳：
        weekly_above_ma: 本週收盤 > 週MA10
        weekly_ma_rising: 週MA10 現在 > 4週前（代表週線趨勢向上）
        weekly_ma_value: 週MA10 數值
        weekly_close: 最近週收盤價
    """
    weekly = to_weekly(df)
    if len(weekly) < ma_period + 4:
        return {}

    weekly[f"wma{ma_period}"] = sma(weekly["close"], ma_period)

    latest = weekly.iloc[-1]
    ma_now = latest.get(f"wma{ma_period}")
    ma_4w_ago = weekly[f"wma{ma_period}"].iloc[-5] if len(weekly) >= 5 else None

    if pd.isna(ma_now):
        return {}

    return {
        "weekly_above_ma": bool(latest["close"] > ma_now),
        "weekly_ma_rising": bool(pd.notna(ma_4w_ago) and ma_now > ma_4w_ago),
        "weekly_ma_value": round(ma_now, 2),
        "weekly_close": round(latest["close"], 2),
        "weekly_df": weekly,           # 供 K 線圖使用
    }


# ── 相對強度（Relative Strength）────────────────────────────

def relative_strength_score(
    df: pd.DataFrame,
    lookback_days: int = 63,        # 約 3 個月
    market_returns: pd.Series = None,  # 大盤同期報酬序列（選填）
) -> dict:
    """
    計算個股相對強度

    RS 概念源自 William O'Neil 的 IBD RS Rating：
    衡量個股在指定期間內的報酬表現，相對於大盤（或市場平均）的優劣。

    若未提供 market_returns，則計算個股自身的動能分數（純價格動能）。

    回傳：
        rs_score:   0-100 的相對強度分數（> 70 視為強勢）
        stock_return_pct: 個股期間報酬%
        market_return_pct: 大盤同期報酬%（若有提供）
        outperforming: 是否跑贏大盤
    """
    if df.empty or len(df) < lookback_days:
        return {}

    close = df["close"].values
    current = close[-1]
    past = close[-lookback_days]

    if past <= 0:
        return {}

    stock_return = (current - past) / past * 100

    if market_returns is not None and len(market_returns) >= lookback_days:
        mkt_current = market_returns.iloc[-1]
        mkt_past = market_returns.iloc[-lookback_days]
        market_return = (mkt_current - mkt_past) / mkt_past * 100 if mkt_past > 0 else 0
        outperforming = stock_return > market_return
        # RS score：相對超額報酬映射到 0-100
        excess = stock_return - market_return
        rs_score = min(100, max(0, 50 + excess * 2))
    else:
        # 無大盤資料時，用絕對動能評分（> 0% 為正向，以 20% 為滿分）
        market_return = 0.0
        outperforming = stock_return > 0
        rs_score = min(100, max(0, 50 + stock_return * 2.5))

    return {
        "rs_score": round(rs_score, 1),
        "stock_return_pct": round(stock_return, 2),
        "market_return_pct": round(market_return, 2),
        "outperforming": outperforming,
    }


def calc_market_return(index_df: pd.DataFrame, lookback_days: int = 63) -> float:
    """從加權指數 DataFrame 計算同期報酬%"""
    if index_df is None or index_df.empty or len(index_df) < lookback_days:
        return 0.0
    c = index_df["close"].values
    if c[-lookback_days] <= 0:
        return 0.0
    return round((c[-1] - c[-lookback_days]) / c[-lookback_days] * 100, 2)
