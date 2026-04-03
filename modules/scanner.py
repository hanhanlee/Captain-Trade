"""
選股雷達 — 核心篩選邏輯（v2，含三項策略增強）
混合型交易風格：波段為主 + 部分長期持有

基礎技術條件（必要，全部達到才入選）：
  1. 收盤價站上 20MA 且 20MA 向上
  2. 成交量 > 5 日均量 × 1.3（量能確認）
  3. MACD 黃金交叉 或 RSI(14) 在 50–70 之間
  4. 收盤價不低於布林通道下軌

進階加分條件（v2 新增）：
  5. 多週期共振：週線 MA10 向上且收盤站上週線 MA10
  6. 相對強度：近 3 個月漲幅優於大盤（抗跌 / 領漲特性）

分數說明：
  基礎條件滿分 100，進階條件最多額外 +18
  分數 > 100 代表強勢股中的精選標的
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from .indicators import sma, rsi, macd, bollinger_bands, weekly_ma_trend, relative_strength_score


@dataclass
class ScanSignal:
    # ── 基礎技術條件 ──────────────────────────────
    above_ma20: bool = False
    ma20_rising: bool = False
    volume_surge: bool = False
    macd_cross: bool = False
    rsi_healthy: bool = False
    above_bb_lower: bool = False
    # ── 籌碼面條件 ────────────────────────────────
    institutional_buy: bool = False
    margin_clean: bool = False
    # ── v2 進階條件 ───────────────────────────────
    weekly_trend_up: bool = False      # 週線 MA10 向上且站上
    rs_positive: bool = False          # 相對強度優於大盤
    rs_score: float = 0.0             # RS 分數（0-100）

    def passes_basic(self) -> bool:
        """必要技術條件全部通過才入選"""
        return (
            self.above_ma20
            and self.ma20_rising
            and self.volume_surge
            and self.above_bb_lower
            and (self.macd_cross or self.rsi_healthy)
        )

    def score(self) -> float:
        """
        技術強度分數
        基礎滿分 100，進階條件額外加分（可超過 100）
        """
        weights = {
            "above_ma20": 20,
            "ma20_rising": 15,
            "volume_surge": 20,
            "macd_cross": 15,
            "rsi_healthy": 10,
            "above_bb_lower": 10,
            "institutional_buy": 7,
            "margin_clean": 3,
            # v2 進階加分
            "weekly_trend_up": 10,
            "rs_positive": 8,
        }
        return round(sum(v for k, v in weights.items() if getattr(self, k)), 1)

    def triggered_labels(self) -> list:
        label_map = {
            "above_ma20": "站上MA20",
            "ma20_rising": "MA20向上",
            "volume_surge": "量增",
            "macd_cross": "MACD黃金交叉",
            "rsi_healthy": "RSI健康",
            "above_bb_lower": "布林正常",
            "institutional_buy": "法人買超",
            "margin_clean": "籌碼乾淨",
            "weekly_trend_up": "週線多頭",
            "rs_positive": "相對強勢",
        }
        return [v for k, v in label_map.items() if getattr(self, k)]


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """計算日線技術指標，回傳加上指標欄位的 DataFrame"""
    df = df.copy()
    close = df["close"]
    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None

    df["ma5"] = sma(close, 5)
    df["ma10"] = sma(close, 10)
    df["ma20"] = sma(close, 20)
    df["ma60"] = sma(close, 60)

    df["rsi14"] = rsi(close, 14)

    dif, dea, hist = macd(close)
    df["macd"] = dif
    df["macd_signal"] = dea
    df["macd_hist"] = hist

    bb_upper, bb_mid, bb_lower = bollinger_bands(close, 20, 2.0)
    df["bb_upper"] = bb_upper
    df["bb_mid"] = bb_mid
    df["bb_lower"] = bb_lower

    if vol_col:
        df["vol_ma5"] = df[vol_col].rolling(5).mean()

    return df


def analyze_stock(
    df: pd.DataFrame,
    inst_net: float = 0.0,
    margin_trend: str = "flat",
    market_close: pd.Series = None,   # 大盤收盤序列，供 RS 計算（選填）
):
    """
    分析單一股票，回傳 ScanSignal 或 None（資料不足）

    inst_net:     近 5 日法人淨買超張數（正為買超）
    margin_trend: 'up' | 'down' | 'flat'
    market_close: 大盤（加權指數）收盤序列，用於相對強度計算
    """
    df = compute_indicators(df)

    if len(df) < 30 or df["ma20"].isna().all():
        return None

    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else latest

    sig = ScanSignal()

    # 1. 站上 MA20
    if pd.notna(latest["ma20"]) and latest["close"] > latest["ma20"]:
        sig.above_ma20 = True

    # 2. MA20 向上（今日 > 5 日前）
    if len(df) >= 6:
        ma20_now = df["ma20"].iloc[-1]
        ma20_5ago = df["ma20"].iloc[-6]
        if pd.notna(ma20_now) and pd.notna(ma20_5ago) and ma20_now > ma20_5ago:
            sig.ma20_rising = True

    # 3. 量能放大
    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
    if vol_col and pd.notna(latest.get("vol_ma5")) and latest["vol_ma5"] > 0:
        if latest[vol_col] > latest["vol_ma5"] * 1.3:
            sig.volume_surge = True

    # 4. MACD（黃金交叉或多頭排列）
    if pd.notna(latest["macd"]) and pd.notna(latest["macd_signal"]):
        cross_up = (latest["macd"] > latest["macd_signal"]
                    and prev["macd"] <= prev["macd_signal"])
        bull_above_zero = latest["macd"] > 0 and latest["macd"] > latest["macd_signal"]
        if cross_up or bull_above_zero:
            sig.macd_cross = True

    # 5. RSI 健康區（50–70）
    if pd.notna(latest["rsi14"]) and 50 <= latest["rsi14"] <= 70:
        sig.rsi_healthy = True

    # 6. 不低於布林下軌
    if pd.notna(latest["bb_lower"]) and latest["close"] >= latest["bb_lower"]:
        sig.above_bb_lower = True

    # 7. 法人買超
    if inst_net > 0:
        sig.institutional_buy = True

    # 8. 融資減少
    if margin_trend == "down":
        sig.margin_clean = True

    # 9. [v2] 多週期共振：週線 MA10 向上且站上
    if len(df) >= 70:   # 至少 14 週資料才計算週線
        wt = weekly_ma_trend(df, ma_period=10)
        if wt and wt.get("weekly_above_ma") and wt.get("weekly_ma_rising"):
            sig.weekly_trend_up = True

    # 10. [v2] 相對強度（抗跌 / 領漲特性）
    if len(df) >= 63:
        rs = relative_strength_score(df, lookback_days=63, market_returns=market_close)
        if rs:
            sig.rs_score = rs["rs_score"]
            if rs["outperforming"] and rs["rs_score"] >= 60:
                sig.rs_positive = True

    return sig


def run_scan(
    price_data: dict,
    stock_info: pd.DataFrame,
    inst_data: dict = None,
    margin_data: dict = None,
    min_price: float = 10.0,
    market_df: pd.DataFrame = None,  # 加權指數日K（選填，供 RS 計算）
) -> pd.DataFrame:
    """
    執行全市場掃描，回傳通過篩選的股票排行榜

    欄位：stock_id, stock_name, industry, close, change_pct,
          volume_ratio, score, rs_score, signals
    """
    results = []
    stock_info_map = {}
    if not stock_info.empty:
        stock_info_map = (
            stock_info.drop_duplicates(subset="stock_id")
            .set_index("stock_id")
            .to_dict("index")
        )

    inst_data = inst_data or {}
    margin_data = margin_data or {}

    # 準備大盤收盤序列
    market_close = None
    if market_df is not None and not market_df.empty and "close" in market_df.columns:
        market_close = market_df["close"].reset_index(drop=True)

    for stock_id, df in price_data.items():
        if df.empty or len(df) < 30:
            continue

        close = df.iloc[-1].get("close", 0)
        if close < min_price:
            continue

        sig = analyze_stock(
            df,
            inst_net=inst_data.get(stock_id, 0.0),
            margin_trend=margin_data.get(stock_id, "flat"),
            market_close=market_close,
        )
        if sig is None or not sig.passes_basic():
            continue

        prev_close = df.iloc[-2]["close"] if len(df) >= 2 else close
        change_pct = round((close - prev_close) / prev_close * 100, 2) if prev_close else 0

        vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
        vol_now = df[vol_col].iloc[-1] if vol_col else 0
        vol_ma5 = df[vol_col].rolling(5).mean().iloc[-1] if vol_col else 0
        volume_ratio = round(vol_now / vol_ma5, 2) if vol_ma5 and vol_ma5 > 0 else 0

        info = stock_info_map.get(stock_id, {})
        results.append({
            "stock_id": stock_id,
            "stock_name": info.get("stock_name", ""),
            "industry": info.get("industry_category", ""),
            "close": close,
            "change_pct": change_pct,
            "volume_ratio": volume_ratio,
            "score": sig.score(),
            "rs_score": sig.rs_score,
            "signals": "、".join(sig.triggered_labels()),
        })

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)


def sector_analysis(result_df: pd.DataFrame) -> pd.DataFrame:
    """
    產業族群分析：計算哪些產業入選最多強勢股

    回傳：DataFrame，欄位：industry, count, avg_score, top_stock
    """
    if result_df.empty or "industry" not in result_df.columns:
        return pd.DataFrame()

    df = result_df[result_df["industry"].notna() & (result_df["industry"] != "")]

    summary = (
        df.groupby("industry")
        .agg(
            count=("stock_id", "count"),
            avg_score=("score", "mean"),
            max_score=("score", "max"),
        )
        .reset_index()
    )

    # 找每個產業分數最高的股票
    best = (
        df.sort_values("score", ascending=False)
        .groupby("industry")
        .first()[["stock_id", "stock_name"]]
        .reset_index()
    )
    best["top_stock"] = best["stock_id"] + " " + best["stock_name"].fillna("")

    summary = summary.merge(best[["industry", "top_stock"]], on="industry", how="left")
    summary["avg_score"] = summary["avg_score"].round(1)

    return summary.sort_values("count", ascending=False).reset_index(drop=True)
