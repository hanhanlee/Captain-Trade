"""
選股雷達 — 核心篩選邏輯（v3）
混合型交易風格：波段為主 + 部分長期持有

基礎技術條件（必要，全部達到才入選）：
  1. 收盤價站上 20MA 且 20MA 向上
  2. 成交量 > 5 日均量 × 1.3（量能確認）
  3. MACD 黃金交叉 或 RSI(14) 在 50–70 之間
  4. 收盤價不低於布林通道下軌

進階加分條件（v2）：
  5. 多週期共振：週線 MA10 向上且收盤站上週線 MA10（+10）
  6. 相對強度：近 3 個月漲幅優於大盤（+8）

進階加分條件（v3 新增）：
  7. 多頭排列：MA5 > MA10 > MA20（+5）
  8. 量能品質：近 10 日上漲日成交量佔比 ≥ 60%（+7）
  9. 突破盤整：收盤突破近 60 日最高收盤價（+8）

分數說明：
  基礎條件滿分 100，進階條件最多額外 +38
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
    institutional_buy: bool = False    # 三大法人連續 2 日齊買
    margin_clean: bool = False
    # ── v2 進階條件 ───────────────────────────────
    weekly_trend_up: bool = False      # 週線 MA10 向上且站上
    rs_positive: bool = False          # 相對強度優於大盤
    rs_score: float = 0.0             # RS 分數（0-100）
    # ── v3 進階條件 ───────────────────────────────
    ma_aligned: bool = False           # MA5 > MA10 > MA20 多頭排列
    vol_quality: bool = False          # 近 10 日量集中在上漲日
    breakout: bool = False             # 突破近 60 日收盤高點

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
            # v3 進階加分
            "ma_aligned": 5,
            "vol_quality": 7,
            "breakout": 8,
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
            "institutional_buy": "三大法人齊買",
            "margin_clean": "籌碼乾淨",
            "weekly_trend_up": "週線多頭",
            "rs_positive": "相對強勢",
            "ma_aligned": "多頭排列",
            "vol_quality": "量能優質",
            "breakout": "突破盤整",
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
    inst_buying: bool = False,
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

    # 7. 三大法人連續 2 日齊買
    if inst_buying:
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

    # 11. [v3] 多頭排列：MA5 > MA10 > MA20
    if (pd.notna(latest.get("ma5")) and pd.notna(latest.get("ma10"))
            and pd.notna(latest.get("ma20"))):
        if latest["ma5"] > latest["ma10"] > latest["ma20"]:
            sig.ma_aligned = True

    # 12. [v3] 量能品質：近 10 日上漲日成交量佔 60% 以上
    if vol_col and len(df) >= 11:
        recent = df.tail(11).copy()
        recent["up_day"] = recent["close"] >= recent["close"].shift(1)
        recent = recent.dropna(subset=["up_day"])
        total_vol = recent[vol_col].sum()
        up_vol = recent.loc[recent["up_day"], vol_col].sum()
        if total_vol > 0 and up_vol / total_vol >= 0.6:
            sig.vol_quality = True

    # 13. [v3] 突破盤整：今日收盤 > 近 60 日最高收盤（排除近 3 日避免自我比較）
    if len(df) >= 20:
        lookback = df.iloc[-63:-3] if len(df) >= 66 else df.iloc[:-3]
        if not lookback.empty:
            resistance = lookback["close"].max()
            if pd.notna(resistance) and latest["close"] > resistance:
                sig.breakout = True

    return sig


def get_top_sector_ids(
    price_data: dict,
    stock_info_map: dict,
    top_n: int = 3,
    lookback: int = 5,
) -> tuple[set, dict]:
    """
    計算近 lookback 個交易日各產業平均漲幅，回傳前 top_n 產業的股票 ID 集合

    回傳：(qualifying_stock_ids, sector_return_dict)
    sector_return_dict 格式：{industry: {"return_pct": float, "stock_count": int}}
    """
    sector_returns: dict = {}
    for stock_id, df in price_data.items():
        if len(df) < lookback + 1:
            continue
        industry = stock_info_map.get(stock_id, {}).get("industry_category", "")
        if not industry:
            continue
        past_close = df["close"].iloc[-(lookback + 1)]
        curr_close = df["close"].iloc[-1]
        if pd.isna(past_close) or past_close <= 0:
            continue
        ret = (curr_close - past_close) / past_close * 100
        if industry not in sector_returns:
            sector_returns[industry] = []
        sector_returns[industry].append(ret)

    sector_avg = {
        ind: {"return_pct": round(sum(rets) / len(rets), 2), "stock_count": len(rets)}
        for ind, rets in sector_returns.items() if rets
    }
    top_industries = set(
        sorted(sector_avg, key=lambda x: sector_avg[x]["return_pct"], reverse=True)[:top_n]
    )
    qualifying_ids = {
        sid for sid in price_data
        if stock_info_map.get(sid, {}).get("industry_category", "") in top_industries
    }
    return qualifying_ids, sector_avg


def compute_sector_breakout(
    price_data: dict,
    stock_info_map: dict,
    use_hp_density: bool = False,
    hp_lookback: int = 20,
    hp_threshold: float = 0.30,
    use_turnover: bool = False,
    turnover_top_n: int = 5,
    return_lookback: int = 5,
) -> dict:
    """
    族群集體突破指標計算

    指標一：族群創高密度（HP Density）
      計算族群內有多少比例的股票今日收盤等於近 hp_lookback 日最高收盤。
      密度越高代表族群成員集體向上突破，是輪動強勢的早期訊號。

    指標二：資金流向比重（Turnover Ratio）
      以「當日收盤 × 成交量」估算各族群成交額，計算其佔全市場的比重。
      排名前 turnover_top_n 的族群視為資金集中流入。

    集體突破（collective_breakout）：
      兩個指標都啟用時：HP Density 達標 AND 排名前 N
      只啟用其中一個：只看該指標

    回傳 dict：{
        sector_name: {
            "return_pct":        float,   # 近 5 日平均漲幅
            "stock_count":       int,     # 族群股票數
            "hp_density":        float,   # 族群創高比例 0-1（use_hp_density=True 時才有）
            "hp_density_pass":   bool,    # 是否達門檻
            "turnover_ratio":    float,   # 成交額市占率 0-1（use_turnover=True 時才有）
            "turnover_rank":     int,     # 成交額排行
            "turnover_top":      bool,    # 是否在前 N 名
            "collective_breakout": bool,  # 是否符合集體突破特徵
        }
    }
    """
    if not use_hp_density and not use_turnover:
        return {}

    sector_high: dict[str, list] = {}   # {sector: [high_count, total_count]}
    sector_amount: dict[str, float] = {}
    sector_returns: dict[str, list] = {}
    total_amount = 0.0

    for stock_id, df in price_data.items():
        if df.empty or len(df) < 5:
            continue
        industry = stock_info_map.get(stock_id, {}).get("industry_category", "")
        if not industry:
            continue
        close_now = df["close"].iloc[-1]
        if pd.isna(close_now) or close_now <= 0:
            continue

        # 近 N 日漲幅（供 return_pct 計算）
        if len(df) >= return_lookback + 1:
            past_close = df["close"].iloc[-(return_lookback + 1)]
            if pd.notna(past_close) and past_close > 0:
                ret = (close_now - past_close) / past_close * 100
                sector_returns.setdefault(industry, []).append(ret)

        # HP Density
        if use_hp_density and len(df) >= hp_lookback:
            high_n = df["close"].tail(hp_lookback).max()
            is_at_high = pd.notna(high_n) and close_now >= high_n
            arr = sector_high.setdefault(industry, [0, 0])
            arr[1] += 1
            if is_at_high:
                arr[0] += 1

        # Turnover（close × volume 估算成交額）
        if use_turnover and "Trading_Volume" in df.columns:
            vol = df["Trading_Volume"].iloc[-1]
            if pd.notna(vol) and vol > 0:
                amount = close_now * float(vol)
                sector_amount[industry] = sector_amount.get(industry, 0.0) + amount
                total_amount += amount

    all_sectors = set(sector_returns) | set(sector_high) | set(sector_amount)
    result: dict = {}

    for sector in all_sectors:
        rets = sector_returns.get(sector, [])
        entry: dict = {
            "return_pct":  round(sum(rets) / len(rets), 2) if rets else 0.0,
            "stock_count": len(rets),
        }
        if use_hp_density:
            arr = sector_high.get(sector, [0, 0])
            density = arr[0] / arr[1] if arr[1] > 0 else 0.0
            entry["hp_density"]      = round(density, 3)
            entry["hp_density_pass"] = density >= hp_threshold
        if use_turnover:
            entry["_amt"] = sector_amount.get(sector, 0.0)
        result[sector] = entry

    # 計算 turnover 排行
    if use_turnover and total_amount > 0:
        sorted_sectors = sorted(result.items(), key=lambda x: x[1].get("_amt", 0), reverse=True)
        top_set = {s for s, _ in sorted_sectors[:turnover_top_n]}
        rank_map = {s: i + 1 for i, (s, _) in enumerate(sorted_sectors)}
        for sector, entry in result.items():
            amt = entry.pop("_amt", 0.0)
            entry["turnover_ratio"] = round(amt / total_amount, 4)
            entry["turnover_rank"]  = rank_map.get(sector, 999)
            entry["turnover_top"]   = sector in top_set

    # 集體突破標記
    for sector, entry in result.items():
        hp_ok = entry.get("hp_density_pass", True) if use_hp_density else True
        to_ok = entry.get("turnover_top",    True) if use_turnover   else True
        entry["collective_breakout"] = hp_ok and to_ok

    return result


def _passes_fundamental(fund: dict, config: dict) -> tuple[bool, str]:
    """
    檢查基本面指標是否通過過濾條件

    回傳 (passed: bool, fail_reason: str)
    fund 為空 dict 時視為「無資料，直接放行」（不過濾 ETF / 資料不足的股票）
    """
    if not fund or not config:
        return True, ""

    if config.get("require_eps_positive") and fund.get("eps_ttm") is not None:
        if fund["eps_ttm"] <= 0:
            return False, f"EPS TTM {fund['eps_ttm']:.2f} ≤ 0（虧損股）"

    if config.get("require_positive_cf") and fund.get("operating_cf") is not None:
        if fund["operating_cf"] <= 0:
            return False, f"營業現金流 {fund['operating_cf']:.0f} ≤ 0"

    min_roe = config.get("min_roe", 0)
    if min_roe > 0 and fund.get("roe") is not None:
        if fund["roe"] < min_roe:
            return False, f"ROE {fund['roe']:.1f}% < {min_roe}%"

    max_debt = config.get("max_debt_ratio", 0)
    if max_debt > 0 and fund.get("debt_ratio") is not None:
        if fund["debt_ratio"] > max_debt:
            return False, f"負債比 {fund['debt_ratio']:.1f}% > {max_debt}%"

    return True, ""


def run_scan(
    price_data: dict,
    stock_info: pd.DataFrame,
    inst_data: dict = None,
    margin_data: dict = None,
    fundamental_data: dict = None,    # {stock_id: metrics_dict}，None = 不做基本面過濾
    fundamental_filter: dict = None,  # 過濾條件設定 dict
    min_price: float = 10.0,
    min_avg_volume: int = 0,          # 最低日均量（張），0 = 不過濾
    top_volume_n: int = 0,            # 只取前日成交量前 N 名，0 = 不限
    top_sector_n: int = 0,            # 只掃描近 5 日漲幅前 N 個產業，0 = 不限
    market_df: pd.DataFrame = None,   # 加權指數日K（選填，供 RS 計算）
    use_hp_density: bool = False,     # 族群創高密度偵測
    hp_density_lookback: int = 20,    # 創高天數 N
    hp_density_threshold: float = 0.30,  # 族群創高比例門檻
    use_turnover_ratio: bool = False,  # 資金流向比重偵測
    turnover_top_n: int = 5,          # 資金前幾大族群
    debug: bool = False,              # True 時回傳第三個元素 debug_info
) -> tuple:
    """
    執行全市場掃描，回傳 (result_df, sector_info, debug_info)

    result_df 欄位：stock_id, stock_name, industry, close, change_pct,
                    volume_ratio, score, rs_score, signals
    sector_info：當 top_sector_n > 0 時，回傳各產業漲幅排行 dict；否則為 {}
    debug_info（debug=True 時才有內容）：
        pre_filter_stages  — 前置過濾各階段存活數 list[dict]
        stock_analysis     — 每檔股票的 ScanSignal 及排除原因 dict
    """
    results = []
    sector_info = {}

    stock_info_map = {}
    if not stock_info.empty:
        stock_info_map = (
            stock_info.drop_duplicates(subset="stock_id")
            .set_index("stock_id")
            .to_dict("index")
        )

    inst_data = inst_data or {}
    margin_data = margin_data or {}
    fundamental_data = fundamental_data or {}
    fundamental_filter = fundamental_filter or {}

    # 準備大盤收盤序列
    market_close = None
    if market_df is not None and not market_df.empty and "close" in market_df.columns:
        market_close = market_df["close"].reset_index(drop=True)

    # ── debug 初始化 ───────────────────────────────────────────
    pre_stages = []          # [{"stage": label, "count": N}, ...]
    stock_analysis = {}      # {stock_id: {"sig", "close", ...}}
    if debug:
        pre_stages.append({"stage": "載入資料", "count": len(price_data)})

    # ── 前日量排行前 N 名（動態過濾）──────────────────────────
    if top_volume_n > 0:
        vol_ranks = {}
        for sid, df in price_data.items():
            if not df.empty and "Trading_Volume" in df.columns:
                prev_vol = df["Trading_Volume"].iloc[-1]
                if pd.notna(prev_vol):
                    vol_ranks[sid] = prev_vol
        top_ids = set(
            sorted(vol_ranks, key=vol_ranks.get, reverse=True)[:top_volume_n]
        )
        price_data = {sid: df for sid, df in price_data.items() if sid in top_ids}
        if debug:
            pre_stages.append({"stage": f"量能前 {top_volume_n} 名", "count": len(price_data)})

    # ── 近 5 日漲幅前 N 個產業過濾 ────────────────────────────
    if top_sector_n > 0:
        qualifying_ids, sector_avg = get_top_sector_ids(
            price_data, stock_info_map, top_n=top_sector_n, lookback=5
        )
        price_data = {sid: df for sid, df in price_data.items() if sid in qualifying_ids}
        sector_info = sector_avg
        if debug:
            pre_stages.append({"stage": f"產業前 {top_sector_n} 名", "count": len(price_data)})

    # ── 族群集體突破偵測（在技術條件迴圈前，對當前 price_data 整體計算）──
    if use_hp_density or use_turnover_ratio:
        breakout_data = compute_sector_breakout(
            price_data, stock_info_map,
            use_hp_density=use_hp_density,
            hp_lookback=hp_density_lookback,
            hp_threshold=hp_density_threshold,
            use_turnover=use_turnover_ratio,
            turnover_top_n=turnover_top_n,
        )
        for sector, bdata in breakout_data.items():
            if sector not in sector_info:
                sector_info[sector] = {
                    "return_pct":  bdata.get("return_pct", 0.0),
                    "stock_count": bdata.get("stock_count", 0),
                }
            sector_info[sector].update(bdata)
        if debug:
            pre_stages.append({
                "stage": "族群集體突破分析",
                "count": sum(1 for v in breakout_data.values() if v.get("collective_breakout")),
            })

    for stock_id, df in price_data.items():
        info = stock_info_map.get(stock_id, {})
        close = df.iloc[-1].get("close", 0) if not df.empty else 0

        # ── 資料不足 ───────────────────────────────────────────
        if df.empty or len(df) < 30:
            if debug:
                stock_analysis[stock_id] = {
                    "stock_name": info.get("stock_name", ""),
                    "industry": info.get("industry_category", ""),
                    "close": close,
                    "exclude_pre": "資料不足（< 30 天）",
                    "sig": None,
                }
            continue

        # ── 最低股價 ───────────────────────────────────────────
        if close < min_price:
            if debug:
                stock_analysis[stock_id] = {
                    "stock_name": info.get("stock_name", ""),
                    "industry": info.get("industry_category", ""),
                    "close": close,
                    "exclude_pre": f"股價 {close:.1f} < {min_price:.0f} 元",
                    "sig": None,
                }
            continue

        # ── 日均量固定門檻（與 top_volume_n 擇一使用）──────────
        if min_avg_volume > 0 and top_volume_n == 0:
            vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
            if vol_col:
                avg_vol_20 = df[vol_col].tail(20).mean()
                if pd.isna(avg_vol_20) or avg_vol_20 < min_avg_volume * 1000:
                    if debug:
                        stock_analysis[stock_id] = {
                            "stock_name": info.get("stock_name", ""),
                            "industry": info.get("industry_category", ""),
                            "close": close,
                            "exclude_pre": f"日均量 {avg_vol_20/1000:.0f} 張 < {min_avg_volume} 張",
                            "sig": None,
                        }
                    continue

        # ── 基本面過濾（EPS / ROE / 現金流 / 負債比）────────────
        if fundamental_filter:
            fund = fundamental_data.get(stock_id, {})
            passed, reason = _passes_fundamental(fund, fundamental_filter)
            if not passed:
                if debug:
                    stock_analysis[stock_id] = {
                        "stock_name": info.get("stock_name", ""),
                        "industry": info.get("industry_category", ""),
                        "close": close,
                        "exclude_pre": f"基本面：{reason}",
                        "sig": None,
                    }
                continue

        sig = analyze_stock(
            df,
            inst_buying=inst_data.get(stock_id, False),
            margin_trend=margin_data.get(stock_id, "flat"),
            market_close=market_close,
        )

        if debug:
            stock_analysis[stock_id] = {
                "stock_name": info.get("stock_name", ""),
                "industry": info.get("industry_category", ""),
                "close": close,
                "exclude_pre": None,
                "sig": sig,
            }

        if sig is None or not sig.passes_basic():
            continue

        prev_close = df.iloc[-2]["close"] if len(df) >= 2 else close
        change_pct = round((close - prev_close) / prev_close * 100, 2) if prev_close else 0

        vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
        vol_now = df[vol_col].iloc[-1] if vol_col else 0
        vol_ma5 = df[vol_col].rolling(5).mean().iloc[-1] if vol_col else 0
        volume_ratio = round(vol_now / vol_ma5, 2) if vol_ma5 and vol_ma5 > 0 else 0

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

    result_df = (
        pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
        if results else pd.DataFrame()
    )

    debug_info = {}
    if debug:
        debug_info = {
            "pre_filter_stages": pre_stages,
            "stock_analysis": stock_analysis,
        }

    return result_df, sector_info, debug_info


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
