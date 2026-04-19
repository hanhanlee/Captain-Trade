"""
選股雷達 — 核心篩選邏輯（v4 領先攻擊版）
核心哲學：找「均線糾結後第一天突破」，捕捉啟動點而非追強勢股

基礎技術條件（必要，全部達到才入選）：
  1. 第一天站上 5/10/20MA（今日三線全穿，昨日全在線下） +35
  2. 均線糾結度 < 3%（昨日三線距離 ÷ MA20，確認盤整後突破）+20
  3. 量能 > 前五日均量 × 1.5 倍（確認突破有足夠動能）    +15
  4. 股價 < MA20 + 3.5 × ATR(14)（動態門檻排除過熱股）   +10
  5. 相對強度 RS > 80（領先大盤，確認個股強勢）            +10
  6. 突破近 60 日收盤高點（確認為真實突破，而非盤整反彈）  +10
  7. 主力連續 3 日買超（三大法人合計淨買超皆為正）          +10

加分條件（滿足越多分數越高）：
  8. 布林頻寬縮減（今日 bandwidth < 20 日前）              +10
  9. 投信第一天買超（昨非正、今轉正）                       +10
 10. 突破近 20 日收盤高點                                  +8
 11. 週線 MA10 扣抵值低位（10週前收盤 < 當前週MA10）        +10
 12. 融資減少 / 籌碼集中                                   +5

滿分：必要 110 + 計分加分最多 35 = 145
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from .indicators import sma, rsi, macd, bollinger_bands, weekly_ma_trend, relative_strength_score, atr


@dataclass
class ScanSignal:
    # ── v4 必要條件 ───────────────────────────────
    ma_triple_breakout: bool = False   # 第一天站上5/10/20MA（昨日全在線下）
    ma_squeeze: bool = False           # 均線糾結度 < 3%（檢查昨日，確認盤整後突破）
    volume_explosion: bool = False     # 量能 > 前五日均量 × 1.5
    atr_ok: bool = False               # 股價 < MA20 + 3.5 × ATR(14)（動態過熱門檻）
    rs_strong: bool = False            # 相對強度 RS > 80（個股領先大盤）
    breakout_60d: bool = False         # 突破近 60 日收盤高點
    ma20_bias_ratio: float = 0.0       # 月線乖離率數值（%）
    atr_overheat: bool = False         # 過熱：收盤 > MA20 + 3.5 × ATR14
    atr14: float = 0.0                 # ATR14 數值
    # ── v4 加分條件 ───────────────────────────────
    bb_bandwidth_shrink: bool = False  # 布林頻寬縮減（vs 20日前）
    trust_first_buy: bool = False      # 投信第一天買超（昨非正→今轉正）
    breakout_20d: bool = False         # 突破近 20 日收盤高點
    weekly_deduction_low: bool = False # 週線MA10扣抵值低位（10週前收盤 < 當前週MA10）
    margin_clean: bool = False         # 融資減少 / 籌碼集中
    rs_positive: bool = False          # 相對強度 RS > 70
    rs_score: float = 0.0              # RS 分數（0-100）
    main_force_buy_3d: bool = False    # 主力買賣超（前15買超分點 − 前15賣超分點）連續 3 日 > 0
    # ── 保留 v3 欄位（供顯示/回測參考，不計入主計分）──
    above_ma20: bool = False
    ma20_rising: bool = False
    volume_surge: bool = False
    macd_cross: bool = False
    rsi_healthy: bool = False
    above_bb_lower: bool = False
    institutional_buy: bool = False
    inst_total_buy: bool = False
    foreign_trust_buy: bool = False
    weekly_trend_up: bool = False
    ma_aligned: bool = False
    vol_quality: bool = False
    breakout: bool = False

    def passes_basic(self) -> bool:
        """v4 必要條件：六項全部達到才入選"""
        return (
            self.ma_triple_breakout
            and self.ma_squeeze
            and self.volume_explosion
            and self.atr_ok
            and self.rs_strong
            and self.breakout_60d
            and self.main_force_buy_3d
        )

    def passes_basic_v3(self) -> bool:
        """v3 必要條件：站上MA20 + MA20向上 + 量增 + 不低於布林下軌 + (MACD 或 RSI)"""
        return (
            self.above_ma20
            and self.ma20_rising
            and self.volume_surge
            and self.above_bb_lower
            and (self.macd_cross or self.rsi_healthy)
            and self.main_force_buy_3d
        )

    def score(self) -> float:
        """
        v4 領先攻擊版計分
        必要條件滿分 110，加分條件最多 +35，上限 145
        """
        weights = {
            # 必要條件（通過才會到達此處，作為基礎分）
            "ma_triple_breakout": 35,
            "ma_squeeze": 20,
            "volume_explosion": 15,
            "atr_ok": 10,
            "rs_strong": 10,
            "breakout_60d": 10,
            "main_force_buy_3d": 10,
            # 加分條件
            "bb_bandwidth_shrink": 10,
            "trust_first_buy": 10,
            "weekly_deduction_low": 10,
            "margin_clean": 5,
        }
        return round(sum(v for k, v in weights.items() if getattr(self, k, False)), 1)

    def score_v3(self) -> float:
        """
        v3 均線突破版計分
        基礎滿分 100，進階條件可超過 100
        """
        weights = {
            "above_ma20": 20, "ma20_rising": 15, "volume_surge": 20,
            "macd_cross": 15, "rsi_healthy": 10, "above_bb_lower": 10,
            "main_force_buy_3d": 10, "institutional_buy": 7, "margin_clean": 3,
            "weekly_trend_up": 10, "rs_positive": 8,
            "ma_aligned": 5, "vol_quality": 7, "breakout": 8,
        }
        return round(sum(v for k, v in weights.items() if getattr(self, k, False)), 1)

    def triggered_labels(self, strategy_version: str | None = None) -> list:
        label_map = {
            # v4 必要
            "ma_triple_breakout": "三線齊穿(首日)",
            "ma_squeeze": "均線糾結<3%",
            "volume_explosion": "量能>均量1.5倍",
            "atr_ok": "股價<MA20+3.5ATR",
            "rs_strong": "RS>80強勢",
            "breakout_60d": "突破60日新高",
            "main_force_buy_3d": "主力連3日買超",
            # v4 加分
            "bb_bandwidth_shrink": "布林頻寬縮減",
            "trust_first_buy": "投信首日買超",
            "weekly_deduction_low": "週線扣抵低位",
            "margin_clean": "籌碼乾淨",
            "rs_positive": "相對強勢RS>70",
            # v3 訊號
            "above_ma20": "站上MA20",
            "ma20_rising": "MA20向上",
            "volume_surge": "量增(均量1.3x)",
            "macd_cross": "MACD黃金交叉",
            "rsi_healthy": "RSI健康",
            "above_bb_lower": "布林正常",
            "institutional_buy": "法人買超",
            "weekly_trend_up": "週線多頭",
            "ma_aligned": "多頭排列",
            "vol_quality": "量能優質",
            "breakout": "突破60日高點",
        }
        if strategy_version == "v4":
            allowed = {
                "ma_triple_breakout", "ma_squeeze", "volume_explosion",
                "atr_ok", "rs_strong", "breakout_60d", "main_force_buy_3d",
                "bb_bandwidth_shrink", "trust_first_buy",
                "weekly_deduction_low", "margin_clean", "rs_positive",
            }
        elif strategy_version == "v3":
            allowed = {
                "above_ma20", "ma20_rising", "volume_surge",
                "macd_cross", "rsi_healthy", "above_bb_lower",
                "main_force_buy_3d", "institutional_buy", "margin_clean",
                "weekly_trend_up", "rs_positive", "ma_aligned",
                "vol_quality", "breakout",
            }
        else:
            allowed = set(label_map)
        return [v for k, v in label_map.items() if k in allowed and getattr(self, k, False)]


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """計算日線技術指標，回傳加上指標欄位的 DataFrame"""
    df = df.copy()
    close = df["close"]
    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None

    df["ma5"] = sma(close, 5)
    df["ma10"] = sma(close, 10)
    df["ma20"] = sma(close, 20)
    df["ma60"] = sma(close, 60)
    # 月線乖離率：衡量收盤價偏離 MA20 的幅度，避免追價買進過熱股
    df["ma20_bias_ratio"] = ((close - df["ma20"]) / df["ma20"] * 100).round(2)

    df["rsi14"] = rsi(close, 14)

    dif, dea, hist = macd(close)
    df["macd"] = dif
    df["macd_signal"] = dea
    df["macd_hist"] = hist

    bb_upper, bb_mid, bb_lower = bollinger_bands(close, 20, 2.0)
    df["bb_upper"] = bb_upper
    df["bb_mid"] = bb_mid
    df["bb_lower"] = bb_lower
    # 布林頻寬：(上軌 - 下軌) / 中軌 × 100，單位 %
    df["bb_bandwidth"] = ((bb_upper - bb_lower) / bb_mid * 100).round(3)

    if vol_col:
        df["vol_ma5"] = df[vol_col].rolling(5).mean()

    # ATR14：供過熱防護使用（需要 high/low 欄位，若無則跳過）
    high_col = "max" if "max" in df.columns else ("high" if "high" in df.columns else None)
    low_col  = "min" if "min" in df.columns else ("low"  if "low"  in df.columns else None)
    if high_col and low_col:
        df["atr14"] = atr(df, 14)

    return df


def analyze_stock(
    df: pd.DataFrame,
    inst_buying=False,
    margin_trend: str = "flat",
    market_close: pd.Series = None,   # 大盤收盤序列，供 RS 計算（選填）
    precomputed: bool = False,        # True 時跳過 compute_indicators（已預先計算）
    ma_breakout_mode: str = "strict", # "strict"：昨日須全在三線下；"loose"：昨日只要在任一線下
    broker_df: pd.DataFrame = None,   # 分點主力買賣超序列（已含 consecutive_buy_days）
):
    """
    分析單一股票，回傳 ScanSignal 或 None（資料不足）

    inst_net:     近 5 日法人淨買超張數（正為買超）
    margin_trend: 'up' | 'down' | 'flat'
    market_close: 大盤（加權指數）收盤序列，用於相對強度計算
    precomputed:  True 時假設 df 已包含所有技術指標欄位，跳過重算
    """
    if not precomputed:
        df = compute_indicators(df)

    if len(df) < 30 or df["ma20"].isna().all():
        return None

    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else latest

    sig = ScanSignal()

    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None

    # ── 月線乖離率（數值，供後續條件使用）────────────────────────
    if pd.notna(latest.get("ma20_bias_ratio")):
        sig.ma20_bias_ratio = float(latest["ma20_bias_ratio"])

    # ── ATR14（數值，供過熱防護使用）─────────────────────────────
    if pd.notna(latest.get("atr14")) and latest["atr14"] > 0:
        sig.atr14 = float(latest["atr14"])
        # 過熱：收盤已高於 MA20 超過 3.5 倍 ATR（動態門檻，對熱門電子股更合理）
        if pd.notna(latest.get("ma20")) and latest["ma20"] > 0:
            sig.atr_overheat = bool(
                latest["close"] > latest["ma20"] + 3.5 * sig.atr14
            )

    # ──────────────────────────────────────────────────────────────
    # v4 必要條件
    # ──────────────────────────────────────────────────────────────

    # 1. [v4] 第一天站上 5/10/20MA（今日三線全穿，昨日三線全在線下）
    ma5_ok  = pd.notna(latest.get("ma5"))  and pd.notna(prev.get("ma5"))
    ma10_ok = pd.notna(latest.get("ma10")) and pd.notna(prev.get("ma10"))
    ma20_ok = pd.notna(latest.get("ma20")) and pd.notna(prev.get("ma20"))
    if ma5_ok and ma10_ok and ma20_ok:
        today_above_all = (
            latest["close"] > latest["ma5"]
            and latest["close"] > latest["ma10"]
            and latest["close"] > latest["ma20"]
        )
        if ma_breakout_mode == "loose":
            # 寬鬆：昨日收盤 < max(MA5, MA10, MA20)，只要還有一條線沒過即可
            yesterday_below_all = (
                prev["close"] < prev["ma5"]
                or prev["close"] < prev["ma10"]
                or prev["close"] < prev["ma20"]
            )
        else:
            # 嚴謹（預設）：昨日收盤 < min(MA5, MA10, MA20)，三線全在線下
            yesterday_below_all = (
                prev["close"] < prev["ma5"]
                and prev["close"] < prev["ma10"]
                and prev["close"] < prev["ma20"]
            )
        sig.ma_triple_breakout = bool(today_above_all and yesterday_below_all)
        # 保留 v3 參考訊號
        sig.above_ma20 = bool(latest["close"] > latest["ma20"])

    # 2. [v4] 均線糾結度 < 3%（檢查「昨日」三線距離，確認盤整後才突破）
    #    用今日狀態會因突破當天的漲幅把均線撐開，反而把最強的突破股篩掉。
    if ma5_ok and ma10_ok and ma20_ok and prev["ma20"] > 0:
        ma_vals_prev = [prev["ma5"], prev["ma10"], prev["ma20"]]
        spread_pct = (max(ma_vals_prev) - min(ma_vals_prev)) / prev["ma20"] * 100
        sig.ma_squeeze = bool(spread_pct < 3.0)

    # 3. [v4] 量能爆發比：今日量 > 前五日均量 × 1.5
    if vol_col:
        today_vol   = latest.get(vol_col)
        vol_ma5_val = latest.get("vol_ma5")
        if pd.notna(today_vol) and pd.notna(vol_ma5_val) and vol_ma5_val > 0:
            sig.volume_explosion = bool(today_vol > vol_ma5_val * 1.5)
        # 保留 v3 量增訊號（5日均量 × 1.3）
        if pd.notna(latest.get("vol_ma5")) and latest["vol_ma5"] > 0:
            sig.volume_surge = bool(latest[vol_col] > latest["vol_ma5"] * 1.3)

    # 4. [v4] 股價 < MA20 + 3.5 × ATR(14)（動態過熱門檻，ATR 無資料時預設通過）
    sig.atr_ok = True if sig.atr14 == 0 else bool(not sig.atr_overheat)

    # ──────────────────────────────────────────────────────────────
    # v4 加分條件
    # ──────────────────────────────────────────────────────────────

    # 5. [v4] 布林頻寬縮減：今日 bandwidth < 20 日前 bandwidth
    if "bb_bandwidth" in df.columns and len(df) >= 22:
        bw_now   = df["bb_bandwidth"].iloc[-1]
        bw_20ago = df["bb_bandwidth"].iloc[-21]
        if pd.notna(bw_now) and pd.notna(bw_20ago) and bw_20ago > 0:
            sig.bb_bandwidth_shrink = bool(bw_now < bw_20ago)

    # 6. [v4] 投信第一天買超（昨日 ≤ 0，今日 > 0）
    if isinstance(inst_buying, dict) and inst_buying:
        recent_inst = inst_buying.get("recent_inst_net", pd.DataFrame())
        if (not recent_inst.empty
                and "投信" in recent_inst.columns
                and len(recent_inst) >= 2):
            trust_today = recent_inst["投信"].iloc[-1]
            trust_prev  = recent_inst["投信"].iloc[-2]
            if pd.notna(trust_today) and pd.notna(trust_prev):
                sig.trust_first_buy = bool(trust_today > 0 and trust_prev <= 0)
        # 保留 v3 法人欄位
        sig.institutional_buy = bool(inst_buying.get("strict_pass", False))
        sig.inst_total_buy     = bool(inst_buying.get("aggregate_pass", False))
        sig.foreign_trust_buy  = sig.inst_total_buy and bool(
            inst_buying.get("foreign_trust_pass", False)
        )
    elif inst_buying:
        sig.institutional_buy = True

    # 主力連 3 日買超：以分點券商資料為準
    # 定義：(前15買超分點總買進 − 前15賣超分點總賣出) 連續 3 日 > 0
    if broker_df is not None and not broker_df.empty:
        latest_broker = broker_df.sort_values("date").iloc[-1]
        streak = pd.to_numeric(latest_broker.get("consecutive_buy_days"), errors="coerce")
        sig.main_force_buy_3d = bool(pd.notna(streak) and int(streak) >= 3)

    # 7. [v4] 突破近 20 日收盤高點（排除今日本身）
    if len(df) >= 22:
        lookback_20 = df["close"].iloc[-21:-1]
        if not lookback_20.empty:
            resistance_20d = lookback_20.max()
            if pd.notna(resistance_20d):
                sig.breakout_20d = bool(latest["close"] > resistance_20d)

    # 8. [v4] 週線 MA10 扣抵值低位 + v3 週線多頭（共用一次計算）
    if len(df) >= 70:
        wt = weekly_ma_trend(df, ma_period=10)
        if wt:
            # v3 參考：週線 MA10 向上且站上
            if wt.get("weekly_above_ma") and wt.get("weekly_ma_rising"):
                sig.weekly_trend_up = True
            # v4 扣抵值低位：10週前收盤 < 當前週MA10，代表未來MA10會自然上揚
            weekly_df = wt.get("weekly_df", pd.DataFrame())
            if len(weekly_df) >= 12:
                deduction_val = weekly_df["close"].iloc[-11]   # 10週前的收盤
                wma10_now = wt.get("weekly_ma_value")
                if pd.notna(deduction_val) and pd.notna(wma10_now):
                    sig.weekly_deduction_low = bool(deduction_val < wma10_now)

    # 9. [v4] 融資減少 / 籌碼集中
    if margin_trend == "down":
        sig.margin_clean = True

    # 10. [v4] 相對強度
    #   必要條件：RS > 80（個股明顯領先大盤，確保選到真正強勢股）
    #   加分條件：RS > 70（寬鬆版，保留供 triggered_labels 顯示）
    if len(df) >= 63:
        rs = relative_strength_score(df, lookback_days=63, market_returns=market_close)
        if rs:
            sig.rs_score = rs["rs_score"]
            if rs["outperforming"] and rs["rs_score"] > 70:
                sig.rs_positive = True
            if rs["outperforming"] and rs["rs_score"] > 80:
                sig.rs_strong = True

    # ── 保留 v3 參考訊號（供 v3 策略計分 & 顯示）──────────────
    # 不低於布林下軌
    if pd.notna(latest.get("bb_lower")) and latest["close"] >= latest["bb_lower"]:
        sig.above_bb_lower = True

    # 量能品質：近 10 日上漲日成交量佔 ≥ 60%
    if vol_col and len(df) >= 11:
        recent = df.tail(11).copy()
        recent["_up"] = recent["close"] >= recent["close"].shift(1)
        recent = recent.dropna(subset=["_up"])
        total_vol = recent[vol_col].sum()
        up_vol = recent.loc[recent["_up"], vol_col].sum()
        if total_vol > 0 and up_vol / total_vol >= 0.6:
            sig.vol_quality = True

    # MA20 向上
    if len(df) >= 6:
        ma20_now  = df["ma20"].iloc[-1]
        ma20_5ago = df["ma20"].iloc[-6]
        if pd.notna(ma20_now) and pd.notna(ma20_5ago) and ma20_now > ma20_5ago:
            sig.ma20_rising = True

    # MACD 黃金交叉
    if pd.notna(latest.get("macd")) and pd.notna(latest.get("macd_signal")):
        cross_up = (latest["macd"] > latest["macd_signal"]
                    and prev["macd"] <= prev["macd_signal"])
        bull_above_zero = latest["macd"] > 0 and latest["macd"] > latest["macd_signal"]
        if cross_up or bull_above_zero:
            sig.macd_cross = True

    # RSI 健康區（50–70）
    if pd.notna(latest.get("rsi14")) and 50 <= latest["rsi14"] <= 70:
        sig.rsi_healthy = True

    # 多頭排列 MA5 > MA10 > MA20
    if ma5_ok and ma10_ok and ma20_ok:
        if latest["ma5"] > latest["ma10"] > latest["ma20"]:
            sig.ma_aligned = True

    # 突破近 60 日收盤高點
    #   v4 必要條件：今日收盤 > 過去 60 個交易日最高收盤（不含今日）
    #   v3 參考欄位（breakout）：同邏輯，-3 天緩衝版
    if len(df) >= 62:
        lb_60 = df["close"].iloc[-61:-1]   # 不含今日，往前 60 天
        resistance_60d = lb_60.max()
        if pd.notna(resistance_60d) and latest["close"] > resistance_60d:
            sig.breakout_60d = True        # v4 必要條件
            sig.breakout = True            # v3 參考（相容）
    elif len(df) >= 20:
        # 資料不足 60 天時，保留 v3 寬鬆版（-3 天緩衝）
        lb = df.iloc[-63:-3] if len(df) >= 66 else df.iloc[:-3]
        if not lb.empty:
            resistance_60d = lb["close"].max()
            if pd.notna(resistance_60d) and latest["close"] > resistance_60d:
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


def compute_fundamental_penalty(
    fund: dict,
    config: dict,
    mode: str = "penalty",
) -> tuple[int, list[str], list[str]]:
    """
    Convert fundamentals into display-only penalty/flag fields.

    Missing fields are reported separately and are not treated as failures.
    """
    mode = (mode or "penalty").lower()
    if mode == "off" or not config:
        return 0, [], []
    if not fund:
        return 0, [], ["fundamentals"]

    penalty = 0
    flags: list[str] = []
    missing: list[str] = []

    def _add(flag: str, points: int):
        nonlocal penalty
        flags.append(flag)
        if mode == "penalty":
            penalty += points

    eps = fund.get("eps_ttm")
    if config.get("require_eps_positive"):
        if eps is None:
            missing.append("eps_ttm")
        elif eps <= 0:
            _add(f"EPS TTM {eps:.2f} <= 0", 10)

    cf = fund.get("operating_cf")
    if config.get("require_positive_cf"):
        if cf is None:
            missing.append("operating_cf")
        elif cf <= 0:
            _add(f"營業現金流 {cf:.0f} <= 0", 10)

    roe = fund.get("roe")
    min_roe = config.get("min_roe", 0)
    if min_roe > 0:
        if roe is None:
            missing.append("roe")
        elif roe < min_roe:
            _add(f"ROE {roe:.1f}% < {min_roe}%", 5)

    debt = fund.get("debt_ratio")
    max_debt = config.get("max_debt_ratio", 0)
    if max_debt > 0:
        if debt is None:
            missing.append("debt_ratio")
        elif debt > max_debt:
            _add(f"負債比 {debt:.1f}% > {max_debt}%", 5)

    return int(penalty), flags, missing


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
    max_bias_ratio: float = 5.0,      # 已棄用（保留向後相容），實際由 overheat_atr_mult 決定
    overheat_atr_mult: float = 3.5,  # 收盤超過 MA20 + N×ATR14 視為過熱（0 = 停用 ATR 過熱防護）
    overheat_action: str = "drop",    # "drop" | "penalty"
    ma_breakout_mode: str = "strict", # "strict"：昨日三線全在線下；"loose"：昨日任一線在線下
    strategy_version: str = "v4",    # "v4"：領先攻擊版；"v3"：均線突破版
    fundamental_mode: str = "exclude", # "off" | "warn" | "penalty" | "exclude"
    broker_data: dict = None,         # {stock_id: DataFrame}，分點主力快取（None = 不用）
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
    broker_data = broker_data or {}
    fundamental_mode = (fundamental_mode or "exclude").lower()
    if fundamental_mode not in {"off", "warn", "penalty", "exclude"}:
        fundamental_mode = "exclude"

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
        fundamental_penalty = 0
        fundamental_flags: list[str] = []
        fundamental_missing: list[str] = []
        if fundamental_filter and fundamental_mode != "off":
            fund = fundamental_data.get(stock_id, {})
            fundamental_penalty, fundamental_flags, fundamental_missing = (
                compute_fundamental_penalty(fund, fundamental_filter, fundamental_mode)
            )
            passed, reason = _passes_fundamental(fund, fundamental_filter)
            if fundamental_mode == "exclude" and not passed:
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
            ma_breakout_mode=ma_breakout_mode,
            broker_df=broker_data.get(stock_id),
        )

        if debug:
            stock_analysis[stock_id] = {
                "stock_name": info.get("stock_name", ""),
                "industry": info.get("industry_category", ""),
                "close": close,
                "exclude_pre": None,
                "sig": sig,
            }

        passes = sig.passes_basic_v3() if strategy_version == "v3" else sig.passes_basic()
        if sig is None or not passes:
            continue

        # 過熱股防護（僅 v4 套用）
        # 優先用 ATR 倍數判斷（對熱門電子股更有彈性），ATR 無效時 fallback 至 BIAS %
        if strategy_version == "v4":
            if overheat_atr_mult > 0 and sig.atr14 > 0:
                if close > 0 and sig.ma20_bias_ratio != -100:
                    ma20_from_bias = close / (1 + sig.ma20_bias_ratio / 100)
                    is_overheated = close > ma20_from_bias + overheat_atr_mult * sig.atr14
                else:
                    is_overheated = sig.atr_overheat
            else:
                is_overheated = (
                    pd.notna(sig.ma20_bias_ratio)
                    and sig.ma20_bias_ratio > max_bias_ratio
                )
        else:
            is_overheated = False
        if is_overheated and overheat_action == "drop":
            continue

        prev_close = df.iloc[-2]["close"] if len(df) >= 2 else close
        change_pct = round((close - prev_close) / prev_close * 100, 2) if prev_close else 0

        vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
        vol_now = df[vol_col].iloc[-1] if vol_col else 0
        vol_ma5 = df[vol_col].rolling(5).mean().iloc[-1] if vol_col else 0
        volume_ratio = round(vol_now / vol_ma5, 2) if vol_ma5 and vol_ma5 > 0 else 0

        base_score = sig.score_v3() if strategy_version == "v3" else sig.score()
        if is_overheated and overheat_action == "penalty":
            base_score = max(base_score - 10, 0)

        premium_score = 0
        risk_penalty = int(fundamental_penalty) if fundamental_mode == "penalty" else 0
        premium_positive_flags: list[str] = []
        premium_negative_flags: list[str] = list(fundamental_flags)
        premium_missing_fields: list[str] = list(fundamental_missing)
        final_score = max(base_score + premium_score - risk_penalty, 0)

        results.append({
            "stock_id": stock_id,
            "stock_name": info.get("stock_name", ""),
            "industry": info.get("industry_category", ""),
            "close": close,
            "change_pct": change_pct,
            "volume_ratio": volume_ratio,
            "base_score": round(base_score, 1),
            "premium_score": round(premium_score, 1),
            "risk_penalty": int(risk_penalty),
            "final_score": round(final_score, 1),
            "score": round(final_score, 1),
            "rs_score": sig.rs_score,
            "bias_ratio": round(sig.ma20_bias_ratio, 2),
            "volatility_pct": round(sig.atr14 / close * 100, 1) if close > 0 and sig.atr14 > 0 else 0.0,
            "heat_room_pct": (
                round(
                    (close / (1 + sig.ma20_bias_ratio / 100) + overheat_atr_mult * sig.atr14 - close)
                    / close * 100, 1
                )
                if sig.atr14 > 0 and close > 0 and overheat_atr_mult > 0
                   and sig.ma20_bias_ratio != -100
                else None
            ),
            "heat_room_abs": (
                round(close / (1 + sig.ma20_bias_ratio / 100) + overheat_atr_mult * sig.atr14 - close, 1)
                if sig.atr14 > 0 and close > 0 and overheat_atr_mult > 0
                   and sig.ma20_bias_ratio != -100
                else None
            ),
            "overheated": is_overheated,
            "fundamental_penalty": int(fundamental_penalty),
            "fundamental_flags": "、".join(fundamental_flags),
            "fundamental_missing_fields": "、".join(fundamental_missing),
            "premium_positive_flags": "、".join(premium_positive_flags),
            "premium_negative_flags": "、".join(premium_negative_flags),
            "premium_missing_fields": "、".join(premium_missing_fields),
            "inst_pass": bool(
                sig.institutional_buy or sig.inst_total_buy or sig.foreign_trust_buy
            ),
            "signals": "、".join(sig.triggered_labels(strategy_version)),
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
