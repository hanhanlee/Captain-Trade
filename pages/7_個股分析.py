"""
個股分析頁面

輸入股票代號，顯示：
  - 基本資訊（名稱、股價、漲跌、產業）
  - v4 選股條件逐項評分（✅ / ❌ + 實際數值）
  - 日K線圖（含均線、布林通道、成交量）
  - 週K線圖（含週MA10 + 扣抵值標示）
  - 法人籌碼摘要（若選擇載入）
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

from db.database import init_db
from db.price_cache import load_prices
from db.settings import get_scanner_preset
from modules.scanner import compute_indicators, analyze_stock
from modules.indicators import weekly_ma_trend, sma

init_db()

st.set_page_config(page_title="個股分析", page_icon="🔎", layout="wide")
st.title("🔎 個股分析")
st.caption("輸入股票代號，逐項檢視選股條件是否達標，掌握每個指標的實際數值")


# ── 工具函式 ─────────────────────────────────────────────────────

def _load_df(stock_id: str) -> pd.DataFrame | None:
    """從快取載入日K並計算指標；無快取時回傳 None"""
    df = load_prices(stock_id, lookback_days=400)
    if df is None or df.empty or len(df) < 30:
        return None
    return compute_indicators(df)


def _load_inst(
    stock_id: str,
    *,
    target_date=None,
    selected_institutions: list | None = None,
    strict_days: int = 3,
    agg_mode: str = "rolling_sum",
    agg_days: int = 5,
) -> dict:
    """載入三大法人資料並彙整；快取優先，失敗回傳空 dict"""
    try:
        from data.finmind_client import smart_get_institutional, summarize_institutional_signal
        if target_date is not None:
            from db.inst_cache import load_institutional_for_date
            idf = load_institutional_for_date(stock_id, target_date, days=14)
        else:
            idf = smart_get_institutional(stock_id, days=10)
        if idf.empty:
            return {}
        return summarize_institutional_signal(
            idf,
            selected_institutions=selected_institutions,
            strict_days=int(strict_days or 3),
            agg_mode=agg_mode,
            agg_days=int(agg_days or 5),
        )
    except Exception:
        return {}


def _to_bool(value, default=False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _fundamental_check_rows(stock_id: str, preset: dict) -> list[dict]:
    """Evaluate saved fundamental filters for a single stock."""
    req_eps = _to_bool(preset.get("sb_req_eps"), True)
    req_cf = _to_bool(preset.get("sb_req_cf"), True)
    min_roe = float(preset.get("sb_min_roe") or 0)
    max_debt = float(preset.get("sb_max_debt") or 0)

    try:
        from data.finmind_client import (
            can_fetch_premium_fundamentals,
            get_fundamentals_mode,
            smart_get_fundamentals,
        )
        from db.fundamental_cache import is_fundamental_fresh, load_fundamental
    except Exception as exc:
        return [{
            "自訂條件": "基本面過濾",
            "結果": "無法檢查",
            "目前數值": f"基本面模組載入失敗：{exc}",
        }]

    mode = get_fundamentals_mode()
    if mode == "off":
        return [{
            "自訂條件": "基本面過濾",
            "結果": "停用",
            "目前數值": "config.toml fundamentals_mode=off",
        }]

    metrics = {}
    source = ""
    if is_fundamental_fresh(stock_id):
        metrics = load_fundamental(stock_id)
        if any(metrics.get(k) is not None for k in ("eps_ttm", "operating_cf", "roe", "debt_ratio")):
            source = "本機快取"
        else:
            metrics = {}

    if not metrics:
        can_fetch, reason = can_fetch_premium_fundamentals()
        if not can_fetch:
            return [{
                "自訂條件": "基本面過濾",
                "結果": "資料不足",
                "目前數值": f"無新鮮快取，且目前不可抓取：{reason}",
            }]
        with st.spinner(f"載入 {stock_id} 基本面資料..."):
            metrics = smart_get_fundamentals(stock_id)
        source = "API / 快取"

    if not any(metrics.get(k) is not None for k in ("eps_ttm", "operating_cf", "roe", "debt_ratio")):
        return [{
            "自訂條件": "基本面過濾",
            "結果": "資料不足",
            "目前數值": f"FinMind 財報無可用指標（來源：{source or '未知'}）",
        }]

    rows: list[dict] = []
    data_date = metrics.get("data_date") or "未標示"

    def _add(label: str, passed: bool | None, value: str):
        rows.append({
            "自訂條件": label,
            "結果": "資料不足" if passed is None else ("通過" if passed else "未通過"),
            "目前數值": f"{value}（{source}，資料日 {data_date}）",
        })

    eps = metrics.get("eps_ttm")
    if req_eps:
        _add(
            "EPS TTM > 0",
            None if eps is None else float(eps) > 0,
            "N/A" if eps is None else f"{float(eps):.2f}",
        )

    operating_cf = metrics.get("operating_cf")
    if req_cf:
        _add(
            "營業現金流 > 0",
            None if operating_cf is None else float(operating_cf) > 0,
            "N/A" if operating_cf is None else f"{float(operating_cf):,.0f}",
        )

    roe = metrics.get("roe")
    if min_roe > 0:
        _add(
            f"ROE >= {min_roe:g}%",
            None if roe is None else float(roe) >= min_roe,
            "N/A" if roe is None else f"{float(roe):.2f}%",
        )

    debt = metrics.get("debt_ratio")
    if max_debt > 0:
        _add(
            f"負債比 <= {max_debt:g}%",
            None if debt is None else float(debt) <= max_debt,
            "N/A" if debt is None else f"{float(debt):.2f}%",
        )

    if not rows:
        rows.append({
            "自訂條件": "基本面過濾",
            "結果": "未設定條件",
            "目前數值": f"已載入基本面資料（{source}，資料日 {data_date}）",
        })
    return rows


def _get_daily_volume_rank(stock_id: str, trade_date) -> tuple[int | None, int, float | None]:
    """回傳指定交易日成交量排名：(rank, total, volume)。rank 為 1-based。"""
    try:
        from db.database import get_session
        from sqlalchemy import text

        d = pd.Timestamp(trade_date).date().isoformat()
        with get_session() as sess:
            rows = sess.execute(
                text("""
                    SELECT stock_id, volume
                    FROM price_cache
                    WHERE date = :d AND volume IS NOT NULL AND volume > 0
                    ORDER BY volume DESC
                """),
                {"d": d},
            ).fetchall()
        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            if row[0] == stock_id:
                return idx, total, float(row[1])
        return None, total, None
    except Exception:
        return None, 0, None


def _heat_room(df: pd.DataFrame, atr_mult: float) -> dict:
    """計算距離 ATR 過熱門檻的百分比與金額。"""
    latest = df.iloc[-1]
    close = float(latest.get("close", 0) or 0)
    ma20 = latest.get("ma20", float("nan"))
    atr14 = latest.get("atr14", float("nan"))
    if close <= 0 or pd.isna(ma20) or pd.isna(atr14) or atr14 <= 0 or atr_mult <= 0:
        return {
            "available": False,
            "threshold": None,
            "room_abs": None,
            "room_pct": None,
            "overheated": False,
        }

    threshold = float(ma20) + float(atr_mult) * float(atr14)
    room_abs = threshold - close
    return {
        "available": True,
        "threshold": threshold,
        "room_abs": room_abs,
        "room_pct": room_abs / close * 100,
        "overheated": room_abs < 0,
    }


def _badge(passed: bool) -> str:
    return "✅" if passed else "❌"


def _color(passed: bool) -> str:
    return "#27ae60" if passed else "#e74c3c"


def render_scorecard(sig, df: pd.DataFrame, ma_mode: str = "strict", has_inst: bool = False):
    """v4 條件逐項評分卡"""
    latest = df.iloc[-1]
    prev   = df.iloc[-2] if len(df) >= 2 else latest

    ma5  = latest.get("ma5",  float("nan"))
    ma10 = latest.get("ma10", float("nan"))
    ma20 = latest.get("ma20", float("nan"))
    close = latest["close"]
    p_close = prev["close"]

    # 均線糾結度（用昨日數值，與 scanner 邏輯一致）
    prev_ma5  = prev.get("ma5",  float("nan"))
    prev_ma10 = prev.get("ma10", float("nan"))
    prev_ma20 = prev.get("ma20", float("nan"))
    squeeze_pct = (
        (max(prev_ma5, prev_ma10, prev_ma20) - min(prev_ma5, prev_ma10, prev_ma20)) / prev_ma20 * 100
        if prev_ma20 > 0 else float("nan")
    )
    # 量能爆發比
    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
    vol_ma5_val = latest.get("vol_ma5", float("nan"))
    vol_ma5_ratio = (
        latest[vol_col] / vol_ma5_val
        if vol_col and not pd.isna(vol_ma5_val) and vol_ma5_val > 0 else float("nan")
    )
    # ATR 動態門檻
    atr14_val = sig.atr14
    atr_threshold = ma20 + 3.5 * atr14_val if atr14_val > 0 else float("nan")
    # 布林頻寬
    bw_now   = latest.get("bb_bandwidth", float("nan"))
    bw_20ago = df["bb_bandwidth"].iloc[-21] if len(df) >= 22 else float("nan")
    # 20日 / 60日高點
    resist_20d = df["close"].iloc[-21:-1].max() if len(df) >= 22 else float("nan")
    resist_60d = df["close"].iloc[-61:-1].max() if len(df) >= 62 else float("nan")
    # RS 分數
    rs_score = sig.rs_score

    # ── 必要條件 ────────────────────────────────────────────────
    st.markdown("##### 必要條件（全部達到才入選）")

    conditions_req = [
        {
            "name": "第一天站上 5/10/20MA",
            "pass": sig.ma_triple_breakout,
            "detail": (
                f"今收 **{close:.1f}**，"
                f"MA5={ma5:.1f} / MA10={ma10:.1f} / MA20={ma20:.1f}  \n"
                f"昨收 {p_close:.1f}（需在{'三線全部' if ma_mode == 'strict' else '至少一條'}線下方）"
            ),
            "rule": (
                "今日收盤 > 三均線，昨日收盤 < min(MA5,MA10,MA20)【嚴謹】"
                if ma_mode == "strict" else
                "今日收盤 > 三均線，昨日收盤 < max(MA5,MA10,MA20)【寬鬆】"
            ),
        },
        {
            "name": "均線糾結度 < 3%（昨日）",
            "pass": sig.ma_squeeze,
            "detail": (
                f"昨日三線最大偏差 ÷ MA20 = **{squeeze_pct:.2f}%**（需 < 3%）  \n"
                f"昨日 MA5={prev_ma5:.1f} / MA10={prev_ma10:.1f} / MA20={prev_ma20:.1f}"
            ),
            "rule": "昨日 (max(MA5,10,20) − min) / MA20 < 3%，確認盤整後突破",
        },
        {
            "name": "量能 > 前五日均量 × 1.5 倍",
            "pass": sig.volume_explosion,
            "detail": (
                f"今日量 / 五日均量 = **{vol_ma5_ratio:.2f}x**（需 > 1.5x）"
                if not pd.isna(vol_ma5_ratio) else "成交量資料不足"
            ),
            "rule": "今日成交量 > 前五日均量 × 1.5，確認突破動能充足",
        },
        {
            "name": "股價 < MA20 + 3.5 × ATR(14)",
            "pass": sig.atr_ok,
            "detail": (
                f"今收 **{close:.1f}**，過熱門檻 **{atr_threshold:.1f}**"
                f"（日均振幅 {atr14_val/close*100:.1f}%，ATR={atr14_val:.2f}）"
                if atr14_val > 0 else "ATR 資料不足（高低價缺失），條件預設通過"
            ),
            "rule": "動態門檻排除過熱股，比固定乖離率更適應不同波動度",
        },
        {
            "name": "相對強度 RS > 80",
            "pass": sig.rs_strong,
            "detail": f"RS 分數 = **{rs_score:.1f}**（需 > 80，個股明顯領先大盤）",
            "rule": "過去 63 交易日報酬率跑贏大盤，RS 分數 > 80",
        },
        {
            "name": "突破 60 日收盤新高",
            "pass": sig.breakout_60d,
            "detail": (
                f"今收 **{close:.1f}** vs 60日高點 **{resist_60d:.1f}**"
                if not pd.isna(resist_60d) else "資料不足（需至少 62 根 K 線）"
            ),
            "rule": "今日收盤 > 過去 60 個交易日最高收盤",
        },
        {
            "name": "主力連續買超 3 日以上",
            "pass": sig.main_force_buy_3d,
            "detail": (
                (
                    f"連續買超 **{int(_broker_streak)}** 日，"
                    f"主力淨買超 **{_broker_net:+.0f}** 張"
                    if pd.notna(_broker_streak) and pd.notna(_broker_net)
                    else "分點資料已載入，條件未達（連續買超 < 3 日）"
                )
                if _has_broker else
                "未載入分點主力資料（需 Premium 並先執行預抓取）"
            ),
            "rule": "前15買超分點 − 前15賣超分點（主力買賣超）連續 3 日 > 0",
        },
    ]

    for c in conditions_req:
        with st.container(border=True):
            col_ic, col_info = st.columns([0.08, 0.92])
            col_ic.markdown(
                f"<div style='font-size:1.6rem;text-align:center'>{_badge(c['pass'])}</div>",
                unsafe_allow_html=True,
            )
            col_info.markdown(
                f"**{c['name']}**  \n"
                f"<span style='font-size:0.82rem;color:#aaa'>{c['rule']}</span>",
                unsafe_allow_html=True,
            )
            col_info.caption(c["detail"])

    # ── 加分條件 ────────────────────────────────────────────────
    st.markdown("##### 加分條件")

    # 分點主力摘要（供卡片 detail 使用）
    _broker_streak = None
    _broker_net = None
    _has_broker = broker_main_force is not None and not broker_main_force.empty
    if _has_broker:
        _latest_b = broker_main_force.sort_values("date").iloc[-1]
        _broker_streak = pd.to_numeric(_latest_b.get("consecutive_buy_days"), errors="coerce")
        _broker_net = pd.to_numeric(_latest_b.get("net"), errors="coerce")

    # 投信今昨資料
    trust_today_val = trust_prev_val = None
    if hasattr(sig, "_trust_detail"):
        trust_today_val, trust_prev_val = sig._trust_detail

    # 週線扣抵值
    deduction_str = "資料不足"
    wma10_val     = None
    deduction_val = None
    if len(df) >= 70:
        wt = weekly_ma_trend(df, ma_period=10)
        if wt and "weekly_df" in wt:
            wdf = wt["weekly_df"]
            wma10_val = wt.get("weekly_ma_value")
            if len(wdf) >= 12:
                deduction_val = wdf["close"].iloc[-11]
                deduction_str = (
                    f"10週前收盤 **{deduction_val:.1f}** vs 週MA10 **{wma10_val:.1f}**"
                    if pd.notna(deduction_val) and pd.notna(wma10_val) else "資料不足"
                )

    conditions_bonus = [
        {
            "name": "布林頻寬縮減（vs 20日前）",
            "pass": sig.bb_bandwidth_shrink,
            "score": 10,
            "detail": (
                f"今日頻寬 **{bw_now:.2f}%** vs 20日前 **{bw_20ago:.2f}%**（需今 < 20日前）"
                if not pd.isna(bw_now) and not pd.isna(bw_20ago) else "資料不足"
            ),
        },
        {
            "name": "投信第一天買超",
            "pass": sig.trust_first_buy,
            "score": 10,
            "detail": (
                (
                    f"昨日投信 **{trust_prev_val:.0f}** 張 → 今日 **{trust_today_val:.0f}** 張（轉正）"
                    if trust_today_val is not None and trust_prev_val is not None
                    else "昨日投信 ≤ 0，今日轉正"
                )
                if has_inst else
                "未載入法人資料（請勾選「載入法人買賣超」）"
            ),
        },
        {
            "name": "週線 MA10 扣抵值低位",
            "pass": sig.weekly_deduction_low,
            "score": 10,
            "detail": deduction_str,
        },
        {
            "name": "融資減少 / 籌碼集中",
            "pass": sig.margin_clean,
            "score": 5,
            "detail": (
                f"融資餘額 **{_margin_latest:,}** 張 ← 前日 **{_margin_prev:,}** 張"
                f"（{'↓ 減少' if _margin_trend == 'down' else '↑ 增加' if _margin_trend == 'up' else '持平'}）"
                if _margin_latest or _margin_prev
                else "無融資資料（特別股或 FinMind 未收錄）"
            ),
        },
    ]

    cols_b = st.columns(2)
    for i, c in enumerate(conditions_bonus):
        with cols_b[i % 2].container(border=True):
            col_ic, col_info = st.columns([0.1, 0.9])
            score_color = "#f39c12" if c["pass"] else "#555"
            col_ic.markdown(
                f"<div style='font-size:1.3rem;text-align:center'>{_badge(c['pass'])}</div>",
                unsafe_allow_html=True,
            )
            col_info.markdown(
                f"**{c['name']}** "
                f"<span style='color:{score_color};font-size:0.85rem'>+{c['score']}</span>",
                unsafe_allow_html=True,
            )
            col_info.caption(c["detail"])

    # ── 總分 ────────────────────────────────────────────────────
    total = sig.score()
    passes = sig.passes_basic()
    score_color = "#27ae60" if passes and total >= 100 else ("#f39c12" if passes else "#e74c3c")
    verdict = "✅ 符合入選條件" if passes else "❌ 不符合必要條件"
    st.markdown(
        f"<div style='text-align:center;padding:12px;border-radius:8px;"
        f"background:{score_color}22;border:1px solid {score_color}'>"
        f"<span style='font-size:1.1rem'>{verdict}</span>&nbsp;&nbsp;"
        f"<span style='font-size:1.6rem;font-weight:bold;color:{score_color}'>{total} 分</span>"
        f"<span style='color:#aaa;font-size:0.85rem'> / 145 滿分</span>"
        f"</div>",
        unsafe_allow_html=True,
    )


def render_scorecard_v3(sig, df: pd.DataFrame, has_inst: bool = False):
    """v3 條件逐項評分卡"""
    latest = df.iloc[-1]
    prev   = df.iloc[-2] if len(df) >= 2 else latest

    ma5   = latest.get("ma5",  float("nan"))
    ma10  = latest.get("ma10", float("nan"))
    ma20  = latest.get("ma20", float("nan"))
    close = latest["close"]

    # MA20 斜率（5日前比較）
    ma20_5ago = df["ma20"].iloc[-6] if len(df) >= 6 else float("nan")

    # 量比
    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
    vol_ma5_val = latest.get("vol_ma5", float("nan"))
    vol_ma5_ratio = (
        latest[vol_col] / vol_ma5_val
        if vol_col and not pd.isna(vol_ma5_val) and vol_ma5_val > 0 else float("nan")
    )

    # 布林下軌
    bb_lower_val = latest.get("bb_lower", float("nan"))

    # MACD
    macd_val   = latest.get("macd", float("nan"))
    signal_val = latest.get("macd_signal", float("nan"))
    prev_macd  = prev.get("macd", float("nan"))
    prev_sig   = prev.get("macd_signal", float("nan"))

    # RSI
    rsi_val = latest.get("rsi14", float("nan"))

    # 60 日高點
    resist_60d = df["close"].iloc[-61:-1].max() if len(df) >= 62 else float("nan")

    # ── 必要條件 ────────────────────────────────────────────────
    st.markdown("##### 必要條件（全部達到才入選）")

    # OR 關係的 MACD/RSI 判斷
    _macd_or_rsi = sig.macd_cross or sig.rsi_healthy
    _macd_or_rsi_detail = []
    if not pd.isna(macd_val) and not pd.isna(signal_val):
        cross_str = "今 DIF>DEA" if macd_val > signal_val else "今 DIF<DEA"
        prev_cross = "昨 DIF<DEA" if (not pd.isna(prev_macd) and not pd.isna(prev_sig)
                                       and prev_macd <= prev_sig) else "昨 DIF>DEA"
        _macd_or_rsi_detail.append(
            f"MACD：{cross_str}，{prev_cross}，DIF={macd_val:.4f}"
            + ("  ✅" if sig.macd_cross else "")
        )
    if not pd.isna(rsi_val):
        _macd_or_rsi_detail.append(
            f"RSI(14)={rsi_val:.1f}（健康區 50–70）"
            + ("  ✅" if sig.rsi_healthy else "")
        )

    conditions_req = [
        {
            "name": "站上 MA20",
            "pass": sig.above_ma20,
            "detail": (
                f"今收 **{close:.1f}** vs MA20 **{ma20:.1f}**"
                if not pd.isna(ma20) else "MA20 資料不足"
            ),
            "rule": "今日收盤 > MA20（20日移動平均）",
        },
        {
            "name": "MA20 向上（近 5 日斜率）",
            "pass": sig.ma20_rising,
            "detail": (
                f"今日 MA20={ma20:.1f}，5日前 MA20={ma20_5ago:.1f}"
                f"（{'↑ 上升' if not pd.isna(ma20) and not pd.isna(ma20_5ago) and ma20 > ma20_5ago else '↓ 下降或持平'}）"
                if not pd.isna(ma20_5ago) else "資料不足"
            ),
            "rule": "MA20 近 5 日持續上揚，確認中期趨勢向上",
        },
        {
            "name": "量增 > 前五日均量 × 1.3 倍",
            "pass": sig.volume_surge,
            "detail": (
                f"今日量 / 五日均量 = **{vol_ma5_ratio:.2f}x**（需 > 1.3x）"
                if not pd.isna(vol_ma5_ratio) else "成交量資料不足"
            ),
            "rule": "今日成交量 > 前五日均量 × 1.3，確認有量撐盤",
        },
        {
            "name": "不低於布林下軌",
            "pass": sig.above_bb_lower,
            "detail": (
                f"今收 **{close:.1f}** ≥ 布林下軌 **{bb_lower_val:.1f}**"
                if not pd.isna(bb_lower_val) else "布林通道資料不足"
            ),
            "rule": "收盤 ≥ BB下軌，確認股價未跌穿通道底部",
        },
        {
            "name": "MACD 黃金交叉  或  RSI 健康（50–70）",
            "pass": _macd_or_rsi,
            "detail": "  \n".join(_macd_or_rsi_detail) if _macd_or_rsi_detail else "資料不足",
            "rule": "MACD 黃金交叉（DIF由下往上穿越DEA 或 DIF>DEA>0）OR RSI在50–70健康區",
        },
        {
            "name": "主力連續買超 3 日以上",
            "pass": sig.main_force_buy_3d,
            "detail": (
                (
                    f"連續買超 **{int(_broker_streak)}** 日，"
                    f"主力淨買超 **{_broker_net:+.0f}** 張"
                    if pd.notna(_broker_streak) and pd.notna(_broker_net)
                    else "分點資料已載入，條件未達（連續買超 < 3 日）"
                )
                if _has_broker else
                "未載入分點主力資料（需 Premium 並先執行預抓取）"
            ),
            "rule": "前15買超分點 − 前15賣超分點（主力買賣超）連續 3 日 > 0",
        },
    ]

    for c in conditions_req:
        with st.container(border=True):
            col_ic, col_info = st.columns([0.08, 0.92])
            col_ic.markdown(
                f"<div style='font-size:1.6rem;text-align:center'>{_badge(c['pass'])}</div>",
                unsafe_allow_html=True,
            )
            col_info.markdown(
                f"**{c['name']}**  \n"
                f"<span style='font-size:0.82rem;color:#aaa'>{c['rule']}</span>",
                unsafe_allow_html=True,
            )
            col_info.caption(c["detail"])

    # ── 加分條件 ────────────────────────────────────────────────
    st.markdown("##### 加分條件")

    conditions_bonus = [
        {
            "name": "MACD 黃金交叉",
            "pass": sig.macd_cross,
            "score": 15,
            "detail": (
                f"DIF={macd_val:.4f}，DEA={signal_val:.4f}"
                if not pd.isna(macd_val) and not pd.isna(signal_val) else "MACD 資料不足"
            ),
        },
        {
            "name": "RSI 健康區（50–70）",
            "pass": sig.rsi_healthy,
            "score": 10,
            "detail": (
                f"RSI(14) = **{rsi_val:.1f}**（需介於 50–70）"
                if not pd.isna(rsi_val) else "RSI 資料不足"
            ),
        },
        {
            "name": "多頭排列（MA5 > MA10 > MA20）",
            "pass": sig.ma_aligned,
            "score": 5,
            "detail": (
                f"MA5={ma5:.1f} / MA10={ma10:.1f} / MA20={ma20:.1f}"
                if not pd.isna(ma5) and not pd.isna(ma10) else "均線資料不足"
            ),
        },
        {
            "name": "量能優質（近10日上漲量占比≥60%）",
            "pass": sig.vol_quality,
            "score": 7,
            "detail": "近 10 個交易日中，上漲日的成交量合計佔比 ≥ 60%",
        },
        {
            "name": "突破近 60 日收盤新高",
            "pass": sig.breakout,
            "score": 8,
            "detail": (
                f"今收 **{close:.1f}** vs 60日高點 **{resist_60d:.1f}**"
                if not pd.isna(resist_60d) else "資料不足（需至少 62 根 K 線）"
            ),
        },
        {
            "name": "週線多頭（週MA10向上且站上）",
            "pass": sig.weekly_trend_up,
            "score": 10,
            "detail": "週K收盤 > 週MA10 且 週MA10 近期向上",
        },
        {
            "name": "法人買超",
            "pass": sig.institutional_buy,
            "score": 7,
            "detail": (
                "外資、投信、自營各自連續買超條件未達成"
                if has_inst else
                "未載入法人資料（請勾選「載入法人買賣超」）"
            ),
        },
        {
            "name": "融資減少 / 籌碼集中",
            "pass": sig.margin_clean,
            "score": 3,
            "detail": (
                f"融資餘額 **{_margin_latest:,}** 張 ← 前日 **{_margin_prev:,}** 張"
                f"（{'↓ 減少' if _margin_trend == 'down' else '↑ 增加' if _margin_trend == 'up' else '持平'}）"
                if _margin_latest or _margin_prev
                else "無融資資料"
            ),
        },
        {
            "name": "相對強度 RS > 70",
            "pass": sig.rs_positive,
            "score": 8,
            "detail": f"RS 分數 = **{sig.rs_score:.1f}**（需 > 70，個股領先大盤）",
        },
    ]

    cols_b = st.columns(2)
    for i, c in enumerate(conditions_bonus):
        with cols_b[i % 2].container(border=True):
            col_ic, col_info = st.columns([0.1, 0.9])
            score_color = "#f39c12" if c["pass"] else "#555"
            col_ic.markdown(
                f"<div style='font-size:1.3rem;text-align:center'>{_badge(c['pass'])}</div>",
                unsafe_allow_html=True,
            )
            col_info.markdown(
                f"**{c['name']}** "
                f"<span style='color:{score_color};font-size:0.85rem'>+{c['score']}</span>",
                unsafe_allow_html=True,
            )
            col_info.caption(c["detail"])

    # ── 總分 ────────────────────────────────────────────────────
    total = sig.score_v3()
    passes = sig.passes_basic_v3()
    score_color = "#27ae60" if passes and total >= 80 else ("#f39c12" if passes else "#e74c3c")
    verdict = "✅ 符合入選條件" if passes else "❌ 不符合必要條件"
    st.markdown(
        f"<div style='text-align:center;padding:12px;border-radius:8px;"
        f"background:{score_color}22;border:1px solid {score_color}'>"
        f"<span style='font-size:1.1rem'>{verdict}</span>&nbsp;&nbsp;"
        f"<span style='font-size:1.6rem;font-weight:bold;color:{score_color}'>{total} 分</span>"
        f"<span style='color:#aaa;font-size:0.85rem'> / 148 滿分</span>"
        f"</div>",
        unsafe_allow_html=True,
    )


def render_daily_chart(stock_id: str, df: pd.DataFrame):
    """日K線圖 + 均線 + 布林 + 成交量"""
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.04, row_heights=[0.58, 0.22, 0.20],
        subplot_titles=[f"{stock_id} 日K線", "成交量", "RSI(14)"],
    )

    # K 線
    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["max"],
        low=df["min"], close=df["close"], name="K線",
        increasing_line_color="#e74c3c", decreasing_line_color="#27ae60",
    ), row=1, col=1)

    # 均線
    for col, color, name in [
        ("ma5", "#f39c12", "MA5"), ("ma10", "#9b59b6", "MA10"),
        ("ma20", "#3498db", "MA20"), ("ma60", "#1abc9c", "MA60"),
    ]:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df[col], name=name,
                line=dict(color=color, width=1.3),
            ), row=1, col=1)

    # 布林通道
    for col, name in [("bb_upper", "BB上軌"), ("bb_lower", "BB下軌")]:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df[col], name=name,
                line=dict(color="rgba(52,152,219,0.35)", width=1, dash="dot"),
                showlegend=(col == "bb_upper"),
            ), row=1, col=1)
    if "bb_upper" in df.columns and "bb_lower" in df.columns:
        fig.add_trace(go.Scatter(
            x=pd.concat([df["date"], df["date"].iloc[::-1]]),
            y=pd.concat([df["bb_upper"], df["bb_lower"].iloc[::-1]]),
            fill="toself", fillcolor="rgba(52,152,219,0.05)",
            line=dict(width=0), showlegend=False, name="BB區間",
        ), row=1, col=1)

    # 成交量
    if "Trading_Volume" in df.columns:
        bar_colors = [
            "#e74c3c" if c >= o else "#27ae60"
            for c, o in zip(df["close"], df["open"])
        ]
        fig.add_trace(go.Bar(
            x=df["date"], y=df["Trading_Volume"],
            marker_color=bar_colors, name="成交量", showlegend=False,
        ), row=2, col=1)

    # RSI
    if "rsi14" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rsi14"], name="RSI(14)",
            line=dict(color="#f39c12", width=1.3),
        ), row=3, col=1)
        for level, color in [(70, "rgba(231,76,60,0.5)"), (30, "rgba(39,174,96,0.5)")]:
            fig.add_hline(y=level, line_dash="dash", line_color=color, row=3, col=1)

    fig.update_layout(
        height=620, template="plotly_dark",
        xaxis_rangeslider_visible=False,
        margin=dict(t=40, b=10),
        legend=dict(orientation="h", y=1.03),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_weekly_chart(stock_id: str, df: pd.DataFrame):
    """週K線圖 + 週MA10 + 扣抵值標示"""
    wt = weekly_ma_trend(df, ma_period=10)
    if not wt or "weekly_df" not in wt:
        st.caption("週線資料不足（需至少 14 週）")
        return

    wdf = wt["weekly_df"]
    wdf = wdf.copy()
    wdf["wma10"] = sma(wdf["close"], 10)

    high_col = "max" if "max" in wdf.columns else "high"
    low_col  = "min" if "min" in wdf.columns else "low"

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=wdf["week_end"], open=wdf["open"], high=wdf[high_col],
        low=wdf[low_col], close=wdf["close"], name="週K",
        increasing_line_color="#e74c3c", decreasing_line_color="#27ae60",
    ))
    fig.add_trace(go.Scatter(
        x=wdf["week_end"], y=wdf["wma10"], name="週MA10",
        line=dict(color="#3498db", width=2),
    ))

    # 扣抵值標示：10週前那根K棒加紅色箭頭
    if len(wdf) >= 12:
        deduct_idx = len(wdf) - 11
        deduct_row = wdf.iloc[deduct_idx]
        deduct_x = pd.to_datetime(deduct_row["week_end"])
        fig.add_shape(
            type="line",
            x0=deduct_x,
            x1=deduct_x,
            y0=0,
            y1=1,
            xref="x",
            yref="paper",
            line=dict(dash="dot", color="rgba(231,76,60,0.6)", width=1.5),
        )
        fig.add_annotation(
            x=deduct_x,
            y=1,
            xref="x",
            yref="paper",
            text="扣抵值",
            showarrow=False,
            yshift=8,
            font=dict(color="#e74c3c"),
        )

    status = "🟢 週線多頭" if wt.get("weekly_above_ma") and wt.get("weekly_ma_rising") else "🔴 週線偏弱"
    deduction_pass = "｜扣抵低位 ✅" if wt.get("weekly_ma_value") else ""
    fig.update_layout(
        height=360, template="plotly_dark",
        title=f"{stock_id} 週K線圖　{status}{deduction_pass}",
        xaxis_rangeslider_visible=False,
        margin=dict(t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_institutional_chart(main_force_df: pd.DataFrame):
    """券商分點主力買賣超長條圖：買超向上，賣超向下。"""
    if main_force_df is None or main_force_df.empty:
        st.info(
            "目前沒有可繪製的券商分點主力買賣超資料。"
            "此資料使用 FinMind TaiwanStockTradingDailyReport，需 sponsor 權限，且單次只能查一檔股票一天。"
        )
        return

    plot_df = main_force_df.copy()
    plot_df["date"] = pd.to_datetime(plot_df["date"])
    plot_df["net"] = pd.to_numeric(plot_df["net"], errors="coerce").fillna(0)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=plot_df["date"],
        y=plot_df["net"],
        name="主力買賣超",
        marker_color="#2f8bd8",
        hovertemplate="%{x|%Y-%m-%d}<br>主力買賣超 %{y:,.0f} 張<extra></extra>",
    ))

    fig.add_hline(y=0, line_color="rgba(80,80,80,0.45)", line_width=1)

    latest = plot_df.iloc[-1]
    latest_date = pd.to_datetime(latest["date"]).strftime("%m/%d")
    latest_net = float(latest["net"])
    latest_label = f"{latest_date} {latest_net:+,.0f}張（主力買賣超）"

    fig.update_layout(
        height=300,
        template="plotly_white",
        title=None,
        yaxis_title=None,
        xaxis_title=None,
        margin=dict(t=16, b=12, l=32, r=24),
        showlegend=False,
        bargap=0.45,
        font=dict(color="#4f4f4f"),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(130,170,200,0.28)",
            zeroline=False,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(130,170,200,0.28)",
            zeroline=False,
        ),
    )
    st.plotly_chart(fig, use_container_width=True)
    latest_concentration = pd.to_numeric(latest.get("top5_buy_concentration"), errors="coerce")
    latest_streak = pd.to_numeric(latest.get("consecutive_buy_days"), errors="coerce")
    latest_reversal_raw = pd.to_numeric(latest.get("reversal_flag"), errors="coerce")
    concentration_text = (
        f"{float(latest_concentration):.1f}%"
        if pd.notna(latest_concentration) else "N/A"
    )
    streak_text = (
        f"{int(latest_streak)} 天"
        if pd.notna(latest_streak) else "N/A"
    )
    reversal_text = (
        "是" if pd.notna(latest_reversal_raw) and bool(latest_reversal_raw) else
        ("否" if pd.notna(latest_reversal_raw) else "N/A")
    )
    st.caption(
        f"■ {latest_label} ｜ 主力買賣超 = 前 15 大買超券商淨買張 - 前 15 大賣超券商淨賣張"
    )
    st.caption(
        f"Top 5 買超集中度：{concentration_text} ｜ "
        f"連續買超：{streak_text} ｜ "
        f"反手訊號：{reversal_text}"
    )


def render_official_risk_flags(stock_id: str, analysis_date, is_historical: bool = False):
    """Display Premium official risk flags without affecting strategy scoring."""
    try:
        from data.finmind_client import (
            get_cached_risk_flags,
            get_premium_state,
            get_stock_risk_flags,
        )
    except Exception as exc:
        st.caption(f"官方風險旗標模組尚不可用：{exc}")
        return

    state = get_premium_state()
    d = pd.Timestamp(analysis_date).date().isoformat()

    with st.expander("🛡️ 官方風險旗標（Premium）", expanded=False):
        c1, c2, c3 = st.columns(3)
        c1.metric("方案", state.tier.upper())
        c2.metric("Premium 啟用", "是" if state.user_enabled else "否")
        c3.metric("運作狀態", "降級" if state.degraded else "正常")

        if not state.user_enabled or state.tier == "free":
            st.info("Premium 未啟用，僅顯示本機快取；不會呼叫 FinMind Premium API。")
            flags = get_cached_risk_flags(stock_id=stock_id, start_date=d, end_date=d)
        elif state.degraded:
            st.warning(f"Premium runtime 已降級：{state.last_error or 'unknown'}。目前僅顯示本機快取。")
            flags = get_cached_risk_flags(stock_id=stock_id, start_date=d, end_date=d)
        else:
            if is_historical:
                st.caption("歷史模式：僅查詢分析基準日的旗標資料。此區塊只供檢視，不影響回測或評分。")
            flags = get_stock_risk_flags(stock_id=stock_id, start_date=d, end_date=d)

    if flags is None or flags.empty:
        st.success(f"{d} 無官方風險旗標快取或回傳資料。")
        return
    if "flag_type" in flags.columns:
        flags = flags[flags["flag_type"].astype(str) != "price_limit"].copy()
    if flags.empty:
        st.success(f"{d} 無官方風險旗標快取或回傳資料。")
        return

    rows = []
    for _, row in flags.iterrows():
        detail = row.get("detail") or {}
        reason = ""
        if isinstance(detail, dict):
            reason = (
                detail.get("reason")
                or detail.get("處置原因")
                or detail.get("note")
                or detail.get("name")
                or ""
            )
            detail_text = json.dumps(detail, ensure_ascii=False, default=str)
        else:
            detail_text = str(detail)
        rows.append({
            "日期": pd.Timestamp(row["date"]).date().isoformat() if pd.notna(row.get("date")) else "",
            "旗標": row.get("flag_type", ""),
            "原因/摘要": reason,
            "原始資料": detail_text,
        })

        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def render_holding_shares(stock_id: str, analysis_date, is_historical: bool = False):
    """Display Premium large-holder distribution trends."""
    try:
        from data.finmind_client import (
            get_cached_holding_shares,
            get_holding_shares,
            get_premium_state,
        )
    except Exception as exc:
        st.caption(f"大戶持股比例載入失敗：{exc}")
        return

    state = get_premium_state()
    end = pd.Timestamp(analysis_date).date().isoformat()
    start = (pd.Timestamp(end) - pd.Timedelta(days=180)).date().isoformat()

    with st.expander("大戶持股比例（Premium）", expanded=False):
        if not state.user_enabled or state.tier == "free":
            st.info("Premium 未啟用，僅顯示本機快取；不會呼叫 FinMind Premium API。")
            hdf = get_cached_holding_shares(stock_id=stock_id, start_date=start, end_date=end)
        elif state.degraded:
            st.warning(f"Premium runtime 已降級：{state.last_error or 'unknown'}。目前僅顯示本機快取。")
            hdf = get_cached_holding_shares(stock_id=stock_id, start_date=start, end_date=end)
        elif is_historical:
            st.caption("歷史分析模式僅讀取截至分析日的本機快取，避免用未來資料回看。")
            hdf = get_cached_holding_shares(stock_id=stock_id, start_date=start, end_date=end)
        else:
            hdf = get_holding_shares(stock_id=stock_id, start_date=start, end_date=end)

        if hdf is None or hdf.empty:
            st.info("目前沒有可顯示的大戶持股比例資料。")
            return

        hdf = hdf.copy()
        hdf["date"] = pd.to_datetime(hdf["date"], errors="coerce")
        hdf = hdf.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        if hdf.empty:
            st.info("目前沒有可顯示的大戶持股比例資料。")
            return

        latest = hdf.iloc[-1]
        prev = hdf.iloc[-2] if len(hdf) >= 2 else None

        def _fmt_pct(value):
            return "N/A" if pd.isna(value) else f"{float(value):.2f}%"

        def _delta(col):
            if prev is None or pd.isna(latest.get(col)) or pd.isna(prev.get(col)):
                return None
            return float(latest[col]) - float(prev[col])

        m1, m2, m3 = st.columns(3)
        m1.metric("400 張以上", _fmt_pct(latest.get("above_400_pct")), delta=_delta("above_400_pct"))
        m2.metric("1000 張以上", _fmt_pct(latest.get("above_1000_pct")), delta=_delta("above_1000_pct"))
        m3.metric("10 張以下", _fmt_pct(latest.get("below_10_pct")), delta=_delta("below_10_pct"), delta_color="inverse")

        plot_df = hdf.tail(24).rename(columns={
            "above_400_pct": "400 張以上",
            "above_1000_pct": "1000 張以上",
            "below_10_pct": "10 張以下",
        })
        fig = go.Figure()
        for col, color in [
            ("400 張以上", "#2f8bd8"),
            ("1000 張以上", "#7c3aed"),
            ("10 張以下", "#d97706"),
        ]:
            if col in plot_df.columns:
                fig.add_trace(go.Scatter(
                    x=plot_df["date"],
                    y=plot_df[col],
                    mode="lines+markers",
                    name=col,
                    line=dict(color=color, width=2),
                ))
        fig.update_layout(
            height=280,
            template="plotly_white",
            margin=dict(t=16, b=12, l=32, r=24),
            yaxis_title="比例 %",
            xaxis_title=None,
            legend=dict(orientation="h", y=1.08),
        )
        st.plotly_chart(fig, use_container_width=True)

        show_df = hdf.tail(12).copy()
        show_df["date"] = show_df["date"].dt.strftime("%Y-%m-%d")
        st.dataframe(
            show_df.rename(columns={
                "date": "日期",
                "above_400_pct": "400 張以上 %",
                "above_1000_pct": "1000 張以上 %",
                "below_10_pct": "10 張以下 %",
                "fetched_at": "快取時間",
            }),
            hide_index=True,
            use_container_width=True,
        )


def render_premium_summary(
    stock_id: str,
    analysis_date,
    broker_main_force: pd.DataFrame,
    is_historical: bool = False,
):
    """Summarize Premium signals from cache/current page data without extra API calls."""
    try:
        from data.finmind_client import (
            get_cached_holding_shares,
            get_cached_risk_flags,
            get_premium_state,
        )
    except Exception as exc:
        st.caption(f"Premium 摘要無法載入：{exc}")
        return

    state = get_premium_state()
    d = pd.Timestamp(analysis_date).date().isoformat()
    start = (pd.Timestamp(d) - pd.Timedelta(days=180)).date().isoformat()

    positive: list[str] = []
    negative: list[str] = []
    missing: list[str] = []

    if broker_main_force is not None and not broker_main_force.empty:
        bdf = broker_main_force.copy().sort_values("date").reset_index(drop=True)
        latest_broker = bdf.iloc[-1]
        net = pd.to_numeric(latest_broker.get("net"), errors="coerce")
        concentration = pd.to_numeric(latest_broker.get("top5_buy_concentration"), errors="coerce")
        streak = pd.to_numeric(latest_broker.get("consecutive_buy_days"), errors="coerce")
        reversal = pd.to_numeric(latest_broker.get("reversal_flag"), errors="coerce")
        if pd.notna(net):
            if net > 0:
                positive.append(f"主力淨買超 {net:,.0f} 張")
            elif net < 0:
                negative.append(f"主力淨賣超 {abs(net):,.0f} 張")
        if pd.notna(concentration) and concentration >= 50:
            positive.append(f"前5大買超集中度 {concentration:.1f}%")
        if pd.notna(streak) and int(streak) >= 3:
            positive.append(f"主力連續買超 {int(streak)} 日")
        if pd.notna(reversal) and bool(reversal):
            negative.append("主力反手訊號")
    elif not is_historical:
        missing.append("券商分點主力（無資料）")

    try:
        risk_df = get_cached_risk_flags(stock_id=stock_id, start_date=d, end_date=d)
    except Exception:
        risk_df = pd.DataFrame()
    _FLAG_LABELS = {
        "disposition": "官方處置股",
        "suspended": "官方停止買賣",
        "shareholding_transfer": "內部人申報轉讓",
        "attention": "官方注意股",
        "treasury_shares": "實施庫藏股",
    }
    if risk_df is not None and not risk_df.empty:
        has_real_risk_flag = False
        for flag_type in risk_df["flag_type"].astype(str).dropna().unique():
            if flag_type == "price_limit":
                continue
            has_real_risk_flag = True
            label = _FLAG_LABELS.get(flag_type, f"官方風險旗標：{flag_type}")
            if flag_type == "treasury_shares":
                positive.append(label)
            else:
                negative.append(label)
        if not has_real_risk_flag:
            positive.append("官方風險旗標無異常記錄")
    else:
        positive.append("官方風險旗標無異常記錄")

    try:
        holding_df = get_cached_holding_shares(stock_id=stock_id, start_date=start, end_date=d)
    except Exception:
        holding_df = pd.DataFrame()
    if holding_df is not None and not holding_df.empty:
        hdf = holding_df.copy()
        hdf["date"] = pd.to_datetime(hdf["date"], errors="coerce")
        hdf = hdf.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        if not hdf.empty:
            latest_holding = hdf.iloc[-1]
            prev_holding = hdf.iloc[-2] if len(hdf) >= 2 else None

            def _holding_delta(col: str):
                if prev_holding is None:
                    return None
                if pd.isna(latest_holding.get(col)) or pd.isna(prev_holding.get(col)):
                    return None
                return float(latest_holding[col]) - float(prev_holding[col])

            d400 = _holding_delta("above_400_pct")
            d1000 = _holding_delta("above_1000_pct")
            d10 = _holding_delta("below_10_pct")
            if d400 is not None and d400 != 0:
                target = positive if d400 > 0 else negative
                target.append(
                    f"大戶(400張+)比例{'上升' if d400 > 0 else '下降'} {d400:+.2f}pp"
                )
            if d1000 is not None and d1000 != 0:
                target = positive if d1000 > 0 else negative
                target.append(
                    f"大戶(1000張+)比例{'上升' if d1000 > 0 else '下降'} {d1000:+.2f}pp"
                )
            if d10 is not None and d10 != 0:
                target = negative if d10 > 0 else positive
                target.append(
                    f"散戶(10張以下)比例{'上升' if d10 > 0 else '下降'} {d10:+.2f}pp"
                )
        else:
            missing.append("大戶持股比例（無快取）")
    else:
        missing.append("大戶持股比例（無快取）")

    with st.expander("Premium 訊號摘要", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("方案", state.tier.upper())
        c2.metric("運作狀態", "降級" if state.degraded else "正常")
        c3.metric("正向訊號", len(positive))
        c4.metric("負向訊號", len(negative))
        if state.degraded:
            st.warning(state.last_error or "Premium runtime 已降級")
        if not state.user_enabled or state.tier == "free":
            st.caption("Premium 未啟用；摘要僅使用本機快取與頁面資料。")

        s1, s2, s3 = st.columns(3)
        with s1:
            st.markdown("**正向訊號**")
            if positive:
                for item in positive:
                    st.write(f"- {item}")
            else:
                st.caption("無")
        with s2:
            st.markdown("**負向 / 風險訊號**")
            if negative:
                for item in negative:
                    st.write(f"- {item}")
            else:
                st.caption("無")
        with s3:
            st.markdown("**資料不足**")
            if missing:
                for item in sorted(set(missing)):
                    st.write(f"- {item}")
            else:
                st.caption("無")


def render_custom_preset_checks(
    *,
    stock_id: str,
    df: pd.DataFrame,
    sig,
    preset: dict,
    volume_rank: tuple[int | None, int, float | None],
    margin_trend: str,
):
    """依目前儲存的自訂模式，列出單股是否符合進階條件。"""
    if not preset:
        st.info("尚未儲存自訂模式；目前僅顯示策略必要條件。")
        return

    latest = df.iloc[-1]
    close = float(latest.get("close", 0) or 0)
    rank, total, _ = volume_rank
    min_price = float(preset.get("sb_min_price") or 0)
    min_rs = float(preset.get("sb_min_rs") or 0)
    require_weekly = _to_bool(preset.get("sb_require_weekly"), False)
    include_inst = _to_bool(preset.get("sb_include_inst"), False)
    include_margin = _to_bool(preset.get("sb_include_margin"), False)
    use_fundamental = _to_bool(preset.get("sb_use_fundamental"), False)
    raw_overheat_mult = preset.get("sb_overheat_atr_mult")
    overheat_mult = 3.5 if raw_overheat_mult in (None, "") else float(raw_overheat_mult)
    overheat_action = str(preset.get("sb_overheat_action_label") or "直接剔除")
    vol_mode = str(preset.get("sb_vol_filter_mode") or "不過濾")
    top_volume_n = int(preset.get("sb_top_volume_n") or 0)
    min_avg_volume = int(preset.get("sb_min_avg_volume") or 0)
    avg_vol_20 = (
        float(df["Trading_Volume"].tail(20).mean()) / 1000
        if "Trading_Volume" in df.columns and len(df) >= 20 else None
    )
    heat = _heat_room(df, overheat_mult)

    rows = []
    if min_price > 0:
        rows.append({
            "自訂條件": f"最低股價 >= {min_price:.0f} 元",
            "結果": "通過" if close >= min_price else "未通過",
            "目前數值": f"{close:.1f} 元",
        })

    if "前日量前" in vol_mode and top_volume_n > 0:
        passed = rank is not None and rank <= top_volume_n
        rows.append({
            "自訂條件": f"日成交量排名前 {top_volume_n} 名",
            "結果": "通過" if passed else "未通過",
            "目前數值": f"第 {rank}/{total} 名" if rank else "無排名資料",
        })
    elif "日均量" in vol_mode and min_avg_volume > 0:
        passed = avg_vol_20 is not None and avg_vol_20 >= min_avg_volume
        rows.append({
            "自訂條件": f"20 日均量 >= {min_avg_volume:,} 張",
            "結果": "通過" if passed else "未通過",
            "目前數值": f"{avg_vol_20:,.0f} 張" if avg_vol_20 is not None else "資料不足",
        })

    if require_weekly:
        rows.append({
            "自訂條件": "週線多頭",
            "結果": "通過" if sig.weekly_trend_up else "未通過",
            "目前數值": "週K收盤 > 週MA10 且週MA10向上" if sig.weekly_trend_up else "未符合",
        })

    if min_rs > 0:
        rows.append({
            "自訂條件": f"最低 RS >= {min_rs:.0f}",
            "結果": "通過" if sig.rs_score >= min_rs else "未通過",
            "目前數值": f"{sig.rs_score:.1f}",
        })

    if include_inst:
        inst_mode = str(preset.get("sb_inst_mode") or "個別法人皆須買超")
        if inst_mode == "個別法人皆須買超":
            selected = preset.get("sb_inst_selection") or ["外資", "投信", "自營商"]
            strict_days = int(preset.get("sb_strict_days") or 3)
            passed = sig.institutional_buy
            label = f"{'、'.join(selected)}各自連續 {strict_days} 日買超"
        else:
            agg_label = str(preset.get("sb_agg_mode_label") or "近 N 日累計 > 0")
            agg_days = int(preset.get("sb_agg_days") or 5)
            passed = sig.inst_total_buy
            label = f"三大法人{agg_label}（N={agg_days}）"
        rows.append({
            "自訂條件": label,
            "結果": "通過" if passed else "未通過",
            "目前數值": "已載入法人資料" if passed else "未符合或法人資料不足",
        })

    if overheat_mult > 0:
        if heat["available"]:
            room_abs = heat["room_abs"]
            room_pct = heat["room_pct"]
            status = "已超過" if heat["overheated"] else "未過熱"
            rows.append({
                "自訂條件": f"ATR 過熱防護：MA20 + {overheat_mult:g} ATR",
                "結果": "未通過" if heat["overheated"] and overheat_action.startswith("直接剔除") else "通過",
                "目前數值": (
                    f"{status}，距門檻 {room_pct:+.2f}% / {room_abs:+.2f} 元；"
                    f"門檻 {heat['threshold']:.2f} 元"
                ),
            })
        else:
            rows.append({
                "自訂條件": f"ATR 過熱防護：MA20 + {overheat_mult:g} ATR",
                "結果": "資料不足",
                "目前數值": "缺少 ATR 或 MA20",
            })
    else:
        rows.append({
            "自訂條件": "ATR 過熱防護",
            "結果": "停用",
            "目前數值": "自訂模式設定為 0",
        })

    if include_margin:
        rows.append({
            "自訂條件": "融資減少 / 籌碼集中",
            "結果": "通過" if margin_trend == "down" else "未通過",
            "目前數值": "融資餘額下降" if margin_trend == "down" else "融資未下降或無資料",
        })

    if use_fundamental:
        rows.extend(_fundamental_check_rows(stock_id, preset))

    if _to_bool(preset.get("sb_use_sector_filter"), False):
        rows.append({
            "自訂條件": f"產業近 5 日漲幅前 {int(preset.get('sb_top_sector_n') or 0)} 名",
            "結果": "未檢查",
            "目前數值": "單股分析不重算全市場產業排行",
        })

    if _to_bool(preset.get("sb_use_hp_density"), False) or _to_bool(preset.get("sb_use_turnover_ratio"), False):
        rows.append({
            "自訂條件": "族群集體突破 / 資金流向",
            "結果": "未檢查",
            "目前數值": "需全市場資料，掃描頁會套用",
        })

    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


# ── 側邊欄 ───────────────────────────────────────────────────────
_custom_preset = get_scanner_preset()

with st.sidebar:
    st.header("⚙️ 查詢設定")

    stock_input = st.text_input(
        "股票代號",
        placeholder="例：2330",
        max_chars=6,
    ).strip()

    from datetime import date as _date_cls
    analysis_date = st.date_input(
        "分析基準日",
        value=_date_cls.today(),
        max_value=_date_cls.today(),
        help="預設為今日。選擇過去日期可查看當日技術條件，並觀察後續盤勢。",
    )
    _is_historical = analysis_date < _date_cls.today()
    if _is_historical:
        st.info(f"📅 歷史模式：{analysis_date}")
        st.caption("歷史模式只回放價格條件；法人與融資不回補當日歷史資料，避免混入今日 API 結果。")

    load_inst = st.checkbox(
        "載入法人買賣超",
        value=True,
        help="v3/v4 皆需判斷主力連續 3 日買超；取消勾選時此必要條件會顯示未通過",
    )

    if _custom_preset:
        st.caption("已套用目前儲存的自訂模式條件檢查")

    strategy_version = st.radio(
        "策略版本",
        options=["v4 領先攻擊版", "v3 均線突破版"],
        index=1 if str(_custom_preset.get("sb_strategy_version", "")).startswith("v3") else 0,
        help=(
            "**v4 領先攻擊版**：7項嚴格必要條件（三線齊穿首日 + 均線糾結 + 量爆發 + ATR過熱保護 + RS>80 + 60日新高 + 主力連3日買超）  \n"
            "**v3 均線突破版**：站上MA20 + MA20向上 + 量增 + 不低於布林下軌 + MACD/RSI + 主力連3日買超"
        ),
    )

    _ma_mode_label = st.radio(
        "三線齊穿判斷模式（v4 專用）",
        options=["嚴謹型（昨日三線全在線下）", "寬鬆型（昨日任一線在線下）"],
        index=1 if str(_custom_preset.get("sb_ma_mode", "")).startswith("寬鬆") else 0,
        help="嚴謹型：昨收 < min(MA5,MA10,MA20)；寬鬆型：昨收 < max(MA5,MA10,MA20)",
        disabled=(strategy_version == "v3 均線突破版"),
    )
    ma_breakout_mode = "strict" if _ma_mode_label.startswith("嚴謹") else "loose"

    analyze_btn = st.button("🔎 開始分析", type="primary", use_container_width=True)

    st.markdown("---")
    st.markdown("""
    **說明**
    - 資料來源：本機快取（需先由工作器更新）
    - 若快取無資料，請前往「資料管理」頁面更新
    - 法人條件需勾選「載入法人買賣超」才能判斷
    """)


# ── 主體 ─────────────────────────────────────────────────────────
if not stock_input:
    st.info("請在左側輸入股票代號後按「開始分析」")
    st.stop()

if not analyze_btn and "analysis_stock" not in st.session_state:
    st.info("請在左側按「開始分析」")
    st.stop()

# 記住上次查詢的股票與日期
if analyze_btn:
    st.session_state["analysis_stock"] = stock_input
    st.session_state["analysis_date"] = analysis_date
    st.session_state["analysis_ma_mode"] = ma_breakout_mode
    st.session_state["analysis_strategy"] = strategy_version
    st.session_state.pop("analysis_df", None)

target_id         = st.session_state.get("analysis_stock", stock_input)
_active_date      = st.session_state.get("analysis_date", _date_cls.today())
_is_hist          = _active_date < _date_cls.today()
_ma_breakout_mode = st.session_state.get("analysis_ma_mode", ma_breakout_mode)
_strategy         = st.session_state.get("analysis_strategy", strategy_version)

# ── 載入資料 ─────────────────────────────────────────────────────
with st.spinner(f"載入 {target_id} 資料中..."):
    df = _load_df(target_id)

if df is None:
    st.error(
        f"找不到 **{target_id}** 的快取資料。  \n"
        "請確認代號正確，或前往「資料管理」頁面先更新資料。"
    )
    st.stop()

# 歷史模式：保留完整 df 供後續盤勢計算，分析用 df slice 至基準日
_full_df = df.copy()
if _is_hist and "date" in df.columns:
    _sliced = df[df["date"] <= pd.Timestamp(_active_date)]
    if _sliced.empty or len(_sliced) < 5:
        st.error(f"基準日 {_active_date} 前的資料不足，無法分析。")
        st.stop()
    df = _sliced.reset_index(drop=True)

# 取得股票名稱
try:
    from data.finmind_client import get_stock_list
    _sl = get_stock_list()
    _info = _sl[_sl["stock_id"] == target_id]
    stock_name = _info["stock_name"].iloc[0] if not _info.empty else ""
    industry   = _info["industry_category"].iloc[0] if not _info.empty else ""
except Exception:
    stock_name = industry = ""

# 法人資料（選填）
inst_buying = {}
if load_inst:
    _preset_inst_selection = _custom_preset.get("sb_inst_selection") or None
    _preset_strict_days = int(_custom_preset.get("sb_strict_days") or 3)
    _preset_agg_mode_label = str(_custom_preset.get("sb_agg_mode_label") or "")
    _preset_agg_mode = "rolling_sum" if _preset_agg_mode_label != "連續 N 日每日 > 0" else "consecutive"
    _preset_agg_days = int(_custom_preset.get("sb_agg_days") or 5)
    with st.spinner("載入法人資料..."):
        inst_buying = _load_inst(
            target_id,
            target_date=_active_date if _is_hist else None,
            selected_institutions=_preset_inst_selection,
            strict_days=_preset_strict_days,
            agg_mode=_preset_agg_mode,
            agg_days=_preset_agg_days,
        )
    if _is_hist and not inst_buying:
        st.caption("歷史模式未找到截至基準日的法人快取資料，因此法人條件仍顯示未載入。")

broker_main_force = pd.DataFrame()
broker_dates = df["date"].tail(30).tolist()
try:
    if _is_hist:
        from db.broker_cache import load_broker_main_force
        broker_main_force = load_broker_main_force(target_id, broker_dates)
    else:
        from data.finmind_client import get_broker_main_force_series
        with st.spinner("載入券商分點主力買賣超..."):
            broker_main_force = get_broker_main_force_series(
                target_id,
                broker_dates,
                top_n=15,
            )
except Exception as e:
    st.caption(f"券商分點主力買賣超暫時無法載入：{e}")

# 融資資料；歷史模式只讀本機快取，避免混入今日 API 結果
_margin_trend, _margin_latest, _margin_prev = "flat", 0, 0
try:
    from data.finmind_client import get_margin_trading, compute_margin_trend
    if _is_hist:
        from db.margin_cache import load_margin_for_date
        _mdf = load_margin_for_date(target_id, _active_date, days=14)
    else:
        _mdf = get_margin_trading(target_id, days=5)
    _margin_trend, _margin_latest, _margin_prev = compute_margin_trend(_mdf)
except Exception:
    pass

# 分析
sig = analyze_stock(df, inst_buying=inst_buying, precomputed=True,
                    margin_trend=_margin_trend,
                    ma_breakout_mode=_ma_breakout_mode,
                    broker_df=broker_main_force if not broker_main_force.empty else None)
_volume_rank = _get_daily_volume_rank(target_id, df.iloc[-1]["date"])
_raw_custom_overheat_mult = _custom_preset.get("sb_overheat_atr_mult")
_custom_overheat_mult = (
    3.5 if _raw_custom_overheat_mult in (None, "") else float(_raw_custom_overheat_mult)
)
_heat = _heat_room(df, _custom_overheat_mult)

# ── 標頭 ─────────────────────────────────────────────────────────
latest  = df.iloc[-1]
prev    = df.iloc[-2] if len(df) >= 2 else latest
close   = latest["close"]
chg_pct = (close - prev["close"]) / prev["close"] * 100 if prev["close"] else 0
chg_col = "#e74c3c" if chg_pct >= 0 else "#27ae60"
chg_sym = "▲" if chg_pct >= 0 else "▼"

title_name = f"　{stock_name}" if stock_name else ""
st.markdown(
    f"## {target_id}{title_name}"
    + (f"　<span style='font-size:0.9rem;color:#aaa'>{industry}</span>" if industry else ""),
    unsafe_allow_html=True,
)
if _is_hist:
    st.info(f"📅 歷史回溯：顯示 **{_active_date}** 當日技術條件（收盤後狀態）")

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("收盤價", f"{close:.1f} 元")
m2.metric("漲跌幅", f"{chg_sym}{abs(chg_pct):.2f}%",
          delta=f"{close - prev['close']:+.1f} 元",
          delta_color="normal" if chg_pct >= 0 else "inverse")
if "Trading_Volume" in df.columns:
    vol = latest["Trading_Volume"]
    m3.metric("成交量", f"{vol/1000:.0f} 張" if vol >= 1000 else f"{vol:.0f} 股")
m4.metric("MA20 乖離", f"{latest.get('ma20_bias_ratio', 0):.2f}%")
m5.metric("RSI(14)", f"{latest.get('rsi14', 0):.1f}")

rank, total_ranked, _rank_vol = _volume_rank
v1, v2 = st.columns(2)
v1.metric(
    "日成交量排行",
    f"第 {rank} / {total_ranked} 名" if rank else "無資料",
    help="以分析基準日全市場成交量由大到小排名",
)
if _heat["available"]:
    _room_abs = _heat["room_abs"]
    _room_pct = _heat["room_pct"]
    v2.metric(
        f"ATR 過熱距離（{_custom_overheat_mult:g}x）",
        f"{_room_pct:+.2f}%",
        delta=f"{_room_abs:+.2f} 元",
        delta_color="inverse" if _heat["overheated"] else "normal",
        help=f"過熱門檻：{_heat['threshold']:.2f} 元；負值代表已超過門檻",
    )
else:
    v2.metric("ATR 過熱距離", "資料不足")

st.markdown("---")

# ── 兩欄佈局：左評分卡 / 右K線 ───────────────────────────────────
col_score, col_chart = st.columns([0.4, 0.6])

with col_score:
    _has_inst = bool(inst_buying)
    if _strategy == "v3 均線突破版":
        st.subheader("📋 v3 條件評分卡")
        render_scorecard_v3(sig, df, has_inst=_has_inst)
    else:
        st.subheader("📋 v4 條件評分卡")
        render_scorecard(sig, df, ma_mode=_ma_breakout_mode, has_inst=_has_inst)

with col_chart:
    st.subheader("📈 日K線圖")
    render_daily_chart(target_id, df.tail(120))

st.markdown("---")

st.subheader("🏦 主力買賣超")
render_institutional_chart(broker_main_force)

st.markdown("---")

render_official_risk_flags(target_id, df.iloc[-1]["date"], is_historical=_is_hist)

st.markdown("---")

render_holding_shares(target_id, df.iloc[-1]["date"], is_historical=_is_hist)

st.markdown("---")

render_premium_summary(
    target_id,
    df.iloc[-1]["date"],
    broker_main_force,
    is_historical=_is_hist,
)

st.markdown("---")

st.subheader("🧩 自訂模式條件檢查")
render_custom_preset_checks(
    stock_id=target_id,
    df=df,
    sig=sig,
    preset=_custom_preset,
    volume_rank=_volume_rank,
    margin_trend=_margin_trend,
)

st.markdown("---")

# ── 週K線 ────────────────────────────────────────────────────────
st.subheader("📅 週K線圖")
render_weekly_chart(target_id, df)

st.markdown("---")

# ── 技術指標數值彙整 ─────────────────────────────────────────────
st.subheader("📊 技術指標數值")

tech_data = {
    "指標": ["MA5", "MA10", "MA20", "MA60",
             "BB上軌", "BB中軌", "BB下軌", "BB頻寬",
             "RSI(14)", "MACD(DIF)", "Signal(DEA)"],
    "數值": [
        f"{latest.get('ma5', float('nan')):.2f}",
        f"{latest.get('ma10', float('nan')):.2f}",
        f"{latest.get('ma20', float('nan')):.2f}",
        f"{latest.get('ma60', float('nan')):.2f}",
        f"{latest.get('bb_upper', float('nan')):.2f}",
        f"{latest.get('bb_mid', float('nan')):.2f}",
        f"{latest.get('bb_lower', float('nan')):.2f}",
        f"{latest.get('bb_bandwidth', float('nan')):.2f}%",
        f"{latest.get('rsi14', float('nan')):.1f}",
        f"{latest.get('macd', float('nan')):.4f}",
        f"{latest.get('macd_signal', float('nan')):.4f}",
    ],
}

# 均線糾結度、BB頻寬縮減
ma5  = latest.get("ma5",  float("nan"))
ma10 = latest.get("ma10", float("nan"))
ma20_v = latest.get("ma20", float("nan"))
if ma20_v > 0:
    squeeze_pct = (max(ma5, ma10, ma20_v) - min(ma5, ma10, ma20_v)) / ma20_v * 100
    tech_data["指標"].append("均線糾結度")
    tech_data["數值"].append(f"{squeeze_pct:.2f}%（需 <3%）")

if len(df) >= 22:
    bw_now   = latest.get("bb_bandwidth", float("nan"))
    bw_20ago = df["bb_bandwidth"].iloc[-21]
    tech_data["指標"].append("BB頻寬（20日前）")
    tech_data["數值"].append(f"{bw_20ago:.2f}%")
    tech_data["指標"].append("BB頻寬變化")
    direction = "縮減 ✅" if bw_now < bw_20ago else "擴張"
    tech_data["數值"].append(f"{bw_now:.2f}% → {direction}")

tech_col1, tech_col2 = st.columns(2)
n = len(tech_data["指標"])
half = (n + 1) // 2
with tech_col1:
    st.dataframe(
        pd.DataFrame({"指標": tech_data["指標"][:half], "數值": tech_data["數值"][:half]}),
        hide_index=True, use_container_width=True,
    )
with tech_col2:
    st.dataframe(
        pd.DataFrame({"指標": tech_data["指標"][half:], "數值": tech_data["數值"][half:]}),
        hide_index=True, use_container_width=True,
    )

# ── 後續盤勢（歷史模式才顯示）───────────────────────────────────
if _is_hist:
    st.markdown("---")
    st.subheader(f"📅 後續盤勢  ·  基準日：{_active_date}")

    _base_close = df.iloc[-1]["close"]
    _future = _full_df[_full_df["date"] > pd.Timestamp(_active_date)].reset_index(drop=True)

    if _future.empty:
        st.info("資料庫中尚無基準日之後的價格資料。")
    else:
        # 逐日報酬表
        _rows = []
        for i, row in _future.iterrows():
            _ret = (row["close"] - _base_close) / _base_close * 100
            _rows.append({
                "交易日": row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"]),
                "第 N 日": i + 1,
                "收盤": round(row["close"], 1),
                "報酬%": round(_ret, 2),
                "最高": round(row.get("max", row["close"]), 1),
                "最低": round(row.get("min", row["close"]), 1),
            })

        _future_df = pd.DataFrame(_rows)

        # 摘要 metrics
        _offsets = [d for d in [1, 3, 5, 10] if d <= len(_future_df)]
        if _offsets:
            _mcols = st.columns(len(_offsets))
            for _mc, _d in zip(_mcols, _offsets):
                _r = _future_df.loc[_d - 1, "報酬%"]
                _mc.metric(
                    f"+{_d} 日",
                    f"{_future_df.loc[_d-1, '收盤']} 元",
                    delta=f"{_r:+.2f}%",
                    delta_color="normal" if _r >= 0 else "inverse",
                )

        # 最高漲幅 / 最大回撤
        _period = min(10, len(_future_df))
        _highs = _future_df["最高"].iloc[:_period]
        _lows  = _future_df["最低"].iloc[:_period]
        _max_gain = (_highs.max() - _base_close) / _base_close * 100
        _max_dd   = (_lows.min()  - _base_close) / _base_close * 100
        _mg1, _mg2 = st.columns(2)
        _mg1.metric(f"前 {_period} 日最高漲幅", f"{_max_gain:+.2f}%",
                    help=f"最高價 {_highs.max():.1f} 元")
        _mg2.metric(f"前 {_period} 日最大回撤", f"{_max_dd:+.2f}%",
                    help=f"最低價 {_lows.min():.1f} 元")

        # 詳細走勢圖
        with st.expander("展開後續走勢圖", expanded=False):
            import plotly.graph_objects as _go
            _fig = _go.Figure()
            _fig.add_trace(_go.Candlestick(
                x=_future_df["交易日"],
                open=_full_df.loc[_full_df["date"] > pd.Timestamp(_active_date), "open"].values[:len(_future_df)],
                high=_future_df["最高"],
                low=_future_df["最低"],
                close=_future_df["收盤"],
                name="K線",
                increasing_line_color="#e74c3c",
                decreasing_line_color="#27ae60",
            ))
            _fig.add_hline(
                y=_base_close, line_dash="dash",
                line_color="rgba(255,255,255,0.4)",
                annotation_text=f"基準收盤 {_base_close:.2f}",
            )
            _fig.update_layout(
                height=320, template="plotly_dark",
                title=f"{target_id} 基準日後走勢",
                xaxis_rangeslider_visible=False,
                margin=dict(t=40, b=10),
            )
            st.plotly_chart(_fig, use_container_width=True)

        # 明細表（可展開）
        with st.expander("展開逐日報酬明細"):
            def _style_ret(val):
                if not isinstance(val, (int, float)):
                    return ""
                return "color: #e74c3c" if val > 0 else ("color: #27ae60" if val < 0 else "")
            st.dataframe(
                _future_df.style.applymap(_style_ret, subset=["報酬%"]),
                hide_index=True, use_container_width=True,
            )
