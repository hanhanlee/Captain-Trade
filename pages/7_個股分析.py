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

from db.database import init_db
from db.price_cache import load_prices
from modules.scanner import compute_indicators, analyze_stock
from modules.indicators import weekly_ma_trend, sma

init_db()

st.set_page_config(page_title="個股分析", page_icon="🔎", layout="wide")
st.title("🔎 個股分析")
st.caption("輸入股票代號，逐項檢視 v4 選股條件是否達標，掌握每個指標的實際數值")


# ── 工具函式 ─────────────────────────────────────────────────────

def _load_df(stock_id: str) -> pd.DataFrame | None:
    """從快取載入日K並計算指標；無快取時回傳 None"""
    df = load_prices(stock_id, lookback_days=400)
    if df is None or df.empty or len(df) < 30:
        return None
    return compute_indicators(df)


def _load_inst(stock_id: str) -> dict:
    """載入三大法人資料並彙整；失敗回傳空 dict"""
    try:
        from data.finmind_client import get_institutional_investors, summarize_institutional_signal
        idf = get_institutional_investors(stock_id, days=10)
        if idf.empty:
            return {}
        return summarize_institutional_signal(idf, strict_days=2, agg_days=3)
    except Exception:
        return {}


def _badge(passed: bool) -> str:
    return "✅" if passed else "❌"


def _color(passed: bool) -> str:
    return "#27ae60" if passed else "#e74c3c"


def render_scorecard(sig, df: pd.DataFrame, ma_mode: str = "strict"):
    """v4 條件逐項評分卡"""
    latest = df.iloc[-1]
    prev   = df.iloc[-2] if len(df) >= 2 else latest

    ma5  = latest.get("ma5",  float("nan"))
    ma10 = latest.get("ma10", float("nan"))
    ma20 = latest.get("ma20", float("nan"))
    close = latest["close"]
    p_close = prev["close"]

    # 均線糾結度
    squeeze_pct = (
        (max(ma5, ma10, ma20) - min(ma5, ma10, ma20)) / ma20 * 100
        if ma20 > 0 else float("nan")
    )
    # 量能爆發比
    vol_col = "Trading_Volume" if "Trading_Volume" in df.columns else None
    vol_ratio = (
        latest[vol_col] / prev[vol_col]
        if vol_col and prev.get(vol_col, 0) > 0 else float("nan")
    )
    # 乖離率
    bias = latest.get("ma20_bias_ratio", float("nan"))
    # 布林頻寬
    bw_now   = latest.get("bb_bandwidth", float("nan"))
    bw_20ago = df["bb_bandwidth"].iloc[-21] if len(df) >= 22 else float("nan")
    # 20日高點
    resist_20d = df["close"].iloc[-21:-1].max() if len(df) >= 22 else float("nan")
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
            "name": "均線糾結度 < 3%",
            "pass": sig.ma_squeeze,
            "detail": (
                f"三線最大偏差 ÷ MA20 = **{squeeze_pct:.2f}%**（需 < 3%）  \n"
                f"MA5={ma5:.1f} / MA10={ma10:.1f} / MA20={ma20:.1f}"
            ),
            "rule": "(max(MA5,10,20) − min(MA5,10,20)) / MA20 < 3%",
        },
        {
            "name": "量能爆發比 > 前一日 1.5 倍",
            "pass": sig.volume_explosion,
            "detail": (
                f"今日量 / 昨日量 = **{vol_ratio:.2f}x**（需 > 1.5x）"
                if not pd.isna(vol_ratio) else "成交量資料不足"
            ),
            "rule": "今日成交量 > 昨日成交量 × 1.5",
        },
        {
            "name": "股價乖離 MA20 < 5%",
            "pass": sig.ma20_bias_ok,
            "detail": f"(收盤 − MA20) / MA20 = **{bias:.2f}%**（需 < 5%）",
            "rule": "排除追高風險，控管買進成本",
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
            "detail": "昨日投信 ≤ 0，今日轉正（需載入法人資料）",
        },
        {
            "name": "突破近 20 日收盤高點",
            "pass": sig.breakout_20d,
            "score": 8,
            "detail": (
                f"今收 **{close:.1f}** vs 20日高點 **{resist_20d:.1f}**"
                if not pd.isna(resist_20d) else "資料不足"
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
        {
            "name": "相對強度 RS > 70",
            "pass": sig.rs_positive,
            "score": 7,
            "detail": f"RS 分數 = **{rs_score:.1f}**（需 > 70）",
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
        f"<span style='color:#aaa;font-size:0.85rem'> / 130 滿分</span>"
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


# ── 側邊欄 ───────────────────────────────────────────────────────
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
        value=False,
        help="需消耗 API 額度，可查詢投信第一天買超條件",
    )

    _ma_mode_label = st.radio(
        "三線齊穿判斷模式",
        options=["嚴謹型（昨日三線全在線下）", "寬鬆型（昨日任一線在線下）"],
        index=0,
        help="嚴謹型：昨收 < min(MA5,MA10,MA20)；寬鬆型：昨收 < max(MA5,MA10,MA20)",
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
    st.session_state.pop("analysis_df", None)

target_id        = st.session_state.get("analysis_stock", stock_input)
_active_date     = st.session_state.get("analysis_date", _date_cls.today())
_is_hist         = _active_date < _date_cls.today()
_ma_breakout_mode = st.session_state.get("analysis_ma_mode", ma_breakout_mode)

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
    if _is_hist:
        st.info("歷史模式已自動忽略法人買賣超，評分卡只使用價格衍生條件。")
        load_inst = False
    else:
        with st.spinner("載入法人資料..."):
            inst_buying = _load_inst(target_id)

# 融資資料（歷史模式下 API 只回傳近期資料，跳過以免誤判）
_margin_trend, _margin_latest, _margin_prev = "flat", 0, 0
if not _is_hist:
    try:
        from data.finmind_client import get_margin_trading, compute_margin_trend
        _mdf = get_margin_trading(target_id, days=5)
        _margin_trend, _margin_latest, _margin_prev = compute_margin_trend(_mdf)
    except Exception:
        pass

# 分析
sig = analyze_stock(df, inst_buying=inst_buying, precomputed=True,
                    margin_trend=_margin_trend,
                    ma_breakout_mode=_ma_breakout_mode)

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

st.markdown("---")

# ── 兩欄佈局：左評分卡 / 右K線 ───────────────────────────────────
col_score, col_chart = st.columns([0.4, 0.6])

with col_score:
    st.subheader("📋 v4 條件評分卡")
    render_scorecard(sig, df, ma_mode=_ma_breakout_mode)

with col_chart:
    st.subheader("📈 日K線圖")
    render_daily_chart(target_id, df.tail(120))

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
                annotation_text=f"基準收盤 {_base_close}",
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
