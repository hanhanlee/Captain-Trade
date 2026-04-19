"""
選股雷達頁面 v2
掃描全市場，找出技術面強勢股票
新增：週線多頭共振、相對強度(RS)、產業族群分析
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import time

from data.finmind_client import (
    can_fetch_premium_fundamentals,
    get_cached_risk_flags,
    get_daily_price,
    get_fundamentals_mode,
    summarize_institutional_signal,
)
from data.data_source import DataSourceManager, FALLBACK_WARNING
from modules.scanner import run_scan, compute_indicators, sector_analysis
from modules.indicators import weekly_ma_trend
from db.scan_history import save_scan_session, load_scan_history, load_session_results, delete_scan_session
from db.price_cache import load_prices_multi
from db.database import init_db

from db.settings import is_market_closed

init_db()

st.set_page_config(page_title="選股雷達", page_icon="🔍", layout="wide")


_RISK_FLAG_LABELS = {
    "disposition": "處置",
    "suspended": "暫停交易",
    "shareholding_transfer": "申報轉讓",
    "attention": "注意股",
    "treasury_shares": "庫藏股",
}

_RISK_FLAG_PENALTIES = {
    "disposition": 10,
    "suspended": 0,
    "shareholding_transfer": 5,
    "attention": 5,
    "treasury_shares": 0,
}


def _ensure_premium_score_columns(result_df: pd.DataFrame) -> pd.DataFrame:
    """Keep old scan history and new Premium scoring columns compatible."""
    if result_df is None or result_df.empty:
        return result_df

    result_df = result_df.copy()
    if "base_score" not in result_df.columns:
        source = result_df["score"] if "score" in result_df.columns else 0
        result_df["base_score"] = pd.to_numeric(source, errors="coerce").fillna(0)
    else:
        result_df["base_score"] = pd.to_numeric(result_df["base_score"], errors="coerce").fillna(0)

    for col in ["premium_score", "risk_penalty"]:
        if col not in result_df.columns:
            result_df[col] = 0
        result_df[col] = pd.to_numeric(result_df[col], errors="coerce").fillna(0)

    for col in ["premium_positive_flags", "premium_negative_flags", "premium_missing_fields"]:
        if col not in result_df.columns:
            result_df[col] = ""
        result_df[col] = result_df[col].fillna("").astype(str)

    result_df["final_score"] = (
        result_df["base_score"] + result_df["premium_score"] - result_df["risk_penalty"]
    ).clip(lower=0).round(1)
    result_df["score"] = result_df["final_score"]
    return result_df.sort_values("score", ascending=False).reset_index(drop=True)


def _format_heat_distance(row) -> str:
    pct = row.get("heat_room_pct")
    abs_val = row.get("heat_room_abs")
    if pd.isna(pct):
        return "—"
    try:
        pct = float(pct)
    except (TypeError, ValueError):
        return "—"

    yuan = ""
    if not pd.isna(abs_val):
        try:
            yuan = f" ({float(abs_val):+.2f} 元)"
        except (TypeError, ValueError):
            yuan = ""
    if pct < 0:
        return f"⚠️ 已過熱 {pct:+.2f}%{yuan}"
    return f"差 {pct:.2f}%{yuan}"


def _attach_cached_risk_flags(result_df: pd.DataFrame, scan_date) -> pd.DataFrame:
    """Attach cached Premium risk flags without calling FinMind during scans."""
    result_df = _ensure_premium_score_columns(result_df)
    result_df["premium_flags"] = ""
    result_df["official_risk_penalty"] = 0

    if result_df.empty or "stock_id" not in result_df.columns or scan_date is None:
        return result_df

    try:
        d = pd.to_datetime(scan_date).strftime("%Y-%m-%d")
        cached = get_cached_risk_flags(start_date=d, end_date=d)
    except Exception:
        return result_df

    if cached is None or cached.empty or "stock_id" not in cached.columns:
        return result_df

    stock_ids = set(result_df["stock_id"].astype(str))
    cached = cached[cached["stock_id"].astype(str).isin(stock_ids)]
    if cached.empty:
        return result_df

    flags_by_stock = {}
    penalty_by_stock = {}
    for sid, group in cached.groupby(cached["stock_id"].astype(str)):
        labels = []
        penalty = 0
        for _, flag in group.iterrows():
            flag_type = str(flag.get("flag_type", "") or "")
            if flag_type == "price_limit":
                continue
            labels.append(_RISK_FLAG_LABELS.get(flag_type, flag_type or "風險旗標"))
            penalty += _RISK_FLAG_PENALTIES.get(flag_type, 0)
        flags_by_stock[sid] = "、".join(dict.fromkeys(labels))
        penalty_by_stock[sid] = int(penalty)

    sid_series = result_df["stock_id"].astype(str)
    result_df["premium_flags"] = sid_series.map(flags_by_stock).fillna("")
    result_df["official_risk_penalty"] = sid_series.map(penalty_by_stock).fillna(0).astype(int)
    result_df["risk_penalty"] = (
        pd.to_numeric(result_df["risk_penalty"], errors="coerce").fillna(0)
        + result_df["official_risk_penalty"]
    ).astype(int)

    has_flag = result_df["premium_flags"].astype(str).str.len() > 0
    result_df.loc[has_flag, "premium_negative_flags"] = result_df.loc[has_flag].apply(
        lambda row: "、".join(
            x for x in [
                str(row.get("premium_negative_flags", "") or ""),
                str(row.get("premium_flags", "") or ""),
            ] if x
        ),
        axis=1,
    )
    return _ensure_premium_score_columns(result_df)


def _has_signal_text(value) -> bool:
    return bool(str(value or "").strip())


def _premium_eval_group(row: pd.Series) -> str:
    risk_penalty = pd.to_numeric(row.get("risk_penalty", 0), errors="coerce")
    premium_score = pd.to_numeric(row.get("premium_score", 0), errors="coerce")
    risk_penalty = 0 if pd.isna(risk_penalty) else float(risk_penalty)
    premium_score = 0 if pd.isna(premium_score) else float(premium_score)
    negative = _has_signal_text(row.get("premium_negative_flags", "")) or risk_penalty > 0
    positive = _has_signal_text(row.get("premium_positive_flags", "")) or premium_score > 0
    if negative:
        return "negative risk/fundamental flags"
    if positive:
        return "positive premium flags"
    return "no tracked flags"


def _forward_return(price_df: pd.DataFrame, scan_date, entry_close, horizon: int):
    if price_df is None or price_df.empty or pd.isna(entry_close) or float(entry_close) <= 0:
        return None
    scan_ts = pd.to_datetime(scan_date, errors="coerce")
    if pd.isna(scan_ts):
        return None
    df = price_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date")
    future = df[df["date"] > scan_ts]
    if len(future) < horizon:
        return None
    exit_close = pd.to_numeric(future.iloc[horizon - 1].get("close"), errors="coerce")
    if pd.isna(exit_close):
        return None
    return round((float(exit_close) - float(entry_close)) / float(entry_close) * 100, 2)


def build_premium_trial_evaluation(history: list[dict], max_sessions: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build a cache-only Premium trial report from scan history and price cache."""
    rows = []
    selected = history[:max_sessions]
    for rec in selected:
        df = load_session_results(rec["id"])
        if df.empty:
            continue
        df = _ensure_premium_score_columns(df)
        scan_date = pd.to_datetime(rec["scanned_at"], errors="coerce")
        if pd.isna(scan_date):
            continue
        for rank, (_, row) in enumerate(df.iterrows(), start=1):
            rows.append({
                "session_id": rec["id"],
                "scanned_at": scan_date,
                "scan_date": scan_date.date().isoformat(),
                "rank": rank,
                "stock_id": str(row.get("stock_id", "")),
                "stock_name": row.get("stock_name", ""),
                "industry": row.get("industry", ""),
                "entry_close": pd.to_numeric(row.get("close"), errors="coerce"),
                "base_score": pd.to_numeric(row.get("base_score"), errors="coerce"),
                "premium_score": pd.to_numeric(row.get("premium_score"), errors="coerce"),
                "risk_penalty": pd.to_numeric(row.get("risk_penalty"), errors="coerce"),
                "final_score": pd.to_numeric(row.get("final_score", row.get("score")), errors="coerce"),
                "premium_group": _premium_eval_group(row),
                "premium_positive_flags": str(row.get("premium_positive_flags", "") or ""),
                "premium_negative_flags": str(row.get("premium_negative_flags", "") or ""),
                "premium_missing_fields": str(row.get("premium_missing_fields", "") or ""),
            })

    detail_df = pd.DataFrame(rows)
    if detail_df.empty:
        return detail_df, pd.DataFrame()

    min_scan_date = detail_df["scanned_at"].min().date().isoformat()
    price_map = load_prices_multi(sorted(detail_df["stock_id"].dropna().unique().tolist()), start_date=min_scan_date)
    for horizon in [5, 10, 20]:
        detail_df[f"return_{horizon}d"] = detail_df.apply(
            lambda row: _forward_return(
                price_map.get(row["stock_id"]),
                row["scanned_at"],
                row["entry_close"],
                horizon,
            ),
            axis=1,
        )

    summary_rows = []
    for group, group_df in detail_df.groupby("premium_group", sort=False):
        item = {
            "premium_group": group,
            "count": int(len(group_df)),
            "avg_final_score": round(pd.to_numeric(group_df["final_score"], errors="coerce").mean(), 2),
            "avg_risk_penalty": round(pd.to_numeric(group_df["risk_penalty"], errors="coerce").mean(), 2),
            "missing_rate": round(group_df["premium_missing_fields"].astype(str).str.len().gt(0).mean() * 100, 1),
        }
        for horizon in [5, 10, 20]:
            col = f"return_{horizon}d"
            returns = pd.to_numeric(group_df[col], errors="coerce").dropna()
            item[f"avg_{horizon}d_return"] = round(returns.mean(), 2) if not returns.empty else None
            item[f"win_rate_{horizon}d"] = round((returns > 0).mean() * 100, 1) if not returns.empty else None
            item[f"samples_{horizon}d"] = int(len(returns))
        summary_rows.append(item)

    order = {"negative risk/fundamental flags": 0, "positive premium flags": 1, "no tracked flags": 2}
    summary_df = pd.DataFrame(summary_rows).sort_values(
        "premium_group",
        key=lambda s: s.map(order).fillna(9),
    ).reset_index(drop=True)
    return detail_df, summary_df


def render_premium_trial_evaluation(history: list[dict]):
    st.markdown("### Premium 試用期評估")
    st.caption("只讀 scan history 與 price cache，不呼叫 Premium API。")

    if not history:
        st.info("目前沒有 scan history，先執行並儲存幾次選股結果後再評估。")
        return
    c1, _ = st.columns([1, 3])
    max_sessions = c1.slider("納入最近幾次掃描", min_value=1, max_value=min(30, len(history)), value=min(10, len(history)))
    detail_df, summary_df = build_premium_trial_evaluation(history, max_sessions)

    if detail_df.empty or summary_df.empty:
        st.info("目前 scan history 沒有足夠資料可產生 Premium 評估。")
        return

    metric_cols = st.columns(4)
    metric_cols[0].metric("納入掃描", f"{max_sessions} 次")
    metric_cols[1].metric("樣本數", f"{len(detail_df)} 筆")
    metric_cols[2].metric("有負向/基本面訊號", f"{(detail_df['premium_group'] == 'negative risk/fundamental flags').sum()} 筆")
    metric_cols[3].metric("有正向訊號", f"{(detail_df['premium_group'] == 'positive premium flags').sum()} 筆")

    display_summary = summary_df.rename(columns={
        "premium_group": "訊號分組",
        "count": "樣本數",
        "avg_final_score": "平均 final_score",
        "avg_risk_penalty": "平均風險扣分",
        "missing_rate": "缺資料率%",
        "avg_5d_return": "5日平均報酬%",
        "win_rate_5d": "5日勝率%",
        "samples_5d": "5日樣本",
        "avg_10d_return": "10日平均報酬%",
        "win_rate_10d": "10日勝率%",
        "samples_10d": "10日樣本",
        "avg_20d_return": "20日平均報酬%",
        "win_rate_20d": "20日勝率%",
        "samples_20d": "20日樣本",
    })
    st.dataframe(display_summary, use_container_width=True, hide_index=True)

    chart_df = summary_df.melt(
        id_vars=["premium_group"],
        value_vars=["avg_5d_return", "avg_10d_return", "avg_20d_return"],
        var_name="horizon",
        value_name="avg_return",
    ).dropna(subset=["avg_return"])
    if not chart_df.empty:
        chart_df["horizon"] = chart_df["horizon"].map({
            "avg_5d_return": "5D",
            "avg_10d_return": "10D",
            "avg_20d_return": "20D",
        })
        fig = px.bar(
            chart_df,
            x="premium_group",
            y="avg_return",
            color="horizon",
            barmode="group",
            title="訊號分組後續平均報酬",
            labels={"premium_group": "訊號分組", "avg_return": "平均報酬%", "horizon": "期間"},
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("查看評估明細", expanded=False):
        detail_display = detail_df.sort_values(["scanned_at", "rank"], ascending=[False, True]).rename(columns={
            "scan_date": "掃描日",
            "rank": "排名",
            "stock_id": "代碼",
            "stock_name": "名稱",
            "industry": "產業",
            "entry_close": "掃描收盤",
            "final_score": "final_score",
            "risk_penalty": "risk_penalty",
            "premium_group": "訊號分組",
            "return_5d": "5日報酬%",
            "return_10d": "10日報酬%",
            "return_20d": "20日報酬%",
            "premium_positive_flags": "正向 flags",
            "premium_negative_flags": "負向 flags",
            "premium_missing_fields": "缺資料欄位",
        })
        cols = [
            "掃描日", "排名", "代碼", "名稱", "產業", "訊號分組",
            "final_score", "risk_penalty", "掃描收盤",
            "5日報酬%", "10日報酬%", "20日報酬%",
            "正向 flags", "負向 flags", "缺資料欄位",
        ]
        st.dataframe(detail_display[[c for c in cols if c in detail_display.columns]], use_container_width=True, hide_index=True)


def render_chart(stock_id: str, df: pd.DataFrame):
    """繪製日K線圖 + 指標"""
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.05, row_heights=[0.6, 0.2, 0.2],
        subplot_titles=[f"{stock_id} 日K線圖", "成交量", "MACD"],
    )

    fig.add_trace(go.Candlestick(
        x=df["date"], open=df["open"], high=df["max"],
        low=df["min"], close=df["close"], name="K線",
        increasing_line_color="#e74c3c", decreasing_line_color="#27ae60",
    ), row=1, col=1)

    for ma, color in [("ma5", "#f39c12"), ("ma20", "#3498db"), ("ma60", "#9b59b6")]:
        if ma in df.columns:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df[ma], name=ma.upper(),
                line=dict(color=color, width=1.2),
            ), row=1, col=1)

    for bb_col, name in [("bb_upper", "BB上軌"), ("bb_lower", "BB下軌")]:
        if bb_col in df.columns:
            fig.add_trace(go.Scatter(
                x=df["date"], y=df[bb_col], name=name,
                line=dict(color="rgba(52,152,219,0.4)", width=0.8, dash="dot"),
            ), row=1, col=1)

    if "Trading_Volume" in df.columns:
        bar_colors = ["#e74c3c" if c >= o else "#27ae60"
                      for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(
            x=df["date"], y=df["Trading_Volume"],
            name="成交量", marker_color=bar_colors, showlegend=False,
        ), row=2, col=1)

    if all(c in df.columns for c in ["macd", "macd_signal", "macd_hist"]):
        fig.add_trace(go.Scatter(x=df["date"], y=df["macd"], name="DIF",
                                  line=dict(color="#e74c3c", width=1)), row=3, col=1)
        fig.add_trace(go.Scatter(x=df["date"], y=df["macd_signal"], name="DEA",
                                  line=dict(color="#3498db", width=1)), row=3, col=1)
        hist_colors = ["#e74c3c" if v >= 0 else "#27ae60"
                       for v in df["macd_hist"].fillna(0)]
        fig.add_trace(go.Bar(x=df["date"], y=df["macd_hist"],
                              marker_color=hist_colors, showlegend=False), row=3, col=1)

    fig.update_layout(height=680, showlegend=True, xaxis_rangeslider_visible=False,
                      template="plotly_dark", margin=dict(t=40, b=10),
                      legend=dict(orientation="h", y=1.02))
    st.plotly_chart(fig, use_container_width=True)

    if "rsi14" in df.columns:
        latest_rsi = df["rsi14"].iloc[-1]
        rsi_color = "#e74c3c" if latest_rsi > 70 else ("#27ae60" if latest_rsi < 30 else "#f39c12")
        st.markdown(f"**RSI(14)：<span style='color:{rsi_color}'>{latest_rsi:.1f}</span>**",
                    unsafe_allow_html=True)
        rfig = go.Figure()
        rfig.add_trace(go.Scatter(x=df["date"], y=df["rsi14"], name="RSI",
                                   line=dict(color="#f39c12"), fill="tozeroy",
                                   fillcolor="rgba(243,156,18,0.1)"))
        rfig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="超買70")
        rfig.add_hline(y=50, line_dash="dot", line_color="gray")
        rfig.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="超賣30")
        rfig.update_layout(height=180, template="plotly_dark",
                            margin=dict(t=10, b=10), showlegend=False)
        st.plotly_chart(rfig, use_container_width=True)


def render_weekly_chart(stock_id: str, df: pd.DataFrame):
    """繪製週K線圖 + 週MA10"""
    wt = weekly_ma_trend(df, ma_period=10)
    if not wt or "weekly_df" not in wt:
        st.caption("週線資料不足（需至少 14 週）")
        return

    weekly = wt["weekly_df"]
    high_col = "max" if "max" in weekly.columns else "high"
    low_col = "min" if "min" in weekly.columns else "low"

    from modules.indicators import sma
    weekly["wma10"] = sma(weekly["close"], 10)

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=weekly["week_end"], open=weekly["open"], high=weekly[high_col],
        low=weekly[low_col], close=weekly["close"], name="週K",
        increasing_line_color="#e74c3c", decreasing_line_color="#27ae60",
    ))
    fig.add_trace(go.Scatter(
        x=weekly["week_end"], y=weekly["wma10"], name="週MA10",
        line=dict(color="#3498db", width=2),
    ))

    status = "🟢 週線多頭" if wt["weekly_above_ma"] and wt["weekly_ma_rising"] else "🔴 週線偏弱"
    fig.update_layout(
        height=380, template="plotly_dark",
        title=f"{stock_id} 週K線圖　{status}",
        xaxis_rangeslider_visible=False,
        margin=dict(t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── 側邊欄 ───────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ 掃描設定")

    from datetime import date as _date_cls
    scan_date = st.date_input(
        "分析基準日",
        value=_date_cls.today(),
        max_value=_date_cls.today(),
        help="預設為今日（最新資料）。選擇過去日期可重現當日選股結果，並查看後續盤勢。",
    )
    _is_historical = scan_date < _date_cls.today()
    if _is_historical:
        st.info(f"📅 歷史回溯模式：{scan_date}（自動使用資料庫）")
        st.caption("歷史模式只回放價格資料；法人、融資與基本面條件不套用，避免混入今日 API 資料。")

    # ── 掃描模式 preset ────────────────────────────────────────
    _PRESET_KEYS = [
        "sb_min_price", "sb_scan_range", "sb_include_inst", "sb_include_margin",
        "sb_inst_mode", "sb_inst_selection", "sb_strict_days", "sb_agg_mode_label",
        "sb_agg_days", "sb_require_inst", "sb_vol_filter_mode", "sb_top_volume_n",
        "sb_min_avg_volume", "sb_use_sector_filter", "sb_top_sector_n",
        "sb_use_hp_density", "sb_hp_density_lookback", "sb_hp_density_threshold_pct",
        "sb_use_turnover_ratio", "sb_turnover_top_n", "sb_require_weekly",
        "sb_min_rs", "sb_overheat_atr_mult", "sb_overheat_action_label",
        "sb_use_fundamental", "sb_req_eps", "sb_req_cf", "sb_min_roe", "sb_max_debt",
        "sb_ma_mode", "sb_strategy_version",
    ]
    _BASE = {
        "sb_min_price": 10.0, "sb_inst_mode": "個別法人皆須買超",
        "sb_inst_selection": ["外資", "投信", "自營商"], "sb_strict_days": 2,
        "sb_agg_mode_label": "近 N 日累計 > 0", "sb_agg_days": 5,
        "sb_require_inst": False, "sb_top_sector_n": 3,
        "sb_hp_density_lookback": 20, "sb_hp_density_threshold_pct": 30,
        "sb_turnover_top_n": 5, "sb_require_weekly": False, "sb_min_rs": 0,
        "sb_overheat_atr_mult": 3.5,
        "sb_overheat_action_label": "直接剔除（不入選候選清單）",
        "sb_use_fundamental": False, "sb_req_eps": True, "sb_req_cf": True,
        "sb_min_roe": 0, "sb_max_debt": 0,
        "sb_ma_mode": "嚴謹型（三線全穿）",
        "sb_vol_filter_mode": "前日量前 N 名（推薦）", "sb_min_avg_volume": 0,
        "sb_strategy_version": "v4 領先攻擊版（精準）",
    }
    _PRESETS = {
        "極速": {**_BASE,
            "sb_scan_range": "快速測試（20 檔）",
            "sb_include_inst": False, "sb_include_margin": False,
            "sb_top_volume_n": 100, "sb_use_sector_filter": False,
            "sb_use_hp_density": False, "sb_use_turnover_ratio": False,
        },
        "標準": {**_BASE,
            "sb_scan_range": "小型掃描（100 檔）",
            "sb_include_inst": True, "sb_include_margin": False,
            "sb_top_volume_n": 200, "sb_use_sector_filter": False,
            "sb_use_hp_density": False, "sb_use_turnover_ratio": False,
        },
        "完整": {**_BASE,
            "sb_scan_range": "全市場掃描（需時較長）",
            "sb_include_inst": True, "sb_include_margin": True,
            "sb_top_volume_n": 300, "sb_use_sector_filter": True,
            "sb_use_hp_density": True, "sb_use_turnover_ratio": True,
        },
    }

    from db.settings import get_scanner_preset as _get_cpreset, set_scanner_preset as _set_cpreset
    _custom_data = _get_cpreset()
    if _custom_data:
        _PRESETS["自訂"] = _custom_data

    _mode_opts = ["⚡ 極速", "🎯 標準", "🔬 完整", "⚙️ 自訂"]
    scan_preset = st.selectbox(
        "掃描模式",
        options=_mode_opts,
        key="sb_preset_select",
        help="快速套用預設組合；「⚙️ 自訂」載入你上次儲存的設定",
    )
    _preset_name = scan_preset.split(" ", 1)[1]

    # 當模式切換時，把 preset 值寫入 session_state（在 widget 渲染前執行）
    if st.session_state.get("_sb_preset_prev") != scan_preset:
        if _preset_name in _PRESETS:
            for _k, _v in _PRESETS[_preset_name].items():
                st.session_state[_k] = _v
        st.session_state["_sb_preset_prev"] = scan_preset

    if _preset_name == "自訂" and not _custom_data:
        st.caption("尚未儲存自訂設定，調整後點擊下方「儲存為自訂模式」")

    st.markdown("---")

    # ── 核心設定 ───────────────────────────────────────────────
    min_price = st.number_input("最低股價（元）", min_value=1.0, max_value=100.0,
                                value=10.0, step=1.0, key="sb_min_price")
    scan_mode = st.radio("掃描範圍",
                          ["快速測試（20 檔）", "小型掃描（100 檔）", "全市場掃描（需時較長）"],
                          key="sb_scan_range")

    strategy_version_label = st.radio(
        "🧠 選股策略版本",
        options=["v4 領先攻擊版（精準）", "v3 均線突破版（寬鬆）"],
        key="sb_strategy_version",
        help=(
            "v4 領先攻擊版：捕捉「均線糾結後第一天突破」，訊號精準但標的較少。\n"
            "v3 均線突破版：站上MA20 + 量增 + MACD/RSI 確認，條件寬鬆標的較多。"
        ),
    )
    strategy_version = "v3" if strategy_version_label.startswith("v3") else "v4"

    if strategy_version == "v4":
        ma_breakout_mode_label = st.radio(
            "🔀 三線齊穿模式",
            options=["嚴謹型（三線全穿）", "寬鬆型（任一線即可）"],
            key="sb_ma_mode",
            help=(
                "嚴謹：昨收 < min(MA5, MA10, MA20)，三線全穿才算首日突破，訊號精準。\n"
                "寬鬆：昨收 < max(MA5, MA10, MA20)，突破任一均線即納入，標的較多。"
            ),
        )
        ma_breakout_mode = "strict" if ma_breakout_mode_label.startswith("嚴謹") else "loose"
    else:
        ma_breakout_mode = "strict"  # v3 不使用此參數，給預設值即可

    # ── 法人 & 融資條件 ────────────────────────────────────────
    _inst_exp = (
        st.session_state.get("sb_include_inst", False)
        or st.session_state.get("sb_include_margin", False)
    )
    with st.expander("📊 法人 & 融資條件", expanded=_inst_exp):
        include_institutional = st.checkbox("納入法人買賣超（較慢）", value=False,
                                            key="sb_include_inst")
        include_margin = st.checkbox(
            "納入融資餘額（消耗 API 額度）", value=False, key="sb_include_margin",
            help="抓取每檔股票近 5 日融資餘額，判斷散戶是否去槓桿（籌碼乾淨）。每檔多 1 次 API 呼叫。",
        )
        inst_selection: list = []
        require_institutional = False
        strict_days = 2
        agg_mode = "rolling_sum"
        agg_days = 5
        if include_institutional:
            inst_mode = st.radio(
                "法人判斷模式",
                options=["個別法人皆須買超", "三大法人合計買超"],
                key="sb_inst_mode",
                help="前者保留原本嚴格邏輯；後者只看整體資金是否淨流入",
            )
            if inst_mode == "個別法人皆須買超":
                inst_selection = st.multiselect(
                    "選擇法人",
                    options=["外資", "投信", "自營商"],
                    default=["外資", "投信", "自營商"],
                    key="sb_inst_selection",
                    help="勾選的法人都必須各自連續買超才算符合",
                )
                strict_days = st.slider("個別法人連續買超天數",
                                        min_value=1, max_value=5, value=2, step=1,
                                        key="sb_strict_days")
                agg_mode = "rolling_sum"
                agg_days = 5
            else:
                inst_selection = ["外資", "投信", "自營商"]
                strict_days = 2
                agg_mode_label = st.radio(
                    "合計買超判斷方式",
                    options=["近 N 日累計 > 0", "連續 N 日每日 > 0"],
                    key="sb_agg_mode_label",
                    horizontal=True,
                    help="可用近 N 日合計淨買超，或要求最近每一天合計都為正",
                )
                agg_mode = "rolling_sum" if agg_mode_label == "近 N 日累計 > 0" else "consecutive"
                agg_days = st.slider("合計買超觀察天數",
                                     min_value=2, max_value=10, value=5, step=1,
                                     key="sb_agg_days")
            require_institutional = st.checkbox(
                "法人條件為必要條件（不符合直接排除）", value=False, key="sb_require_inst",
                help="勾選後，未符合法人條件的股票直接剔除，而非只是少加分",
            )
        else:
            inst_mode = "個別法人皆須買超"

    # ── 流動性 & 產業過濾 ──────────────────────────────────────
    with st.expander("💧 流動性 & 產業過濾", expanded=True):
        vol_filter_mode = st.radio(
            "流動性過濾",
            ["不過濾", "前日量前 N 名（推薦）", "日均量 ≥ N 張"],
            key="sb_vol_filter_mode",
            help="先過濾低流動性股票，加快掃描速度並聚焦主流股",
        )
        if vol_filter_mode == "前日量前 N 名（推薦）":
            top_volume_n = st.number_input("取前 N 名", min_value=50, max_value=500,
                                            value=100, step=50, key="sb_top_volume_n")
            min_avg_volume = 0
            st.caption("💡 鎖定當天最活躍的股票，動態追蹤市場熱點")
        elif vol_filter_mode == "日均量 ≥ N 張":
            min_avg_volume = st.number_input("最低日均量（張）", min_value=0,
                                              max_value=50000, value=1000, step=100,
                                              key="sb_min_avg_volume")
            top_volume_n = 0
            st.caption("💡 日均量 > 1000 張通常對應資本額 20 億以上")
        else:
            min_avg_volume, top_volume_n = 0, 0

        st.markdown("---")
        use_sector_filter = st.checkbox("只掃描近一週漲幅前 N 類股", value=False,
                                         key="sb_use_sector_filter",
                                         help="自動算出哪些產業最強，只在那幾個產業裡選股")
        top_sector_n = 0
        if use_sector_filter:
            top_sector_n = st.number_input("取前 N 個產業", min_value=1, max_value=10,
                                            value=3, step=1, key="sb_top_sector_n")
            st.caption("💡 資金正在流入的產業，勝率更高")

    # ── 族群偵測 ───────────────────────────────────────────────
    _cluster_exp = (
        st.session_state.get("sb_use_hp_density", False)
        or st.session_state.get("sb_use_turnover_ratio", False)
    )
    with st.expander("🔥 族群偵測", expanded=_cluster_exp):
        use_hp_density = st.checkbox(
            "族群創高密度（HP Density）", value=False, key="sb_use_hp_density",
            help="統計族群內有多少比例的股票今日收盤等於近 N 日高點。"
                 "比例越高代表該族群成員集體向上突破，是強勢輪動的早期訊號。",
        )
        hp_density_lookback = 20
        hp_density_threshold = 0.30
        if use_hp_density:
            hp_density_lookback = st.slider("創高天數 N", 10, 60, 20, 5,
                                            key="sb_hp_density_lookback",
                                            help="計算「近幾天新高」的回溯天數")
            hp_density_threshold_pct = st.slider("創高比例門檻 %", 10, 80, 30, 5,
                                                  key="sb_hp_density_threshold_pct",
                                                  help="族群內達到此比例的股票在創高，才視為有效突破")
            hp_density_threshold = hp_density_threshold_pct / 100.0

        use_turnover_ratio = st.checkbox(
            "資金流向比重（Turnover Ratio）", value=False, key="sb_use_turnover_ratio",
            help="以「收盤價 × 成交量」估算各族群成交額佔全市場比重，"
                 "鎖定資金集中流入的強勢族群。",
        )
        turnover_top_n = 5
        if use_turnover_ratio:
            turnover_top_n = st.number_input("資金前幾大族群", min_value=1, max_value=15,
                                              value=5, step=1, key="sb_turnover_top_n",
                                              help="成交額排名在此範圍內的族群視為資金集中")

    # ── 進階選項 ───────────────────────────────────────────────
    _adv_exp = (
        st.session_state.get("sb_require_weekly", False)
        or st.session_state.get("sb_min_rs", 0) > 0
        or st.session_state.get("sb_overheat_atr_mult", 3.5) != 3.5
    )
    with st.expander("🔬 進階選項", expanded=_adv_exp):
        require_weekly = st.checkbox("必須週線多頭（更嚴格，結果更少）", value=False,
                                     key="sb_require_weekly")
        min_rs = st.slider("最低相對強度 RS 分數", 0, 80, 0, 5, key="sb_min_rs",
                           help="0 = 不限制；60 以上 = 強勢股")
        st.markdown("**🌡️ 過熱股防護**")
        overheat_atr_mult = st.slider(
            "過熱 ATR 倍數", min_value=0.0, max_value=6.0, value=3.5, step=0.5,
            key="sb_overheat_atr_mult",
            help="收盤 > MA20 + N × ATR14 時視為過熱。0 = 停用 ATR 防護。"
                 "半導體等熱門電子股噴發期可調高至 4.5~5.0。",
        )
        overheat_action_label = st.radio(
            "超過門檻時的處置",
            options=["直接剔除（不入選候選清單）", "扣分懲罰（總強度分數扣減 10 分）"],
            key="sb_overheat_action_label",
            help="直接剔除較保守；扣分可保留強勢但過熱的標的供人工觀察。",
        )
        overheat_action = "drop" if overheat_action_label.startswith("直接剔除") else "penalty"

    # ── 基本面過濾 ─────────────────────────────────────────────
    _fund_exp = st.session_state.get("sb_use_fundamental", False)
    with st.expander("📋 基本面過濾", expanded=_fund_exp):
        use_fundamental = st.checkbox(
            "啟用基本面過濾", value=False, key="sb_use_fundamental",
            help="從財報剔除虧損股與地雷股，快取 90 天不重複耗 API",
        )
        fundamental_filter: dict = {}
        if use_fundamental:
            req_eps = st.checkbox("EPS TTM > 0（排除虧損股）", value=True, key="sb_req_eps")
            req_cf  = st.checkbox("營業現金流 > 0（排除假獲利）", value=True, key="sb_req_cf")
            min_roe = st.number_input(
                "最低 ROE (%)　（0 = 不限）", min_value=0, max_value=50, value=0, step=5,
                key="sb_min_roe", help="巴菲特標準 ≥ 15%，一般優質公司 ≥ 10%",
            )
            max_debt = st.number_input(
                "負債比上限 (%)　（0 = 不限）", min_value=0, max_value=100, value=0, step=10,
                key="sb_max_debt", help="一般產業 < 60%，金融業本身高負債屬正常可設 0",
            )
            fundamental_filter = {
                "require_eps_positive": req_eps,
                "require_positive_cf":  req_cf,
                "min_roe":              float(min_roe),
                "max_debt_ratio":       float(max_debt),
            }
            _fund_mode_label = get_fundamentals_mode()
            st.caption(f"目前基本面模式：`{_fund_mode_label}`（off/warn/penalty/exclude 由 config.toml 控制）")
            st.caption("💡 首次啟用時需從 API 抓財報資料，建議先讓背景工作器預先填充")

    st.markdown("---")

    # ── 儲存自訂模式 ───────────────────────────────────────────
    if st.button("💾 儲存為自訂模式", use_container_width=True,
                 help="將目前所有設定儲存為「⚙️ 自訂」模式，下次可從掃描模式直接套用"):
        _to_save = {k: st.session_state.get(k) for k in _PRESET_KEYS}
        _to_save = {k: v for k, v in _to_save.items() if v is not None}
        _set_cpreset(_to_save)
        st.success("✅ 已儲存！下次選「⚙️ 自訂」即可套用")
        st.rerun()

    st.caption("全市場掃描約需 30-50 分鐘")


# ── 主頁面 ───────────────────────────────────────────────────
st.title("🔍 選股雷達")
st.markdown("掃描全市場，依技術強度找出值得關注的標的")

if is_market_closed():
    st.info(
        "🏖️ **休市模式啟用中** — 法人資料使用本機快取，不消耗 API 額度。  \n"
        "恢復交易後請至「資料管理」頁面關閉此模式。",
        icon=None,
    )

st.markdown("---")

tab_scan, tab_funnel, tab_sector, tab_chart, tab_history = st.tabs(
    ["📊 掃描結果", "🔬 篩選漏斗", "🏭 產業族群", "📈 個股圖表", "📋 歷史紀錄"]
)


# ══ Tab：掃描結果 ════════════════════════════════════════════
with tab_scan:
    # ── 今日資料就緒提示（歷史日期不顯示）─────────────────────────
    _today_core_pct      = 1.0
    _today_supp_ready    = True
    if not _is_historical:
        try:
            from db.price_cache import get_delisted_stocks as _get_del, get_known_stock_ids as _get_known
            from db.database import get_session as _get_sess
            from sqlalchemy import text as _sql_text
            _today_str = _date_cls.today().strftime("%Y-%m-%d")
            with _get_sess() as _sess:
                _core_done_n = _sess.execute(
                    _sql_text("SELECT COUNT(DISTINCT stock_id) FROM price_cache WHERE date = :d"),
                    {"d": _today_str}
                ).fetchone()[0]
                _inst_done_n = _sess.execute(
                    _sql_text("SELECT COUNT(DISTINCT stock_id) FROM inst_cache WHERE date = :d"),
                    {"d": _today_str}
                ).fetchone()[0]
            _skip_n    = set(_get_del(include_legacy_no_update=True))
            _known_n   = _get_known()
            _active_n  = max(len(set(_known_n) - _skip_n), 1)
            _today_core_pct   = _core_done_n / _active_n
            _today_supp_ready = (_inst_done_n / _active_n) >= 0.9
        except Exception:
            pass

        if _today_core_pct < 1.0:
            st.info(
                f"今日價格資料仍在更新中（{_today_core_pct*100:.0f}%），"
                "掃描結果可能不完整。建議等資料補齊後再掃描，或使用昨日資料。"
            )
        if not _today_supp_ready:
            st.info(
                "法人 / 融資資料尚未就緒，掃描時相關加分條件將自動停用。"
            )

    clicked_scan = st.button("🚀 開始掃描", type="primary", use_container_width=True)

    if clicked_scan:
        # 歷史日期：資料已在 DB，直接走 cache-only 不需要確認
        if _is_historical:
            st.session_state["scan_cache_only"] = True
            st.session_state["scan_date"] = scan_date
            st.session_state["do_scan"] = True
        else:
            from db.price_cache import get_cache_summary as _get_cache_summary
            from datetime import datetime as _dt, date as _date
            _now = _dt.now()
            _today = _date.today()
            _after_close = _now.hour >= 15
            _summary = _get_cache_summary()
            if not _summary.empty:
                _max_date_str = _summary["latest"].max()
                _max_date = _date.fromisoformat(str(_max_date_str))
                _already_updated = _after_close and _max_date >= _today
            else:
                _already_updated = False
                _max_date = None

            if _already_updated:
                st.session_state["scan_date"] = scan_date
                st.session_state["do_scan"] = True
            else:
                st.session_state["scan_pending"] = True
                st.session_state["scan_cache_max"] = str(_max_date) if _max_date else "無資料"

    # 確認對話框
    if st.session_state.get("scan_pending"):
        _max_disp = st.session_state.get("scan_cache_max", "無資料")
        st.warning(f"⚠️ 資料庫最新報價日期：**{_max_disp}**，與今日不同。")
        _c_ok, _c_cache, _c_cancel = st.columns(3)
        with _c_ok:
            if st.button("🌐 更新後掃描", type="primary", use_container_width=True):
                st.session_state["scan_pending"] = False
                st.session_state["scan_cache_only"] = False
                st.session_state["scan_date"] = scan_date
                st.session_state["do_scan"] = True
                st.rerun()
        with _c_cache:
            if st.button("📦 直接用資料庫掃描", use_container_width=True):
                st.session_state["scan_pending"] = False
                st.session_state["scan_cache_only"] = True
                st.session_state["scan_date"] = scan_date
                st.session_state["do_scan"] = True
                st.rerun()
        with _c_cancel:
            if st.button("❌ 取消", use_container_width=True):
                st.session_state["scan_pending"] = False
                st.rerun()

    if st.session_state.pop("do_scan", False):
        _cache_only_mode = st.session_state.pop("scan_cache_only", False)
        _active_scan_date = st.session_state.get("scan_date", _date_cls.today())
        _is_hist_scan = _active_scan_date < _date_cls.today()
        # 歷史日期 OR 今日附加資料未就緒 → 停用法人/融資加分條件
        _disable_non_price_extras = _is_hist_scan or (not _is_hist_scan and not _today_supp_ready)
        # 建立資料來源管理器（每次掃描重新初始化，確保從 FinMind 優先）
        dsm = DataSourceManager()

        with st.spinner("載入股票清單..."):
            try:
                stock_list = dsm.get_stock_list()
            except Exception as e:
                st.error(f"無法取得股票清單：{e}")
                st.stop()

        if stock_list.empty:
            st.warning("股票清單為空，請確認 API Token")
            st.stop()

        if scan_mode.startswith("快速"):
            sample_ids = stock_list["stock_id"].head(20).tolist()
        elif scan_mode.startswith("小型"):
            sample_ids = stock_list["stock_id"].head(100).tolist()
        else:
            sample_ids = stock_list["stock_id"].tolist()

        total = len(sample_ids)
        st.info(f"準備掃描 **{total}** 檔股票...")

        prog = st.progress(0)
        status_txt = st.empty()
        fallback_banner = st.empty()
        price_data, inst_data, margin_data = {}, {}, {}
        api_calls, cache_hits = 0, 0

        for i, sid in enumerate(sample_ids):
            prog.progress((i + 1) / total)
            try:
                if _cache_only_mode:
                    # 純資料庫模式：直接讀快取，完全不呼叫 API
                    from db.price_cache import load_prices as _load_prices
                    df = _load_prices(sid)
                    if not df.empty:
                        cache_hits += 1
                        status_txt.text(f"資料庫：{sid}（{i+1}/{total}）")
                        if _is_hist_scan and "date" in df.columns:
                            df = df[df["date"] <= pd.Timestamp(_active_scan_date)]
                        price_data[sid] = df
                    # 歷史掃描：從 inst_cache 讀取截至掃描日的法人資料
                    if _is_hist_scan:
                        from db.inst_cache import load_institutional_for_date
                        idf = load_institutional_for_date(sid, _active_scan_date, days=14)
                        if not idf.empty:
                            inst_data[sid] = summarize_institutional_signal(
                                idf,
                                selected_institutions=inst_selection or None,
                                strict_days=strict_days,
                                agg_mode=agg_mode,
                                agg_days=agg_days,
                            )
                else:
                    from db.price_cache import get_cached_dates
                    from datetime import date as _date, timedelta as _td
                    _min, _max = get_cached_dates(sid)
                    _today = _date.today()
                    _wd = _today.weekday()
                    _latest_td = (_today - _td(days=1) if _wd == 5 else
                                  _today - _td(days=2) if _wd == 6 else _today)
                    _max_d = (_max if isinstance(_max, _date)
                              else _date.fromisoformat(str(_max))) if _max else None
                    _fresh = _max_d is not None and _max_d >= _latest_td
                    if _fresh:
                        cache_hits += 1
                        status_txt.text(f"快取：{sid}（{i+1}/{total}）")
                    else:
                        api_calls += 1
                        src = "備援" if dsm.fallback_mode else "下載"
                        status_txt.text(f"{src}：{sid}（{i+1}/{total}）｜快取 {cache_hits} / API {api_calls}")

                    df = dsm.get_price(sid, required_days=150)

                    # 切換備援後顯示警告橫幅
                    if dsm.fallback_mode:
                        fallback_banner.warning(FALLBACK_WARNING)

                    if not df.empty:
                        if _is_hist_scan and "date" in df.columns:
                            df = df[df["date"] <= pd.Timestamp(_active_scan_date)]
                        price_data[sid] = df

                    # 法人資料
                    if not _disable_non_price_extras and dsm.institutional_available:
                        # 當日掃描：呼叫 live API
                        idf = dsm.get_institutional(sid, days=10)
                        if not idf.empty:
                            inst_data[sid] = summarize_institutional_signal(
                                idf,
                                selected_institutions=inst_selection or None,
                                strict_days=strict_days,
                                agg_mode=agg_mode,
                                agg_days=agg_days,
                            )
                    elif _is_hist_scan:
                        # 歷史掃描：從 inst_cache 讀取截至掃描日的資料
                        from db.inst_cache import load_institutional_for_date
                        idf = load_institutional_for_date(sid, _active_scan_date, days=14)
                        if not idf.empty:
                            inst_data[sid] = summarize_institutional_signal(
                                idf,
                                selected_institutions=inst_selection or None,
                                strict_days=strict_days,
                                agg_mode=agg_mode,
                                agg_days=agg_days,
                            )

                    # 融資餘額
                    if not _disable_non_price_extras and include_margin and not dsm.fallback_mode:
                        from data.finmind_client import get_margin_trading, compute_margin_trend
                        mdf = get_margin_trading(sid, days=5)
                        trend, _, _ = compute_margin_trend(mdf)
                        if trend != "flat":
                            margin_data[sid] = trend

                    if not _fresh and not dsm.fallback_mode:
                        time.sleep(0.05)   # 只有真正呼叫 FinMind API 時才需要 rate limit
            except Exception:
                pass

        if _cache_only_mode:
            st.info(f"資料取得完成（資料庫模式）：共讀取 **{cache_hits}** 檔快取資料")
        else:
            src_label = "yfinance 備援" if dsm.fallback_mode else "FinMind"
            st.info(f"資料取得完成（{src_label}）：快取命中 **{cache_hits}** 檔 / API 呼叫 **{api_calls}** 次")

        prog.empty()
        status_txt.empty()

        # ── 基本面資料預載（快取優先，首次可能需要 API）──────────
        fund_data: dict = {}
        fundamental_mode = get_fundamentals_mode()
        fundamental_enabled_for_scan = (
            use_fundamental
            and bool(fundamental_filter)
            and not _disable_non_price_extras
            and fundamental_mode != "off"
        )
        if use_fundamental and fundamental_mode == "off":
            st.info("基本面模式目前為 off，本次掃描跳過基本面條件。")
        if fundamental_enabled_for_scan:
            _can_fetch_fund, _fund_reason = can_fetch_premium_fundamentals()
            if not _can_fetch_fund:
                st.warning(
                    "基本面資料需要 FinMind Premium。"
                    f"目前狀態：{_fund_reason}。本次掃描會整批跳過基本面條件，"
                    "不會逐檔呼叫財報 API。"
                )
                fundamental_enabled_for_scan = False

        if fundamental_enabled_for_scan:
            from data.finmind_client import smart_get_fundamentals
            fund_prog = st.progress(0, text="載入基本面資料（讀取快取）...")
            fund_api, fund_cache = 0, 0
            for i, sid in enumerate(sample_ids):
                fund_prog.progress((i + 1) / total,
                                   text=f"基本面資料：{sid}（{i+1}/{total}）")
                from db.fundamental_cache import is_fundamental_fresh
                is_fresh = is_fundamental_fresh(sid)
                if not is_fresh:
                    fund_api += 1
                else:
                    fund_cache += 1
                metrics = smart_get_fundamentals(sid)
                if metrics:
                    fund_data[sid] = metrics
            fund_prog.empty()

            if fund_api > 0 and not fund_data:
                # 所有 API 呼叫均無資料，很可能是 402 付費牆
                st.warning(
                    "⚠️ **基本面資料無法取得（402 Payment Required）**  \n"
                    "FinMind 財報端點（`TaiwanStockFinancialStatements`）需要付費方案，"
                    "免費帳號無法使用。基本面篩選條件本次**自動停用**，選股結果不受影響。  \n"
                    "若需基本面過濾，請升級 FinMind 方案後重試。"
                )
                # 強制清空，讓 run_scan 跳過基本面過濾
                fund_data = {}
            else:
                st.caption(
                    f"基本面資料：快取 **{fund_cache}** 檔 / API **{fund_api}** 次"
                )
        else:
            fundamental_filter = {}

        with st.spinner("計算指標，篩選中..."):
            use_inst = bool(inst_data) or (
                dsm.institutional_available and not _disable_non_price_extras
            )
            if not use_inst:
                st.warning(
                    "目前無法取得法人資料；v3/v4 都需要「主力連續 3 日買超」作為必要條件，"
                    "本次掃描可能不會產生入選結果。"
                )
            elif _is_hist_scan and inst_data:
                st.caption(f"歷史掃描：從本機快取讀取 {len(inst_data)} 檔法人資料（截至 {_active_scan_date}）")
            result_df, sector_info, debug_info = run_scan(
                price_data=price_data,
                stock_info=stock_list,
                inst_data=inst_data if use_inst else {},
                margin_data=margin_data if not _disable_non_price_extras else {},
                fundamental_data=fund_data if use_fundamental and not _disable_non_price_extras else {},
                fundamental_filter=fundamental_filter if use_fundamental and not _disable_non_price_extras else {},
                min_price=min_price,
                min_avg_volume=min_avg_volume,
                top_volume_n=top_volume_n,
                top_sector_n=top_sector_n,
                use_hp_density=use_hp_density,
                hp_density_lookback=hp_density_lookback,
                hp_density_threshold=hp_density_threshold,
                use_turnover_ratio=use_turnover_ratio,
                turnover_top_n=turnover_top_n,
                overheat_atr_mult=float(overheat_atr_mult),
                overheat_action=overheat_action,
                ma_breakout_mode=ma_breakout_mode,
                strategy_version=strategy_version,
                fundamental_mode=fundamental_mode,
                debug=True,
            )
            st.session_state["debug_info"] = debug_info
            st.session_state["sector_info"] = sector_info
            st.session_state["sector_breakout_enabled"] = (use_hp_density or use_turnover_ratio)
            st.session_state["scan_strategy_version"] = strategy_version

        # 顯示產業輪動過濾結果
        if sector_info and top_sector_n > 0:
            top_inds = sorted(sector_info, key=lambda x: sector_info[x]["return_pct"], reverse=True)[:top_sector_n]
            ind_tags = "　".join(
                [f"**{ind}** ({sector_info[ind]['return_pct']:+.1f}%)" for ind in top_inds]
            )
            st.info(f"📊 本次鎖定產業（近 5 日漲幅前 {top_sector_n} 名）：{ind_tags}")

        # 套用 v2 進階過濾
        if not result_df.empty:
            if require_weekly:
                result_df = result_df[result_df["signals"].str.contains("週線多頭", na=False)]
            if min_rs > 0:
                result_df = result_df[result_df["rs_score"] >= min_rs]
            if require_institutional and use_inst:
                before = len(result_df)
                result_df = result_df[result_df["inst_pass"]]
                filtered = before - len(result_df)
                inst_label = (
                    f"{'、'.join(inst_selection)} 各自連續 {strict_days} 日買超"
                    if inst_mode == "個別法人皆須買超"
                    else (
                        f"三大法人近 {agg_days} 日累計合計買超"
                        if agg_mode == 'rolling_sum'
                        else f"三大法人連續 {agg_days} 日每日合計買超"
                    )
                )
                if filtered > 0:
                    st.info(f"法人必要條件（{inst_label}）：已過濾 **{filtered}** 檔未符合")

        result_df = _attach_cached_risk_flags(result_df, _active_scan_date)

        if result_df.empty:
            st.warning("沒有符合條件的股票，可嘗試調整篩選條件")
        else:
            st.success(f"找到 **{len(result_df)}** 檔符合條件的股票")
            st.session_state["scan_results"] = result_df
            st.session_state["price_data"] = price_data
            st.session_state["result_scan_date"] = _active_scan_date

            # ── 自動儲存掃描歷史 ──────────────────────────────
            sector_filter_str = ""
            if sector_info and top_sector_n > 0:
                top_inds = sorted(sector_info, key=lambda x: sector_info[x]["return_pct"], reverse=True)[:top_sector_n]
                sector_filter_str = "、".join(
                    [f"{ind}({sector_info[ind]['return_pct']:+.1f}%)" for ind in top_inds]
                )

            vol_filter_str = (
                f"前日量前{top_volume_n}名" if top_volume_n > 0
                else f"日均量≥{min_avg_volume}張" if min_avg_volume > 0
                else "不過濾"
            )

            try:
                save_scan_session(
                    result_df=result_df,
                    scan_mode=scan_mode,
                    min_price=min_price,
                    vol_filter=vol_filter_str,
                    sector_filter=sector_filter_str,
                    require_weekly=require_weekly,
                    min_rs=float(min_rs),
                    include_institutional=use_inst,
                    top_sectors=sector_info,
                )
            except Exception as _save_err:
                st.warning(f"⚠️ 掃描結果未能儲存至歷史紀錄：{_save_err}")

    # ── 顯示結果 ──────────────────────────────────────────────
    if "scan_results" in st.session_state:
        result_df = _ensure_premium_score_columns(st.session_state["scan_results"])
        st.session_state["scan_results"] = result_df

        if "scan_results" not in st.session_state or not st.session_state.get("_just_scanned"):
            st.info(f"上次掃描結果（{len(result_df)} 檔）")

        # 分數 > 100 的標記
        has_elite = (result_df["score"] > 100).any()
        if has_elite:
            n_elite = (result_df["score"] > 100).sum()
            st.success(f"⭐ 其中 **{n_elite}** 檔為「精選強勢股」（分數 > 100，週線 + RS 雙重確認），列於下表最前方")

        display_df = result_df.copy()

        # 合併「差多少%」+「多少元」為單一易讀欄位
        display_df["距過熱"] = display_df.apply(_format_heat_distance, axis=1)
        display_df = display_df.drop(columns=["heat_room_pct", "heat_room_abs"], errors="ignore")

        display_df = display_df.rename(columns={
            "stock_id": "代碼", "stock_name": "名稱", "industry": "產業",
            "close": "收盤", "change_pct": "漲跌%", "volume_ratio": "量比",
            "score": "強度分數", "rs_score": "RS分數", "bias_ratio": "乖離率(%)",
            "volatility_pct": "日均振幅%", "premium_flags": "Premium旗標",
            "fundamental_penalty": "基本面扣分(未套用)",
            "fundamental_flags": "基本面旗標",
            "fundamental_missing_fields": "基本面缺資料",
            "risk_penalty": "風險扣分(未套用)", "signals": "觸發條件",
        })
        display_df.index = range(1, len(display_df) + 1)

        if "premium_flags" in result_df.columns and result_df["premium_flags"].astype(str).str.len().gt(0).any():
            st.caption("Premium 風險旗標目前只顯示，不影響 v3/v4 必要條件、分數與排序。")
        if "fundamental_flags" in result_df.columns and result_df["fundamental_flags"].astype(str).str.len().gt(0).any():
            st.caption("基本面 penalty / flags 目前只顯示，不影響 v3/v4 必要條件與排序；exclude 模式除外。")

        st.dataframe(display_df, use_container_width=True, height=500)

        # ── 後續盤勢（歷史回溯模式才顯示）──────────────────────
        _result_scan_date = st.session_state.get("result_scan_date")
        if _result_scan_date and _result_scan_date < _date_cls.today():
            st.markdown("---")
            st.subheader(f"📅 後續盤勢  ·  基準日：{_result_scan_date}")
            st.caption("以基準日收盤價為基礎，計算後續各日的報酬率與最高漲幅")

            _day_offsets = [1, 3, 5, 10]
            _perf_rows = []

            from db.price_cache import load_prices as _lp_hist
            for _, row in result_df.iterrows():
                sid = row["stock_id"]
                full_df = _lp_hist(sid)
                if full_df.empty or "date" not in full_df.columns:
                    continue
                full_df = full_df.sort_values("date").reset_index(drop=True)
                full_df["date"] = pd.to_datetime(full_df["date"])

                # 找基準日（若非交易日則取最近前一個交易日）
                base_rows = full_df[full_df["date"] <= pd.Timestamp(_result_scan_date)]
                if base_rows.empty:
                    continue
                base_idx = base_rows.index[-1]
                base_close = full_df.loc[base_idx, "close"]
                if not base_close or base_close == 0:
                    continue

                perf = {
                    "代碼": sid,
                    "名稱": row.get("stock_name", ""),
                    "基準收盤": round(base_close, 1),
                }
                future = full_df.iloc[base_idx + 1:]
                for days in _day_offsets:
                    if len(future) >= days:
                        target_close = future.iloc[days - 1]["close"]
                        ret = (target_close - base_close) / base_close * 100
                        perf[f"+{days}日%"] = round(ret, 1)
                        period_high = future.iloc[:days]["close"].max()
                        max_ret = (period_high - base_close) / base_close * 100
                        perf[f"+{days}日最高%"] = round(max_ret, 1)
                    else:
                        perf[f"+{days}日%"] = None
                        perf[f"+{days}日最高%"] = None
                _perf_rows.append(perf)

            if _perf_rows:
                _perf_df = pd.DataFrame(_perf_rows)

                def _color_ret(val):
                    if val is None or not isinstance(val, (int, float)):
                        return ""
                    color = "#e74c3c" if val > 0 else ("#27ae60" if val < 0 else "")
                    return f"color: {color}" if color else ""

                ret_cols = [c for c in _perf_df.columns if c.endswith("%")]
                styled = _perf_df.style.applymap(_color_ret, subset=ret_cols)
                st.dataframe(styled, use_container_width=True, hide_index=True)

                # 勝率小結
                _plus_cols = [c for c in ret_cols if "最高" not in c]
                for dc in _plus_cols:
                    vals = _perf_df[dc].dropna()
                    if len(vals) == 0:
                        continue
                    win = (vals > 0).sum()
                    avg = vals.mean()
                    st.caption(
                        f"{dc}：勝率 **{win}/{len(vals)}**（{win/len(vals)*100:.0f}%）　"
                        f"平均報酬 **{avg:+.1f}%**"
                    )
            else:
                st.info("資料庫中尚無基準日之後的價格資料，無法計算後續盤勢。")

    else:
        if inst_mode == "個別法人皆須買超":
            _inst_label = "、".join(inst_selection) if inst_selection else "三大法人"
            _inst_desc = f"{_inst_label}各自連續 {strict_days} 日買超"
            _inst_score = "+7"
            _inst_note = "保留原本嚴格邏輯"
        else:
            _inst_desc = (
                f"三大法人近 {agg_days} 日累計合計買超"
                if agg_mode == "rolling_sum"
                else f"三大法人連續 {agg_days} 日每日合計買超"
            )
            _inst_score = "+4 / +7"
            _inst_note = "合計買超 +4；若外資與投信同步買超再 +3"
        _inst_type = "**必要**（額外強制過濾）" if (include_institutional and require_institutional and inst_selection) else "加分"
        _strategy_label = "v4 領先攻擊版" if strategy_version == "v4" else "v3 均線突破版"
        if strategy_version == "v4":
            st.markdown(f"""
        #### 篩選條件（{_strategy_label}）

        | 條件 | 類型 | 分數 | 說明 |
        |------|------|------|------|
        | 第一天站上 5/10/20MA | 必要 | +35 | 今日三線齊穿，昨日全在線下 |
        | 均線糾結度 < 3%（昨日）| 必要 | +20 | 突破前一天三線緊貼，確認盤整後突破 |
        | 量能 > 前五日均量 × 1.5 倍 | 必要 | +15 | 突破時量能明顯超越近期均量，確認有效性 |
        | 股價 < MA20 + 3.5 × ATR(14) | 必要 | +10 | 動態門檻排除過熱股，較固定乖離率更合理 |
        | 相對強度 RS > 80 | 必要 | +10 | 個股明顯領先大盤，確認為真正強勢股 |
        | 突破 60 日收盤新高 | 必要 | +10 | 確認為真實突破，而非盤整區間反彈 |
        | 主力連續 3 日買超 | 必要 | +10 | 三大法人合計淨買超連續 3 個交易日為正 |
        | 布林頻寬縮減（vs 20日前） | 加分 | +10 | 盤整極致後的變盤第一天 |
        | 投信第一天買超 | 加分 | +10 | 法人資金剛開始表態的訊號 |
        | 週線 MA10 扣抵值低位 | 加分 | +10 | 10週前收盤 < 週MA10，週趨勢即將轉強 |
        | {_inst_desc} | {_inst_type} | {_inst_score} | {_inst_note} |
        | 融資減少 / 籌碼集中 | 加分 | +5 | 散戶下車、主力上車 |
        """)
        else:
            st.markdown(f"""
        #### 篩選條件（{_strategy_label}）

        | 條件 | 類型 | 分數 | 說明 |
        |------|------|------|------|
        | 站上 MA20 | 必要 | +20 | 今日收盤高於 20 日均線 |
        | MA20 向上 | 必要 | +15 | 近 5 日 MA20 持續上升 |
        | 量增（前五日均量 × 1.3）| 必要 | +20 | 成交量超越近期均量，確認有效性 |
        | 布林正常（不低於下軌）| 必要 | +10 | 收盤不破布林下軌，避免弱勢股 |
        | MACD 黃金交叉 | 必要（擇一）| +15 | DIF 上穿 DEA，或 MACD 在零軸上方 |
        | RSI 健康區（50–70）| 必要（擇一）| +10 | RSI 在多頭健康區間，尚未超買 |
        | 主力連續 3 日買超 | 必要 | +10 | 三大法人合計淨買超連續 3 個交易日為正 |
        | {_inst_desc} | {_inst_type} | {_inst_score} | {_inst_note} |
        | 週線多頭 | 加分 | +10 | 週MA10 向上且收盤站上 |
        | 相對強勢 RS > 70 | 加分 | +8 | 個股表現領先大盤 |
        | 多頭排列 MA5 > MA10 > MA20 | 加分 | +5 | 短中長均線同向向上 |
        | 量能優質（上漲量佔比 ≥ 60%）| 加分 | +7 | 近 10 日漲日成交量比重高 |
        | 突破 60 日收盤新高 | 加分 | +8 | 突破近期壓力區 |
        | 融資減少 / 籌碼集中 | 加分 | +3 | 散戶下車、主力上車 |
        """)


# ══ Tab：篩選漏斗 ════════════════════════════════════════════
with tab_funnel:
    st.subheader("🔬 篩選漏斗分析")
    st.caption("顯示每個條件篩掉了多少股票，幫助你了解條件的嚴格程度與調整方向")

    if "debug_info" not in st.session_state or not st.session_state["debug_info"]:
        st.info("請先執行掃描，結果會自動顯示在這裡")
    else:
        dbg = st.session_state["debug_info"]
        analysis: dict = dbg.get("stock_analysis", {})
        _scan_sv = st.session_state.get("scan_strategy_version", "v4")

        # ── 建立漏斗資料 ────────────────────────────────────────
        # 前置過濾階段
        funnel_rows = list(dbg.get("pre_filter_stages", []))

        # 進入條件計算的股票（通過前置過濾的）
        cond_stocks = [
            v for v in analysis.values()
            if v["exclude_pre"] is None and v["sig"] is not None
        ]
        n_cond = len(cond_stocks)
        funnel_rows.append({"stage": "進入條件計算", "count": n_cond})

        # 逐條件累積計數（每個條件都是在前一條件基礎上累積）
        if _scan_sv == "v3":
            MANDATORY = [
                ("站上 MA20",            lambda s: s.above_ma20),
                ("MA20 向上",            lambda s: s.above_ma20 and s.ma20_rising),
                ("量增（均量 1.3 倍）",   lambda s: s.above_ma20 and s.ma20_rising and s.volume_surge),
                ("布林正常",             lambda s: s.above_ma20 and s.ma20_rising and s.volume_surge and s.above_bb_lower),
                ("MACD 或 RSI",          lambda s: s.above_ma20 and s.ma20_rising and s.volume_surge and s.above_bb_lower and (s.macd_cross or s.rsi_healthy)),
                ("主力連 3 日買超",       lambda s: s.passes_basic_v3()),
            ]
        else:
            MANDATORY = [
                ("三線齊穿（首日）",   lambda s: s.ma_triple_breakout),
                ("均線糾結 < 3%",     lambda s: s.ma_triple_breakout and s.ma_squeeze),
                ("量能 > 均量 1.5 倍", lambda s: s.ma_triple_breakout and s.ma_squeeze and s.volume_explosion),
                ("股價 < MA20+3.5ATR", lambda s: s.ma_triple_breakout and s.ma_squeeze and s.volume_explosion and s.atr_ok),
                ("RS > 80",            lambda s: s.ma_triple_breakout and s.ma_squeeze and s.volume_explosion and s.atr_ok and s.rs_strong),
                ("突破 60 日新高",      lambda s: s.ma_triple_breakout and s.ma_squeeze and s.volume_explosion and s.atr_ok and s.rs_strong and s.breakout_60d),
                ("主力連 3 日買超",     lambda s: s.passes_basic()),
            ]
        for label, fn in MANDATORY:
            cnt = sum(1 for v in cond_stocks if fn(v["sig"]))
            funnel_rows.append({"stage": label, "count": cnt})

        funnel_df = pd.DataFrame(funnel_rows)

        # ── 漏斗長條圖 ──────────────────────────────────────────
        fig_funnel = go.Figure(go.Bar(
            x=funnel_df["count"],
            y=funnel_df["stage"],
            orientation="h",
            text=funnel_df["count"],
            textposition="outside",
            marker_color=[
                "#3498db" if i < len(dbg.get("pre_filter_stages", [])) + 1
                else "#e74c3c" if i == len(funnel_df) - 1
                else "#f39c12"
                for i in range(len(funnel_df))
            ],
        ))
        fig_funnel.update_layout(
            height=max(300, len(funnel_df) * 45),
            template="plotly_dark",
            title="各階段存活股票數量",
            xaxis_title="股票數",
            margin=dict(t=40, b=10, l=160),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig_funnel, use_container_width=True)

        # ── 每個條件的篩選明細（可展開查看通過名單）──────────────
        st.markdown("#### 各必要條件篩選明細")
        prev_count = n_cond
        for label, fn in MANDATORY:
            passed_items = [
                (sid, v) for sid, v in analysis.items()
                if v["exclude_pre"] is None and v["sig"] is not None and fn(v["sig"])
            ]
            cnt = len(passed_items)
            excl = prev_count - cnt
            excl_rate = f"{excl/prev_count*100:.1f}%" if prev_count > 0 else "—"

            col_lbl, col_pass, col_excl, col_rate = st.columns([3, 1, 1, 1])
            col_lbl.markdown(f"**{label}**")
            col_pass.metric("通過", cnt)
            col_excl.metric("排除", excl)
            col_rate.metric("排除率", excl_rate)

            with st.expander(f"查看「{label}」通過的 {cnt} 檔"):
                if passed_items:
                    pass_df = pd.DataFrame([
                        {
                            "代碼": sid,
                            "名稱": v.get("stock_name", ""),
                            "產業": v.get("industry", ""),
                            "收盤": v.get("close", ""),
                        }
                        for sid, v in sorted(passed_items, key=lambda x: x[0])
                    ])
                    st.dataframe(pass_df, use_container_width=True, hide_index=True)
                else:
                    st.info("無符合條件的股票")

            prev_count = cnt

        # ── 加分條件命中率（在最終入選股中）────────────────────
        if _scan_sv == "v3":
            final_sigs = [v["sig"] for v in cond_stocks if v["sig"] and v["sig"].passes_basic_v3()]
        else:
            final_sigs = [v["sig"] for v in cond_stocks if v["sig"] and v["sig"].passes_basic()]
        if final_sigs:
            st.markdown("#### 加分條件命中率（最終入選股）")
            if _scan_sv == "v3":
                ADDITIVE = [
                    ("法人買超",            lambda s: s.institutional_buy),
                    ("週線多頭",            lambda s: s.weekly_trend_up),
                    ("相對強勢 RS > 70",    lambda s: s.rs_positive),
                    ("多頭排列",            lambda s: s.ma_aligned),
                    ("量能優質",            lambda s: s.vol_quality),
                    ("突破 60 日新高",      lambda s: s.breakout),
                    ("籌碼乾淨",            lambda s: s.margin_clean),
                ]
            else:
                ADDITIVE = [
                    ("布林頻寬縮減",        lambda s: s.bb_bandwidth_shrink),
                    ("投信首日買超",        lambda s: s.trust_first_buy),
                    ("週線扣抵低位",        lambda s: s.weekly_deduction_low),
                    ("相對強勢 RS > 70",    lambda s: s.rs_positive),
                    ("籌碼乾淨",            lambda s: s.margin_clean),
                ]
            add_rows = []
            n_final = len(final_sigs)
            for label, fn in ADDITIVE:
                cnt = sum(1 for s in final_sigs if fn(s))
                add_rows.append({
                    "條件": label,
                    "命中": cnt,
                    "命中率": f"{cnt/n_final*100:.1f}%",
                })
            st.dataframe(pd.DataFrame(add_rows), use_container_width=True, hide_index=True)

        # ── 差一條件入選（Near-miss）────────────────────────────
        st.markdown("#### 差一條件入選的股票")
        st.caption("只差一個必要條件就會入選，可作為條件調整的參考")

        if _scan_sv == "v3":
            MANDATORY_CHECKS = [
                ("站上 MA20",          lambda s: s.above_ma20),
                ("MA20 向上",          lambda s: s.ma20_rising),
                ("量增（均量 1.3 倍）", lambda s: s.volume_surge),
                ("布林正常",           lambda s: s.above_bb_lower),
                ("MACD 或 RSI",        lambda s: s.macd_cross or s.rsi_healthy),
                ("主力連 3 日買超",     lambda s: s.main_force_buy_3d),
            ]
        else:
            MANDATORY_CHECKS = [
                ("三線齊穿（首日）",   lambda s: s.ma_triple_breakout),
                ("均線糾結 < 3%",     lambda s: s.ma_squeeze),
                ("量能 > 均量 1.5 倍", lambda s: s.volume_explosion),
                ("股價 < MA20+3.5ATR", lambda s: s.atr_ok),
                ("RS > 80",            lambda s: s.rs_strong),
                ("突破 60 日新高",      lambda s: s.breakout_60d),
                ("主力連 3 日買超",     lambda s: s.main_force_buy_3d),
            ]
        near_miss = []
        for sid, v in analysis.items():
            if v["exclude_pre"] is not None or v["sig"] is None:
                continue
            sig = v["sig"]
            if (sig.passes_basic_v3() if _scan_sv == "v3" else sig.passes_basic()):
                continue  # 已入選
            failed = [lbl for lbl, fn in MANDATORY_CHECKS if not fn(sig)]
            if len(failed) == 1:
                near_miss.append({
                    "代碼": sid,
                    "名稱": v["stock_name"],
                    "產業": v["industry"],
                    "收盤": v["close"],
                    "差的條件": failed[0],
                    "已通過": "、".join(lbl for lbl, fn in MANDATORY_CHECKS if fn(sig)),
                })

        if near_miss:
            near_df = pd.DataFrame(near_miss).sort_values("差的條件")
            # 按差的條件分組顯示
            for cond_name, grp in near_df.groupby("差的條件"):
                with st.expander(f"差「{cond_name}」— {len(grp)} 檔", expanded=True):
                    st.dataframe(
                        grp[["代碼", "名稱", "產業", "收盤", "已通過"]].reset_index(drop=True),
                        use_container_width=True, hide_index=True,
                    )
        else:
            st.info("沒有差一條件入選的股票")

        # ── 前置過濾排除原因分佈 ────────────────────────────────
        pre_excl = [v["exclude_pre"] for v in analysis.values() if v["exclude_pre"]]
        if pre_excl:
            with st.expander("前置過濾排除原因分佈", expanded=False):
                from collections import Counter
                # 只取原因大類（去掉具體數字）
                def _reason_category(reason: str) -> str:
                    if "股價" in reason:
                        return "股價低於門檻"
                    if "日均量" in reason:
                        return "日均量不足"
                    if "資料不足" in reason:
                        return "資料不足"
                    return reason
                cats = Counter(_reason_category(r) for r in pre_excl)
                excl_pre_df = pd.DataFrame(
                    [{"原因": k, "排除檔數": v} for k, v in cats.most_common()]
                )
                st.dataframe(excl_pre_df, use_container_width=True, hide_index=True)


# ══ Tab：產業族群 ════════════════════════════════════════════
with tab_sector:
    st.subheader("🏭 產業族群分析")
    st.markdown("哪些產業目前有最多強勢股出現？資金往往集中在特定族群。")

    if "scan_results" not in st.session_state:
        st.info("請先執行掃描")
    else:
        result_df = st.session_state["scan_results"]
        sector_df = sector_analysis(result_df)

        if sector_df.empty:
            st.info("無足夠的產業資料")
        else:
            col_bar, col_table = st.columns([3, 2])

            with col_bar:
                top_sectors = sector_df.head(10)
                colors = [
                    "#e74c3c" if c >= 3 else "#f39c12" if c == 2 else "#3498db"
                    for c in top_sectors["count"]
                ]
                fig_s = go.Figure(go.Bar(
                    x=top_sectors["count"],
                    y=top_sectors["industry"],
                    orientation="h",
                    marker_color=colors,
                    text=[f"{c} 檔  均分{s}" for c, s in
                          zip(top_sectors["count"], top_sectors["avg_score"])],
                    textposition="outside",
                ))
                fig_s.update_layout(
                    height=380, template="plotly_dark",
                    title="入選強勢股數量（依產業）",
                    xaxis_title="入選檔數",
                    margin=dict(t=40, b=10, l=120),
                )
                st.plotly_chart(fig_s, use_container_width=True)

            with col_table:
                st.markdown("#### 各族群代表股")
                display_sector = sector_df.rename(columns={
                    "industry": "產業", "count": "入選檔數",
                    "avg_score": "平均分數", "top_stock": "最強個股",
                })
                st.dataframe(display_sector[["產業", "入選檔數", "平均分數", "最強個股"]],
                             use_container_width=True, hide_index=True)

            # 解讀提示
            if not sector_df.empty:
                top1 = sector_df.iloc[0]
                st.markdown("---")
                st.info(
                    f"**資金集中分析：**「{top1['industry']}」目前入選 {top1['count']} 檔強勢股，"
                    f"平均強度 {top1['avg_score']} 分，是本次掃描中資金最活躍的族群。\n\n"
                    f"操作建議：考慮從最強族群中挑選分數最高的個股優先研究。"
                )

        # ── 集體突破族群偵測結果 ───────────────────────────────
        _si = st.session_state.get("sector_info", {})
        _breakout_on = st.session_state.get("sector_breakout_enabled", False)

        if _breakout_on and _si:
            st.markdown("---")
            st.subheader("🔥 集體突破族群")
            st.caption(
                "同時滿足啟用指標的族群：創高密度達門檻（HP Density）＆ 成交額前 N 名（Turnover）"
            )

            breakout_sectors = {k: v for k, v in _si.items() if v.get("collective_breakout")}
            all_sectors_list = sorted(
                _si.items(),
                key=lambda x: (
                    -int(x[1].get("collective_breakout", False)),
                    -x[1].get("hp_density", 0),
                    x[1].get("turnover_rank", 999),
                ),
            )

            if not breakout_sectors:
                st.warning("本次掃描範圍內無族群同時符合所有啟用的突破條件，可嘗試降低門檻。")
            else:
                n_breakout = len(breakout_sectors)
                names = "、".join(f"**{s}**" for s in list(breakout_sectors)[:5])
                st.success(f"偵測到 **{n_breakout}** 個族群符合集體突破特徵：{names}")

            # 建立完整族群偵測表格
            rows = []
            for sector, info in all_sectors_list:
                row = {
                    "族群": sector,
                    "近5日漲幅": f"{info.get('return_pct', 0):+.2f}%",
                    "集體突破": "🔥 是" if info.get("collective_breakout") else "—",
                }
                if "hp_density" in info:
                    pct = info["hp_density"] * 100
                    mark = "✅" if info.get("hp_density_pass") else "  "
                    row["創高密度"] = f"{mark} {pct:.0f}%"
                if "turnover_ratio" in info:
                    rank = info.get("turnover_rank", 999)
                    mark = "✅" if info.get("turnover_top") else "  "
                    row["成交額比重"] = f"{mark} {info['turnover_ratio']*100:.1f}%（第{rank}名）"
                rows.append(row)

            if rows:
                bt_df = pd.DataFrame(rows)
                st.dataframe(bt_df, use_container_width=True, hide_index=True)


# ══ Tab：個股圖表 ════════════════════════════════════════════
with tab_chart:
    st.subheader("個股圖表（日K + 週K）")

    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        if "scan_results" in st.session_state and "price_data" in st.session_state:
            result_df = st.session_state["scan_results"]
            opts = ["（手動輸入）"] + [
                f"{r['stock_id']} {r['stock_name']}（{r['score']}分）"
                for _, r in result_df.iterrows()
            ]
            sel = st.selectbox("從掃描結果選擇", opts)
            selected_id = sel.split(" ")[0] if sel != "（手動輸入）" else None
        else:
            selected_id = None

        manual_id = st.text_input("或直接輸入股票代碼", placeholder="2330")

    with col_btn:
        st.markdown("<br><br>", unsafe_allow_html=True)
        fetch = st.button("查詢圖表", use_container_width=True)

    target = manual_id.strip() if manual_id.strip() else (selected_id or None)

    if fetch and target:
        price_cache = st.session_state.get("price_data", {})
        if target in price_cache:
            df = compute_indicators(price_cache[target])
        else:
            with st.spinner(f"下載 {target}..."):
                try:
                    from data.finmind_client import smart_get_price
                    df = smart_get_price(target, required_days=150)
                    if df.empty:
                        st.warning("找不到此股票")
                        df = None
                    else:
                        df = compute_indicators(df)
                except Exception as e:
                    st.error(str(e))
                    df = None
        if df is not None:
            st.session_state["chart_df"] = df
            st.session_state["chart_id"] = target

    if "chart_df" in st.session_state:
        chart_id = st.session_state["chart_id"]
        chart_df = st.session_state["chart_df"]

        day_tab, week_tab = st.tabs(["日K線", "週K線"])
        with day_tab:
            render_chart(chart_id, chart_df)
        with week_tab:
            render_weekly_chart(chart_id, chart_df)
    else:
        st.info("請選擇股票或輸入代碼後點擊「查詢圖表」")


# ══ Tab：歷史紀錄 ════════════════════════════════════════════
with tab_history:
    st.subheader("掃描歷史紀錄")
    st.caption("每次掃描完成後自動儲存，最新紀錄在最上方")

    history = load_scan_history(limit=30)
    render_premium_trial_evaluation(history)
    st.markdown("---")

    if not history:
        st.info("尚無掃描紀錄，執行第一次掃描後會自動出現在這裡。")
    else:
        for rec in history:
            ts = rec["scanned_at"].strftime("%Y-%m-%d %H:%M")
            label = f"🕐 {ts}　｜　{rec['scan_mode']}　｜　找到 **{rec['result_count']}** 檔"

            with st.expander(label, expanded=False):
                # 條件摘要
                c1, c2, c3 = st.columns(3)
                c1.markdown(f"**最低股價：** {rec['min_price']} 元")
                c1.markdown(f"**量能過濾：** {rec['vol_filter'] or '不過濾'}")
                c2.markdown(f"**週線多頭：** {'是' if rec['require_weekly'] else '否'}")
                c2.markdown(f"**最低 RS：** {int(rec['min_rs']) if rec['min_rs'] else '不限'}")
                c3.markdown(f"**法人條件：** {'已啟用' if rec['include_institutional'] else '未啟用'}")
                if rec["sector_filter"]:
                    st.markdown(f"**鎖定產業：** {rec['sector_filter']}")

                st.markdown("---")

                # 載入並顯示結果
                col_load, col_del, _ = st.columns([2, 2, 6])
                if col_load.button("載入結果", key=f"load_{rec['id']}"):
                    df = load_session_results(rec["id"])
                    if not df.empty:
                        st.session_state["scan_results"] = df
                        st.success(f"已載入 {len(df)} 筆結果到「掃描結果」頁籤")
                    else:
                        st.warning("此紀錄無結果資料")

                if col_del.button("刪除", key=f"del_{rec['id']}"):
                    delete_scan_session(rec["id"])
                    st.rerun()

                # 展開後直接顯示結果預覽（前 10 筆）
                df_prev = load_session_results(rec["id"])
                if not df_prev.empty:
                    df_prev = df_prev.copy()
                    df_prev["距過熱"] = df_prev.apply(_format_heat_distance, axis=1)
                    show_df = df_prev.head(10).rename(columns={
                        "stock_id": "代碼", "stock_name": "名稱",
                        "industry": "產業", "close": "收盤",
                        "change_pct": "漲跌%", "score": "分數",
                        "rs_score": "RS", "signals": "觸發條件",
                    })
                    preview_cols = [
                        "代碼", "名稱", "產業", "收盤", "漲跌%", "分數",
                        "RS", "距過熱", "觸發條件",
                    ]
                    st.dataframe(
                        show_df[[c for c in preview_cols if c in show_df.columns]],
                        use_container_width=True,
                        hide_index=True,
                    )
                    if len(df_prev) > 10:
                        st.caption(f"僅顯示前 10 筆，共 {len(df_prev)} 筆。點「載入結果」可在掃描頁查看全部。")
