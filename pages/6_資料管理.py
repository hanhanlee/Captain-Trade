"""
資料管理頁面 — 監控 & 管理本機快取與背景預抓取工作器
"""
import time
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, date, timedelta

from db.database import init_db, vacuum_db, get_session
from db.price_cache import (get_cache_summary, delete_old_prices, get_all_cached_stocks,
                            diagnose_cache, get_failed_today_detail, set_fetch_status,
                            get_delisted_stocks, get_known_stock_ids)
from db.settings import is_market_closed, set_market_closed, get_force_yahoo, set_force_yahoo
from db.inst_cache import get_inst_cache_stats
from db.fundamental_cache import get_fundamental_stats
from db.margin_cache import get_margin_stats as get_margin_cache_stats
from sqlalchemy import text as _sqla_text

init_db()

st.set_page_config(page_title="資料管理", page_icon="🗄️", layout="wide")

# ══ 自訂 CSS（使用 Streamlit CSS 變數，自動適配 Light/Dark）══════
st.markdown("""
<style>
/* ── 狀態卡：使用 Streamlit 主題變數 ── */
.status-card {
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.18);
    border-left-width: 3px;
    border-radius: 8px;
    padding: 13px 18px;
    margin: 2px 0 10px 0;
    display: flex;
    align-items: flex-start;
    gap: 14px;
}
.status-card.ok    { border-left-color: #16a34a; }
.status-card.info  { border-left-color: #2563eb; }
.status-card.warn  { border-left-color: #d97706; }
.status-card.error { border-left-color: #dc2626; }
.status-card .sc-icon { font-size: 20px; flex-shrink: 0; padding-top: 2px; }
.status-card .sc-main { font-size: 14px; font-weight: 600;
                        color: var(--text-color); line-height: 1.35; }
.status-card .sc-sub  { font-size: 12px; color: var(--text-color);
                        opacity: 0.55; margin-top: 3px; line-height: 1.5; }

/* ── 指標格 ── */
.metric-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
    margin: 8px 0 14px 0;
}
.metric-box {
    background: var(--secondary-background-color);
    border: 1px solid rgba(128,128,128,0.18);
    border-radius: 8px;
    padding: 11px 15px;
}
.metric-box .mb-label { font-size: 10.5px; color: var(--text-color);
                        opacity: 0.5; text-transform: uppercase; letter-spacing: 0.06em; }
.metric-box .mb-val   { font-size: 24px; font-weight: 700;
                        color: var(--text-color); margin: 2px 0 1px; line-height: 1; }
.metric-box .mb-sub   { font-size: 11px; color: var(--text-color); opacity: 0.45; }
.metric-box.warn  .mb-val { color: #d97706 !important; }
.metric-box.error .mb-val { color: #dc2626 !important; }
.metric-box.ok    .mb-val { color: #16a34a !important; }

/* ── 區塊標題 ── */
.sec-header {
    font-size: 10.5px;
    font-weight: 700;
    letter-spacing: 0.09em;
    text-transform: uppercase;
    color: var(--text-color);
    opacity: 0.45;
    padding: 18px 0 7px 0;
    border-bottom: 1px solid rgba(128,128,128,0.15);
    margin-bottom: 12px;
}

/* ── 最近嘗試列 ── */
.attempt-row {
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 5px 2px 10px 2px;
    font-size: 12px;
    color: var(--text-color);
    opacity: 0.55;
    flex-wrap: wrap;
}
.attempt-row .ar-label { opacity: 0.7; }
.attempt-row .ar-src   { font-weight: 600; opacity: 1; }
.attempt-row .ar-stock { font-family: monospace; opacity: 0.85; }
.attempt-row .ar-time  { opacity: 0.7; }
.attempt-row .ar-ok    { color: #16a34a; opacity: 1; font-weight: 500; }
.attempt-row .ar-warn  { color: #d97706; opacity: 1; font-weight: 500; }
.attempt-row .ar-err   { color: #dc2626; opacity: 1; font-weight: 500; }
.attempt-row .ar-dim   { opacity: 0.45; }
.attempt-row .ar-phase {
  margin-left: auto;
  font-size: 10px;
  opacity: 0.5;
  font-style: italic;
  white-space: nowrap;
}

summary, .streamlit-expanderHeader p { font-size: 13.5px !important; }
</style>
""", unsafe_allow_html=True)

# ══ 頁首：標題 + 常用開關 + 自動刷新 ═════════════════════════════
h_title, h_mc, h_fy, h_rf = st.columns([3, 1.3, 1.6, 1.6])
h_title.title("🗄️ 資料管理")

_mc = is_market_closed()
_fy = get_force_yahoo()

with h_mc:
    st.markdown("<br>", unsafe_allow_html=True)
    new_mc = st.toggle("🏖️ 休市模式", value=_mc,
                        help="開啟後法人資料快取永不過期，不消耗 API 額度。適用連假/停市期間。")
    if new_mc != _mc:
        set_market_closed(new_mc); st.rerun()

with h_fy:
    st.markdown("<br>", unsafe_allow_html=True)
    new_fy = st.toggle("🔀 強制 Yahoo Finance", value=_fy,
                        help="FinMind 異常時切換。三大法人條件自動停用；價格有 15 分鐘延遲。")
    if new_fy != _fy:
        set_force_yahoo(new_fy); st.rerun()

with h_rf:
    st.markdown("<br>", unsafe_allow_html=True)
    auto_refresh = st.toggle("🔄 自動刷新（每 5 秒）", value=False)

# 休市/Yahoo 啟用提示（只在開啟時顯示）
if new_mc:
    st.warning("**🏖️ 休市模式啟用中** — 法人資料使用快取，不呼叫 API。恢復交易後請關閉。",
                icon=None)
if new_fy:
    st.warning("**🔀 Yahoo Finance 模式啟用中** — 三大法人條件停用；價格有 15 分鐘延遲。"
                "FinMind 恢復後請關閉。", icon=None)

# ══ 取得工作器 ════════════════════════════════════════════════════
@st.cache_resource
def _get_worker():
    try:
        from scheduler.prefetch import get_worker
        return get_worker()
    except Exception:
        return None

worker = _get_worker()

if worker is None:
    st.error("工作器載入失敗，請重新啟動應用程式")
    st.stop()

s = worker.status()

def _market_day_context(latest_trading_day):
    today = date.today()
    is_weekend = today.weekday() >= 5
    manual_closed = is_market_closed()
    is_today_data = latest_trading_day == today
    is_expected_open = (not is_weekend) and (not manual_closed)
    return {
        "today": today,
        "is_weekend": is_weekend,
        "manual_closed": manual_closed,
        "is_expected_open": is_expected_open,
        "is_today_data": is_today_data,
        "is_waiting_today": is_expected_open and (not is_today_data),
    }

# ══ 區塊 1：狀態 & 控制 ══════════════════════════════════════════
st.markdown('<div class="sec-header">系統狀態</div>', unsafe_allow_html=True)

# ── 狀態卡 ────────────────────────────────────────────────────────
_cur_stock = s.get("current_stock", "")
_yb_prog   = s.get("yahoo_bridge_in_progress", False)

if s.get("backtest_rebuild_mode"):
    bt_q = s.get("backtest_queue_size", 0)
    bt_i = s.get("backtest_initial_queue_size", 0)
    _cls, _icon = "warn", "🟠"
    _main = f"回測歷史重建中 — 已完成 {max(bt_i-bt_q,0)}/{bt_i or '—'} 檔"
    _sub  = f"待處理 {bt_q} 檔　完成後自動退出重建模式"
elif s.get("rebuild_mode"):
    _cls, _icon = "error", "🔴"
    _main = "全速重建模式 — API 額度全開（600 次/小時）"
    _sub  = "請勿進行手動掃描，避免額度衝突"
elif not s["running"]:
    _cls, _icon = "error", "🔴"
    _main = "工作器已停止"
    _sub  = "點擊下方「▶ 啟動」重新啟動"
elif s.get("pause_remaining_sec", 0) > 0:
    rm = s["pause_remaining_sec"] // 60
    rs = s["pause_remaining_sec"] % 60
    resume_str = s["paused_until"].strftime("%H:%M") if s.get("paused_until") else "—"
    _cls, _icon = "warn", "🟠"
    _main = f"429 限流暫停中（第 {s.get('rate_limit_count',1)} 次）"
    _sub  = f"剩餘 {rm} 分 {rs} 秒，預計 {resume_str} 恢復　｜　或按「⚡ 立即恢復」提前繼續"
elif s["paused_for_market"]:
    _cls, _icon = "warn", "🟡"
    _main = "交易時間降速模式（09:00–盤後）"
    _sub  = "每小時上限 100 次，保留額度給手動操作"
else:
    _ltd          = s.get("latest_trading_day")
    _yb_done      = s.get("yahoo_bridge_done", False)
    _day_ctx      = _market_day_context(_ltd)
    _finmind_ok   = _day_ctx["is_today_data"]
    _queue        = s.get("queue_size", 0)
    _fund_q       = s.get("fund_queue_size", 0)

    # 判斷當前工作階段
    if _cur_stock.startswith("[法人]"):
        _w_phase = "inst"
    elif _cur_stock.startswith("[融資]"):
        _w_phase = "margin"
    elif _cur_stock.startswith("[基本面]"):
        _w_phase = "fund"
    elif _cur_stock and not _cur_stock.startswith("["):
        # 純股票代碼：queue > 0 表示真正在更新；queue = 0 則是在巡檢快取命中的股票。
        _w_phase = "ohlcv" if _queue > 0 else "cache_check"
    else:
        _w_phase = "idle"

    def _strip_prefix(s_: str) -> str:
        return s_.split("] ", 1)[-1] if "] " in s_ else s_

    if _yb_prog:
        bd, bt = s.get("yahoo_bridge_batch_done", 0), s.get("yahoo_bridge_batch_total", 0)
        _cls, _icon = "info", "🔵"
        _main = f"Yahoo Bridge 抓取中（批次 {bd}/{bt}）"
        _sub  = f"從 Yahoo Finance 批次補充今日收盤資料，共 {s.get('yahoo_bridge_total',0)} 檔待抓"

    elif _w_phase == "ohlcv":
        _cls, _icon = "ok", "🟢"
        _main = f"OHLCV 核心價格更新中（待完成 {_queue} 檔）"
        _sub  = f"正在抓取：{_cur_stock}　｜　完成後自動開始法人/融資補充"

    elif _w_phase == "cache_check":
        _cls, _icon = "ok", "🟢"
        _main = "快取巡檢中"
        _sub  = f"核心 OHLCV 待更新 0 檔　｜　正在檢查：{_cur_stock}"

    elif _w_phase in ("inst", "margin"):
        _inst_d = s.get("inst_supplementary_done", 0)
        _inst_t = s.get("inst_supplementary_total", 0) or "?"
        _marg_d = s.get("margin_supplementary_done", 0)
        _marg_t = s.get("margin_supplementary_total", 0) or "?"
        _phase_name = "法人資料" if _w_phase == "inst" else "融資融券"
        _cls, _icon = "ok", "🟢"
        _main = f"附加資料補充中（{_phase_name}）"
        _sub  = (f"法人：{_inst_d}/{_inst_t}　融資：{_marg_d}/{_marg_t}"
                 f"　｜　正在抓取：{_strip_prefix(_cur_stock)}")

    elif _w_phase == "fund":
        _cls, _icon = "ok", "🟢"
        _main = f"基本面快取填充中（{_fund_q} 檔待處理）"
        _sub  = f"正在抓取：{_strip_prefix(_cur_stock)}"

    elif _finmind_ok and s.get("supplementary_completed_at"):
        _sup = s["supplementary_completed_at"]
        _cls, _icon = "ok", "🟢"
        _main = f"今日資料全部更新完成（{_sup.strftime('%H:%M')}）"
        _sub  = "核心 OHLCV + 法人 + 融資融券 皆已就緒"

    elif _day_ctx["is_weekend"]:
        _cls, _icon = "info", "🔵"
        _main = "非交易日"
        _sub = f"今天是週末，資料參考最近交易日 {_ltd}；不需等待今日資料"

    elif _day_ctx["manual_closed"]:
        _cls, _icon = "info", "🔵"
        _main = "休市模式啟用中"
        _sub = f"資料參考最近交易日 {_ltd}；關閉休市模式後才會恢復一般補抓"

    elif _yb_done and _day_ctx["is_waiting_today"] and datetime.now().hour >= 15:
        _cls, _icon = "info", "🔵"
        _main = "Yahoo Bridge 已完成　等待 FinMind 盤後更新"
        _sub  = f"核心資料由 Yahoo 補充（基準日 {_ltd}），法人/融資待 FinMind 上線後補充"

    elif _day_ctx["is_waiting_today"] and datetime.now().hour >= 15:
        _cls, _icon = "info", "🔵"
        _main = "等待 FinMind 更新今日資料"
        _sub  = "FinMind 通常 15:30–19:00 發布，Worker 每輪自動重查，無需手動操作"

    elif _queue > 0:
        _cls, _icon = "ok", "🟢"
        _main = f"OHLCV 核心價格更新中（待完成 {_queue} 檔）"
        _sub  = "等待下一輪抓取　｜　完成後自動開始法人/融資補充"

    else:
        _cls, _icon = "ok", "🟢"
        _main = "工作器正常運行中"
        _sub  = "盤後/非交易時間，每小時上限 600 次"

st.markdown(f"""
<div class="status-card {_cls}">
  <span class="sc-icon">{_icon}</span>
  <div>
    <div class="sc-main">{_main}</div>
    <div class="sc-sub">{_sub}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── 最近嘗試資訊列 ────────────────────────────────────────────────
_la_at     = s.get("last_attempt_at")
_la_result = s.get("last_attempt_result", "")
_la_stock  = s.get("last_attempt_stock", "")

if _la_at and _la_stock:
    # 來源標籤
    _src_map = {
        "[法人]":  ("法人資料",  "#60a5fa"),
        "[融資]":  ("融資融券",  "#60a5fa"),
        "[基本面]":("基本面",    "#60a5fa"),
        "[回測]":  ("回測歷史",  "#a78bfa"),
        "[Yahoo]": ("Yahoo Finance", "#34d399"),
        "[重建]":  ("全速重建",  "#f87171"),
    }
    _src_label, _src_color = "FinMind 價格", "#60a5fa"
    _clean_stock = _la_stock
    for _prefix, (_lbl, _col) in _src_map.items():
        if _la_stock.startswith(_prefix):
            _src_label, _src_color = _lbl, _col
            _clean_stock = _la_stock[len(_prefix):].strip()
            break

    # 結果標籤
    _result_map = {
        "normal":     ("✅", "已更新",   "ar-ok"),
        "ok":         ("✅", "成功",     "ar-ok"),
        "cached":     ("⏭",  "快取命中", "ar-dim"),
        "suspended":  ("⚠️", "暫無資料","ar-warn"),
        "no_update":  ("⚠️", "無資料",  "ar-warn"),
        "delisted":   ("🚫", "已下市",  "ar-dim"),
        "rate_limit": ("🚫", "429 限流","ar-err"),
        "error":      ("❌", "錯誤",    "ar-err"),
    }
    _r_icon, _r_label, _r_cls = _result_map.get(
        _la_result, ("—", _la_result or "—", "ar-dim")
    )

    # 時間
    _el = int((datetime.now() - _la_at).total_seconds())
    _ago = f"{_el}s 前" if _el < 60 else f"{_el//60}m{_el%60:02d}s 前"
    _time_str = _la_at.strftime("%H:%M:%S")

    # 工作階段上下文標籤
    _now_phase = s.get("current_stock", "")
    if _now_phase.startswith("[法人]"):
        _inst_d2 = s.get("inst_supplementary_done", 0)
        _inst_t2 = s.get("inst_supplementary_total", 0) or "?"
        _phase_ctx = f"補充法人 {_inst_d2}/{_inst_t2}"
    elif _now_phase.startswith("[融資]"):
        _marg_d2 = s.get("margin_supplementary_done", 0)
        _marg_t2 = s.get("margin_supplementary_total", 0) or "?"
        _phase_ctx = f"補充融資 {_marg_d2}/{_marg_t2}"
    elif _now_phase and not _now_phase.startswith("["):
        _q2 = s.get("queue_size", 0)
        _phase_ctx = f"OHLCV 更新 剩 {_q2} 檔" if _q2 > 0 else "快取巡檢"
    else:
        _phase_ctx = ""

    _phase_ctx_html = (
        f'  <span class="ar-phase">{_phase_ctx}</span>'
        if _phase_ctx else ""
    )

    st.markdown(f"""
<div class="attempt-row">
  <span class="ar-label">最近嘗試</span>
  <span class="ar-src" style="color:{_src_color};">{_src_label}</span>
  <span class="ar-stock">{_clean_stock}</span>
  <span class="ar-time">{_time_str}（{_ago}）</span>
  <span class="{_r_cls}">{_r_icon} {_r_label}</span>
{_phase_ctx_html}
</div>
""", unsafe_allow_html=True)

# ── 控制按鈕 ─────────────────────────────────────────────────────
is_paused = s.get("pause_remaining_sec", 0) > 0
bc1, bc2, bc3, bc4, _ = st.columns([1, 1, 1.2, 1, 4])

if bc1.button("▶ 啟動", disabled=s["running"], use_container_width=True):
    worker.start(); st.rerun()
if bc2.button("⏹ 停止", disabled=not s["running"], use_container_width=True):
    worker.stop(); st.rerun()
if bc3.button("⚡ 立即恢復", disabled=not is_paused, use_container_width=True,
               type="primary" if is_paused else "secondary"):
    worker.resume(); st.rerun()
if bc4.button("🔄 重整", use_container_width=True):
    st.rerun()

# ── 統一計算：以 resolve_latest_trading_day() 為基準 ─────────────
# 此函式確認 FinMind 實際有資料的最新交易日（含台灣假日/休市判斷）
try:
    from db.price_cache import get_suspended_stocks
    from data.finmind_client import resolve_latest_trading_day

    _ref_date  = resolve_latest_trading_day()
    _ref_str   = _ref_date.isoformat()
    _ref_ctx   = _market_day_context(_ref_date)
    _smr       = get_cache_summary()
    _skip_ids  = set(get_delisted_stocks(include_legacy_no_update=True))
    _susp_ids  = set(get_suspended_stocks(today_only=True))
    _known_ids = get_known_stock_ids()
    _active_total = max(len(set(_known_ids) - _skip_ids), 1)

    # OHLCV 待更新
    if _smr.empty:
        _ohlcv_pending = len([x for x in _known_ids if x not in _skip_ids])
    else:
        _stale_ids = (
            set(_smr.loc[_smr["latest"] < _ref_str, "stock_id"]) - _skip_ids - _susp_ids
        )
        _miss_ids  = set(_known_ids) - set(_smr["stock_id"]) - _skip_ids
        _ohlcv_pending = len(_stale_ids) + len(_miss_ids)

    # 法人待更新（同一參考日）
    _inst_no_upd = s.get("inst_no_update_count", 0)
    _inst_active = max(_active_total - _inst_no_upd, 1)
    with get_session() as _sess:
        _inst_done_ref = _sess.execute(
            _sqla_text("SELECT COUNT(DISTINCT stock_id) FROM inst_cache WHERE date=:d"),
            {"d": _ref_str}).fetchone()[0]
    _inst_pending = max(_inst_active - _inst_done_ref, 0)

    # 融資融券待更新（同一參考日）
    _margin_no_upd   = s.get("margin_no_update_count", 0)
    _margin_active   = max(_active_total - _margin_no_upd, 1)
    _m_ref_stats     = get_margin_cache_stats(_ref_date)
    _margin_done_ref = _m_ref_stats["done_today"]
    _margin_pending  = max(_margin_active - _margin_done_ref, 0)

    # 今日是否為交易日但 FinMind 尚未發布（例如下午 2 點）
    _ref_behind_today = (
        _ref_ctx["is_waiting_today"]
        and datetime.now().hour >= 13
    )

except Exception:
    _ref_date = date.today()
    _ref_str  = _ref_date.isoformat()
    _ohlcv_pending   = s["queue_size"]
    _inst_pending    = None
    _margin_pending  = None
    _active_total    = 950
    _inst_no_upd     = s.get("inst_no_update_count", 0)
    _margin_no_upd   = s.get("margin_no_update_count", 0)
    _inst_active     = 950
    _margin_active   = 950
    _inst_done_ref   = 0
    _margin_done_ref = 0
    _ref_behind_today = False
    _ref_ctx = _market_day_context(_ref_date)

elapsed_str = "—"
if s["last_fetch_at"]:
    el = int((datetime.now() - s["last_fetch_at"]).total_seconds())
    elapsed_str = f"{el}s 前" if el < 60 else f"{el//60}m 前"

# ── Worker 健康指標（3格）────────────────────────────────────────
_rate_cls  = "warn" if s.get("rate_limit_count", 0) > 0 else ""
_total_cls = "ok"   if s["total_fetched"] > 0 else ""

st.markdown(f"""
<div class="metric-grid" style="grid-template-columns:repeat(3,1fr);">
  <div class="metric-box">
    <div class="mb-label">本小時用量</div>
    <div class="mb-val">{s['hour_fetched']}</div>
    <div class="mb-sub">上限 {s['hourly_limit']}　剩餘 {s['hourly_remaining']} 次</div>
  </div>
  <div class="metric-box {_total_cls}">
    <div class="mb-label">本次累計抓取</div>
    <div class="mb-val">{s['total_fetched']}</div>
    <div class="mb-sub">最近：{elapsed_str}</div>
  </div>
  <div class="metric-box {_rate_cls}">
    <div class="mb-label">429 限流次數</div>
    <div class="mb-val">{s.get('rate_limit_count', 0)}</div>
    <div class="mb-sub">已略過 {s.get('skip_count', 0)} 檔無資料</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── 資料待更新（3格，以 resolve_latest_trading_day 為準）────────
_ref_display = _ref_str
if _ref_date == date.today():
    _ref_display = f"{_ref_str}（今日）"
elif _ref_ctx.get("is_weekend"):
    _ref_display = f"{_ref_str}（週末非交易日）"
elif _ref_ctx.get("manual_closed"):
    _ref_display = f"{_ref_str}（休市模式）"
elif _ref_behind_today:
    _ref_display = f"{_ref_str}　⚠️ 今日 {date.today()} 尚待 FinMind 發布"

# Worker supplementary 計數器 — 提前計算，供 metric 框與 progress bars 共用
_w_inst_done  = s.get("inst_supplementary_done",   0)
_w_inst_total = s.get("inst_supplementary_total",  0)
_w_marg_done  = s.get("margin_supplementary_done", 0)
_w_marg_total = s.get("margin_supplementary_total",0)

_ohlcv_cls  = "warn" if _ohlcv_pending > 20 else ("ok" if _ohlcv_pending == 0 else "")
_inst_cls   = "warn" if (_inst_pending or 0) > 20 else ("ok" if (_inst_pending or 0) == 0 else "")
_margin_cls = "warn" if (_margin_pending or 0) > 20 else ("ok" if (_margin_pending or 0) == 0 else "")
_inst_val   = str(_inst_pending)   if _inst_pending   is not None else "—"
_margin_val = str(_margin_pending) if _margin_pending is not None else "—"

st.markdown(f"""
<div style="font-size:10.5px; color:var(--text-color); opacity:0.42;
            letter-spacing:0.05em; text-transform:uppercase;
            padding: 16px 0 6px 0; border-bottom:1px solid rgba(128,128,128,0.12);
            margin-bottom:8px;">
  資料待更新　參考交易日：{_ref_display}
</div>
<div class="metric-grid" style="grid-template-columns:repeat(3,1fr); margin-top:8px;">
  <div class="metric-box {_ohlcv_cls}">
    <div class="mb-label">核心 OHLCV</div>
    <div class="mb-val">{_ohlcv_pending}</div>
    <div class="mb-sub">Worker 快照 {s['queue_size']} 檔</div>
  </div>
  <div class="metric-box {_inst_cls}">
    <div class="mb-label">法人資料</div>
    <div class="mb-val">{_inst_val}</div>
    <div class="mb-sub">已完成 {_inst_done_ref} / {_inst_active} 檔</div>
  </div>
  <div class="metric-box {_margin_cls}">
    <div class="mb-label">融資融券</div>
    <div class="mb-val">{_margin_val}</div>
    <div class="mb-sub">已完成 {_margin_done_ref} / {_margin_active} 檔</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── 重建模式進度條 ────────────────────────────────────────────────
if s.get("backtest_rebuild_mode"):
    bt_i3 = s.get("backtest_initial_queue_size", 0)
    bt_q3 = s.get("backtest_queue_size", 0)
    if bt_i3 > 0:
        bp = min((bt_i3 - bt_q3) / bt_i3, 1.0)
        st.progress(bp, text=f"回測重建：{bt_i3-bt_q3}/{bt_i3} 檔（{bp*100:.0f}%）")
elif s.get("rebuild_mode"):
    iq = s.get("initial_queue_size", 0)
    rq = s.get("queue_size", 0)
    if iq > 0:
        rp = min((iq - rq) / iq, 1.0)
        st.progress(rp, text=f"全速重建：{iq-rq}/{iq} 檔（{rp*100:.0f}%）")

# ══ 區塊 2：資料更新進度 ══════════════════════════════════════════
if date.today().weekday() < 5 and not s.get("backtest_rebuild_mode"):
    _prog_title = f"資料更新進度（{_ref_str}）"
    st.markdown(f'<div class="sec-header">{_prog_title}</div>', unsafe_allow_html=True)

    try:
        with get_session() as _sess:
            _core_done = _sess.execute(
                _sqla_text("SELECT COUNT(DISTINCT stock_id) FROM price_cache WHERE date=:d"),
                {"d": _ref_str}).fetchone()[0]
    except Exception:
        _core_done = 0

    # Worker supplementary 計數器比 DB date 查詢更即時（不受法人/融資發布時間落差影響）
    _inst_done  = _inst_done_ref
    _inst_total = _inst_active
    _margin_done  = _margin_done_ref
    _margin_total = _margin_active
    _core_pct      = _core_done / _active_total if _active_total else 0
    _progress_ctx = _market_day_context(s.get("latest_trading_day"))
    _finmind_updated = _progress_ctx["is_today_data"]
    _is_waiting_now  = (_progress_ctx["is_waiting_today"] and datetime.now().hour >= 15)

    def _pbar(label, done, total, note=""):
        pct = min(done / total, 1.0) if total > 0 else 0.0
        pct_str = f"{pct*100:.0f}%"
        if pct >= 1.0:
            text = f"{label}：{pct_str}（{done}/{total} 檔）✅"
        elif note:
            text = f"{label}：{pct_str}（{done}/{total} 檔）　{note}"
        else:
            text = f"{label}：{pct_str}（{done}/{total} 檔）"
        st.progress(pct, text=text)

    _ohlcv_queue_left = s.get("queue_size", 0)
    if _is_waiting_now:
        _supp_note = "⏳ 等待 FinMind 盤後更新（通常 17–19 時）"
    elif _progress_ctx["is_weekend"]:
        _supp_note = "非交易日，使用最近交易日資料"
    elif _progress_ctx["manual_closed"]:
        _supp_note = "休市模式中，使用最近交易日資料"
    elif _ohlcv_queue_left > 0:
        _supp_note = f"⏳ 等待 OHLCV 核心資料完成（還剩 {_ohlcv_queue_left} 檔）"
    elif _core_pct < 0.5:
        _supp_note = "等待核心資料完成"
    else:
        _supp_note = ""

    _pbar("核心資料（OHLCV）", _core_done, _active_total,
          note="Yahoo Bridge 已補充" if s.get("yahoo_bridge_done") and not _finmind_updated else "")
    _pbar("法人資料",     _inst_done,   _inst_total,   note=_supp_note)
    _pbar("融資融券資料", _margin_done, _margin_total, note=_supp_note)

    # ── Yahoo Bridge 進度 / 結果 ──────────────────────────────────
    _yb_done2  = s.get("yahoo_bridge_done", False)
    _yb_count  = s.get("yahoo_bridge_count", 0)
    _yb_total  = s.get("yahoo_bridge_total", 0)
    _yb_bd     = s.get("yahoo_bridge_batch_done", 0)
    _yb_bt     = s.get("yahoo_bridge_batch_total", 0)
    _yb_failed = s.get("yahoo_bridge_failed_ids", [])

    if _yb_prog:
        yp = (_yb_bd / _yb_bt) if _yb_bt > 0 else 0.0
        st.progress(min(yp, 1.0),
                    text=f"🌐 Yahoo Bridge：批次 {_yb_bd}/{_yb_bt}（共 {_yb_total} 檔）")

    elif _yb_done2 and len(_yb_failed) > 0:
        with st.expander(
            f"🌐 Yahoo Bridge 結果 — ✅ {_yb_count} 檔補充成功　⚠️ {len(_yb_failed)} 檔無資料",
            expanded=False
        ):
            st.caption(
                "以下股票今日在 Yahoo Finance 查無收盤資料（停牌、剛上市、或資料延遲），"
                "待 FinMind 盤後更新後自動補入。"
            )
            for chunk in [_yb_failed[i:i+15] for i in range(0, len(_yb_failed), 15)]:
                st.code("  ".join(chunk))
            ybc1, ybc2, _ = st.columns([1.3, 1.3, 5])
            if ybc1.button("🔄 重新抓取", key="yb_retry",
                            help="重置 Yahoo Bridge，Worker 下輪重跑（已有資料自動跳過）"):
                if hasattr(worker, "reset_yahoo_bridge"):
                    worker.reset_yahoo_bridge()
                st.rerun()
            if ybc2.button("✓ 略過", key="yb_skip",
                            help="清除失敗清單，待 FinMind 盤後自動補入"):
                worker.yahoo_bridge_failed_ids = []
                st.rerun()

    # ── 今日 FinMind 抓取失敗清單 ─────────────────────────────────
    failed_df = get_failed_today_detail()
    if not failed_df.empty:
        with st.expander(f"⚠️ 今日 FinMind 抓取失敗 — {len(failed_df)} 檔", expanded=True):
            st.caption(
                "常見原因：FinMind 盤後尚未更新（通常 17–18 時補齊）。"
                "失敗超過 3 小時的股票會自動放回重試佇列。"
            )
            fc1, fc2, _ = st.columns([2, 1.5, 4])
            search_kw = fc1.text_input("搜尋代碼或名稱", placeholder="2330 / 台積電",
                                        key="failed_search", label_visibility="collapsed")
            if fc2.button(f"🔄 全部重設（{len(failed_df)} 檔）",
                           type="primary", use_container_width=True):
                for sid in failed_df["stock_id"]:
                    set_fetch_status(sid, "normal")
                st.success(f"已重設 {len(failed_df)} 檔，Worker 下一輪自動重試")
                st.rerun()

            disp_f = failed_df.copy()
            if search_kw:
                kw = search_kw.strip()
                disp_f = disp_f[
                    disp_f["stock_id"].str.contains(kw, case=False, na=False) |
                    disp_f["stock_name"].str.contains(kw, case=False, na=False)
                ]
            st.dataframe(
                disp_f.rename(columns={"stock_id":"代碼","stock_name":"名稱",
                                        "industry":"產業","failed_at":"失敗時間"}),
                use_container_width=True, hide_index=True,
                height=min(300, 40 + len(disp_f) * 35),
            )

            opt_f = {sid: f"{sid}　{nm}" if nm else sid
                     for sid, nm in zip(failed_df["stock_id"], failed_df["stock_name"])}
            cs, csa = st.columns([5, 1])
            sel = cs.multiselect("選取要重抓的股票", options=list(opt_f.keys()),
                                  format_func=lambda x: opt_f[x],
                                  placeholder="選擇一或多檔...",
                                  label_visibility="collapsed", key="refetch_select")
            if csa.button("全選", key="refetch_selall"):
                st.session_state["refetch_select"] = list(opt_f.keys()); st.rerun()

            if st.button(f"⬇️ 立即重抓（{len(sel)} 檔）",
                          disabled=not sel, type="primary", key="refetch_btn"):
                from data.finmind_client import smart_get_price
                from scheduler.prefetch import PREFETCH_DAYS
                ok_l, fail_l, rl = [], [], False
                prog_r = st.progress(0, text="準備重抓...")
                for i, sid in enumerate(sel):
                    prog_r.progress((i+1)/len(sel), text=f"抓取 {opt_f[sid]}（{i+1}/{len(sel)}）")
                    try:
                        smart_get_price(sid, required_days=PREFETCH_DAYS)
                        set_fetch_status(sid, "normal"); ok_l.append(opt_f[sid])
                    except Exception as e:
                        err = str(e)
                        if "429" in err or "rate limit" in err.lower():
                            rl = True; fail_l.append(f"{opt_f[sid]}（429）"); break
                        fail_l.append(f"{opt_f[sid]}（{err[:50]}）")
                prog_r.empty()
                if ok_l:   st.success(f"✅ 成功 {len(ok_l)} 檔：{'、'.join(ok_l[:10])}"
                                       + ("…" if len(ok_l) > 10 else ""))
                if fail_l: st.error(f"❌ 失敗：{'、'.join(fail_l[:5])}"
                                     + ("…" if len(fail_l) > 5 else ""))
                if rl:     st.warning("⚠️ 遇到 429 限流，請等 20 分鐘後再試")
                if ok_l or fail_l: st.rerun()

    # ── 附加資料排除清單 ─────────────────────────────────────────
    _inst_err_ids = s.get("inst_error_ids", [])
    _margin_err_ids = s.get("margin_error_ids", [])
    if _inst_err_ids or _margin_err_ids:
        with st.expander(
            f"⚠️ 附加資料抓取錯誤（法人 {len(_inst_err_ids)} 檔 ／ 融資 {len(_margin_err_ids)} 檔）",
            expanded=True
        ):
            st.caption(
                "以下股票本次 worker 啟動期間抓取附加資料時發生錯誤。"
                "這類錯誤通常是暫時性 API/網路問題，worker 下一輪仍會重試。"
            )
            if _inst_err_ids:
                st.markdown(f"**法人抓取錯誤（{len(_inst_err_ids)} 檔）：**")
                st.code("  ".join(_inst_err_ids[:200]) + ("  ..." if len(_inst_err_ids) > 200 else ""))
            if _margin_err_ids:
                st.markdown(f"**融資融券抓取錯誤（{len(_margin_err_ids)} 檔）：**")
                st.code("  ".join(_margin_err_ids[:200]) + ("  ..." if len(_margin_err_ids) > 200 else ""))

    if _inst_no_upd > 0 or _margin_no_upd > 0:
        with st.expander(
            f"ℹ️ 附加資料排除清單（法人 {_inst_no_upd} 檔 ／ 融資 {_margin_no_upd} 檔）",
            expanded=False
        ):
            st.caption("以下股票 FinMind 無對應附加資料（通常為 ETF、權證等），"
                        "已從完成率分母中排除，不影響整體完成率。")
            if _inst_no_upd > 0 and worker:
                inst_l = sorted(worker._inst_no_update)
                st.markdown(f"**法人無資料（{len(inst_l)} 檔）：**")
                st.code("  ".join(inst_l[:100]) + ("  ..." if len(inst_l) > 100 else ""))
            if _margin_no_upd > 0 and worker:
                margin_l = sorted(worker._margin_no_update)
                st.markdown(f"**融資無資料（{len(margin_l)} 檔）：**")
                st.code("  ".join(margin_l[:100]) + ("  ..." if len(margin_l) > 100 else ""))

# ══ 區塊 3：快取狀態 & 診斷 ══════════════════════════════════════
with st.expander("📦 快取狀態 & 診斷", expanded=False):
    ec1, ec2, ec3, _ = st.columns([1, 1.3, 1.3, 4])
    load_c  = ec1.button("載入快取摘要", use_container_width=True)
    clean_c = ec2.button("清理 400 天前資料", type="secondary", use_container_width=True)
    vac_c   = ec3.button("最佳化資料庫", type="secondary", use_container_width=True)

    if clean_c:
        deleted = delete_old_prices(keep_days=400)
        st.success(f"已刪除 {deleted} 筆舊資料")
    if vac_c:
        with st.spinner("最佳化中..."): vacuum_db()
        st.success("資料庫最佳化完成（碎片整理、空間回收）")
    if load_c or "cache_summary" in st.session_state:
        with st.spinner("讀取快取摘要..."):
            summary = get_cache_summary()
            st.session_state["cache_summary"] = summary

    if "cache_summary" in st.session_state:
        summary = st.session_state["cache_summary"]
        if summary.empty:
            st.info("快取為空，工作器啟動後將自動開始填充")
        else:
            stale_cut  = (date.today() - timedelta(days=5)).isoformat()
            total      = len(summary)
            fresh      = (summary["latest"] >= stale_cut).sum()
            total_days = summary["days"].sum()
            sm1,sm2,sm3,sm4 = st.columns(4)
            sm1.metric("已快取股票", f"{total} 檔")
            sm2.metric("新鮮（5天內）", f"{fresh} 檔")
            sm3.metric("需要更新", f"{total-fresh} 檔")
            sm4.metric("資料總筆數", f"{total_days:,} 筆")

            summary["status"] = summary["latest"].apply(
                lambda x: "新鮮（5天內）" if x >= stale_cut else "過期")
            fig = px.histogram(
                summary, x="latest", color="status",
                color_discrete_map={"新鮮（5天內）":"#4ade80","過期":"#f87171"},
                labels={"latest":"最新資料日期","count":"股票數"},
                title="各股最新快取日期分佈", nbins=60,
            )
            fig.update_layout(height=270, template="plotly_dark",
                               margin=dict(t=40,b=10), showlegend=True,
                               legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("查看詳細清單"):
                srch = st.text_input("搜尋股票代碼", placeholder="2330", key="cache_srch")
                disp_c = summary.copy()
                if srch: disp_c = disp_c[disp_c["stock_id"].str.contains(srch)]
                st.dataframe(
                    disp_c.rename(columns={"stock_id":"代碼","earliest":"最早日期",
                                            "latest":"最新日期","days":"天數"}),
                    use_container_width=True, hide_index=True, height=400)

    # 快取品質診斷
    st.markdown("---")
    st.markdown("**🔍 快取品質診斷**")
    st.caption("找出完全缺失、資料不足或過舊的股票，並可一鍵批次補抓。")
    dg1, dg2, _ = st.columns([1, 1, 5])
    min_days_thresh = dg1.number_input("最低天數門檻", min_value=30, max_value=1000,
                                        value=200, step=50,
                                        help="快取筆數低於此值視為資料不足")
    run_diag = dg2.button("執行診斷", type="primary", use_container_width=True)

    if run_diag:
        with st.spinner("診斷中，讀取快取資料庫..."):
            diag = diagnose_cache(min_days=min_days_thresh)
        st.session_state["cache_diag"] = diag

    if "cache_diag" in st.session_state:
        diag        = st.session_state["cache_diag"]
        summary_df  = diag["summary"]
        missing_ids = diag["missing"]
        thin_df     = diag["thin"]
        stale_df    = diag["stale"]
        problem_ids = diag["problem_ids"]
        latest_td   = diag.get("latest_trading_day", "—")
        delisted_df = diag.get("delisted", pd.DataFrame())

        st.caption(f"過舊基準：最新交易日 **{latest_td}**")

        dd1,dd2,dd3,dd4,dd5,dd6 = st.columns(6)
        dd1.metric("已知股票", f"{len(missing_ids)+len(summary_df)} 檔")
        dd2.metric("已下市", f"{len(delisted_df)} 檔")
        dd3.metric("完全缺失", f"{len(missing_ids)} 檔")
        dd4.metric(f"不足 {min_days_thresh} 天", f"{len(thin_df)} 檔")
        dd5.metric("資料過舊", f"{len(stale_df)} 檔")
        dd6.metric("需補抓合計", f"{len(problem_ids)} 檔")

        if not summary_df.empty:
            fig2 = px.histogram(summary_df, x="days", nbins=50,
                                labels={"days":"快取天數","count":"股票數"},
                                title="快取天數分佈",
                                color_discrete_sequence=["#60a5fa"])
            fig2.add_vline(x=min_days_thresh, line_dash="dash", line_color="#f87171",
                           annotation_text=f"門檻 {min_days_thresh} 天")
            fig2.update_layout(height=250, margin=dict(t=40,b=10))
            st.plotly_chart(fig2, use_container_width=True)

        tab_m, tab_t, tab_s, tab_d, tab_a = st.tabs([
            f"完全缺失 ({len(missing_ids)})",
            f"資料不足 ({len(thin_df)})",
            f"資料過舊 ({len(stale_df)})",
            f"已下市 ({len(delisted_df)})",
            f"全部有快取 ({len(summary_df)})",
        ])

        with tab_m:
            if missing_ids:
                st.warning(f"以下 **{len(missing_ids)}** 檔股票在股票清單中，但快取完全沒有資料：")
                for chunk in [missing_ids[i:i+10] for i in range(0,len(missing_ids),10)]:
                    st.code("  ".join(chunk))
            else:
                st.success("所有已知股票皆有快取資料。")

        with tab_t:
            if not thin_df.empty:
                st.warning(f"以下 **{len(thin_df)}** 檔股票快取天數不足 {min_days_thresh} 天：")
                st.dataframe(
                    thin_df.rename(columns={"stock_id":"代碼","earliest":"最早日期",
                                             "latest":"最新日期","days":"天數","status":"狀態"
                                            }).sort_values("天數"),
                    use_container_width=True, hide_index=True, height=320)
            else:
                st.success(f"所有快取股票均達 {min_days_thresh} 天門檻。")

        with tab_s:
            if not stale_df.empty:
                st.warning(f"以下 **{len(stale_df)}** 檔股票最新資料早於最新交易日：")
                st.dataframe(
                    stale_df.rename(columns={"stock_id":"代碼","earliest":"最早日期",
                                              "latest":"最新日期","days":"天數","status":"狀態"
                                             }).sort_values("latest"),
                    use_container_width=True, hide_index=True, height=320)
            else:
                st.success("所有快取資料均在時效內。")

        with tab_d:
            if not delisted_df.empty:
                st.info(f"以下 **{len(delisted_df)}** 檔已下市，快取僅供歷史參考，不計入問題清單。")
                st.dataframe(
                    delisted_df[["stock_id","earliest","latest","days"]].rename(
                        columns={"stock_id":"代碼","earliest":"最早日期",
                                  "latest":"最新日期","days":"天數"}),
                    use_container_width=True, hide_index=True, height=360)
            else:
                st.info("快取中目前沒有已下市股票的資料。")

        with tab_a:
            if not summary_df.empty:
                sa = st.text_input("搜尋代碼", placeholder="2330", key="diag_search")
                show_a = summary_df.copy()
                if sa: show_a = show_a[show_a["stock_id"].str.contains(sa)]
                st.dataframe(
                    show_a.rename(columns={"stock_id":"代碼","earliest":"最早日期",
                                            "latest":"最新日期","days":"天數","status":"狀態"
                                           }).sort_values("天數"),
                    use_container_width=True, hide_index=True, height=400)

        if problem_ids:
            st.markdown("---")
            st.info(f"診斷出 **{len(problem_ids)}** 檔有問題（缺失 + 不足 + 過舊）")
            st.code(", ".join(problem_ids[:50]) + ("..." if len(problem_ids) > 50 else ""),
                    language=None)
            if worker and st.button("🚀 排入工作器優先補抓", type="primary"):
                if hasattr(worker, "priority_enqueue"):
                    worker.priority_enqueue(problem_ids)
                    st.success(f"已將 {len(problem_ids)} 檔排入優先佇列")
                else:
                    st.warning("工作器不支援優先佇列，請使用手動補抓")
                st.rerun()

# ══ 區塊 4：手動補抓 & 基本面快取 ════════════════════════════════
with st.expander("🔧 手動補抓 & 基本面快取", expanded=False):
    st.markdown("**手動補抓指定股票**")
    st.caption("若特定股票快取不足，可在此手動更新（消耗 FinMind API 額度）")
    mi_col, mb_col = st.columns([3, 1])
    manual_ids = mi_col.text_input("輸入股票代碼（多檔用逗號分隔）",
                                    placeholder="2330, 2317, 0050")
    mb_col.markdown("<br>", unsafe_allow_html=True)
    if mb_col.button("立即補抓", use_container_width=True, type="primary") and manual_ids.strip():
        ids_m = [x.strip() for x in manual_ids.split(",") if x.strip()]
        pg_m  = st.progress(0)
        res_m = []
        for i, sid in enumerate(ids_m):
            pg_m.progress((i + 1) / len(ids_m))
            try:
                from data.finmind_client import smart_get_price
                df_m = smart_get_price(sid, required_days=150)
                res_m.append({"代碼": sid, "狀態": "✅ 成功",
                               "筆數": len(df_m) if not df_m.empty else 0})
            except Exception as e:
                res_m.append({"代碼": sid, "狀態": f"❌ {e}", "筆數": 0})
        pg_m.empty()
        st.dataframe(pd.DataFrame(res_m), use_container_width=True, hide_index=True)
        st.session_state.pop("cache_summary", None)

    st.markdown("---")
    st.markdown("**基本面快取狀態**")
    st.caption("財報每季發布，90 天 TTL 已足夠。背景工作器會在所有價格快取補齊後自動填充。")
    fund_stats  = get_fundamental_stats()
    fund_stale  = fund_stats["total"] - fund_stats["fresh"]
    newest_fund = str(fund_stats.get("newest_fetch", ""))[:16] or "尚無資料"
    bf1,bf2,bf3,bf4 = st.columns(4)
    bf1.metric("已快取股票", f"{fund_stats['total']} 檔")
    bf2.metric("有效（90天內）", f"{fund_stats['fresh']} 檔")
    bf3.metric("需要更新", f"{fund_stale} 檔")
    bf4.metric("最新抓取", newest_fund)

    fw_st = worker.status()
    if fw_st.get("fund_queue_size", 0) > 0:
        st.info(f"背景工作器待填充基本面：**{fw_st['fund_queue_size']} 檔**"
                 "（價格快取補齊後自動填充）")

    if st.button("🗑️ 清除基本面快取", type="secondary"):
        from sqlalchemy import text
        from db.database import get_session
        with get_session() as sess:
            sess.execute(text("DELETE FROM fundamental_cache")); sess.commit()
        st.success("基本面快取已清除，背景工作器將在下次閒置時自動重新填充")
        st.rerun()

# ══ 區塊 5：維護操作 ══════════════════════════════════════════════
with st.expander("🔨 維護操作（重建資料庫）", expanded=False):
    # 全速重建
    st.markdown("**全速重建本機資料庫**")
    st.caption("適用：首次安裝、資料庫損毀、長時間未更新。API 額度全開至 600 次/小時，建議睡前執行。")

    is_rebuild    = s.get("rebuild_mode", False)
    completed_at  = s.get("rebuild_completed_at")

    if completed_at:
        st.success(f"✅ 全速重建已完成（{completed_at.strftime('%m/%d %H:%M')}）"
                    " — 系統已自動退出重建模式，恢復正常限速")
    elif is_rebuild:
        iq2 = s.get("initial_queue_size", 0)
        rq2 = s.get("queue_size", 0)
        if iq2 > 0:
            rp2 = min((iq2 - rq2) / iq2, 1.0)
            st.progress(rp2, text=f"全速重建進度：{iq2-rq2}/{iq2} 檔（{rp2*100:.0f}%）")
        st.error("重建模式進行中，API 額度全開（600次/小時）— 請勿使用選股雷達等手動功能")
        if st.button("⏹ 停止重建模式，恢復正常限速", type="secondary"):
            if hasattr(worker, "disable_rebuild_mode"): worker.disable_rebuild_mode()
            else: worker.rebuild_mode = False
            st.rerun()
    else:
        if "rebuild_confirm" not in st.session_state:
            st.session_state.rebuild_confirm = False
        if not st.session_state.rebuild_confirm:
            if st.button("🔨 重建資料庫", type="secondary"):
                st.session_state.rebuild_confirm = True; st.rerun()
        else:
            st.warning("⚠️ 重建期間 API 全開（600次/小時），選股掃描會與背景搶額度。"
                        "全市場約 950 檔，完整重建約需 1.5–2 小時。")
            rc1, rc2, _ = st.columns([1.2, 1, 5])
            if rc1.button("✅ 確認，全速重建", type="primary", use_container_width=True):
                st.session_state.rebuild_confirm = False
                if not worker.running: worker.start()
                if hasattr(worker, "resume"): worker.resume()
                if hasattr(worker, "enable_rebuild_mode"): worker.enable_rebuild_mode()
                else: worker.rebuild_mode = True
                st.rerun()
            if rc2.button("取消", use_container_width=True):
                st.session_state.rebuild_confirm = False; st.rerun()

    st.markdown("---")
    # 回測歷史重建
    st.markdown("**回測歷史資料重建（最多往前 10 年）**")
    st.caption("補充全市場最多 10 年歷史日K，提升回測涵蓋範圍。約 950 次 API，建議睡前啟動。")

    is_bt_rebuild   = s.get("backtest_rebuild_mode", False)
    bt_completed_at = s.get("backtest_completed_at")
    bt_q4           = s.get("backtest_queue_size", 0)
    bt_i4           = s.get("backtest_initial_queue_size", 0)

    if bt_completed_at:
        st.success(f"✅ 回測歷史重建已完成（{bt_completed_at.strftime('%m/%d %H:%M')}）"
                    " — 所有股票均已具備 10 年歷史資料")
    elif is_bt_rebuild:
        if bt_i4 > 0:
            btp = min((bt_i4 - bt_q4) / bt_i4, 1.0)
            st.progress(btp, text=f"回測重建進度：{bt_i4-bt_q4}/{bt_i4} 檔（{btp*100:.0f}%）")
        st.warning(f"回測歷史重建進行中，待處理 **{bt_q4} 檔**，完成後自動退出")
        if st.button("⏹ 停止回測重建", type="secondary"):
            if hasattr(worker, "disable_backtest_rebuild_mode"):
                worker.disable_backtest_rebuild_mode()
            st.rerun()
    else:
        if "bt_rebuild_confirm" not in st.session_state:
            st.session_state.bt_rebuild_confirm = False
        if not st.session_state.bt_rebuild_confirm:
            if st.button("📼 重建回測歷史資料", type="secondary"):
                st.session_state.bt_rebuild_confirm = True; st.rerun()
        else:
            st.warning("⚠️ 每檔抓取 10 年資料，消耗 ~950 次 API 配額。"
                        "已有足夠歷史的股票自動跳過，建議睡前執行。")
            bc1, bc2, _ = st.columns([1.2, 1, 5])
            if bc1.button("✅ 確認，開始重建", type="primary", use_container_width=True):
                st.session_state.bt_rebuild_confirm = False
                if not worker.running: worker.start()
                if hasattr(worker, "resume"): worker.resume()
                if hasattr(worker, "enable_backtest_rebuild_mode"):
                    worker.enable_backtest_rebuild_mode()
                st.rerun()
            if bc2.button("取消", use_container_width=True):
                st.session_state.bt_rebuild_confirm = False; st.rerun()

# ══ 區塊 6：系統設定（進階）══════════════════════════════════════
# 頁首的開關控制開/關；此 expander 提供進階設定（如清除法人快取）
with st.expander("⚙️ 系統設定（進階）", expanded=False):
    st.caption("休市模式與強制 Yahoo 開關已在頁首，此處提供進階操作。")
    if new_mc:
        st.markdown("**🏖️ 休市模式 — 法人快取管理**")
        stats_mc = get_inst_cache_stats()
        if stats_mc["stock_count"] > 0 and stats_mc["newest_fetch"]:
            st.caption(f"法人快取：**{stats_mc['stock_count']}** 檔，"
                        f"最新抓取 {str(stats_mc['newest_fetch'])[:16]}")
        if st.button("🗑️ 清除法人快取", help="清除後下次掃描將重新從 API 抓取"):
            from sqlalchemy import text
            from db.database import get_session
            with get_session() as sess:
                sess.execute(text("DELETE FROM inst_cache")); sess.commit()
            st.success("法人快取已清除，下次掃描將重新抓取"); st.rerun()
    else:
        st.info("目前無進階設定需要操作。"
                "開啟休市模式後，此處會出現法人快取管理選項。")

# ══ 區塊 7：LINE 推播訂閱者管理 ══════════════════════════════════
with st.expander("📣 LINE 推播訂閱者管理", expanded=False):
    st.caption("管理群播名單。選股警示、週報會推送給所有已啟用的訂閱者；停用可暫停接收而不刪除記錄。")

    from notifications.line_notify import (
        get_all_subscribers, add_subscriber, remove_subscriber,
        set_subscriber_enabled, send_message as _line_send_single, sync_env_subscriber,
    )
    sync_env_subscriber()
    subscribers = get_all_subscribers()

    if not subscribers:
        st.info("尚無訂閱者。使用下方表單新增 LINE User ID。")
    else:
        for sub in subscribers:
            uid  = sub["user_id"]
            name = sub["display_name"] or uid
            cn, ct, ctst, cd = st.columns([4, 2, 2, 1])
            cn.markdown(
                f"**{name}**  \n"
                f"<span style='font-size:0.8em;color:gray'>{uid}</span>",
                unsafe_allow_html=True,
            )
            new_en = ct.toggle("群播啟用", value=sub["enabled"], key=f"sub_toggle_{uid}")
            if new_en != sub["enabled"]:
                set_subscriber_enabled(uid, new_en); st.rerun()
            if ctst.button("📨 測試", key=f"sub_test_{uid}"):
                ok = _line_send_single("✅ LINE 推播測試訊息（來自 srock tool）", user_id=uid)
                st.toast(f"已傳送給 {name}" if ok else "傳送失敗，請確認 Token 與 User ID",
                          icon="✅" if ok else "❌")
            if cd.button("🗑️", key=f"sub_del_{uid}", help=f"刪除 {name}"):
                remove_subscriber(uid); st.rerun()

    st.markdown("---")
    with st.expander("➕ 新增訂閱者", expanded=not subscribers):
        st.caption(
            "LINE User ID 格式為 `Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`（32碼）。  \n"
            "取得方式：在 LINE Bot 聊天視窗輸入任意訊息後，"
            "到 LINE Developers Console → Messaging API → Webhook 日誌查看。"
        )
        new_uid  = st.text_input("LINE User ID", placeholder="Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        new_name = st.text_input("顯示名稱（選填）", placeholder="例：王小明")
        if st.button("新增", type="primary"):
            if not new_uid.strip():
                st.warning("請填入 LINE User ID")
            else:
                result = add_subscriber(new_uid.strip(), new_name.strip())
                if result == "added":    st.success(f"已新增訂閱者：{new_name or new_uid}"); st.rerun()
                elif result == "updated": st.info("User ID 已存在，已更新顯示名稱"); st.rerun()
                elif result == "invalid": st.error("User ID 格式錯誤，需為 U 開頭加 32 碼十六進位字元")
                else:                     st.error("新增失敗，請查看日誌")

# ── 自動刷新執行（頁面最底部）────────────────────────────────────
if auto_refresh:
    time.sleep(5)
    st.rerun()
