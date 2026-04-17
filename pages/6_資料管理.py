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

# ══ 自訂 CSS ══════════════════════════════════════════════════════
st.markdown("""
<style>
/* ── 色彩系統 ── */
:root {
    --c-bg-card:    rgba(255,255,255,0.03);
    --c-border:     rgba(255,255,255,0.07);
    --c-text-dim:   rgba(255,255,255,0.40);
    --c-green:  #4ade80;
    --c-amber:  #fbbf24;
    --c-red:    #f87171;
    --c-blue:   #60a5fa;
}

/* ── 狀態卡 ── */
.status-card {
    background: var(--c-bg-card);
    border: 1px solid var(--c-border);
    border-left-width: 3px;
    border-radius: 8px;
    padding: 13px 18px;
    margin: 2px 0 14px 0;
    display: flex;
    align-items: flex-start;
    gap: 14px;
}
.status-card.ok    { border-left-color: var(--c-green); }
.status-card.info  { border-left-color: var(--c-blue);  }
.status-card.warn  { border-left-color: var(--c-amber); }
.status-card.error { border-left-color: var(--c-red);   }
.status-card .sc-icon { font-size: 20px; flex-shrink: 0; padding-top: 1px; }
.status-card .sc-main { font-size: 14px; font-weight: 600;
                        color: #e2e8f0; line-height: 1.35; }
.status-card .sc-sub  { font-size: 12px; color: var(--c-text-dim);
                        margin-top: 3px; line-height: 1.5; }

/* ── 指標格 ── */
.metric-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
    margin: 8px 0 14px 0;
}
.metric-box {
    background: var(--c-bg-card);
    border: 1px solid var(--c-border);
    border-radius: 8px;
    padding: 11px 15px;
}
.metric-box .mb-label { font-size: 10.5px; color: var(--c-text-dim);
                        text-transform: uppercase; letter-spacing: 0.06em; }
.metric-box .mb-val   { font-size: 24px; font-weight: 700;
                        color: #f1f5f9; margin: 2px 0 1px; line-height: 1; }
.metric-box .mb-sub   { font-size: 11px; color: var(--c-text-dim); }
.metric-box.warn  .mb-val { color: var(--c-amber); }
.metric-box.error .mb-val { color: var(--c-red);   }
.metric-box.ok    .mb-val { color: var(--c-green);  }

/* ── 區塊標題 ── */
.sec-header {
    font-size: 10.5px;
    font-weight: 700;
    letter-spacing: 0.09em;
    text-transform: uppercase;
    color: var(--c-text-dim);
    padding: 18px 0 7px 0;
    border-bottom: 1px solid var(--c-border);
    margin-bottom: 12px;
}

/* expander header 字體稍小 */
summary, .streamlit-expanderHeader p {
    font-size: 13.5px !important;
}
</style>
""", unsafe_allow_html=True)

# ══ 頁首 ══════════════════════════════════════════════════════════
h_left, h_right = st.columns([5, 2])
h_left.title("🗄️ 資料管理")
with h_right:
    st.markdown("<br>", unsafe_allow_html=True)
    auto_refresh = st.toggle("🔄 自動刷新（每 5 秒）", value=False)

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
    _finmind_ok   = (_ltd == date.today())

    if _yb_prog:
        bd, bt = s.get("yahoo_bridge_batch_done", 0), s.get("yahoo_bridge_batch_total", 0)
        _cls, _icon = "info", "🔵"
        _main = f"Yahoo Bridge 抓取中（批次 {bd}/{bt}）"
        _sub  = f"從 Yahoo Finance 批次補充今日收盤資料，共 {s.get('yahoo_bridge_total',0)} 檔待抓"
    elif _finmind_ok and s.get("supplementary_completed_at"):
        _sup = s["supplementary_completed_at"]
        _cls, _icon = "ok", "🟢"
        _main = f"今日資料全部更新完成（{_sup.strftime('%H:%M')}）"
        _sub  = "核心 OHLCV + 法人 + 融資融券 皆已就緒"
    elif _yb_done and not _finmind_ok and datetime.now().hour >= 15:
        _cls, _icon = "info", "🔵"
        _main = "Yahoo Bridge 已完成　等待 FinMind 盤後更新"
        _sub  = f"核心資料由 Yahoo 補充（基準日 {_ltd}），法人/融資待 FinMind 上線後補充"
    elif not _finmind_ok and datetime.now().hour >= 15:
        _cls, _icon = "info", "🔵"
        _main = "等待 FinMind 更新今日資料"
        _sub  = "FinMind 通常 15:30–19:00 發布，Worker 每輪自動重查，無需手動操作"
    else:
        _cls, _icon = "ok", "🟢"
        _main = "工作器正常運行中"
        _sub  = "盤後/非交易時間，每小時上限 600 次"

    if _cur_stock and not _yb_prog:
        _sub += f"　｜　正在抓取：{_cur_stock}"

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
        "normal":     ("✅", "已更新",    "#4ade80"),
        "ok":         ("✅", "成功",      "#4ade80"),
        "cached":     ("⏭",  "快取命中",  "rgba(255,255,255,0.3)"),
        "suspended":  ("⚠️", "暫無資料", "#fbbf24"),
        "no_update":  ("⚠️", "無資料",   "#fbbf24"),
        "delisted":   ("🚫", "已下市",   "rgba(255,255,255,0.3)"),
        "rate_limit": ("🚫", "429 限流", "#f87171"),
        "error":      ("❌", "錯誤",     "#f87171"),
    }
    _r_icon, _r_label, _r_color = _result_map.get(
        _la_result, ("—", _la_result or "—", "rgba(255,255,255,0.35)")
    )

    # 時間
    _el = int((datetime.now() - _la_at).total_seconds())
    _ago = f"{_el}s 前" if _el < 60 else f"{_el//60}m{_el%60:02d}s 前"
    _time_str = _la_at.strftime("%H:%M:%S")

    st.markdown(f"""
<div style="display:flex; align-items:center; gap:14px; padding:5px 2px 10px 2px;
            font-size:12px; color:rgba(255,255,255,0.4); flex-wrap:wrap;">
  <span>最近嘗試</span>
  <span style="color:{_src_color}; font-weight:600;">{_src_label}</span>
  <span style="color:rgba(255,255,255,0.75); font-family:monospace;">{_clean_stock}</span>
  <span>{_time_str}（{_ago}）</span>
  <span style="color:{_r_color};">{_r_icon} {_r_label}</span>
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

# ── 關鍵指標（4格）────────────────────────────────────────────────
# 即時待更新數
try:
    from db.price_cache import get_suspended_stocks
    from data.finmind_client import resolve_latest_trading_day
    _ltd2   = resolve_latest_trading_day()
    _smr    = get_cache_summary()
    _skip2  = set(get_delisted_stocks(include_legacy_no_update=True))
    _susp2  = set(get_suspended_stocks(today_only=True))
    if _smr.empty:
        _pending = len([x for x in get_known_stock_ids() if x not in _skip2])
    else:
        _stale2  = set(_smr.loc[_smr["latest"] < _ltd2.isoformat(), "stock_id"]) - _skip2 - _susp2
        _miss2   = set(get_known_stock_ids()) - set(_smr["stock_id"]) - _skip2
        _pending = len(_stale2) + len(_miss2)
except Exception:
    _pending = s["queue_size"]

elapsed_str = "—"
if s["last_fetch_at"]:
    el = int((datetime.now() - s["last_fetch_at"]).total_seconds())
    elapsed_str = f"{el}s 前" if el < 60 else f"{el//60}m 前"

_pend_cls  = "warn"  if _pending  > 50 else ""
_rate_cls  = "warn"  if s.get("rate_limit_count", 0) > 0 else ""
_total_cls = "ok"    if s["total_fetched"] > 0 else ""

st.markdown(f"""
<div class="metric-grid">
  <div class="metric-box">
    <div class="mb-label">本小時用量</div>
    <div class="mb-val">{s['hour_fetched']}</div>
    <div class="mb-sub">上限 {s['hourly_limit']} 次 / 小時</div>
  </div>
  <div class="metric-box {_pend_cls}">
    <div class="mb-label">待更新股票</div>
    <div class="mb-val">{_pending}</div>
    <div class="mb-sub">Worker 快照 {s['queue_size']} 檔</div>
  </div>
  <div class="metric-box {_total_cls}">
    <div class="mb-label">累計抓取</div>
    <div class="mb-val">{s['total_fetched']}</div>
    <div class="mb-sub">最近：{elapsed_str}</div>
  </div>
  <div class="metric-box {_rate_cls}">
    <div class="mb-label">429 次數</div>
    <div class="mb-val">{s.get('rate_limit_count', 0)}</div>
    <div class="mb-sub">已略過 {s.get('skip_count', 0)} 檔</div>
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

# ══ 區塊 2：今日資料進度 ══════════════════════════════════════════
if date.today().weekday() < 5 and not s.get("backtest_rebuild_mode"):
    st.markdown('<div class="sec-header">今日資料進度</div>', unsafe_allow_html=True)
    today_str = date.today().strftime("%Y-%m-%d")

    try:
        _skip_ids     = set(get_delisted_stocks(include_legacy_no_update=True))
        _active_total = max(len(set(get_known_stock_ids()) - _skip_ids), 1)
    except Exception:
        _active_total = 950

    try:
        with get_session() as _sess:
            _core_done = _sess.execute(
                _sqla_text("SELECT COUNT(DISTINCT stock_id) FROM price_cache WHERE date=:d"),
                {"d": today_str}).fetchone()[0]
    except Exception:
        _core_done = 0

    try:
        with get_session() as _sess:
            _inst_done = _sess.execute(
                _sqla_text("SELECT COUNT(DISTINCT stock_id) FROM inst_cache WHERE date=:d"),
                {"d": today_str}).fetchone()[0]
    except Exception:
        _inst_done = 0

    try:
        _m_stats    = get_margin_cache_stats(date.today())
        _margin_done = _m_stats["done_today"]
    except Exception:
        _margin_done = 0

    _inst_no_upd   = s.get("inst_no_update_count", 0)
    _margin_no_upd = s.get("margin_no_update_count", 0)
    _inst_total    = max(_active_total - _inst_no_upd, 1)
    _margin_total  = max(_active_total - _margin_no_upd, 1)
    _core_pct      = _core_done / _active_total if _active_total else 0
    _finmind_updated = (s.get("latest_trading_day") == date.today())
    _is_waiting_now  = (not _finmind_updated and datetime.now().hour >= 15)

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

    _supp_note = (
        "⏳ 等待 FinMind 盤後更新（通常 17–19 時）" if _is_waiting_now
        else ("等待核心資料完成" if _core_pct < 0.5 else "")
    )

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

# ══ 區塊 6：系統設定 ══════════════════════════════════════════════
with st.expander("⚙️ 系統設定", expanded=False):
    sg1, sg2 = st.columns(2)

    with sg1:
        _mc = is_market_closed()
        new_mc = st.toggle(
            "🏖️ 休市模式",
            value=_mc,
            help="開啟後法人資料快取永不過期，完全不消耗 API 額度。適用於連假、停市期間。",
        )
        if new_mc != _mc:
            set_market_closed(new_mc); st.rerun()
        if new_mc:
            st.caption("**啟用中** — 法人資料使用快取，不重新呼叫 API。恢復交易後請關閉。")
            stats_mc = get_inst_cache_stats()
            if stats_mc["stock_count"] > 0 and stats_mc["newest_fetch"]:
                st.caption(f"法人快取：**{stats_mc['stock_count']}** 檔，"
                            f"最新 {str(stats_mc['newest_fetch'])[:16]}")
            if st.button("🗑️ 清除法人快取", help="清除後下次掃描將重新從 API 抓取"):
                from sqlalchemy import text
                from db.database import get_session
                with get_session() as sess:
                    sess.execute(text("DELETE FROM inst_cache")); sess.commit()
                st.success("法人快取已清除"); st.rerun()
        else:
            st.caption("正常模式 — 法人資料快取 24 小時後自動更新。"
                        "若為連假/停市建議開啟以避免浪費 API 額度。")

    with sg2:
        _fy = get_force_yahoo()
        new_fy = st.toggle(
            "🔀 強制 Yahoo Finance",
            value=_fy,
            help="FinMind 異常時手動切換至 Yahoo Finance。三大法人條件自動停用。",
        )
        if new_fy != _fy:
            set_force_yahoo(new_fy); st.rerun()
        if new_fy:
            st.caption("**啟用中** — 選股掃描與持股監控改用 Yahoo Finance 取價。"
                        "⚠️ 三大法人條件停用；價格可能有 15 分鐘延遲。"
                        "FinMind 恢復後請關閉。")
        else:
            st.caption("正常模式 — 使用 FinMind 取價（本機快取優先）。")

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
