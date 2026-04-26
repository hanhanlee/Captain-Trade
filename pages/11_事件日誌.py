"""
事件日誌頁面
- 查詢 event_log：依日期、股票、事件類型、模組篩選
- 展開 payload_json 查看詳細內容
- 匯出 CSV / JSON 供 AI 分析
"""
import json
import io
import streamlit as st
import pandas as pd
from datetime import date, timedelta

from db.database import init_db
from db.event_log import query_events, count_events, get_scan_timeline

st.set_page_config(page_title="事件日誌", page_icon="📋", layout="wide")
init_db()

st.title("📋 事件日誌")
st.caption("記錄選股、警示、風控、交易計畫、推播的完整決策時間線，供 AI 分析使用。")
st.markdown("---")

EVENT_TYPE_OPTIONS = [
    "（全部）",
    "scan_completed",
    "stock_selected",
    "near_miss",
    "alert_triggered",
    "notification_sent",
    "trade_plan_created",
    "risk_check_passed",
    "risk_check_failed",
    "trade_executed",
    "user_cancelled",
]

MODULE_OPTIONS = ["（全部）", "scanner", "portfolio", "trade_plan", "scheduler"]
SEVERITY_OPTIONS = ["（全部）", "info", "warning", "danger"]

# ── 篩選列 ────────────────────────────────────────────────────────────────────
col_date, col_type, col_stock, col_mod, col_sev = st.columns([2, 2, 1.5, 1.5, 1.5])

with col_date:
    date_range = st.date_input(
        "日期範圍",
        value=(date.today() - timedelta(days=30), date.today()),
        max_value=date.today(),
    )

with col_type:
    event_type_sel = st.selectbox("事件類型", EVENT_TYPE_OPTIONS)

with col_stock:
    stock_id_input = st.text_input("股票代碼", placeholder="e.g. 2330").strip()

with col_mod:
    module_sel = st.selectbox("模組", MODULE_OPTIONS)

with col_sev:
    severity_sel = st.selectbox("嚴重度", SEVERITY_OPTIONS)

# 解析日期範圍
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    _date_from, _date_to = str(date_range[0]), str(date_range[1])
else:
    _date_from = _date_to = str(date.today())

_params = {
    "event_type": None if event_type_sel.startswith("（") else event_type_sel,
    "module": None if module_sel.startswith("（") else module_sel,
    "stock_id": stock_id_input or None,
    "severity": None if severity_sel.startswith("（") else severity_sel,
    "date_from": _date_from,
    "date_to": _date_to,
}

# ── 分頁設定 ──────────────────────────────────────────────────────────────────
PAGE_SIZE = 100
total = count_events(**{k: v for k, v in _params.items()})

col_info, col_page = st.columns([3, 1])
with col_info:
    st.caption(f"共 **{total}** 筆事件")
with col_page:
    max_page = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page_num = st.number_input("頁碼", min_value=1, max_value=max_page, value=1, step=1)

events = query_events(**_params, limit=PAGE_SIZE, offset=(page_num - 1) * PAGE_SIZE)

# ── 主表格 ────────────────────────────────────────────────────────────────────
if not events:
    st.info("此條件無事件記錄。")
else:
    _display_rows = []
    for e in events:
        _display_rows.append({
            "時間": e["created_at"],
            "事件類型": e["event_type"],
            "模組": e["module"] or "",
            "股票": (e["stock_id"] or "") + (" " + (e["stock_name"] or "") if e["stock_name"] else ""),
            "嚴重度": e["severity"] or "info",
            "摘要": e["summary"] or "",
            "scan_id": e["scan_id"] or "",
        })

    _df = pd.DataFrame(_display_rows)

    # 嚴重度著色
    def _color_severity(val):
        return {
            "danger": "color: #e74c3c; font-weight:bold",
            "warning": "color: #f39c12",
        }.get(val, "")

    st.dataframe(
        _df.style.applymap(_color_severity, subset=["嚴重度"]),
        use_container_width=True,
        height=420,
    )

    # ── 展開查看 payload ───────────────────────────────────────────────────────
    st.markdown("#### 展開查看 payload")
    _event_labels = [
        f"[{e['created_at']}] {e['event_type']} {e['stock_id'] or ''} {e['summary'] or ''}"
        for e in events
    ]
    _selected_idx = st.selectbox("選擇事件", range(len(events)), format_func=lambda i: _event_labels[i])
    if _selected_idx is not None:
        _sel_event = events[_selected_idx]
        c1, c2 = st.columns(2)
        with c1:
            st.json({k: v for k, v in _sel_event.items() if k != "payload_json"})
        with c2:
            try:
                _payload = json.loads(_sel_event.get("payload_json") or "{}")
            except Exception:
                _payload = {"raw": _sel_event.get("payload_json", "")}
            st.json(_payload)

    # ── scan_id 鑽取 ──────────────────────────────────────────────────────────
    _scan_ids = sorted({e["scan_id"] for e in events if e.get("scan_id")}, reverse=True)
    if _scan_ids:
        st.markdown("#### 掃描批次時間線")
        _selected_scan = st.selectbox("選擇掃描批次（scan_id）", _scan_ids)
        if _selected_scan:
            _timeline = get_scan_timeline(_selected_scan)
            if _timeline:
                _tl_rows = []
                for e in _timeline:
                    _tl_rows.append({
                        "時間": e["created_at"],
                        "事件": e["event_type"],
                        "股票": (e["stock_id"] or "") + (" " + (e["stock_name"] or "") if e["stock_name"] else ""),
                        "摘要": e["summary"] or "",
                    })
                st.dataframe(pd.DataFrame(_tl_rows), use_container_width=True, height=300)

st.markdown("---")

# ── 匯出區 ────────────────────────────────────────────────────────────────────
st.subheader("📤 匯出供 AI 分析")
st.caption(
    "將篩選後的事件匯出成 CSV 或 JSON，可直接上傳到 Claude / ChatGPT 進行分析。"
    "匯出時 payload_json 會展開成獨立欄位（CSV）或完整保留（JSON）。"
)

_export_limit = st.number_input(
    "最多匯出筆數（0 = 全部，最多 10,000）",
    min_value=0, max_value=10000, value=0, step=100,
)
_export_expand = st.checkbox("展開 payload_json 欄位（CSV 模式，方便 AI 逐欄分析）", value=True)

col_csv, col_json, col_jsonl = st.columns(3)

def _fetch_export_events() -> list[dict]:
    lim = int(_export_limit) if _export_limit > 0 else 10000
    return query_events(**_params, limit=lim, offset=0)


def _build_export_df(raw: list[dict], expand: bool) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    rows = []
    for e in raw:
        row = {
            "id": e["id"],
            "created_at": e["created_at"],
            "event_type": e["event_type"],
            "module": e["module"] or "",
            "scan_id": e["scan_id"] or "",
            "stock_id": e["stock_id"] or "",
            "stock_name": e["stock_name"] or "",
            "severity": e["severity"] or "info",
            "summary": e["summary"] or "",
        }
        if expand:
            try:
                payload = json.loads(e.get("payload_json") or "{}")
                for k, v in payload.items():
                    # 巢狀 dict 轉 json string，其餘直接放
                    row[f"payload.{k}"] = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
            except Exception:
                row["payload_json"] = e.get("payload_json", "")
        else:
            row["payload_json"] = e.get("payload_json", "")
        rows.append(row)
    return pd.DataFrame(rows)


with col_csv:
    if st.button("⬇️ 下載 CSV", use_container_width=True):
        _raw = _fetch_export_events()
        _edf = _build_export_df(_raw, _export_expand)
        if _edf.empty:
            st.warning("無資料可匯出")
        else:
            _csv_buf = io.StringIO()
            _edf.to_csv(_csv_buf, index=False, encoding="utf-8-sig")
            st.download_button(
                label=f"📥 儲存 CSV（{len(_edf)} 筆）",
                data=_csv_buf.getvalue().encode("utf-8-sig"),
                file_name=f"event_log_{_date_from}_{_date_to}.csv",
                mime="text/csv",
            )

with col_json:
    if st.button("⬇️ 下載 JSON", use_container_width=True):
        _raw = _fetch_export_events()
        if not _raw:
            st.warning("無資料可匯出")
        else:
            _json_rows = []
            for e in _raw:
                _row = {k: v for k, v in e.items() if k != "payload_json"}
                try:
                    _row["payload"] = json.loads(e.get("payload_json") or "{}")
                except Exception:
                    _row["payload"] = {}
                _json_rows.append(_row)
            _json_str = json.dumps(_json_rows, ensure_ascii=False, indent=2)
            st.download_button(
                label=f"📥 儲存 JSON（{len(_json_rows)} 筆）",
                data=_json_str.encode("utf-8"),
                file_name=f"event_log_{_date_from}_{_date_to}.json",
                mime="application/json",
            )

with col_jsonl:
    if st.button("⬇️ 下載 JSONL（AI 友善）", use_container_width=True):
        _raw = _fetch_export_events()
        if not _raw:
            st.warning("無資料可匯出")
        else:
            _lines = []
            for e in _raw:
                _row = {k: v for k, v in e.items() if k != "payload_json"}
                try:
                    _row["payload"] = json.loads(e.get("payload_json") or "{}")
                except Exception:
                    _row["payload"] = {}
                _lines.append(json.dumps(_row, ensure_ascii=False))
            _jsonl_str = "\n".join(_lines)
            st.download_button(
                label=f"📥 儲存 JSONL（{len(_lines)} 筆）",
                data=_jsonl_str.encode("utf-8"),
                file_name=f"event_log_{_date_from}_{_date_to}.jsonl",
                mime="application/x-ndjson",
            )

st.markdown("---")
st.caption(
    "**AI 分析提示：** 下載後可將檔案上傳到 Claude 或 ChatGPT，詢問例如：\n\n"
    "- 「這批 stock_selected 事件中，score 最高的股票後來表現如何？」\n"
    "- 「哪些 risk_check_failed 事件的失敗原因最常見？」\n"
    "- 「近 30 天有哪些 alert_triggered 事件是 danger 等級？」"
)
