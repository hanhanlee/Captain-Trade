"""
問題回報 / 功能建議

讓使用者把遇到的問題或想新增的功能整理成 Markdown，
存放到專案的 user_reports 目錄，方便後續集中處理。
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import streamlit as st

from db.database import init_db
from modules.auth import current_user, require_login


st.set_page_config(page_title="問題回報", page_icon="📝", layout="wide")
require_login()
init_db()


REPORT_DIR = Path(__file__).resolve().parents[1] / "user_reports"
PAGE_OPTIONS = [
    "首頁",
    "1 - 選股雷達",
    "2 - 持股監控",
    "3 - 風險控制",
    "4 - 市場環境",
    "5 - 交易日誌",
    "6 - 回測模組",
    "6 - 資料管理",
    "7 - 個股分析",
    "8 - 問題回報",
    "其他",
]


def _slug(value: str) -> str:
    text = re.sub(r"\s+", "-", value.strip().lower())
    text = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff_-]+", "", text)
    return text[:40] or "report"


def _markdown_value(value: str) -> str:
    value = (value or "").strip()
    return value if value else "未填寫"


def _build_issue_report(page: str, description: str, expected: str, actual: str) -> str:
    now = datetime.now()
    return f"""# 問題回報：{page}

- 類型：問題回報
- 發生頁面：{page}
- 回報時間：{now:%Y-%m-%d %H:%M:%S}
- 回報者：{current_user() or "unknown"}

## 複製步驟 / 問題描述

{_markdown_value(description)}

## 預期結果

{_markdown_value(expected)}

## 實際結果

{_markdown_value(actual)}
"""


def _build_feature_report(page: str, description: str) -> str:
    now = datetime.now()
    return f"""# 功能建議：{page}

- 類型：功能新增
- 建議頁面：{page}
- 回報時間：{now:%Y-%m-%d %H:%M:%S}
- 回報者：{current_user() or "unknown"}

## 功能描述

{_markdown_value(description)}
"""


def _save_report(kind: str, page: str, content: str) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    prefix = "bug" if kind == "問題回報" else "feature"
    filename = f"{datetime.now():%Y%m%d_%H%M%S}_{prefix}_{_slug(page)}_{uuid4().hex[:6]}.md"
    path = REPORT_DIR / filename
    path.write_text(content, encoding="utf-8")
    return path


st.title("📝 問題回報 / 功能建議")
st.caption("送出後會存成 Markdown 檔，放在專案的 user_reports 資料夾。")

report_kind = st.radio(
    "請選擇回報類型",
    ["問題回報", "功能新增"],
    horizontal=True,
)

if report_kind == "問題回報":
    with st.form("issue_report_form", clear_on_submit=True):
        page = st.selectbox("哪一個頁面發生問題？", PAGE_OPTIONS)
        description = st.text_area(
            "複製步驟 / 問題描述 *",
            height=180,
            placeholder="例：進入持股監控 → 點立即測試 → 6116 沒有跳出虧損警示",
        )
        expected = st.text_area("預期結果 *", height=120)
        actual = st.text_area("實際結果 *", height=120)
        submitted = st.form_submit_button("送出問題回報", type="primary", use_container_width=True)

    if submitted:
        if not description.strip() or not expected.strip() or not actual.strip():
            st.error("請填寫問題描述、預期結果與實際結果。")
        else:
            report = _build_issue_report(page, description, expected, actual)
            path = _save_report(report_kind, page, report)
            st.success(f"已建立問題回報：{path.name}")
            st.code(str(path), language="text")

else:
    with st.form("feature_report_form", clear_on_submit=True):
        page = st.selectbox("想新增哪個頁面的功能？", PAGE_OPTIONS)
        description = st.text_area(
            "功能描述 *",
            height=220,
            placeholder="例：希望資料管理頁能顯示每個 Sponsor dataset 的補完百分比",
        )
        submitted = st.form_submit_button("送出功能建議", type="primary", use_container_width=True)

    if submitted:
        if not description.strip():
            st.error("請填寫功能描述。")
        else:
            report = _build_feature_report(page, description)
            path = _save_report(report_kind, page, report)
            st.success(f"已建立功能建議：{path.name}")
            st.code(str(path), language="text")

st.markdown("---")
st.info("這個頁面只負責保存文字回報，不會自動發送到外部服務。")
