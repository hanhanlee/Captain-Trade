"""
交易計畫頁
- 建立買進/賣出計畫，自動跑風控檢查
- 停損與理由必填；違反規則顯示紅色警告
- 確認後寫入交易日誌並移至待執行清單
"""
import json
import streamlit as st
import pandas as pd
from datetime import datetime

from db.database import init_db
from modules.trade_plan import (
    check_trade_rules,
    create_plan,
    get_pending_plans,
    get_all_plans,
    execute_plan,
    cancel_plan,
    RULES,
)

st.set_page_config(page_title="交易計畫", page_icon="📋", layout="wide")
init_db()

st.title("📋 交易計畫")
st.markdown("*「沒有計畫的進場，是最貴的衝動。」*")
st.markdown("---")

# ── Sidebar：帳戶設定 ─────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ 帳戶設定")
    account_size = st.number_input(
        "帳戶總資金（元）",
        min_value=100_000, max_value=100_000_000,
        value=1_000_000, step=100_000, format="%d",
    )
    st.caption("此數值用於單筆風險計算")

tab_new, tab_pending, tab_history = st.tabs(["➕ 新建計畫", "⏳ 待執行清單", "📜 歷史計畫"])


# ══ Tab：新建計畫 ═════════════════════════════════════════════════════════════
with tab_new:
    col_form, col_check = st.columns([1, 1], gap="large")

    with col_form:
        st.subheader("填寫計畫")

        direction = st.radio("方向", ["BUY 買進", "SELL 放空"], horizontal=True)
        direction_val = direction.split()[0]

        c1, c2 = st.columns(2)
        stock_id = c1.text_input("股票代碼 *", placeholder="e.g. 2330").strip()
        stock_name = c2.text_input("股票名稱（選填）", placeholder="e.g. 台積電")

        entry_price = st.number_input("預計進場價（元）*", min_value=0.01, value=100.0, step=0.5)

        stop_loss = st.number_input(
            "停損價（元）* 必填",
            min_value=0.01,
            value=round(entry_price * (0.92 if direction_val == "BUY" else 1.08), 1),
            step=0.5,
        )

        target_price_raw = st.number_input(
            "目標價（元，選填）",
            min_value=0.0,
            value=round(entry_price * (1.15 if direction_val == "BUY" else 0.85), 1),
            step=0.5,
        )
        target_price = target_price_raw if target_price_raw > 0 else None

        shares = st.number_input("預計張數 *", min_value=1, value=1, step=1)

        reason = st.text_area(
            "進場理由 * 必填（至少 20 字）",
            placeholder="說明技術面/基本面依據、停損設定邏輯……",
            height=120,
        )

    # ── 即時風控預覽 ─────────────────────────────────────────────────────────
    with col_check:
        st.subheader("風控自動檢查")

        rule_results = check_trade_rules(
            entry_price=entry_price,
            stop_loss=stop_loss,
            target_price=target_price,
            shares=shares,
            account_size=account_size,
            reason=reason,
            direction=direction_val,
        )

        blocking_fail = any(r["blocking"] and not r["pass"] for r in rule_results)
        warning_fail  = any(not r["blocking"] and not r["pass"] for r in rule_results)

        for r in rule_results:
            if r["pass"]:
                icon = "✅"
                color = "#1a7a3f"
                bg = "rgba(26,122,63,0.08)"
                border = "rgba(26,122,63,0.4)"
            elif r["blocking"]:
                icon = "🚫"
                color = "#c0392b"
                bg = "rgba(192,57,43,0.10)"
                border = "#c0392b"
            else:
                icon = "⚠️"
                color = "#e67e22"
                bg = "rgba(230,126,34,0.10)"
                border = "#e67e22"

            st.markdown(
                f"""
                <div style="
                    border-left: 4px solid {border};
                    background: {bg};
                    padding: 8px 12px;
                    margin-bottom: 8px;
                    border-radius: 4px;
                ">
                  <span style="font-weight:600;color:{color};">{icon} {r['name']}</span><br>
                  <span style="font-size:0.88em;color:var(--text-color);opacity:0.85;">{r['message']}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown("---")

        # 說明
        if blocking_fail:
            st.error("🚫 **強制規則未通過**，請修正後才能送出計畫。")
        elif warning_fail:
            st.warning("⚠️ 有警告規則未通過，請確認後勾選同意再送出。")
        else:
            st.success("✅ 所有風控規則通過！")

    # ── 送出按鈕區 ───────────────────────────────────────────────────────────
    st.markdown("---")

    acknowledged = False
    if warning_fail and not blocking_fail:
        acknowledged = st.checkbox(
            "⚠️ 我已知悉上述風險警告，仍決定按計畫進場",
            value=False,
        )

    can_submit = (
        bool(stock_id)
        and not blocking_fail
        and (not warning_fail or acknowledged)
    )

    if st.button("✅ 確認送出計畫", type="primary", disabled=not can_submit):
        plan = create_plan(
            stock_id=stock_id,
            stock_name=stock_name,
            direction=direction_val,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target_price=target_price,
            shares=shares,
            reason=reason,
            account_size=account_size,
        )
        st.success(f"計畫 #{plan.id if plan else '?'} 已建立，進入「待執行清單」。")
        st.rerun()

    if not stock_id:
        st.caption("請填入股票代碼")


# ══ Tab：待執行清單 ═══════════════════════════════════════════════════════════
with tab_pending:
    pending = get_pending_plans()

    if not pending:
        st.info("目前沒有待執行的計畫。")
    else:
        st.caption(f"共 {len(pending)} 筆待執行計畫")

        for plan in pending:
            rules = json.loads(plan.risk_check_json or "[]")
            fail_names = [r["name"] for r in rules if not r["pass"]]
            rr_str = "—"
            for r in rules:
                if r["id"] == "R3" and r.get("value") is not None:
                    rr_str = f"{r['value']:.2f}"

            with st.expander(
                f"{'🟢' if not plan.has_violation else '🟡'} "
                f"#{plan.id}  {plan.stock_id} {plan.stock_name}  "
                f"{plan.direction}  進場 {plan.entry_price:.2f}  停損 {plan.stop_loss:.2f}  "
                f"{plan.shares} 張  ｜  {plan.created_at.strftime('%Y-%m-%d %H:%M')}",
                expanded=False,
            ):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("進場價", f"{plan.entry_price:.2f}")
                c2.metric("停損價", f"{plan.stop_loss:.2f}")
                c3.metric("目標價", f"{plan.target_price:.2f}" if plan.target_price else "—")
                c4.metric("RR 比", rr_str)

                st.markdown(f"**進場理由：** {plan.reason}")

                if fail_names:
                    st.warning("⚠️ 送出時有警告：" + "、".join(fail_names))

                st.markdown("---")
                col_exec, col_cancel, col_price = st.columns([1, 1, 2])

                actual_price = col_price.number_input(
                    "實際成交價（留空用計畫價）",
                    min_value=0.0, value=0.0, step=0.5,
                    key=f"exec_price_{plan.id}",
                )
                if col_exec.button("▶ 執行 → 寫入日誌", key=f"exec_{plan.id}", type="primary"):
                    journal_id = execute_plan(
                        plan.id,
                        actual_price=actual_price if actual_price > 0 else None,
                    )
                    st.success(f"已執行！交易日誌 #{journal_id}")
                    st.rerun()

                if col_cancel.button("✕ 取消計畫", key=f"cancel_{plan.id}"):
                    cancel_plan(plan.id)
                    st.info("計畫已取消。")
                    st.rerun()


# ══ Tab：歷史計畫 ═════════════════════════════════════════════════════════════
with tab_history:
    all_plans = get_all_plans()
    non_pending = [p for p in all_plans if p.status != "pending"]

    if not non_pending:
        st.info("尚無歷史計畫記錄。")
    else:
        rows = []
        for p in non_pending:
            rules = json.loads(p.risk_check_json or "[]")
            rr_val = next((r.get("value") for r in rules if r["id"] == "R3"), None)
            rows.append({
                "ID":       p.id,
                "代碼":     p.stock_id,
                "名稱":     p.stock_name,
                "方向":     p.direction,
                "進場價":   p.entry_price,
                "停損價":   p.stop_loss,
                "目標價":   p.target_price,
                "RR 比":    round(rr_val, 2) if rr_val else None,
                "張數":     p.shares,
                "狀態":     p.status,
                "有警告":   "是" if p.has_violation else "否",
                "建立時間": p.created_at.strftime("%Y-%m-%d %H:%M") if p.created_at else "",
                "執行時間": p.executed_at.strftime("%Y-%m-%d %H:%M") if p.executed_at else "",
                "日誌ID":   p.journal_id,
            })

        df = pd.DataFrame(rows)

        def _style_status(val):
            if val == "executed":
                return "color: #27ae60; font-weight: 600"
            if val == "cancelled":
                return "color: #7f8c8d"
            return ""

        def _style_violation(val):
            return "color: #e74c3c; font-weight:600" if val == "是" else ""

        styled = df.style.map(_style_status, subset=["狀態"]).map(_style_violation, subset=["有警告"])
        st.dataframe(styled, use_container_width=True, hide_index=True)
