"""
交易計畫模組
- 建立買進/賣出計畫
- 自動風控規則檢查（5 條）
- 執行計畫時寫入交易日誌
"""
import json
from datetime import date, datetime
from db.database import get_session
from db.models import TradePlan
from modules.journal import add_trade
from db.models import TradeJournal


# ── 風控規則定義 ──────────────────────────────────────────────────────────────

RULES = [
    {"id": "R1", "name": "停損價必填且合理",       "blocking": True},
    {"id": "R2", "name": "停損距離 ≤ 8%",          "blocking": False},
    {"id": "R3", "name": "報酬風險比 ≥ 2.0",       "blocking": False},
    {"id": "R4", "name": "單筆風險 ≤ 帳戶 2%",     "blocking": False},
    {"id": "R5", "name": "進場理由（≥ 20 字）",    "blocking": True},
]


def check_trade_rules(
    entry_price: float,
    stop_loss: float | None,
    target_price: float | None,
    shares: int,
    account_size: float,
    reason: str,
    direction: str = "BUY",
) -> list[dict]:
    """
    回傳 5 條規則的檢查結果，每筆含：
      id / name / pass / blocking / message / value
    """
    results = []

    # R1：停損必填且合理（BUY: stop < entry；SELL: stop > entry）
    r1_pass = False
    r1_msg = "未設定停損，禁止建倉"
    if stop_loss and stop_loss > 0:
        if direction == "BUY":
            r1_pass = stop_loss < entry_price
            r1_msg = "停損價需低於進場價" if not r1_pass else f"停損 {stop_loss:.2f}"
        else:
            r1_pass = stop_loss > entry_price
            r1_msg = "放空停損價需高於進場價" if not r1_pass else f"停損 {stop_loss:.2f}"
    results.append({"id": "R1", "name": "停損價必填且合理",
                    "pass": r1_pass, "blocking": True, "message": r1_msg})

    # R2：停損距離 ≤ 8%
    if stop_loss and stop_loss > 0 and entry_price > 0:
        stop_pct = abs(entry_price - stop_loss) / entry_price * 100
        r2_pass = stop_pct <= 8.0
        r2_msg = f"停損距離 {stop_pct:.1f}%（建議 ≤ 8%）"
    else:
        stop_pct = None
        r2_pass = False
        r2_msg = "請先填入停損價"
    results.append({"id": "R2", "name": "停損距離 ≤ 8%",
                    "pass": r2_pass, "blocking": False, "message": r2_msg,
                    "value": stop_pct})

    # R3：RR 比 ≥ 2.0（只在有目標價且停損有效時檢查）
    if (target_price and target_price > 0 and stop_loss and stop_loss > 0
            and abs(entry_price - stop_loss) > 0):
        if direction == "BUY":
            rr = (target_price - entry_price) / (entry_price - stop_loss)
        else:
            rr = (entry_price - target_price) / (stop_loss - entry_price)
        r3_pass = rr >= 2.0
        r3_msg = f"RR 比 {rr:.2f}（建議 ≥ 2.0）"
    else:
        rr = None
        r3_pass = True   # 未填目標價則略過此規則（不強制）
        r3_msg = "未填目標價，略過 RR 比檢查"
    results.append({"id": "R3", "name": "報酬風險比 ≥ 2.0",
                    "pass": r3_pass, "blocking": False, "message": r3_msg,
                    "value": rr})

    # R4：單筆最大虧損 ≤ 帳戶 2%
    if stop_loss and stop_loss > 0 and shares > 0 and account_size > 0:
        risk_amount = abs(entry_price - stop_loss) * shares * 1000
        risk_pct = risk_amount / account_size * 100
        r4_pass = risk_pct <= 2.0
        r4_msg = f"最大虧損 {risk_amount:,.0f} 元（帳戶 {risk_pct:.1f}%，上限 2%）"
    else:
        risk_pct = None
        r4_pass = True
        r4_msg = "請填入停損與張數"
    results.append({"id": "R4", "name": "單筆風險 ≤ 帳戶 2%",
                    "pass": r4_pass, "blocking": False, "message": r4_msg,
                    "value": risk_pct})

    # R5：進場理由 ≥ 20 字
    reason_len = len((reason or "").strip())
    r5_pass = reason_len >= 20
    r5_msg = f"已填 {reason_len} 字（最少 20 字）"
    results.append({"id": "R5", "name": "進場理由（≥ 20 字）",
                    "pass": r5_pass, "blocking": True, "message": r5_msg})

    return results


# ── CRUD ─────────────────────────────────────────────────────────────────────

def create_plan(
    stock_id: str,
    stock_name: str,
    direction: str,
    entry_price: float,
    stop_loss: float,
    shares: int,
    reason: str,
    account_size: float,
    target_price: float | None = None,
) -> TradePlan:
    rule_results = check_trade_rules(
        entry_price=entry_price,
        stop_loss=stop_loss,
        target_price=target_price,
        shares=shares,
        account_size=account_size,
        reason=reason,
        direction=direction,
    )
    has_violation = any(not r["pass"] for r in rule_results)

    with get_session() as sess:
        plan = TradePlan(
            stock_id=stock_id.strip(),
            stock_name=(stock_name or "").strip(),
            direction=direction.upper(),
            entry_price=entry_price,
            stop_loss=stop_loss,
            target_price=target_price,
            shares=shares,
            reason=reason.strip(),
            status="pending",
            risk_check_json=json.dumps(rule_results, ensure_ascii=False),
            has_violation=has_violation,
        )
        sess.add(plan)
        sess.flush()
        plan_id = plan.id
    return get_plan(plan_id)


def get_plan(plan_id: int) -> TradePlan | None:
    with get_session() as sess:
        return sess.get(TradePlan, plan_id)


def get_pending_plans() -> list[TradePlan]:
    with get_session() as sess:
        return sess.query(TradePlan).filter_by(status="pending").order_by(
            TradePlan.created_at.desc()).all()


def get_all_plans() -> list[TradePlan]:
    with get_session() as sess:
        return sess.query(TradePlan).order_by(TradePlan.created_at.desc()).all()


def execute_plan(plan_id: int, actual_price: float | None = None) -> int:
    """執行計畫：寫入交易日誌，回傳 journal_id。"""
    with get_session() as sess:
        plan = sess.get(TradePlan, plan_id)
        if not plan or plan.status != "pending":
            raise ValueError(f"計畫 #{plan_id} 不存在或已非 pending 狀態")

        price = actual_price if actual_price else plan.entry_price
        add_trade(
            stock_id=plan.stock_id,
            stock_name=plan.stock_name,
            action=plan.direction,
            price=price,
            shares=plan.shares,
            trade_date=date.today(),
            reason=f"[計畫#{plan_id}] {plan.reason}",
            emotion="冷靜",
        )
        journal = (sess.query(TradeJournal)
                   .filter_by(stock_id=plan.stock_id, action=plan.direction)
                   .order_by(TradeJournal.created_at.desc())
                   .first())
        journal_id = journal.id if journal else None

        plan.status = "executed"
        plan.executed_at = datetime.now()
        plan.journal_id = journal_id
        sess.add(plan)

    return journal_id


def cancel_plan(plan_id: int) -> None:
    with get_session() as sess:
        plan = sess.get(TradePlan, plan_id)
        if plan and plan.status == "pending":
            plan.status = "cancelled"
            sess.add(plan)
