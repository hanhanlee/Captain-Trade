"""
風險控制引擎
- 固定風險法：每筆最多虧損帳戶 X%
- Kelly Criterion：依勝率/盈虧比計算最佳下注比例
- 總帳戶曝險監控
- 最大回撤追蹤
"""
import math
from dataclasses import dataclass


@dataclass
class PositionResult:
    method: str
    account_size: float
    risk_amount: float          # 本筆願意承受的最大虧損（元）
    entry_price: float
    stop_loss: float
    recommended_shares: int     # 建議張數
    total_cost: float           # 建議投入金額
    risk_pct_of_account: float  # 此部位佔帳戶的風險比例
    reward_risk_ratio: float    # 報酬風險比
    note: str = ""


def calc_position_fixed_risk(
    account_size: float,
    risk_pct: float,
    entry_price: float,
    stop_loss: float,
    target_price: float = None,
) -> PositionResult:
    """
    固定風險法：每筆最多虧損帳戶的 risk_pct%

    公式：
        最大風險金額 = 帳戶 × risk_pct%
        每股風險    = entry - stop_loss
        股數        = 最大風險金額 / 每股風險
        張數        = floor(股數 / 1000)
    """
    if entry_price <= stop_loss:
        raise ValueError("進場價必須高於停損價")

    risk_amount = account_size * risk_pct / 100
    risk_per_share = entry_price - stop_loss
    shares_float = risk_amount / risk_per_share
    recommended_shares = max(1, math.floor(shares_float / 1000))

    total_cost = recommended_shares * 1000 * entry_price
    actual_risk = recommended_shares * 1000 * risk_per_share
    actual_risk_pct = actual_risk / account_size * 100

    rr = ((target_price - entry_price) / risk_per_share) if target_price else 0

    return PositionResult(
        method="固定風險法",
        account_size=account_size,
        risk_amount=round(actual_risk),
        entry_price=entry_price,
        stop_loss=stop_loss,
        recommended_shares=recommended_shares,
        total_cost=round(total_cost),
        risk_pct_of_account=round(actual_risk_pct, 2),
        reward_risk_ratio=round(rr, 2),
        note=f"每股風險 {risk_per_share:.2f} 元，建議 {recommended_shares} 張",
    )


def calc_position_kelly(
    account_size: float,
    win_rate: float,         # 0–100
    avg_win: float,          # 平均獲利（元/張）
    avg_loss: float,         # 平均虧損（元/張，正值）
    entry_price: float,
    stop_loss: float,
    kelly_fraction: float = 0.25,   # 使用 Kelly 的幾分之一（保守起見）
) -> PositionResult:
    """
    Kelly Criterion：依歷史勝率與盈虧比計算最佳部位

    Kelly% = W - (1-W)/R
    W = 勝率, R = 盈虧比（avg_win / avg_loss）
    使用 Fractional Kelly（預設 1/4）降低波動
    """
    if avg_loss <= 0:
        raise ValueError("avg_loss 必須為正值")

    w = win_rate / 100
    r = avg_win / avg_loss
    kelly_pct = w - (1 - w) / r

    if kelly_pct <= 0:
        return PositionResult(
            method="Kelly Criterion",
            account_size=account_size,
            risk_amount=0,
            entry_price=entry_price,
            stop_loss=stop_loss,
            recommended_shares=0,
            total_cost=0,
            risk_pct_of_account=0,
            reward_risk_ratio=round(r, 2),
            note=f"Kelly% = {kelly_pct:.1%}，期望值為負，不建議交易",
        )

    fractional_kelly = kelly_pct * kelly_fraction
    invest_amount = account_size * fractional_kelly
    recommended_shares = max(1, math.floor(invest_amount / (entry_price * 1000)))
    total_cost = recommended_shares * 1000 * entry_price
    risk_per_share = entry_price - stop_loss
    actual_risk = recommended_shares * 1000 * risk_per_share

    return PositionResult(
        method=f"Kelly Criterion（{kelly_fraction:.0%} Kelly）",
        account_size=account_size,
        risk_amount=round(actual_risk),
        entry_price=entry_price,
        stop_loss=stop_loss,
        recommended_shares=recommended_shares,
        total_cost=round(total_cost),
        risk_pct_of_account=round(actual_risk / account_size * 100, 2),
        reward_risk_ratio=round(r, 2),
        note=f"Full Kelly={kelly_pct:.1%}，使用 {kelly_fraction:.0%} = {fractional_kelly:.1%}",
    )


def calc_portfolio_exposure(holdings_stats: list, account_size: float) -> dict:
    """
    計算整體帳戶的風險曝險

    holdings_stats：portfolio.calc_holding_stats() 的結果列表
    """
    if not holdings_stats or account_size <= 0:
        return {}

    total_cost = sum(s["cost_price"] * s["shares"] * 1000 for s in holdings_stats)
    total_market_value = sum(s["close"] * s["shares"] * 1000 for s in holdings_stats)
    total_pnl = sum(s["pnl"] for s in holdings_stats)

    # 各持股帳戶佔比
    positions = []
    for s in holdings_stats:
        market_val = s["close"] * s["shares"] * 1000
        weight = market_val / account_size * 100
        positions.append({
            "stock_id": s["stock_id"],
            "stock_name": s["stock_name"],
            "market_value": round(market_val),
            "weight_pct": round(weight, 1),
            "pnl_pct": s["pnl_pct"],
        })

    positions.sort(key=lambda x: x["weight_pct"], reverse=True)

    exposure_pct = total_market_value / account_size * 100
    cash_pct = max(0, 100 - exposure_pct)

    # 集中度警示
    warnings = []
    for p in positions:
        if p["weight_pct"] > 25:
            warnings.append(f"⚠️ {p['stock_id']} 佔帳戶 {p['weight_pct']:.1f}%，部位過度集中")
    if exposure_pct > 80:
        warnings.append(f"⚠️ 總曝險 {exposure_pct:.1f}%，剩餘現金不足，流動性風險偏高")

    return {
        "total_cost": round(total_cost),
        "total_market_value": round(total_market_value),
        "total_pnl": round(total_pnl),
        "exposure_pct": round(exposure_pct, 1),
        "cash_pct": round(cash_pct, 1),
        "positions": positions,
        "warnings": warnings,
    }


def calc_sector_exposure(
    holdings_with_industry: list,
    account_size: float,
    max_sector_pct: float = 7.0,
) -> dict:
    """
    產業集中度曝險分析

    holdings_with_industry：每筆需含 stock_id, stock_name, industry, close, shares
    max_sector_pct：單一產業市值佔帳戶的上限警戒（預設 7%）

    回傳：
        sectors: list[dict]  各產業市值、佔比、持股清單
        warnings: list[str]  超限警示
    """
    if not holdings_with_industry or account_size <= 0:
        return {"sectors": [], "warnings": []}

    sector_map: dict = {}
    for h in holdings_with_industry:
        industry = h.get("industry") or "未分類"
        mkt_val = h.get("close", 0) * h.get("shares", 0) * 1000
        if industry not in sector_map:
            sector_map[industry] = {"market_value": 0.0, "stocks": []}
        sector_map[industry]["market_value"] += mkt_val
        sector_map[industry]["stocks"].append(
            f"{h.get('stock_id', '')} {h.get('stock_name', '')}".strip()
        )

    sectors = []
    warnings = []
    for ind, data in sorted(sector_map.items(), key=lambda x: x[1]["market_value"], reverse=True):
        pct = data["market_value"] / account_size * 100
        sectors.append({
            "industry": ind,
            "market_value": round(data["market_value"]),
            "pct_of_account": round(pct, 1),
            "stocks": "、".join(data["stocks"]),
        })
        if pct > max_sector_pct:
            warnings.append(
                f"⚠️ 產業「{ind}」佔帳戶 {pct:.1f}%"
                f"（上限 {max_sector_pct}%），同產業利空將同時觸發停損"
            )

    return {"sectors": sectors, "warnings": warnings}


def calc_atr_trailing_stop(
    entry_price: float,
    current_price: float,
    highest_price: float,
    atr_value: float,
    multiplier: float = 2.0,
) -> dict:
    """
    ATR 移動停利（Trailing Stop）計算

    出場邏輯：
        移動停損線 = 持有期間最高價 − multiplier × ATR
        當收盤跌破此線時出場

    回傳：
        trailing_stop:   當前移動停損價
        locked_profit:   已鎖定獲利%（相對成本）
        profit_buffer:   目前價距停損的緩衝%
        status:          'safe' | 'warning' | 'triggered'
    """
    if atr_value <= 0 or entry_price <= 0:
        return {}

    trailing_stop = highest_price - multiplier * atr_value
    locked_pct = (trailing_stop - entry_price) / entry_price * 100
    buffer_pct = (current_price - trailing_stop) / entry_price * 100

    if current_price <= trailing_stop:
        status = "triggered"
    elif buffer_pct < 2:
        status = "warning"
    else:
        status = "safe"

    return {
        "trailing_stop": round(trailing_stop, 2),
        "locked_profit_pct": round(locked_pct, 2),
        "profit_buffer_pct": round(buffer_pct, 2),
        "status": status,
        "atr_value": round(atr_value, 2),
        "multiplier": multiplier,
    }


def calc_max_drawdown(equity_curve: list) -> dict:
    """
    計算最大回撤

    equity_curve：帳戶淨值序列（list of float）
    """
    if len(equity_curve) < 2:
        return {"max_drawdown_pct": 0, "peak": 0, "trough": 0}

    peak = equity_curve[0]
    max_dd = 0
    peak_val = peak
    trough_val = peak

    for val in equity_curve:
        if val > peak:
            peak = val
        dd = (val - peak) / peak * 100
        if dd < max_dd:
            max_dd = dd
            peak_val = peak
            trough_val = val

    return {
        "max_drawdown_pct": round(max_dd, 2),
        "peak": round(peak_val),
        "trough": round(trough_val),
    }
