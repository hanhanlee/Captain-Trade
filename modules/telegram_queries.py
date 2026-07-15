"""
Telegram Bot 查詢指令的資料層（telegram_bot.py 的 handler 呼叫這裡）

設計視角：操盤手的日常決策迴圈——
  /brief  晨會摘要（部位風險 + 今日機會，一則看完）
  /watch  今日狙擊清單（四軌 watchlist、距觸發價、已觸發）
  /hold   部位與風險（未實現損益、距停損、警戒旗標）
  /p      單一個股報價 + 技術位置（MA 乖離、10日高、量比、距漲跌停）
  /chips  籌碼驗證（法人近5日、融資增減）
  /pnl    平倉績效（勝率、盈虧比、期望值）

所有函式回傳純文字字串（Telegram 上限 4096 字，各函式自行節制長度）。
即時價一律走 broker.live_price.fetch_snapshots_safe：Shioaji 不可用時
自動 fallback 到 price_cache 收盤價，並在輸出標注資料時點。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

_TG_LIMIT = 3900  # 保留 buffer，避免碰到 4096 硬上限


def _truncate(text: str) -> str:
    if len(text) <= _TG_LIMIT:
        return text
    return text[:_TG_LIMIT] + "\n…（內容過長已截斷）"


# ── 共用小工具 ──────────────────────────────────────────────────

def _fmt(v, nd=2) -> str:
    """千分位數字；None 回 '—'。"""
    if v is None:
        return "—"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if f == int(f) and abs(f) >= 100:
        return f"{int(f):,}"
    return f"{f:,.{nd}f}"


def _pct(v, signed=True) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}%" if signed else f"{v:.2f}%"


def _arrow(pct: float | None) -> str:
    if pct is None:
        return ""
    return "▲" if pct > 0 else ("▼" if pct < 0 else "─")


def _stock_name(stock_id: str) -> str:
    try:
        from sqlalchemy import text
        from db.database import get_session
        with get_session() as sess:
            row = sess.execute(
                text("SELECT stock_name FROM stock_info_cache WHERE stock_id = :sid"),
                {"sid": stock_id},
            ).fetchone()
        return row[0] if row and row[0] else ""
    except Exception:
        return ""


def _snapshots(stock_ids: list[str]) -> dict[str, dict]:
    try:
        from broker.live_price import fetch_snapshots_safe
        return fetch_snapshots_safe(stock_ids)
    except Exception as e:
        logger.warning("snapshot 取得失敗：%s", e)
        return {}


def _cached_last(stock_id: str, lookback: int = 90) -> pd.DataFrame:
    from db.price_cache import load_prices
    return load_prices(stock_id, lookback_days=lookback)


def _resolve_price(stock_id: str, snaps: dict) -> tuple[float | None, float | None, str]:
    """回 (現價, 今日漲跌%, 資料來源標記)。優先即時，fallback 快取收盤。"""
    snap = snaps.get(str(stock_id))
    if snap and snap.get("last_price"):
        rate = snap.get("change_rate")
        return float(snap["last_price"]), (float(rate) if rate is not None else None), f"即時 {snap.get('ts', '')}"
    df = _cached_last(stock_id, lookback=3)
    if df.empty:
        return None, None, "無資料"
    close = float(df["close"].iloc[-1])
    chg = None
    if len(df) >= 2 and df["close"].iloc[-2]:
        chg = (close / float(df["close"].iloc[-2]) - 1) * 100
    return close, chg, f"收盤 {df['date'].iloc[-1].strftime('%m/%d')}"


# ── /p 個股報價 + 技術位置 ──────────────────────────────────────

def query_price(stock_id: str) -> str:
    stock_id = stock_id.strip()
    if not stock_id.isalnum():
        return "用法：/p 股票代號（例：/p 2330）"

    snaps = _snapshots([stock_id])
    snap = snaps.get(stock_id)
    df = _cached_last(stock_id, lookback=90)

    if snap is None and df.empty:
        return f"查無 {stock_id} 的資料（不在快取中，代號可能有誤）"

    name = (snap or {}).get("stock_name") or _stock_name(stock_id)
    price, chg, src = _resolve_price(stock_id, snaps)

    lines = [f"📊 {stock_id} {name}"]
    chg_amt = (snap or {}).get("change_price")
    chg_str = f" {_arrow(chg)}{_fmt(abs(chg_amt))}" if chg_amt else ""
    lines.append(f"現價 {_fmt(price)}（{_pct(chg)}{chg_str}）  [{src}]")

    # 今日盤面（僅即時資料有）
    vol_ma5_lots = None
    if not df.empty and len(df) >= 6:
        # 排除今日（df 末列可能已被收盤補正寫入今日），取前 5 個交易日均量
        hist = df.iloc[:-1] if df["date"].iloc[-1].date() == datetime.now().date() else df
        vol_ma5_lots = float(hist["Trading_Volume"].tail(5).mean()) / 1000
    if snap:
        vol_lots = snap.get("total_volume")
        ratio = (float(vol_lots) / vol_ma5_lots) if (vol_lots and vol_ma5_lots) else None
        ratio_str = f"  量比 {ratio:.1f}x" if ratio else ""
        lines.append(
            f"開 {_fmt(snap.get('open'))}  高 {_fmt(snap.get('high'))}  "
            f"低 {_fmt(snap.get('low'))}  量 {_fmt(vol_lots, 0)} 張{ratio_str}"
        )

    # 技術位置（用快取日K算 MA；現價 vs MA 的乖離）
    if not df.empty and price:
        closes = df["close"]
        lines.append("─ 技術位置 ─")
        for n in (5, 10, 20, 60):
            if len(closes) >= n:
                ma = float(closes.tail(n).mean())
                dev = (price / ma - 1) * 100
                pos = "↑" if price >= ma else "↓"
                lines.append(f"MA{n:<3} {_fmt(ma)}  {pos} {_pct(dev)}")
        if len(df) >= 10:
            hi10 = float(df["max"].tail(10).max())
            lines.append(f"近10日高 {_fmt(hi10)}（距 {_pct((price / hi10 - 1) * 100)}）")

    if snap:
        lu, ld = snap.get("limit_up"), snap.get("limit_down")
        if lu and ld:
            lines.append(
                f"漲停 {_fmt(lu)}（+{_fmt(snap.get('dist_to_limit_up_pct'))}%）  "
                f"跌停 {_fmt(ld)}（-{_fmt(snap.get('dist_to_limit_down_pct'))}%）"
            )

    return _truncate("\n".join(lines))


# ── /hold 部位與風險 ────────────────────────────────────────────

def _load_holdings() -> list[dict]:
    from db.database import get_session
    from db.models import Portfolio
    with get_session() as sess:
        rows = sess.query(Portfolio).all()
        return [
            {
                "stock_id": r.stock_id, "stock_name": r.stock_name or "",
                "shares": r.shares, "cost_price": r.cost_price,
                "stop_loss": r.stop_loss, "take_profit": r.take_profit,
            }
            for r in rows
        ]


def query_holdings() -> str:
    holdings = _load_holdings()
    if not holdings:
        return "📭 目前無持股。"

    snaps = _snapshots([h["stock_id"] for h in holdings])

    total_cost = total_value = 0.0
    alert_count = 0
    lines = []
    live_any = False

    for h in holdings:
        sid = h["stock_id"]
        price, chg, src = _resolve_price(sid, snaps)
        if "即時" in src:
            live_any = True
        if price is None:
            lines.append(f"\n• {sid} {h['stock_name']}：無價格資料")
            continue

        cost, shares = float(h["cost_price"]), int(h["shares"])
        pnl_pct = (price / cost - 1) * 100
        pnl_amt = (price - cost) * shares
        total_cost += cost * shares
        total_value += price * shares

        flags = []
        stop = h.get("stop_loss")
        if stop:
            stop_dist = (price / float(stop) - 1) * 100
            if price <= float(stop):
                flags.append("🔴 已破停損")
                alert_count += 1
            elif stop_dist <= 3:
                flags.append(f"⚠️ 距停損 {stop_dist:.1f}%")
                alert_count += 1
        tp = h.get("take_profit")
        if tp and price >= float(tp):
            flags.append("🎯 達停利價")

        emoji = "🔺" if pnl_pct > 0 else ("🔻" if pnl_pct < 0 else "▪️")
        lines.append(
            f"\n{emoji} {sid} {h['stock_name']}  {shares:,} 股"
            f"\n   現價 {_fmt(price)}（今日 {_pct(chg)}）成本 {_fmt(cost)}"
            f"\n   未實現 {_pct(pnl_pct)}（{pnl_amt:+,.0f}）"
            + (f"  停損 {_fmt(stop)}" if stop else "  停損未設 ⚠️")
        )
        if flags:
            lines.append("   " + "  ".join(flags))

    total_pnl = total_value - total_cost
    total_pct = (total_value / total_cost - 1) * 100 if total_cost else 0
    src_note = "" if live_any else "（Shioaji 不可用，以快取收盤價計）"

    head = (
        f"💼 持股 {len(holdings)} 檔{src_note}"
        f"\n市值 {total_value:,.0f}  成本 {total_cost:,.0f}"
        f"\n未實現 {total_pnl:+,.0f}（{_pct(total_pct)}）"
        + (f"\n⚠️ 警戒 {alert_count} 檔" if alert_count else "")
    )
    return _truncate(head + "\n" + "".join(lines))


# ── /watch 今日狙擊清單 ─────────────────────────────────────────

def _latest_rows(model, sess):
    """取該 watchlist 最新 scan_date 的所有列。"""
    from sqlalchemy import func
    latest = sess.query(func.max(model.scan_date)).scalar()
    if latest is None:
        return latest, []
    return latest, sess.query(model).filter(model.scan_date == latest).all()


def query_watchlists() -> str:
    from db.database import get_session
    from db.models import NPatternWatchlist, V3BreakoutWatchlist, V5SniperWatchlist

    today = datetime.now().date()
    sections: list[str] = []
    all_ids: set[str] = set()
    tracks: list[tuple[str, object, list]] = []

    with get_session() as sess:
        for label, model in (
            ("N 字底", NPatternWatchlist),
            ("V3 三線齊穿", V3BreakoutWatchlist),
            ("V5 狙擊手", V5SniperWatchlist),
        ):
            scan_date, rows = _latest_rows(model, sess)
            data = []
            for r in rows:
                if isinstance(r, NPatternWatchlist):
                    target = r.b_price
                elif isinstance(r, V3BreakoutWatchlist):
                    target = max(r.ma5, r.ma10, r.ma20)
                else:
                    target = r.breakout_target
                data.append({
                    "stock_id": r.stock_id,
                    "stock_name": r.stock_name or "",
                    "target": float(target) if target else None,
                    "alerted": bool(getattr(r, "alerted_today", False)),
                    "late": getattr(r, "entry_path", "") == "late",
                    "pattern": getattr(r, "pattern_type", ""),
                })
                all_ids.add(r.stock_id)
            tracks.append((label, scan_date, data))

    snaps = _snapshots(sorted(all_ids)) if all_ids else {}

    for label, scan_date, data in tracks:
        if not data:
            sections.append(f"◇ {label}：無候選")
            continue
        stale = f"（{scan_date}，非今日名單）" if scan_date != today else ""
        alerted = [d for d in data if d["alerted"]]
        head = f"◆ {label}：{len(data)} 檔{stale}"
        if alerted:
            head += f"，🔔 已觸發 {len(alerted)}"
        sections.append(head)

        # 已觸發的全列；未觸發的依「距觸發價」排序取前 3
        for d in alerted:
            tag = "（補抓）" if d["late"] else ""
            pt = f"[{d['pattern']}]" if d["pattern"] else ""
            sections.append(f"  🔔 {d['stock_id']} {d['stock_name']}{pt}{tag}")

        pending = []
        for d in data:
            if d["alerted"] or not d["target"]:
                continue
            price, _, _ = _resolve_price(d["stock_id"], snaps)
            if price is None:
                continue
            dist = (d["target"] / price - 1) * 100  # 還差幾 % 到觸發價
            pending.append((dist, d, price))
        pending.sort(key=lambda x: abs(x[0]))
        for dist, d, price in pending[:3]:
            pt = f"[{d['pattern']}]" if d["pattern"] else ""
            sections.append(
                f"  ・{d['stock_id']} {d['stock_name']}{pt} "
                f"現 {_fmt(price)} → 觸發 {_fmt(d['target'])}（差 {_pct(dist)}）"
            )

    if not sections:
        return "📡 目前沒有任何 watchlist 資料。"
    return _truncate("🎯 今日狙擊清單\n" + "\n".join(sections))


# ── /chips 籌碼 ─────────────────────────────────────────────────

_INST_LABELS = {
    "Foreign_Investor": "外資",
    "Foreign_Dealer_Self": "外資自營",
    "Investment_Trust": "投信",
    "Dealer_self": "自營(自行)",
    "Dealer_Hedging": "自營(避險)",
}


def query_chips(stock_id: str) -> str:
    stock_id = stock_id.strip()
    if not stock_id.isalnum():
        return "用法：/chips 股票代號（例：/chips 2330）"

    from db.inst_cache import load_institutional
    from db.margin_cache import get_margin

    name = _stock_name(stock_id)
    lines = [f"🧮 {stock_id} {name} 籌碼"]

    inst = load_institutional(stock_id, days=10)
    if inst.empty:
        lines.append("法人：快取無資料")
    else:
        recent_dates = sorted(inst["date"].unique())[-5:]
        recent = inst[inst["date"].isin(recent_dates)]
        lines.append(f"─ 法人買賣超（近 {len(recent_dates)} 個交易日，張）─")
        for key in ("Foreign_Investor", "Investment_Trust"):
            sub = recent[recent["name"] == key]
            if sub.empty:
                continue
            total_lots = sub["net"].sum() / 1000
            daily = "  ".join(
                f"{d.strftime('%m/%d')} {row_net / 1000:+,.0f}"
                for d, row_net in zip(sub["date"], sub["net"])
            )
            streak_mark = "🔥" if (sub["net"] > 0).all() and len(sub) >= 3 else ""
            lines.append(f"{_INST_LABELS[key]}：合計 {total_lots:+,.0f} {streak_mark}")
            lines.append(f"  {daily}")

    margin = get_margin(stock_id, days=10)
    if margin.empty:
        lines.append("融資融券：快取無資料")
    else:
        m = margin.tail(6)
        bal = m["MarginPurchaseTodayBalance"]
        chg = bal.diff().dropna().tail(5)
        latest_bal = bal.iloc[-1]
        chg_sum = chg.sum()
        lines.append("─ 融資（張）─")
        lines.append(f"餘額 {_fmt(latest_bal, 0)}（近 5 日 {chg_sum:+,.0f}）")
        short_bal = m["ShortSaleTodayBalance"]
        if short_bal.notna().any():
            s_chg = short_bal.diff().dropna().tail(5).sum()
            lines.append(f"融券餘額 {_fmt(short_bal.iloc[-1], 0)}（近 5 日 {s_chg:+,.0f}）")

    return _truncate("\n".join(lines))


# ── /pnl 平倉績效 ───────────────────────────────────────────────

def query_pnl() -> str:
    from modules.journal import get_all_trades, calc_performance

    df = get_all_trades()
    if df.empty:
        return "📒 交易日誌尚無紀錄。"

    perf = calc_performance(df)
    if not perf:
        return "📒 尚無平倉紀錄（只有買進，未有賣出）。"

    lines = [
        "📈 平倉績效",
        f"總損益 {perf['total_pnl']:+,}",
        f"筆數 {perf['total_trades']}（勝 {perf['win_trades']} / 敗 {perf['loss_trades']}）"
        f"  勝率 {perf['win_rate']}%",
        f"平均賺 {perf['avg_win']:+,}  平均賠 {perf['avg_loss']:+,}",
        f"盈虧比 {perf['profit_factor']}（{perf['pf_rating']}）  期望值 {perf['expected_value']:+,}/筆",
        f"最佳 {perf['best_trade']:+,}  最差 {perf['worst_trade']:+,}",
    ]

    # 近 30 天已實現
    sell = df[(df["action"] == "SELL") & df["pnl"].notna()].copy()
    if not sell.empty and "trade_date" in sell.columns:
        sell["trade_date"] = pd.to_datetime(sell["trade_date"])
        cutoff = datetime.now() - timedelta(days=30)
        m30 = sell[sell["trade_date"] >= cutoff]
        if not m30.empty:
            lines.append(f"近 30 日 {m30['pnl'].sum():+,.0f}（{len(m30)} 筆）")

        lines.append("─ 最近平倉 ─")
        for _, r in sell.sort_values("trade_date").tail(3).iloc[::-1].iterrows():
            d = r["trade_date"].strftime("%m/%d") if pd.notna(r["trade_date"]) else "—"
            lines.append(f"{d} {r['stock_id']} {r.get('stock_name', '')} {r['pnl']:+,.0f}")

    return _truncate("\n".join(lines))


# ── /brief 晨會摘要 ─────────────────────────────────────────────

def query_brief() -> str:
    now = datetime.now()
    lines = [f"☀️ 晨會摘要  {now.strftime('%Y-%m-%d %H:%M')}"]

    # 1. 部位風險（重用 holdings 計算，但只給摘要）
    try:
        holdings = _load_holdings()
        if holdings:
            snaps = _snapshots([h["stock_id"] for h in holdings])
            total_cost = total_value = 0.0
            alerts: list[str] = []
            for h in holdings:
                price, _, _ = _resolve_price(h["stock_id"], snaps)
                if price is None:
                    continue
                total_cost += h["cost_price"] * h["shares"]
                total_value += price * h["shares"]
                stop = h.get("stop_loss")
                if stop and price <= float(stop):
                    alerts.append(f"🔴 {h['stock_id']} 已破停損")
                elif stop and (price / float(stop) - 1) * 100 <= 3:
                    alerts.append(f"⚠️ {h['stock_id']} 距停損 {(price / float(stop) - 1) * 100:.1f}%")
            pct = (total_value / total_cost - 1) * 100 if total_cost else 0
            lines.append(
                f"\n💼 持股 {len(holdings)} 檔  未實現 {total_value - total_cost:+,.0f}（{_pct(pct)}）"
            )
            lines.extend(alerts if alerts else ["   無警戒部位"])
        else:
            lines.append("\n💼 空手（無持股）")
    except Exception as e:
        lines.append(f"\n💼 持股讀取失敗：{e}")

    # 2. 今日機會（四軌 watchlist 檔數）
    try:
        from db.database import get_session
        from db.models import NPatternWatchlist, V3BreakoutWatchlist, V5SniperWatchlist
        today = now.date()
        parts = []
        with get_session() as sess:
            for label, model in (("N", NPatternWatchlist), ("V3", V3BreakoutWatchlist), ("V5", V5SniperWatchlist)):
                scan_date, rows = _latest_rows(model, sess)
                if not rows:
                    parts.append(f"{label} 0")
                    continue
                alerted = sum(1 for r in rows if getattr(r, "alerted_today", False))
                stale = "*" if scan_date != today else ""
                parts.append(f"{label} {len(rows)}{stale}" + (f"(🔔{alerted})" if alerted else ""))
        lines.append("\n🎯 狙擊清單  " + "  ".join(parts))
        if any("*" in p for p in parts):
            lines.append("   （* 為非今日名單，08:00–08:55 建構）")
        lines.append("   細節 → /watch")
    except Exception as e:
        lines.append(f"\n🎯 watchlist 讀取失敗：{e}")

    # 3. 昨日盤後選股
    try:
        from db.scan_history import load_scan_history, load_session_results
        hist = load_scan_history(limit=1)
        if hist:
            h = hist[0]
            df = load_session_results(h["id"])
            top = df.head(3)
            names = "、".join(f"{r['stock_id']} {r.get('stock_name', '')}" for _, r in top.iterrows())
            when = h["scanned_at"].strftime("%m/%d %H:%M") if h.get("scanned_at") else ""
            lines.append(f"\n📊 最近選股（{when}）共 {h.get('result_count') or len(df)} 檔")
            if names:
                lines.append(f"   前段班：{names}")
    except Exception as e:
        logger.debug("brief scan history failed: %s", e)

    return _truncate("\n".join(lines))
