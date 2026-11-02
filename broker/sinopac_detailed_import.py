"""
永豐「詳細交易匯入」—— 唯讀撈每一筆實際成交,依真實日期逐筆重建交易日誌。

相對於 sinopac_holdings 的 net-diff 同步（只比對部位差,多日多筆會被壓成一次、
日期也不準）,本模組用三個帳務查詢還原逐筆明細：

  - list_position_detail : 仍持有的每一筆「買進」lot（含真實買進日、張數、總金額、dseq）
  - list_profit_loss     : 每一筆「賣出」平倉（真實賣出日、每股價、損益、dseq）
  - list_profit_loss_detail : 已平倉對應的「買進」明細（真實買進日、每股買價、dseq）

單位（2026-11-02 dump 實測確認,三者不同,務必照這換算）：
  - position_detail.price = 整筆總成交金額（= quantity張 × 1000股 × 每股價）；每股 = price/(qty*1000)
  - profit_loss.price      = 每股賣出價（已是每股）
  - profit_loss_detail.price = 每股買進價（已是每股）；cost = 整筆總買進成本
  - 三者 quantity 皆為「張」；shares = quantity * 1000

去重：每筆成交都有唯一 `dseq`，寫入 trade_journal.broker_dseq；重匯不會重複。

政策：唯讀（list_* 查詢），絕不下單。沿用 OPAL 帳務金鑰 + CA。
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_CA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "secrets", "Sinopac_Opal.pfx")


def _to_date(s) -> Optional[date]:
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def fetch_detailed_trades(begin_date, end_date) -> list[dict]:
    """唯讀撈 begin~end 區間的逐筆成交,回傳排序後的交易清單。

    每筆：{trade_date(date), stock_id, stock_name, action('BUY'/'SELL'),
          shares(int股), price(float每股), pnl(float或None), fee(int或None), dseq, source}
    金鑰缺失 / 帳戶未開通 / 查詢失敗會 raise,由呼叫端捕捉。
    """
    api_key = os.getenv("SINOTRADE_APIKEY_OPAL", "").strip()
    secret_key = os.getenv("SINOTRADE_SECRETKEY_OPAL", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("永豐帳務金鑰未設定(SINOTRADE_APIKEY_OPAL / SECRETKEY_OPAL)")

    b, e = str(begin_date)[:10], str(end_date)[:10]

    import shioaji as sj
    api = sj.Shioaji(simulation=False)
    try:
        api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)
        acc = api.stock_account
        if acc is None:
            raise RuntimeError("永豐登入成功但取不到證券帳戶")
        pid = acc.person_id
        try:
            api.activate_ca(ca_path=_CA_PATH, ca_passwd=pid, person_id=pid)
        except Exception as exc:
            logger.warning("activate_ca 失敗(續)：%s", exc)

        def _name(code: str) -> str:
            try:
                c = api.Contracts.Stocks[str(code)]
                return str(getattr(c, "name", "") or "")
            except Exception:
                return ""

        def _in(d) -> bool:
            dd = _to_date(d)
            return dd is not None and str(begin_date)[:10] <= dd.isoformat() <= str(end_date)[:10]

        by_dseq: dict[str, dict] = {}

        # ── 1) 仍持有的買進 lot ──────────────────────────────────
        for p in api.list_positions(acc):
            for d in api.list_position_detail(acc, p.id):
                if not _in(d.date):
                    continue
                qty_lots = int(d.quantity or 0)
                shares = qty_lots * 1000
                total = float(d.price or 0)   # 整筆總金額
                price_ps = round(total / shares, 4) if shares else 0.0
                dseq = str(d.dseq)
                by_dseq[dseq] = {
                    "trade_date": _to_date(d.date), "stock_id": str(d.code),
                    "stock_name": _name(d.code), "action": "BUY",
                    "shares": shares, "price": price_ps, "pnl": None,
                    "fee": int(getattr(d, "fee", 0) or 0), "dseq": dseq,
                    "source": "position_detail",
                }

        # ── 2) 賣出平倉 + 其對應買進 ─────────────────────────────
        for r in api.list_profit_loss(acc, b, e):
            if _in(r.date):
                dseq = str(r.dseq)
                by_dseq[dseq] = {
                    "trade_date": _to_date(r.date), "stock_id": str(r.code),
                    "stock_name": _name(r.code), "action": "SELL",
                    "shares": int(r.quantity or 0) * 1000, "price": round(float(r.price or 0), 4),
                    "pnl": float(r.pnl or 0), "fee": None, "dseq": dseq,
                    "source": "profit_loss",
                }
            for x in api.list_profit_loss_detail(acc, r.id):
                if not _in(x.date):
                    continue
                dseq = str(x.dseq)
                if dseq in by_dseq:
                    continue
                by_dseq[dseq] = {
                    "trade_date": _to_date(x.date), "stock_id": str(x.code),
                    "stock_name": _name(x.code), "action": "BUY",
                    "shares": int(x.quantity or 0) * 1000, "price": round(float(x.price or 0), 4),
                    "pnl": None, "fee": int(getattr(x, "fee", 0) or 0), "dseq": dseq,
                    "source": "profit_loss_detail",
                }

        trades = [t for t in by_dseq.values() if t["trade_date"] is not None]
        trades.sort(key=lambda t: (t["trade_date"], t["stock_id"], t["action"]))
        logger.info("永豐詳細交易 %s~%s：%d 筆", b, e, len(trades))
        return trades
    finally:
        try:
            api.logout()
        except Exception:
            pass


def apply_detailed_import(
    trades: list[dict], *,
    clear_range: tuple | None = None,
    clear_stock_ids: set | None = None,
) -> dict:
    """把 fetch_detailed_trades 的結果寫入 trade_journal,回傳統計。

    去重：已存在相同 broker_dseq 的不重寫。
    clear_range=(begin_date, end_date)：先刪該區間、**且 broker_dseq 為空**（＝舊 net-diff
      產生、無 dseq）的日誌,再寫入逐筆;避免與舊的合併列並存。可用 clear_stock_ids 限定股票。
    """
    from db.database import get_session, init_db
    from db.models import TradeJournal

    init_db()
    stats = {"cleared": 0, "inserted": 0, "skipped_dup": 0}
    with get_session() as sess:
        if clear_range:
            b, e = clear_range
            q = sess.query(TradeJournal).filter(
                TradeJournal.trade_date >= b,
                TradeJournal.trade_date <= e,
                TradeJournal.broker_dseq.is_(None),   # 只清沒有 dseq 的舊列
            )
            if clear_stock_ids:
                q = q.filter(TradeJournal.stock_id.in_(list(clear_stock_ids)))
            stats["cleared"] = int(q.delete(synchronize_session=False) or 0)

        existing = {
            d for (d,) in sess.query(TradeJournal.broker_dseq)
            .filter(TradeJournal.broker_dseq.isnot(None)).all()
        }
        for t in trades:
            dseq = t["dseq"]
            if dseq in existing:
                stats["skipped_dup"] += 1
                continue
            sess.add(TradeJournal(
                stock_id=t["stock_id"], stock_name=t.get("stock_name") or "",
                action=t["action"], price=float(t["price"]), shares=int(t["shares"]),
                trade_date=t["trade_date"], pnl=t.get("pnl"),
                reason="", emotion="", broker_dseq=dseq,
            ))
            existing.add(dseq)
            stats["inserted"] += 1
        sess.commit()
    return stats
