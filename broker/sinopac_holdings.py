"""
永豐持股同步 — 唯讀查詢庫存 + 套用券商標記(broker)。

政策：只查詢(list_positions),不進行任何真實下單/交易
      (下單在 broker/shioaji_adapter.py 已硬鎖死)。

標記規則(使用者定義):
  - 永豐 API 查得到的持股      → broker = '永豐'
  - 查不到、但原本有設定       → 保留原設定(不動)
  - 查不到、且原本沒設定       → broker = '待確認'
broker 欄位平常仍可手動修改。
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_CA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "secrets", "Sinopac_Opal.pfx")

BROKER_SINOPAC = "永豐"
BROKER_PENDING = "待確認"


def get_sinopac_positions() -> dict:
    """
    以永豐帳務 key(OPAL)查詢庫存。回傳 {stock_id: {'shares': int, 'avg_price': float}}。

    尚未開通(帳戶未認證)或設定缺失時會 raise，由呼叫端捕捉並顯示訊息。
    ※ 唯讀，不下單。shares 單位待帳戶開通後以實測校準(先存原始 quantity)。
    """
    api_key = os.getenv("SINOTRADE_APIKEY_OPAL", "").strip()
    secret_key = os.getenv("SINOTRADE_SECRETKEY_OPAL", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("永豐帳務金鑰未設定(SINOTRADE_APIKEY_OPAL / SECRETKEY_OPAL)")

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
        except Exception as e:
            logger.warning("activate_ca 失敗(續)：%s", e)

        # unit=Share → quantity 直接以「股」為單位(含零股),不用再 *1000
        try:
            positions = api.list_positions(acc, unit=sj.constant.Unit.Share)
        except Exception:
            positions = api.list_positions(acc)   # 後備:預設單位(張)
        out: dict[str, dict] = {}
        for p in positions:
            sid = str(getattr(p, "code", "") or "").strip()
            if not sid:
                continue
            name = ""
            try:
                c = api.Contracts.Stocks[sid]
                name = getattr(c, "name", "") or ""
            except Exception:
                pass
            out[sid] = {
                "shares": int(getattr(p, "quantity", 0) or 0),
                "avg_price": float(getattr(p, "price", 0) or 0),
                "stock_name": name,
            }
        logger.info("永豐 list_positions 取得 %d 檔", len(out))
        return out
    finally:
        try:
            api.logout()
        except Exception:
            pass


def get_sinopac_realized_pnl(begin_date, end_date) -> list[dict]:
    """
    唯讀查詢永豐已實現損益(list_profit_loss)。回傳
    [{'stock_id','date','quantity','price','pnl','raw'}...]。

    政策：僅查詢帳務,不下單(與 get_sinopac_positions 同一把 OPAL 金鑰、同一 CA)。
    begin_date / end_date：可傳 date 或 'YYYY-MM-DD' 字串。
    帳戶未開通、金鑰缺失或查詢失敗會 raise,由呼叫端捕捉並降級(不影響同步主流程)。

    ※ Shioaji ProfitLoss 欄位名稱依版本可能不同,這裡用 getattr 防禦式讀取,
      並在首筆記 raw 到 log,方便盤後實測一次後校準(price 是否為賣出成交價)。
    """
    api_key = os.getenv("SINOTRADE_APIKEY_OPAL", "").strip()
    secret_key = os.getenv("SINOTRADE_SECRETKEY_OPAL", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("永豐帳務金鑰未設定(SINOTRADE_APIKEY_OPAL / SECRETKEY_OPAL)")

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
        except Exception as e:
            logger.warning("activate_ca 失敗(續)：%s", e)

        b, e2 = str(begin_date), str(end_date)
        try:
            pls = api.list_profit_loss(acc, b, e2)
        except TypeError:
            pls = api.list_profit_loss(acc, begin_date=b, end_date=e2)

        out: list[dict] = []
        for pl in pls or []:
            out.append({
                "stock_id": str(getattr(pl, "code", "") or "").strip(),
                "date": str(getattr(pl, "date", "") or ""),
                "quantity": int(getattr(pl, "quantity", 0) or 0),
                "price": float(getattr(pl, "price", 0) or 0),
                "pnl": float(getattr(pl, "pnl", 0) or 0),
                "raw": repr(pl),
            })
        if out:
            logger.info(
                "永豐 list_profit_loss(%s~%s) 取得 %d 筆；首筆 raw=%s",
                b, e2, len(out), out[0]["raw"],
            )
        return out
    finally:
        try:
            api.logout()
        except Exception:
            pass


def _qty_matches(record_qty: int, target_shares: int) -> bool:
    """已實現損益的 quantity 單位可能是股或張,兩種都當作命中。"""
    if record_qty <= 0 or target_shares <= 0:
        return False
    return (
        record_qty == target_shares
        or record_qty * 1000 == target_shares
        or record_qty == target_shares * 1000
    )


def summarize_realized_sell(records: list[dict], stock_id: str, shares: int) -> dict | None:
    """
    從已實現損益清單中彙整某檔的賣出結果,供回填 SELL 骨架。

    回傳 {'price': 加權平均賣價, 'pnl': 損益合計, 'qty_matched': bool} 或 None(查無)。
    - 彙整該 stock_id 在清單中的所有紀錄(涵蓋同日多筆分批成交、整檔出清)。
    - price 以有效賣價(>0)加權平均;pnl 直接加總(這是「已交易損益」要的數)。
    - qty_matched 只作信心標記(單位可能股/張),不作為是否回填的門檻——
      pnl 本身與單位無關,查得到就有價值。
    """
    recs = [r for r in records if str(r.get("stock_id")) == str(stock_id)]
    if not recs:
        return None
    tot_qty = sum(int(r.get("quantity") or 0) for r in recs)
    tot_pnl = sum(float(r.get("pnl") or 0) for r in recs)
    priced = [r for r in recs if float(r.get("price") or 0) > 0 and int(r.get("quantity") or 0) > 0]
    denom = sum(int(r["quantity"]) for r in priced)
    avg_price = (
        sum(float(r["price"]) * int(r["quantity"]) for r in priced) / denom
        if denom > 0 else 0.0
    )
    return {
        "price": round(avg_price, 4),
        "pnl": round(tot_pnl, 2),
        "qty_matched": _qty_matches(tot_qty, int(shares or 0)),
    }


def compute_broker_sync(rows: list, found_ids: set) -> list:
    """
    計算每筆持股的 broker 標記變更(供差異預覽,不寫入)。

    rows: 可迭代,每項需含 'stock_id' 與 'broker'(dict 或有同名屬性)。
    found_ids: 永豐查到的 stock_id 集合。
    回傳: [{'stock_id','stock_name','old','new','changed'}...]
    """
    def _g(r, k, default=None):
        if isinstance(r, dict):
            return r.get(k, default)
        return getattr(r, k, default)

    found = {str(x) for x in found_ids}
    result = []
    for r in rows:
        sid = str(_g(r, "stock_id", "") or "")
        old = (_g(r, "broker") or "").strip()
        if sid in found:
            new = BROKER_SINOPAC
        elif old:
            new = old
        else:
            new = BROKER_PENDING
        result.append({
            "stock_id": sid,
            "stock_name": _g(r, "stock_name", "") or "",
            "old": old or "(無)",
            "new": new,
            "changed": new != old,
        })
    return result


def compute_add_remove(holdings: list, positions: dict) -> dict:
    """
    計算「新增(永豐有、清單沒有)」與「可能已賣出(清單標永豐、但永豐已無庫存)」。
    回傳 {'to_add':[{stock_id,stock_name,shares,avg_price}], 'to_remove':[{stock_id,stock_name,shares}]}。
    ※ 只是候選清單供預覽;交易日誌一律由使用者手動記錄。
    """
    def _g(r, k, default=None):
        return r.get(k, default) if isinstance(r, dict) else getattr(r, k, default)

    pos_ids = {str(k) for k in positions}
    held_ids = {str(_g(h, "stock_id")) for h in holdings}

    to_add = [
        {"stock_id": str(sid), "stock_name": p.get("stock_name", ""),
         "shares": int(p.get("shares") or 0), "avg_price": float(p.get("avg_price") or 0)}
        for sid, p in positions.items() if str(sid) not in held_ids
    ]
    to_remove = [
        {"stock_id": str(_g(h, "stock_id")), "stock_name": _g(h, "stock_name", "") or "",
         "shares": int(_g(h, "shares") or 0)}
        for h in holdings
        if (_g(h, "broker") or "").strip() == BROKER_SINOPAC and str(_g(h, "stock_id")) not in pos_ids
    ]
    return {"to_add": to_add, "to_remove": to_remove}


def apply_broker_sync(session, positions, overwrite_shares: bool = False,
                      add_ids=None, remove_ids=None,
                      create_journal: bool = True, trade_date=None,
                      realized_records=None) -> dict:
    """
    套用同步：券商標記規則(必做)+ 可選 覆蓋股數/均價、新增買進、移除已賣出。
    呼叫端負責 commit/rollback。
    positions: {stock_id: {'shares','avg_price','stock_name'}}(或 set/list 只標記)。

    create_journal=True 時，會為「明確的交易」自動建一筆交易日誌骨架(不改持股)：
      - 新增(add) → BUY，price = 永豐均價。
      - 移除(remove) → SELL，price = 0(賣價待補)、pnl 留空。
      ※ 只處理「新部位/整個出清」這類明確交易；既有持股的部分加減碼(overwrite 股數變動)
        因難以區分「真交易」與「單純校正數字」,v1 不自動建日誌,由使用者手動記。

    回傳 {'to_sinopac','to_pending','kept','shares_updated','added','removed','journaled'}。
    """
    from datetime import date as _date
    from db.models import Portfolio
    from modules.journal import build_sync_journal_entry

    td = trade_date or _date.today()
    _realized = list(realized_records or [])

    def _sell_price_pnl(sid: str, shares: int):
        """回傳 (price, pnl)：能從永豐已實現損益查到就帶入,否則 (0, None)。"""
        if not _realized:
            return 0, None
        summary = summarize_realized_sell(_realized, sid, shares)
        if not summary:
            return 0, None
        stat["journaled_pnl_filled"] = stat.get("journaled_pnl_filled", 0) + 1
        return summary["price"], summary["pnl"]

    if isinstance(positions, dict):
        pos_map = {str(k): v for k, v in positions.items()}
    else:
        pos_map = {str(x): None for x in positions}
    found = set(pos_map.keys())
    remove_set = {str(x) for x in (remove_ids or [])}
    add_set = {str(x) for x in (add_ids or [])}

    stat = {"to_sinopac": 0, "to_pending": 0, "kept": 0,
            "shares_updated": 0, "added": 0, "removed": 0, "journaled": 0}

    for row in session.query(Portfolio).all():
        sid = str(row.stock_id)
        if sid in remove_set:
            continue  # 待移除的先略過標記處理
        old = (row.broker or "").strip()
        if sid in found:
            if old != BROKER_SINOPAC:
                row.broker = BROKER_SINOPAC
                stat["to_sinopac"] += 1
            else:
                stat["kept"] += 1
            if overwrite_shares and pos_map.get(sid):
                p = pos_map[sid]
                new_sh = int(p.get("shares") or 0)
                new_avg = float(p.get("avg_price") or 0)
                old_sh = int(row.shares or 0)
                touched = False
                if new_sh > 0 and old_sh != new_sh:
                    row.shares = new_sh
                    touched = True
                if new_avg > 0 and abs(float(row.cost_price or 0) - new_avg) > 1e-6:
                    row.cost_price = new_avg
                    touched = True
                if touched:
                    stat["shares_updated"] += 1
                # 股數有增減 → 建差額交易日誌骨架(加碼→BUY、減碼→SELL)。
                # 只在股數真的變動時建;純均價校正(股數不變)不算交易,不建。
                if create_journal and new_sh > 0 and new_sh != old_sh and old_sh >= 0:
                    _delta = new_sh - old_sh
                    if _delta > 0:
                        _price, _pnl = new_avg, None      # 加碼→BUY,用均價
                    else:
                        _price, _pnl = _sell_price_pnl(sid, abs(_delta))  # 減碼→SELL,查賣價/損益
                    made = build_sync_journal_entry(
                        session,
                        stock_id=sid,
                        stock_name=(p.get("stock_name") or row.stock_name or ""),
                        action=("BUY" if _delta > 0 else "SELL"),
                        price=_price,
                        shares=abs(_delta),
                        trade_date=td,
                        pnl=_pnl,
                    )
                    if made is not None:
                        stat["journaled"] += 1
        elif old:
            stat["kept"] += 1
        else:
            row.broker = BROKER_PENDING
            stat["to_pending"] += 1

    # 移除已賣出(使用者已勾選) → SELL 日誌骨架(賣價待補)
    if remove_set:
        for row in session.query(Portfolio).filter(Portfolio.stock_id.in_(list(remove_set))).all():
            if create_journal:
                _price, _pnl = _sell_price_pnl(str(row.stock_id), int(row.shares or 0))
                made = build_sync_journal_entry(
                    session, stock_id=str(row.stock_id), stock_name=row.stock_name or "",
                    action="SELL", price=_price, shares=int(row.shares or 0), trade_date=td,
                    pnl=_pnl,
                )
                if made is not None:
                    stat["journaled"] += 1
            session.delete(row)
            stat["removed"] += 1

    # 新增買進(使用者已勾選) → BUY 日誌骨架(price = 永豐均價)
    if add_set:
        existing = {str(sid) for (sid,) in session.query(Portfolio.stock_id).all()}
        for sid in add_set:
            if sid in existing:
                continue
            p = pos_map.get(sid, {}) or {}
            session.add(Portfolio(
                stock_id=sid,
                stock_name=(p.get("stock_name") or ""),
                shares=int(p.get("shares") or 0),
                cost_price=float(p.get("avg_price") or 0),
                broker=BROKER_SINOPAC,
                notes="永豐同步新增",
            ))
            stat["added"] += 1
            if create_journal:
                made = build_sync_journal_entry(
                    session, stock_id=sid, stock_name=(p.get("stock_name") or ""),
                    action="BUY", price=float(p.get("avg_price") or 0),
                    shares=int(p.get("shares") or 0), trade_date=td,
                )
                if made is not None:
                    stat["journaled"] += 1

    return stat
