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
            out[sid] = {
                "shares": int(getattr(p, "quantity", 0) or 0),
                "avg_price": float(getattr(p, "price", 0) or 0),
            }
        logger.info("永豐 list_positions 取得 %d 檔", len(out))
        return out
    finally:
        try:
            api.logout()
        except Exception:
            pass


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


def apply_broker_sync(session, positions, overwrite_shares: bool = False) -> dict:
    """
    套用券商標記規則並寫入;overwrite_shares=True 時,永豐持股一併以 API 的股數/均價覆蓋。

    positions: {stock_id: {'shares','avg_price'}}(或 set/list 的 stock_ids,只標記)。
    呼叫端負責 commit/rollback。回傳 {'to_sinopac','to_pending','kept','shares_updated'}。
    """
    from db.models import Portfolio

    if isinstance(positions, dict):
        pos_map = {str(k): v for k, v in positions.items()}
    else:
        pos_map = {str(x): None for x in positions}
    found = set(pos_map.keys())

    stat = {"to_sinopac": 0, "to_pending": 0, "kept": 0, "shares_updated": 0}
    for row in session.query(Portfolio).all():
        sid = str(row.stock_id)
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
                touched = False
                if new_sh > 0 and int(row.shares or 0) != new_sh:
                    row.shares = new_sh
                    touched = True
                if new_avg > 0 and abs(float(row.cost_price or 0) - new_avg) > 1e-6:
                    row.cost_price = new_avg
                    touched = True
                if touched:
                    stat["shares_updated"] += 1
        elif old:
            stat["kept"] += 1
        else:
            row.broker = BROKER_PENDING
            stat["to_pending"] += 1
    return stat
