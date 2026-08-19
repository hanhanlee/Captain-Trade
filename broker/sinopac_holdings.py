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


def apply_broker_sync(session, found_ids: set) -> dict:
    """
    對 portfolio 套用券商標記規則並寫入。回傳統計 {'to_sinopac','to_pending','kept'}。
    使用傳入的 session(呼叫端負責 commit/rollback)。
    """
    from db.models import Portfolio

    found = {str(x) for x in found_ids}
    stat = {"to_sinopac": 0, "to_pending": 0, "kept": 0}
    for row in session.query(Portfolio).all():
        sid = str(row.stock_id)
        old = (row.broker or "").strip()
        if sid in found:
            new = BROKER_SINOPAC
        elif old:
            new = old
        else:
            new = BROKER_PENDING
        if new != old:
            row.broker = new
            if new == BROKER_SINOPAC:
                stat["to_sinopac"] += 1
            else:
                stat["to_pending"] += 1
        else:
            stat["kept"] += 1
    return stat
