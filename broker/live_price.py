"""Shioaji 即時報價共用工具（給排程 job 用：持股監控、收盤補正）。

封裝「確保登入（含失敗 cooldown）→ 批次 get_snapshots」。
任何失敗一律回傳空 dict，讓呼叫端自然 fallback 到 FinMind/Yahoo，不阻斷主流程。
"""
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_LOGIN_RETRY_MINUTES = 10           # 登入失敗後至少等幾分鐘再重試
_last_login_attempt: Optional[datetime] = None


def fetch_snapshots_safe(stock_ids: list[str]) -> dict[str, dict]:
    """批次取得 Shioaji 即時 snapshot，{stock_id: snapshot_dict}；任何失敗回 {}。"""
    global _last_login_attempt

    ids = [str(s).strip() for s in stock_ids if str(s).strip()]
    if not ids:
        return {}

    try:
        from broker.shioaji_adapter import get_adapter
        adapter = get_adapter()

        # 尚未登入時嘗試自動登入（失敗後等 _LOGIN_RETRY_MINUTES 分鐘再重試）
        if not adapter.is_logged_in():
            now = datetime.now()
            retry_ok = (
                _last_login_attempt is None
                or (now - _last_login_attempt).total_seconds() >= _LOGIN_RETRY_MINUTES * 60
            )
            if retry_ok:
                logger.info("Shioaji 尚未登入，嘗試自動登入…")
                _last_login_attempt = now
                adapter.login()

        if not adapter.is_logged_in():
            logger.debug("Shioaji 未登入，略過即時報價")
            return {}

        snaps = adapter.get_snapshots(ids)
        if snaps:
            logger.info("Shioaji 批次報價：取得 %d 檔", len(snaps))
        return snaps or {}
    except Exception as exc:
        logger.debug("Shioaji snapshot 取得失敗：%s", exc)
        return {}
