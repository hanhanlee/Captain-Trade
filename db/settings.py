"""
應用程式設定持久化（SQLite key-value）

用途：儲存需要跨 session 記憶的設定，例如「休市模式」開關。
"""
from datetime import datetime
from sqlalchemy import text
from .database import get_session


def get_setting(key: str, default: str = "") -> str:
    """讀取設定值，不存在時回傳 default"""
    with get_session() as sess:
        row = sess.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": key},
        ).fetchone()
    return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    """寫入（或更新）設定值"""
    with get_session() as sess:
        sess.execute(
            text("""
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (:k, :v, :ts)
                ON CONFLICT(key) DO UPDATE SET value = :v, updated_at = :ts
            """),
            {"k": key, "v": str(value), "ts": datetime.now().isoformat()},
        )
        sess.commit()


# ── 常用包裝 ────────────────────────────────────────────────

def is_market_closed() -> bool:
    """回傳「休市模式」是否啟用"""
    return get_setting("market_closed", "false").lower() == "true"


def set_market_closed(closed: bool) -> None:
    """設定「休市模式」開關"""
    set_setting("market_closed", "true" if closed else "false")
