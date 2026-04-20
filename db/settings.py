"""
應用程式設定持久化（SQLite key-value）

用途：儲存需要跨 session 記憶的設定，例如「休市模式」開關。
"""
import json
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


def get_prefetch_optimal_time() -> str:
    """
    回傳上次記錄的盤後首筆更新時間（格式 HH:MM）。
    尚未記錄時回傳空字串。
    """
    return get_setting("prefetch_first_update_hhmm", "")


def set_prefetch_optimal_time(hhmm: str) -> None:
    """儲存當日盤後首筆成功更新的時間（格式 HH:MM）"""
    set_setting("prefetch_first_update_hhmm", hhmm)


def get_scanner_preset() -> dict:
    """讀取使用者自訂掃描預設（JSON dict），尚未儲存時回傳空 dict"""
    raw = get_setting("scanner_custom_preset", "")
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def set_scanner_preset(preset: dict) -> None:
    """儲存使用者自訂掃描預設"""
    set_setting("scanner_custom_preset", json.dumps(preset, ensure_ascii=False))


def get_force_yahoo() -> bool:
    """回傳是否強制使用 Yahoo Finance 作為資料來源"""
    return get_setting("force_yahoo_finance", "false").lower() == "true"


def set_force_yahoo(enabled: bool) -> None:
    """設定是否強制使用 Yahoo Finance（FinMind 異常時手動切換）"""
    set_setting("force_yahoo_finance", "true" if enabled else "false")


def get_premium_broker_backfill_enabled() -> bool:
    return get_setting("premium_broker_backfill_enabled", "false").lower() == "true"


def set_premium_broker_backfill_enabled(enabled: bool) -> None:
    set_setting("premium_broker_backfill_enabled", "true" if enabled else "false")


def get_premium_broker_backfill_days() -> int:
    try:
        return max(1, int(get_setting("premium_broker_backfill_days", "30")))
    except Exception:
        return 30


def set_premium_broker_backfill_days(days: int) -> None:
    set_setting("premium_broker_backfill_days", str(max(1, int(days))))


def get_intraday_monitor_scheduler_enabled() -> bool:
    """Return whether the built-in intraday portfolio scheduler should run."""
    return get_setting("intraday_monitor_scheduler_enabled", "false").lower() == "true"


def set_intraday_monitor_scheduler_enabled(enabled: bool) -> None:
    """Persist the built-in intraday portfolio scheduler switch."""
    set_setting("intraday_monitor_scheduler_enabled", "true" if enabled else "false")
