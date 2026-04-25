"""
Telegram Bot 輪詢伺服器

每一則訊息（包含加入群組事件）都會自動記錄發送者到 DB。

支援指令：
  /getid    — 取得當前 chat_id 與 user_id（設定 .env 用）
  /members  — 列出本群組所有已記錄成員
  /status   — 服務狀態、上線時間、公開網址、最後心跳
  /url      — 目前公開服務網址
  /health   — 資料庫 / 模組 / 排程器健康狀態
  /help     — 顯示可用指令

啟動方式：python telegram_bot.py
.env 必填：TELEGRAM_BOT_TOKEN=...
"""
import os
import sys
import time
import logging
import threading
import argparse
from datetime import datetime

import requests
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org/bot{token}/{method}"
_RUNTIME_DIR = os.path.join(_ROOT, "runtime")
_URL_FILE = os.path.join(_RUNTIME_DIR, "cloudflared_url.txt")

_start_time = datetime.now()
_last_heartbeat: datetime | None = None
_heartbeat_lock = threading.Lock()


# ── Telegram API helpers ────────────────────────────────────────

def _token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")


def _call(method: str, *, http_timeout: int = 10, **kwargs) -> dict | None:
    token = _token()
    if not token:
        return None
    url = _API_BASE.format(token=token, method=method)
    try:
        resp = requests.post(url, json=kwargs, timeout=http_timeout)
        return resp.json()
    except Exception as e:
        logger.error("Telegram API %s failed: %s", method, e)
        return None


def _get_updates(offset: int = 0, timeout: int = 30) -> list[dict]:
    result = _call(
        "getUpdates",
        http_timeout=timeout + 10,   # HTTP timeout must exceed Telegram polling timeout
        offset=offset,
        timeout=timeout,
        allowed_updates=["message"],   # includes new_chat_members
    )
    if result and result.get("ok"):
        return result.get("result", [])
    return []


def _reply(chat_id: int | str, text: str):
    result = _call("sendMessage", chat_id=chat_id, text=text)
    if not (result and result.get("ok")):
        logger.warning("Failed to send reply to %s", chat_id)


# ── Member tracking ─────────────────────────────────────────────

def _user_display(user: dict) -> str:
    """'first last (@username)' or just first/last"""
    parts = [user.get("first_name", ""), user.get("last_name", "")]
    name = " ".join(p for p in parts if p).strip() or "—"
    uname = user.get("username", "")
    return f"{name} (@{uname})" if uname else name


def _record_user(user: dict, chat_id: int | str, via: str = "message"):
    """Write a user to DB (non-blocking, errors are logged not raised)"""
    if not user or user.get("is_bot"):
        return
    try:
        from db.telegram_members import upsert_member
        upsert_member(
            chat_id=chat_id,
            user_id=user["id"],
            username=user.get("username", ""),
            full_name=_user_display(user),
            joined_via=via,
        )
    except Exception as e:
        logger.error("_record_user failed: %s", e)


# ── Service state helpers ───────────────────────────────────────

def _service_url() -> str:
    try:
        with open(_URL_FILE, encoding="utf-8") as f:
            return f.read().strip() or "（URL 檔案為空）"
    except FileNotFoundError:
        return "（尚未取得公開網址）"
    except Exception as e:
        return f"（讀取失敗：{e}）"


def _uptime() -> str:
    delta = datetime.now() - _start_time
    total = int(delta.total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def _check_health() -> str:
    lines = []

    try:
        from db.database import get_session
        import sqlalchemy
        with get_session() as sess:
            sess.execute(sqlalchemy.text("SELECT 1"))
        lines.append("✅ 資料庫（SQLite）：正常")
    except Exception as e:
        lines.append(f"❌ 資料庫：{e}")

    streamlit_pid = os.path.join(_RUNTIME_DIR, "streamlit.pid")
    try:
        with open(streamlit_pid) as f:
            pid = f.read().strip()
        lines.append(f"✅ Streamlit：執行中（PID {pid}）")
    except FileNotFoundError:
        lines.append("⚠️ Streamlit：PID 檔案不存在")
    except Exception as e:
        lines.append(f"⚠️ Streamlit：{e}")

    try:
        import importlib
        importlib.import_module("data.finmind_client")
        lines.append("✅ FinMind 模組：已載入")
    except Exception as e:
        lines.append(f"❌ FinMind 模組：{e}")

    scheduler_pid = os.path.join(_RUNTIME_DIR, "scheduler.pid")
    if os.path.exists(scheduler_pid):
        try:
            with open(scheduler_pid) as f:
                pid = f.read().strip()
            lines.append(f"✅ 排程器：執行中（PID {pid}）")
        except Exception as e:
            lines.append(f"⚠️ 排程器：{e}")
    else:
        lines.append("ℹ️ 排程器：未偵測到 PID 檔案（可能未啟動）")

    return "\n".join(lines)


# ── Command handlers ────────────────────────────────────────────

def _handle_getid(msg: dict):
    chat = msg["chat"]
    user = msg.get("from", {})
    chat_id = chat["id"]
    user_id = user.get("id", "N/A")
    chat_type = chat.get("type", "unknown")
    chat_title = chat.get("title") or chat.get("username") or "私人對話"

    text = (
        "📋 ID 資訊\n"
        f"\nchat_id：{chat_id}"
        f"\nuser_id：{user_id}"
        f"\n類型：{chat_type}"
        f"\n名稱：{chat_title}"
        "\n\n💡 將 chat_id 填入 .env："
        "\n  TELEGRAM_STOCK_CHAT_ID=..."
        "\n  TELEGRAM_SYSTEM_CHAT_ID=..."
    )
    _reply(chat_id, text)


def _handle_joinlist(msg: dict):
    """主動登記加入推播名單"""
    chat_id = msg["chat"]["id"]
    user = msg.get("from", {})
    if not user or user.get("is_bot"):
        return

    user_id = user["id"]
    full_name = _user_display(user)

    try:
        from db.telegram_members import upsert_member
        upsert_member(
            chat_id=chat_id,
            user_id=user_id,
            username=user.get("username", ""),
            full_name=full_name,
            joined_via="joinlist",
        )
    except Exception as e:
        _reply(chat_id, f"⚠️ 登記失敗：{e}")
        return

    _reply(
        chat_id,
        f"✅ {full_name} 已登記！\n"
        f"user_id：{user_id}\n\n"
        "往後股票警示、選股推播會傳到這個群組。\n"
        "傳 /leavelist 可取消登記。",
    )


def _handle_leavelist(msg: dict):
    """取消登記（從 DB 移除）"""
    chat_id = msg["chat"]["id"]
    user = msg.get("from", {})
    if not user or user.get("is_bot"):
        return

    user_id = str(user["id"])
    full_name = _user_display(user)

    try:
        from db.database import get_session
        from db.models import TelegramChatMember
        with get_session() as sess:
            row = sess.query(TelegramChatMember).filter_by(
                chat_id=str(chat_id), user_id=user_id
            ).first()
            if row:
                sess.delete(row)
                sess.commit()
                _reply(chat_id, f"👋 {full_name} 已取消登記。")
            else:
                _reply(chat_id, "你目前不在登記名單中。")
    except Exception as e:
        _reply(chat_id, f"⚠️ 取消失敗：{e}")


def _handle_members(msg: dict):
    """列出本群組所有已記錄成員"""
    chat_id = msg["chat"]["id"]
    try:
        from db.telegram_members import list_members
        members = list_members(chat_id)
    except Exception as e:
        _reply(chat_id, f"⚠️ 無法讀取成員清單：{e}")
        return

    if not members:
        _reply(chat_id, "📋 尚未有人登記。\n傳 /joinlist 即可加入。")
        return

    lines = [f"👥 已登記成員（{len(members)} 人）"]
    for m in members:
        uid = m["user_id"]
        name = m["full_name"] or m["username"] or "—"
        uname = f"@{m['username']}" if m["username"] else ""
        via_map = {"joinlist": "主動登記", "join": "加入群組", "message": "發言"}
        via = via_map.get(m["joined_via"], m["joined_via"])
        last = m["last_seen"].strftime("%m/%d %H:%M") if m["last_seen"] else "—"
        lines.append(f"\n• {name}{(' ' + uname) if uname else ''}")
        lines.append(f"  ID: {uid}  ({via}，最後活動 {last})")

    _reply(chat_id, "\n".join(lines))


def _handle_status(msg: dict):
    chat_id = msg["chat"]["id"]
    with _heartbeat_lock:
        hb_str = _last_heartbeat.strftime("%H:%M:%S") if _last_heartbeat else "尚無"

    text = (
        "🟢 srock tool 服務狀態\n"
        f"\n上線時間：{_uptime()}"
        f"\n啟動時刻：{_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        f"\n公開網址：{_service_url()}"
        f"\n最後心跳：{hb_str}"
    )
    _reply(chat_id, text)


def _handle_url(msg: dict):
    chat_id = msg["chat"]["id"]
    _reply(chat_id, f"🔗 目前服務網址：\n{_service_url()}")


def _handle_health(msg: dict):
    chat_id = msg["chat"]["id"]
    _reply(chat_id, f"🏥 系統健康狀態\n\n{_check_health()}")


def _handle_help(msg: dict):
    chat_id = msg["chat"]["id"]
    text = (
        "📖 可用指令：\n"
        "\n/joinlist  — 登記加入推播名單"
        "\n/leavelist — 取消登記"
        "\n/members   — 列出已登記成員"
        "\n/getid     — 查詢自己的 chat_id / user_id"
        "\n/status    — 服務狀態與上線時間"
        "\n/url       — 目前公開網址"
        "\n/health    — 資料庫 / 排程器健康狀態"
        "\n/help      — 顯示此說明"
    )
    _reply(chat_id, text)


_COMMANDS: dict[str, callable] = {
    "/joinlist":  _handle_joinlist,
    "/leavelist": _handle_leavelist,
    "/members":   _handle_members,
    "/getid":     _handle_getid,
    "/status":    _handle_status,
    "/url":       _handle_url,
    "/health":    _handle_health,
    "/help":      _handle_help,
    "/start":     _handle_help,
}


# ── Update dispatcher ───────────────────────────────────────────

def _dispatch(update: dict):
    msg = update.get("message")
    if not msg:
        return

    chat_id = msg["chat"]["id"]
    sender = msg.get("from", {})

    # ── 1. Record whoever sent this message ──────────────────────
    _record_user(sender, chat_id, via="message")

    # ── 2. Record any new members that just joined ───────────────
    for new_user in msg.get("new_chat_members", []):
        _record_user(new_user, chat_id, via="join")
        logger.info("New member joined chat %s: %s", chat_id, _user_display(new_user))

    # ── 3. Route commands ────────────────────────────────────────
    raw_text = (msg.get("text") or "").strip()
    if not raw_text.startswith("/"):
        return

    cmd = raw_text.split()[0].split("@")[0].lower()
    handler = _COMMANDS.get(cmd)
    if not handler:
        return

    try:
        handler(msg)
        logger.info("Handled %s from chat %s (user %s)", cmd, chat_id, sender.get("id"))
    except Exception as e:
        logger.error("Handler error for %s: %s", cmd, e)
        try:
            _reply(chat_id, f"⚠️ 指令處理失敗：{e}")
        except Exception:
            pass


# ── Background heartbeat ────────────────────────────────────────

def _heartbeat_worker(interval: int):
    global _last_heartbeat
    while True:
        time.sleep(interval)
        with _heartbeat_lock:
            _last_heartbeat = datetime.now()
        logger.debug("Heartbeat: %s", _last_heartbeat.strftime("%H:%M:%S"))


# ── Polling main loop ───────────────────────────────────────────

def run_polling(heartbeat_interval: int = 60):
    if not _token():
        logger.error("TELEGRAM_BOT_TOKEN 未設定，Bot 無法啟動")
        return

    logger.info("Telegram Bot 啟動（long-polling）")
    logger.info("功能：自動記錄群組成員 + 指令：%s", " ".join(_COMMANDS))
    threading.Thread(target=_heartbeat_worker, args=(heartbeat_interval,), daemon=True).start()

    offset = 0
    while True:
        try:
            updates = _get_updates(offset=offset, timeout=30)
            for upd in updates:
                offset = upd["update_id"] + 1
                _dispatch(upd)
        except KeyboardInterrupt:
            logger.info("Bot 已停止")
            break
        except Exception as e:
            logger.error("Polling error: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="srock tool Telegram Bot")
    parser.add_argument("--heartbeat", type=int, default=60, help="心跳間隔秒數（預設 60）")
    args = parser.parse_args()
    run_polling(heartbeat_interval=args.heartbeat)
