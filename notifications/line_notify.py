"""
LINE Messaging API 推播模組

設定步驟：
1. 前往 LINE Developers Console 建立 Messaging API Channel
2. 取得 Channel Access Token（長期）
3. 將 Bot 加為好友，取得你的 User ID（可用 /getid 指令或 webhook）
4. 將以上資訊填入 .env

.env 範例：
  LINE_CHANNEL_ACCESS_TOKEN=your_token
  LINE_USER_ID=Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   ← 無 DB 訂閱者時的 fallback

推播模式：
  - send_message()    單人推播（指定 user_id 或用 env 預設值）
  - send_multicast()  群播（DB 所有 enabled 訂閱者；DB 空時 fallback env USER_ID）
"""
import os
import re
import requests
from dotenv import load_dotenv

load_dotenv()

PUSH_URL       = "https://api.line.me/v2/bot/message/push"
MULTICAST_URL  = "https://api.line.me/v2/bot/message/multicast"


_LINE_USER_ID_RE = re.compile(r"^U[0-9A-Fa-f]{32}$")


def _is_valid_user_id(user_id: str) -> bool:
    """檢查是否為 LINE User ID（U + 32 碼 hex）"""
    return bool(_LINE_USER_ID_RE.fullmatch((user_id or "").strip()))




# ── 單人推播 ────────────────────────────────────────────────────

def send_message(message: str, user_id: str = None, token: str = None) -> bool:
    """
    傳送 LINE 訊息給指定使用者

    Args:
        message: 要傳送的文字內容
        user_id: LINE User ID（Uxxxxxxx...），若未傳入從環境變數讀取
        token:   Channel Access Token，若未傳入從環境變數讀取
    """
    token   = token   or os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
    user_id = user_id or os.getenv("LINE_USER_ID", "")

    if not token:
        print("LINE_CHANNEL_ACCESS_TOKEN 未設定，跳過推播。")
        return False
    if not user_id:
        print("LINE_USER_ID 未設定，跳過推播。")
        return False

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": user_id,
        "messages": [{"type": "text", "text": message}],
    }
    try:
        resp = requests.post(PUSH_URL, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            return True
        print(f"LINE 推播失敗：{resp.status_code} {resp.text}")
        return False
    except Exception as e:
        print(f"LINE 推播例外：{e}")
        return False


# ── 群播 ────────────────────────────────────────────────────────

def send_multicast(message: str, user_ids: list[str] = None) -> bool:
    """
    群播訊息

    Args:
        message:  要傳送的文字內容
        user_ids: 指定收件人清單；None 表示推給 DB 所有 enabled 訂閱者。
                  DB 無訂閱者時自動 fallback 到 env LINE_USER_ID。

    LINE multicast API 每次最多 500 人，超過自動分批。
    """
    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
    if not token:
        print("LINE_CHANNEL_ACCESS_TOKEN 未設定，跳過推播。")
        return False

    if user_ids is None:
        user_ids = _get_enabled_user_ids()

    # DB 空時 fallback 到 env（向下相容舊部署）
    if not user_ids:
        env_uid = os.getenv("LINE_USER_ID", "")
        if env_uid:
            return send_message(message)
        print("無訂閱者且 LINE_USER_ID 未設定，跳過推播。")
        return False

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    success = True
    for i in range(0, len(user_ids), 500):
        batch = user_ids[i : i + 500]
        payload = {
            "to": batch,
            "messages": [{"type": "text", "text": message}],
        }
        try:
            resp = requests.post(MULTICAST_URL, headers=headers, json=payload, timeout=10)
            if resp.status_code != 200:
                print(f"LINE 群播失敗（批次 {i//500 + 1}）：{resp.status_code} {resp.text}")
                success = False
        except Exception as e:
            print(f"LINE 群播例外：{e}")
            success = False
    return success


# ── 訂閱者管理 ───────────────────────────────────────────────────

def _get_enabled_user_ids() -> list[str]:
    """從 DB 讀取所有 enabled=True 的訂閱者 User ID"""
    try:
        from db.database import get_session
        from db.models import LineSubscriber
        with get_session() as sess:
            rows = sess.query(LineSubscriber).filter_by(enabled=True).all()
            return [r.user_id for r in rows]
    except Exception as e:
        print(f"讀取訂閱者清單失敗：{e}")
        return []


def get_all_subscribers() -> list[dict]:
    """回傳所有訂閱者（含 enabled=False），供 UI 顯示"""
    try:
        from db.database import get_session
        from db.models import LineSubscriber
        with get_session() as sess:
            rows = sess.query(LineSubscriber).order_by(LineSubscriber.created_at).all()
            return [
                {
                    "user_id":      r.user_id,
                    "display_name": r.display_name or "",
                    "enabled":      r.enabled,
                    "created_at":   r.created_at,
                }
                for r in rows
            ]
    except Exception as e:
        print(f"讀取訂閱者清單失敗：{e}")
        return []


def add_subscriber(user_id: str, display_name: str = "") -> str:
    """
    新增訂閱者。

    Returns:
        'added'   — 新增成功
        'updated' — 已存在，更新顯示名稱
        'invalid' — User ID 格式錯誤
        'error'   — 失敗
    """
    user_id = (user_id or "").strip()
    display_name = (display_name or "").strip()
    if not _is_valid_user_id(user_id):
        return "invalid"

    try:
        from db.database import get_session
        from db.models import LineSubscriber
        with get_session() as sess:
            existing = sess.query(LineSubscriber).filter_by(user_id=user_id).first()
            if existing:
                if display_name:
                    existing.display_name = display_name
                    sess.commit()
                return "updated"
            sess.add(LineSubscriber(user_id=user_id, display_name=display_name, enabled=True))
            sess.commit()
            return "added"
    except Exception as e:
        print(f"新增訂閱者失敗：{e}")
        return "error"


def remove_subscriber(user_id: str) -> bool:
    """移除訂閱者"""
    try:
        from db.database import get_session
        from db.models import LineSubscriber
        with get_session() as sess:
            row = sess.query(LineSubscriber).filter_by(user_id=user_id).first()
            if row:
                sess.delete(row)
                sess.commit()
                return True
        return False
    except Exception as e:
        print(f"刪除訂閱者失敗：{e}")
        return False


def set_subscriber_enabled(user_id: str, enabled: bool) -> bool:
    """啟用 / 暫停單一訂閱者的群播接收"""
    try:
        from db.database import get_session
        from db.models import LineSubscriber
        with get_session() as sess:
            row = sess.query(LineSubscriber).filter_by(user_id=user_id).first()
            if row:
                row.enabled = enabled
                sess.commit()
                return True
        return False
    except Exception as e:
        print(f"更新訂閱者狀態失敗：{e}")
        return False


# ── 選股結果推播（支援群播與單人）──────────────────────────────────

def sync_env_subscriber() -> bool:
    """
    若 .env 的 LINE_USER_ID 尚未在 DB 裡，自動補進去（顯示名稱「管理員」）。
    頁面載入時呼叫一次，確保 env 設定的用戶一定出現在訂閱者列表。
    """
    uid = os.getenv("LINE_USER_ID", "").strip()
    if not uid:
        return False
    result = add_subscriber(uid, display_name="管理員")
    return result == "added"


# ── 選股結果推播（支援群播與單人）──────────────────────────────────

def send_scan_results(results: list, top_n: int = 5, user_ids: list[str] = None) -> bool:
    """
    推播選股雷達結果前 N 名

    Args:
        results:  run_scan() 回傳的 DataFrame.to_dict('records')
        top_n:    最多推播幾檔
        user_ids: 指定收件人；None = 群播全部 enabled 訂閱者
    """
    if not results:
        return send_multicast("📊 今日選股雷達：無符合條件的股票", user_ids=user_ids)

    lines = ["📊 今日選股雷達 — 強勢候選股"]
    for i, row in enumerate(results[:top_n], 1):
        change_emoji = "🔴" if row.get("change_pct", 0) > 0 else "🟢"
        lines.append(
            f"\n{i}. {row['stock_id']} {row['stock_name']}"
            f"\n   收盤：{row['close']} 元  {change_emoji}{row['change_pct']:+.2f}%"
            f"\n   量比：{row['volume_ratio']:.1f}x  分數：{row['score']}"
            f"\n   [{row['signals']}]"
        )

    return send_multicast("\n".join(lines), user_ids=user_ids)
