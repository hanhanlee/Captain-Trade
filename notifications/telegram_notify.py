"""
Telegram 推播模組

.env 設定：
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_STOCK_CHAT_ID=...       ← 股票警示群組 / 頻道
  TELEGRAM_SYSTEM_CHAT_ID=...      ← 系統訊息群組（選填；未設定時 fallback 到 STOCK）
"""
import os
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org/bot{token}/{method}"


# ── Internal ────────────────────────────────────────────────────

def _post(method: str, token: str, max_retries: int = 3, **kwargs) -> dict | None:
    url = _API_BASE.format(token=token, method=method)
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=kwargs, timeout=10)
            if resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
                logger.warning("Telegram 429 rate limit，等待 %s 秒後重試", retry_after)
                time.sleep(retry_after)
                continue
            data = resp.json()
            if not data.get("ok"):
                logger.warning("Telegram %s error: %s", method, data.get("description"))
            return data
        except Exception as e:
            logger.error("Telegram request failed (%s): %s", method, e)
            return None
    logger.error("Telegram %s 超過最大重試次數（%s）", method, max_retries)
    return None


def _token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")


# ── Public API ──────────────────────────────────────────────────

def send_message(text: str, chat_id: str | int = None) -> bool:
    """
    傳送純文字訊息到指定 chat_id。
    未傳入 chat_id 時使用 TELEGRAM_STOCK_CHAT_ID。
    """
    token = _token()
    if not token:
        logger.warning("TELEGRAM_BOT_TOKEN 未設定，跳過推播")
        return False

    target = str(chat_id) if chat_id else os.getenv("TELEGRAM_STOCK_CHAT_ID", "")
    if not target:
        logger.warning("Telegram chat_id 未設定，跳過推播")
        return False

    result = _post("sendMessage", token, chat_id=target, text=text)
    return bool(result and result.get("ok"))


def send_stock_alert(text: str) -> bool:
    """推播股票警示到 TELEGRAM_STOCK_CHAT_ID"""
    return send_message(text, chat_id=os.getenv("TELEGRAM_STOCK_CHAT_ID", ""))


def send_system_message(text: str) -> bool:
    """
    推播系統訊息到 TELEGRAM_SYSTEM_CHAT_ID。
    未設定時 fallback 到 TELEGRAM_STOCK_CHAT_ID。
    """
    chat_id = os.getenv("TELEGRAM_SYSTEM_CHAT_ID") or os.getenv("TELEGRAM_STOCK_CHAT_ID", "")
    return send_message(text, chat_id=chat_id)


def send_scan_results(results: list, top_n: int = 5) -> bool:
    """推播選股雷達結果前 N 名到 TELEGRAM_STOCK_CHAT_ID"""
    if not results:
        return send_stock_alert("📊 今日選股雷達：無符合條件的股票")

    lines = ["📊 今日選股雷達 — 強勢候選股"]
    for i, row in enumerate(results[:top_n], 1):
        change_emoji = "🔴" if row.get("change_pct", 0) > 0 else "🟢"
        lines.append(
            f"\n{i}. {row['stock_id']} {row['stock_name']}"
            f"\n   收盤：{row['close']} 元  {change_emoji}{row['change_pct']:+.2f}%"
            f"\n   量比：{row['volume_ratio']:.1f}x  分數：{row['score']}"
            f"\n   [{row['signals']}]"
        )

    return send_stock_alert("\n".join(lines))
