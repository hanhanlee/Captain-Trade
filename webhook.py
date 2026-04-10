"""
LINE Webhook 伺服器

功能：
  - 有人加 Bot 好友 (follow)    → 自動加入推播訂閱者
  - 有人傳訊息給 Bot            → 若未訂閱則自動加入，並回覆歡迎訊息
  - 使用者傳「我的ID」           → 回覆其 LINE User ID（方便手動核對）
  - 使用者傳「取消訂閱」         → 停用群播（保留記錄）
  - 使用者傳「重新訂閱」         → 重新啟用群播

啟動方式：
  pip install flask
  python webhook.py              ← 預設 port 5000
  python webhook.py --port 8080

對外曝光（本機開發）：
  ngrok http 5000
  → 取得 https://xxxx.ngrok.io，填入 LINE Developers Console → Webhook URL

.env 需要設定：
  LINE_CHANNEL_ACCESS_TOKEN=...
  LINE_CHANNEL_SECRET=...         ← 用於驗證 LINE 請求簽章（防偽造）
"""
import os
import hmac
import hashlib
import base64
import json
import logging
import argparse
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

REPLY_URL = "https://api.line.me/v2/bot/message/reply"


# ── 簽章驗證 ────────────────────────────────────────────────────

def _verify_signature(body: bytes, signature: str) -> bool:
    """驗證 LINE 請求的 X-Line-Signature，防止偽造"""
    secret = os.getenv("LINE_CHANNEL_SECRET", "")
    if not secret:
        logger.warning("LINE_CHANNEL_SECRET 未設定，跳過簽章驗證（不建議用於正式環境）")
        return True
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode()
    return hmac.compare_digest(expected, signature)


# ── 回覆訊息 ────────────────────────────────────────────────────

def _reply(reply_token: str, text: str):
    import requests as _req
    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
    if not token or not reply_token:
        return
    _req.post(
        REPLY_URL,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"replyToken": reply_token, "messages": [{"type": "text", "text": text}]},
        timeout=5,
    )


# ── 事件處理 ────────────────────────────────────────────────────

def _handle_follow(user_id: str, reply_token: str):
    """加好友事件：自動訂閱"""
    from notifications.line_notify import add_subscriber
    result = add_subscriber(user_id)
    if result == "added":
        logger.info(f"新訂閱者（加好友）：{user_id}")
        _reply(reply_token,
               "👋 您已成功訂閱 srock tool 推播！\n"
               "每日盤後選股、持股警示、週績效報告將自動傳送給您。\n\n"
               "輸入「取消訂閱」可暫停接收，「重新訂閱」可恢復。")
    else:
        logger.info(f"重複加好友（已訂閱）：{user_id}")
        _reply(reply_token, "您已在訂閱名單中，無需重複操作。")


def _handle_unfollow(user_id: str):
    """封鎖 / 刪除好友事件：停用訂閱（保留記錄）"""
    from notifications.line_notify import set_subscriber_enabled
    set_subscriber_enabled(user_id, False)
    logger.info(f"訂閱者封鎖/刪除好友，已停用：{user_id}")


def _handle_message(user_id: str, reply_token: str, text: str):
    """訊息事件：自動訂閱 + 指令處理"""
    from notifications.line_notify import add_subscriber, set_subscriber_enabled, get_all_subscribers

    text = text.strip()

    # 指令處理（優先）
    if text == "我的ID":
        _reply(reply_token, f"您的 LINE User ID：\n{user_id}")
        return

    if text == "取消訂閱":
        set_subscriber_enabled(user_id, False)
        _reply(reply_token, "已暫停群播推播。輸入「重新訂閱」可恢復。")
        return

    if text == "重新訂閱":
        add_subscriber(user_id)
        set_subscriber_enabled(user_id, True)
        _reply(reply_token, "✅ 已重新開啟群播推播！")
        return

    # 非指令訊息：確保自動訂閱
    result = add_subscriber(user_id)
    if result == "added":
        logger.info(f"新訂閱者（傳訊息）：{user_id}")
        _reply(reply_token,
               "👋 已自動加入推播訂閱！\n"
               "可用指令：\n"
               "  我的ID    — 查詢您的 LINE User ID\n"
               "  取消訂閱  — 暫停接收推播\n"
               "  重新訂閱  — 恢復接收推播")


# ── Webhook 路由 ────────────────────────────────────────────────

@app.route("/webhook", methods=["GET"])
def webhook_verify():
    """LINE Developers Console 的 Verify 按鈕會打 GET，回 200 即可"""
    return jsonify({"status": "ok"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_data()
    signature = request.headers.get("X-Line-Signature", "")

    if not _verify_signature(body, signature):
        logger.warning("Webhook 簽章驗證失敗，拒絕請求")
        return jsonify({"error": "invalid signature"}), 403

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return jsonify({"error": "invalid json"}), 400

    for event in payload.get("events", []):
        event_type = event.get("type")
        user_id    = event.get("source", {}).get("userId", "")
        reply_token = event.get("replyToken", "")

        if not user_id:
            continue

        if event_type == "follow":
            _handle_follow(user_id, reply_token)

        elif event_type == "unfollow":
            _handle_unfollow(user_id)

        elif event_type == "message":
            msg_text = event.get("message", {}).get("text", "")
            _handle_message(user_id, reply_token, msg_text)

    return jsonify({"status": "ok"}), 200


# ── 啟動 ────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    logger.info(f"LINE Webhook 伺服器啟動：http://{args.host}:{args.port}/webhook")
    logger.info("對外曝光請使用 ngrok：ngrok http %d", args.port)
    app.run(host=args.host, port=args.port)
