"""
LINE Messaging API 推播模組

設定步驟：
1. 前往 LINE Developers Console 建立 Messaging API Channel
2. 取得 Channel Access Token（長期）
3. 將 Bot 加為好友，取得你的 User ID（可用 /getid 指令或 webhook）
4. 將以上資訊填入 .env

.env 範例：
  LINE_CHANNEL_ACCESS_TOKEN=your_token
  LINE_USER_ID=Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

PUSH_URL = "https://api.line.me/v2/bot/message/push"


def send_message(message: str, user_id: str = None, token: str = None) -> bool:
    """
    傳送 LINE 訊息給指定使用者

    Args:
        message: 要傳送的文字內容
        user_id: LINE User ID（Uxxxxxxx...），若未傳入從環境變數讀取
        token: Channel Access Token，若未傳入從環境變數讀取

    Returns:
        True 代表成功，False 代表失敗
    """
    token = token or os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
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


def send_scan_results(results: list, top_n: int = 5) -> bool:
    """
    推播選股雷達結果前 N 名

    Args:
        results: run_scan() 回傳的 DataFrame.to_dict('records')
        top_n: 最多推播幾檔
    """
    if not results:
        return send_message("📊 今日選股雷達：無符合條件的股票")

    lines = ["📊 今日選股雷達 — 強勢候選股"]
    for i, row in enumerate(results[:top_n], 1):
        change_emoji = "🔴" if row.get("change_pct", 0) > 0 else "🟢"
        lines.append(
            f"\n{i}. {row['stock_id']} {row['stock_name']}"
            f"\n   收盤：{row['close']} 元  {change_emoji}{row['change_pct']:+.2f}%"
            f"\n   量比：{row['volume_ratio']:.1f}x  分數：{row['score']}"
            f"\n   [{row['signals']}]"
        )

    return send_message("\n".join(lines))
