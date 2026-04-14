"""
回測報告 AI 分析 Agent

使用 Gemini 1.5 Flash 對 generate_text_report() 產生的 Markdown 報告
進行深度解讀，輸出繁體中文策略診斷。
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_REVIEWER_MODEL_NAME = "gemini-2.5-flash"

_SYSTEM_PROMPT = """\
你是一位資深的量化交易分析師。請根據這份包含 8 個區段的回測報告進行深度解讀。

你的任務是：
1. 判斷策略的「穩定性」：從月度績效與 MDD 判斷這是否為過度擬合（Overfitting）或只適合特定行情。
2. 分析「出場品質」：觀察移動停損/停利的佔比，評估策略是否能有效「截斷虧損、讓利潤奔跑」。
3. 挖掘「盲點」：從略過原因與最差交易中，找出策略可能遺漏的風險因素（如流動性不足或波動過大）。
4. 給出「具體優化方向」：根據數據建議調整參數（如 ATR 倍數、BIAS 門檻或最大持倉數）。

請用繁體中文回答，使用專業、客觀的語氣，並適當使用 Markdown 標題與列表讓報告易於閱讀。\
"""


def analyze_backtest_report(report_md: str) -> str:
    """
    使用 Gemini 1.5 Flash 解讀回測報告。

    Args:
        report_md: generate_text_report() 產生的 Markdown 字串

    Returns:
        AI 分析結果字串；若 API 不可用或發生錯誤，回傳帶有說明的錯誤訊息字串。
    """
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        return "⚠️ 未設定 GEMINI_API_KEY，無法使用 AI 分析功能。請在 .env 填入金鑰後重新啟動。"

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=_REVIEWER_MODEL_NAME,
            contents=report_md,
            config=types.GenerateContentConfig(
                system_instruction=_SYSTEM_PROMPT,
                temperature=0.4,
            ),
        )
        return response.text

    except Exception as e:
        logger.error(f"Gemini 分析失敗：{e}")
        return f"⚠️ AI 分析執行失敗：{e}\n\n請確認 API 金鑰有效，或稍後再試。"
