"""
Google Gemini LLM 客戶端

使用方式：
    from data.llm_client import get_gemini_client

    client = get_gemini_client()
    if client is None:
        # 金鑰未設定，跳過 AI 功能
        ...
    else:
        from google.genai import types
        response = client.models.generate_content(
            model="gemini-1.5-flash",
            contents="你的 prompt",
        )
        print(response.text)
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_client = None  # 模組層級快取，同一 Python 進程只初始化一次


def get_gemini_client():
    """
    取得已初始化的 Gemini Client 實例（Singleton）。

    使用新版 google-genai SDK（google.genai.Client）。

    - 讀取 .env 的 GEMINI_API_KEY
    - 金鑰不存在或初始化失敗時回傳 None，不拋例外

    Returns:
        google.genai.Client | None
    """
    global _client

    if _client is not None:
        return _client

    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        logger.debug("GEMINI_API_KEY 未設定，Gemini 功能停用")
        return None

    try:
        from google import genai

        _client = genai.Client(api_key=api_key)
        logger.info("Gemini Client 初始化成功")
        return _client

    except Exception as e:
        logger.warning(f"Gemini Client 初始化失敗：{e}")
        return None
