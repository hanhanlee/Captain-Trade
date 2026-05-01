"""
DB 備份運行旗標（file-based lock）

備份期間 srock.db 會被 VACUUM INTO 鎖住寫入長達 1–3 分鐘，因此：
  - scripts/backup_gdrive.py 開始前 acquire_lock() 寫入 runtime/backup.lock
  - 結束時 release_lock() 移除（不論成功失敗，try/finally 確保）
  - app.py 啟動時呼叫 read_lock() 判斷是否要在 UI 顯示警示

跨程序共享：jobs.py（外部排程行程）與 app.py（Streamlit）兩個獨立 Python
程序，透過檔案系統交換狀態。lock 檔內容是 JSON，方便 UI 顯示開始時間。
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent.parent
LOCK_FILE = _BASE_DIR / "runtime" / "backup.lock"


def acquire_lock() -> None:
    """建立 lock 檔。已存在則覆蓋（前一次可能異常未釋放，新的覆蓋掉即可）。"""
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pid": os.getpid(),
    }
    LOCK_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    logger.info("backup lock 已建立: %s", LOCK_FILE)


def release_lock() -> None:
    """移除 lock 檔。檔案不存在不算錯。"""
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
            logger.info("backup lock 已釋放")
    except OSError as exc:
        logger.warning("釋放 backup lock 失敗: %s", exc)


def read_lock() -> dict | None:
    """讀取 lock 檔。回傳 dict 或 None（不存在/讀取失敗）。"""
    if not LOCK_FILE.exists():
        return None
    try:
        return json.loads(LOCK_FILE.read_text(encoding="utf-8"))
    except Exception:
        # 檔案存在但內容壞掉，仍視為「正在備份」以策安全
        return {"started_at": "未知", "pid": None}


def is_backup_running() -> bool:
    return LOCK_FILE.exists()
