"""
srock.db → Google Drive 備份模組

執行流程：
  1. sqlite3.backup()  安全複製 DB（WAL 相容）
  2. gzip 壓縮       → temp_backup.db.gz
  3. rclone copyto   上傳至 gdrive:srock_backup_YYYYMMDD.db.gz
  4. 刪除本機暫存
  5. rclone delete   移除雲端超過 14 天的舊備份

.env 必填：
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID   （或 TELEGRAM_SYSTEM_CHAT_ID / TELEGRAM_STOCK_CHAT_ID）
"""

import gzip
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR    = Path(__file__).resolve().parent.parent
DB_PATH     = BASE_DIR / "srock.db"
TEMP_DB     = BASE_DIR / "temp_backup.db"
TEMP_GZ     = BASE_DIR / "temp_backup.db.gz"
RCLONE_REMOTE = "gdrive:"


# ── Telegram 錯誤通知 ──────────────────────────────────────────────

def _notify(msg: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = (
        os.getenv("TELEGRAM_CHAT_ID")
        or os.getenv("TELEGRAM_SYSTEM_CHAT_ID")
        or os.getenv("TELEGRAM_STOCK_CHAT_ID", "")
    )
    if not token or not chat_id:
        logger.warning("Telegram 未設定，跳過推播")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=10)
        if not resp.json().get("ok"):
            logger.error("Telegram 推播未成功: %s", resp.text)
    except Exception as exc:
        logger.error("Telegram 推播失敗: %s", exc)


def _notify_error(msg: str) -> None:
    _notify(msg)


# ── 暫存清理 ───────────────────────────────────────────────────────

def _cleanup(*paths: Path) -> None:
    for p in paths:
        try:
            if p.exists():
                p.unlink()
                logger.info("已刪除暫存: %s", p.name)
        except OSError as exc:
            logger.warning("刪除暫存失敗 %s: %s", p.name, exc)


# ── 各步驟 ─────────────────────────────────────────────────────────

def step_backup_db() -> None:
    """使用 sqlite3.backup API 安全複製 DB（禁用 shutil.copy）"""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"來源 DB 不存在: {DB_PATH}")

    src = sqlite3.connect(str(DB_PATH))
    dst = sqlite3.connect(str(TEMP_DB))
    try:
        src.backup(dst, pages=100)   # pages=100 每批 100 頁，減輕鎖競爭
        logger.info("sqlite3.backup 完成 → %s (%.1f KB)",
                    TEMP_DB.name, TEMP_DB.stat().st_size / 1024)
    finally:
        dst.close()
        src.close()


def step_compress() -> None:
    """串流 gzip 壓縮，不整塊讀入記憶體"""
    with open(TEMP_DB, "rb") as f_in, gzip.open(TEMP_GZ, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)
    logger.info("gzip 壓縮完成 → %s (%.1f KB)",
                TEMP_GZ.name, TEMP_GZ.stat().st_size / 1024)


def step_upload() -> None:
    """透過 rclone copyto 上傳至 gdrive:（list 格式，路徑含空格安全）"""
    today       = datetime.now().strftime("%Y%m%d")
    remote_name = f"srock_backup_{today}.db.gz"

    cmd = [
        "rclone", "copyto",
        str(TEMP_GZ),               # 本機來源（Path.str 正確處理含空格路徑）
        f"{RCLONE_REMOTE}{remote_name}",
        "--stats", "10s",
    ]
    logger.info("上傳指令: %s", cmd)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"rclone upload 失敗 (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    logger.info("上傳完成: %s%s", RCLONE_REMOTE, remote_name)


def step_verify_upload() -> None:
    """確認雲端確實存在剛上傳的檔案"""
    today  = datetime.now().strftime("%Y%m%d")
    target = f"srock_backup_{today}.db.gz"

    cmd = ["rclone", "ls", RCLONE_REMOTE, "--include", target]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 or target not in result.stdout:
        raise RuntimeError(f"上傳驗證失敗，雲端找不到 {target}:\n{result.stdout}{result.stderr}")
    logger.info("上傳驗證通過: %s 存在於 %s", target, RCLONE_REMOTE)


def step_delete_old_backups() -> None:
    """刪除雲端超過 14 天的舊備份（限定 pattern，不誤刪其他檔案）"""
    cmd = [
        "rclone", "delete",
        RCLONE_REMOTE,
        "--min-age", "14d",
        "--include", "srock_backup_*.db.gz",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"rclone delete 舊備份失敗 (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    logger.info("已清除 gdrive: 中超過 14 天的舊備份")


# ── 主流程 ─────────────────────────────────────────────────────────

def run_backup() -> None:
    logger.info("=== srock.db 備份開始 [%s] ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        step_backup_db()
        step_compress()
        _cleanup(TEMP_DB)           # 壓縮成功後立即刪除未壓縮暫存
        step_upload()
        step_verify_upload()
        _cleanup(TEMP_GZ)           # 上傳驗證通過後刪除壓縮暫存
        step_delete_old_backups()
        gz_kb = TEMP_GZ.stat().st_size / 1024 if TEMP_GZ.exists() else 0
        _notify(
            f"[srock 備份成功] {datetime.now():%Y-%m-%d %H:%M}\n"
            f"srock_backup_{datetime.now():%Y%m%d}.db.gz 已上傳至 gdrive:\n"
            f"（14 天前舊備份已自動清除）"
        )
        logger.info("=== 備份流程全部完成 ===")
    except Exception:
        tb = traceback.format_exc()
        logger.error("備份失敗:\n%s", tb)
        _notify_error(
            f"[srock 備份失敗] {datetime.now():%Y-%m-%d %H:%M}\n\n{tb[:3800]}"  # Telegram 上限 4096 字元
        )
        _cleanup(TEMP_DB, TEMP_GZ)  # 確保清除殘留暫存
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    run_backup()
