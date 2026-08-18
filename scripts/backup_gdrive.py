"""
srock.db → Google Drive 備份模組

執行流程：
  0. rclone preflight       確認 OAuth token 有效（避免 VACUUM 完才在 upload 死）
  1. VACUUM INTO            產生乾淨備份（defrag + WAL 安全，不卡鎖）
  2. gzip 壓縮              → temp_backup.db.gz
  3. rclone copyto          上傳至 gdrive:srock_backup_YYYYMMDD.db.gz
  4. rclone ls 驗證          雲端確實存在後才刪本機暫存
  5. rclone delete --min-age 30d  移除雲端超過 30 天的舊備份

Rclone remote 由使用者自行設定（建議 OAuth 綁定個人 Drive）：
  rclone config → 名稱 gdrive → 個人帳號 OAuth → 不要 service_account_file

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
import time
import traceback
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

# 確保 scheduler/db 等專案模組可被 import（直接執行 python scripts/backup_gdrive.py 時）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scheduler._backup_lock import acquire_lock, release_lock

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR      = Path(__file__).resolve().parent.parent
DB_PATH       = BASE_DIR / "srock.db"
TEMP_DB       = BASE_DIR / "temp_backup.db"
TEMP_GZ       = BASE_DIR / "temp_backup.db.gz"
RCLONE_REMOTE = "gdrive:"
RETENTION_DAYS = 30

# VACUUM INTO 等待鎖的最長時間（ms）
# 背景 Worker 瘋狂寫入時，SQLite 最多等這麼久才放棄
_BUSY_TIMEOUT_MS = 120_000   # 2 分鐘

# Rclone 子程序預設逾時（秒）。preflight/ls/delete 走較短逾時，upload 不限。
_RCLONE_QUICK_TIMEOUT = 30

# preflight 專用：shared client_id 偶爾被 Google 限流，lsd 會突然變慢
# （平常約 7 秒，限流時可能 >30 秒）。用較長逾時 + 重試，避免 06:50 備份
# 因一次卡頓就整個 abort。
_PREFLIGHT_TIMEOUT = 90
_PREFLIGHT_RETRIES = 3
_PREFLIGHT_BACKOFF = 5   # 每次失敗後等待秒數


# ── rclone 路徑解析 ────────────────────────────────────────────────

def _rclone_path() -> str:
    """自動找 rclone 執行檔；WinGet 安裝時不一定在 PATH 裡。"""
    found = shutil.which("rclone")
    if found:
        return found
    # WinGet 預設安裝路徑
    winget = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft/WinGet/Links/rclone.exe"
    if winget.exists():
        return str(winget)
    raise FileNotFoundError(
        "找不到 rclone 執行檔。請確認已安裝並加入 PATH，"
        "或檢查 WinGet 路徑是否正確。"
    )


# ── Telegram 推播 ──────────────────────────────────────────────────

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


def _log_event_safe(severity: str, summary: str, payload: dict | None = None) -> None:
    """
    寫入 event_log；任何失敗都吞掉，不影響備份流程。

    刻意延後 import：
      - 避免和 scheduler/jobs.py 的 import 形成循環
      - 部分執行情境（單純測試 rclone）可能沒有 DB
    """
    try:
        from db.event_log import log_event
        log_event(
            event_type="db_backup",
            module="backup_gdrive",
            severity=severity,
            summary=summary,
            payload=payload or {},
        )
    except Exception as exc:
        logger.warning("event_log 寫入失敗（severity=%s）：%s", severity, exc)


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

def step_preflight_remote() -> None:
    """
    在開始 VACUUM 之前，先確認 rclone remote 可用。

    不做這個檢查的話，OAuth token 過期或網路斷線時會走完 1–3 分鐘 VACUUM、
    再 gzip 250MB，最後才在 upload 階段炸掉，工作 4 分鐘等於白做。
    """
    rclone = _rclone_path()
    cmd = [rclone, "lsd", RCLONE_REMOTE, "--max-depth", "1"]
    last_err = ""
    for attempt in range(1, _PREFLIGHT_RETRIES + 1):
        try:
            result = subprocess.run(
                cmd, capture_output=True,
                encoding="utf-8", errors="replace", timeout=_PREFLIGHT_TIMEOUT
            )
        except subprocess.TimeoutExpired:
            last_err = f"逾時（>{_PREFLIGHT_TIMEOUT}s）"
            logger.warning(
                "rclone preflight 第 %s/%s 次逾時，%s 秒後重試…",
                attempt, _PREFLIGHT_RETRIES, _PREFLIGHT_BACKOFF,
            )
        else:
            if result.returncode == 0:
                logger.info("rclone preflight OK（第 %s 次）", attempt)
                return
            last_err = f"exit {result.returncode}：{result.stderr.strip()}"
            logger.warning(
                "rclone preflight 第 %s/%s 次失敗（%s），%s 秒後重試…",
                attempt, _PREFLIGHT_RETRIES, last_err, _PREFLIGHT_BACKOFF,
            )
        if attempt < _PREFLIGHT_RETRIES:
            time.sleep(_PREFLIGHT_BACKOFF)

    raise RuntimeError(
        f"rclone preflight 連續 {_PREFLIGHT_RETRIES} 次失敗（最後：{last_err}）。"
        f"OAuth token 可能過期、網路斷線，或 shared client_id 被限流。"
    )


def step_backup_db() -> None:
    """
    用 VACUUM INTO 產生備份。

    相較於 sqlite3.backup()：
    - 直接在 SQLite 引擎層執行，不受 WAL 積壓影響
    - 輸出是已 defrag、去除空洞的乾淨 DB（2GB DB 通常縮到 1GB 以下）
    - busy_timeout 確保 Worker 瘋狂寫入時最多等 2 分鐘，不會無限卡死
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"來源 DB 不存在: {DB_PATH}")

    # VACUUM INTO 要求目標檔案不存在
    if TEMP_DB.exists():
        TEMP_DB.unlink()

    conn = sqlite3.connect(str(DB_PATH), timeout=_BUSY_TIMEOUT_MS / 1000)
    try:
        conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        logger.info("執行 VACUUM INTO（可能需要 1–3 分鐘）...")
        conn.execute(f"VACUUM INTO '{TEMP_DB}'")
        size_mb = TEMP_DB.stat().st_size / 1024 / 1024
        logger.info("VACUUM INTO 完成 → %s (%.1f MB)", TEMP_DB.name, size_mb)
    finally:
        conn.close()


def step_compress() -> None:
    """串流 gzip 壓縮，不整塊讀入記憶體"""
    with open(TEMP_DB, "rb") as f_in, gzip.open(TEMP_GZ, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)
    size_mb = TEMP_GZ.stat().st_size / 1024 / 1024
    logger.info("gzip 壓縮完成 → %s (%.1f MB)", TEMP_GZ.name, size_mb)


def step_upload() -> None:
    """透過 rclone copyto 上傳至 gdrive:（list 格式，路徑含空格安全）"""
    rclone   = _rclone_path()
    today    = datetime.now().strftime("%Y%m%d")
    remote   = f"{RCLONE_REMOTE}srock_backup_{today}.db.gz"

    cmd = [rclone, "copyto", str(TEMP_GZ), remote, "--stats", "10s"]
    logger.info("上傳指令: %s", cmd)
    result = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        raise RuntimeError(
            f"rclone upload 失敗 (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    logger.info("上傳完成: %s", remote)


def step_verify_upload() -> None:
    """確認雲端確實存在剛上傳的檔案"""
    rclone = _rclone_path()
    today  = datetime.now().strftime("%Y%m%d")
    target = f"srock_backup_{today}.db.gz"

    cmd    = [rclone, "ls", RCLONE_REMOTE, "--include", target]
    result = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace")
    if result.returncode != 0 or target not in result.stdout:
        raise RuntimeError(
            f"上傳驗證失敗，雲端找不到 {target}:\n{result.stdout}{result.stderr}"
        )
    logger.info("上傳驗證通過: %s 存在於 %s", target, RCLONE_REMOTE)


def step_delete_old_backups() -> None:
    """刪除雲端超過 N 天的舊備份（限定 pattern，不誤刪其他檔案）"""
    rclone = _rclone_path()
    min_age = f"{RETENTION_DAYS}d"
    cmd = [rclone, "delete", RCLONE_REMOTE,
           "--min-age", min_age, "--include", "srock_backup_*.db.gz"]
    result = subprocess.run(
        cmd, capture_output=True,
        encoding="utf-8", errors="replace", timeout=_RCLONE_QUICK_TIMEOUT
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"rclone delete 舊備份失敗 (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    logger.info("已清除 gdrive: 中超過 %s 的舊備份", min_age)


def list_remote_backups() -> list[str]:
    """列出雲端目前所有 srock_backup_*.db.gz；只用於成功訊息回報。"""
    rclone = _rclone_path()
    cmd = [rclone, "lsf", RCLONE_REMOTE, "--include", "srock_backup_*.db.gz"]
    try:
        result = subprocess.run(
            cmd, capture_output=True,
        encoding="utf-8", errors="replace", timeout=_RCLONE_QUICK_TIMEOUT
        )
    except subprocess.TimeoutExpired:
        return []
    if result.returncode != 0:
        return []
    return sorted(line.strip() for line in result.stdout.splitlines() if line.strip())


# ── 主流程 ─────────────────────────────────────────────────────────

def run_backup() -> None:
    started_at = datetime.now()
    logger.info("=== srock.db 備份開始 [%s] ===", started_at.strftime("%Y-%m-%d %H:%M:%S"))

    src_size_mb = DB_PATH.stat().st_size / 1024 / 1024 if DB_PATH.exists() else 0.0

    # ── 建立 lock（給 Streamlit UI 顯示警示用）+ 寫入「開始」事件 ──
    # event_log 寫入 srock.db；必須在 VACUUM INTO 之前，否則會卡 SHARED lock
    acquire_lock()
    _log_event_safe(
        severity="info",
        summary="DB 備份開始（VACUUM INTO + gzip + 上傳 Google Drive）",
        payload={"phase": "started", "src_db_mb": round(src_size_mb, 1)},
    )

    try:
        step_preflight_remote()
        step_backup_db()
        vacuum_size_mb = TEMP_DB.stat().st_size / 1024 / 1024
        step_compress()
        gz_size_mb = TEMP_GZ.stat().st_size / 1024 / 1024
        _cleanup(TEMP_DB)           # 壓縮完成後立即刪除未壓縮暫存
        step_upload()
        step_verify_upload()
        _cleanup(TEMP_GZ)           # 上傳驗證通過後刪除壓縮暫存
        step_delete_old_backups()
        remote_files = list_remote_backups()

        elapsed_sec = int((datetime.now() - started_at).total_seconds())
        msg_lines = [
            f"[srock 備份成功] {datetime.now():%Y-%m-%d %H:%M}",
            f"srock_backup_{datetime.now():%Y%m%d}.db.gz 已上傳至 gdrive:",
            "",
            f"原始 DB：{src_size_mb:.1f} MB",
            f"VACUUM 後：{vacuum_size_mb:.1f} MB",
            f"gzip 壓縮：{gz_size_mb:.1f} MB",
            f"耗時：{elapsed_sec // 60} 分 {elapsed_sec % 60} 秒",
            f"雲端目前保留：{len(remote_files)} 份（>{RETENTION_DAYS} 天自動清除）",
        ]
        _notify("\n".join(msg_lines))
        # VACUUM 已結束，可以安全寫 event_log
        _log_event_safe(
            severity="info",
            summary=f"DB 備份成功（{src_size_mb:.0f}MB → {gz_size_mb:.0f}MB，{elapsed_sec}s）",
            payload={
                "phase": "finished",
                "src_db_mb": round(src_size_mb, 1),
                "vacuum_mb": round(vacuum_size_mb, 1),
                "gz_mb": round(gz_size_mb, 1),
                "elapsed_sec": elapsed_sec,
                "remote_count": len(remote_files),
            },
        )
        logger.info("=== 備份流程全部完成（雲端共 %d 份） ===", len(remote_files))
    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("備份失敗:\n%s", tb)
        _notify_error(
            f"[srock 備份失敗] {datetime.now():%Y-%m-%d %H:%M}\n\n{tb[:3800]}"
        )
        _cleanup(TEMP_DB, TEMP_GZ)
        # 失敗時 srock.db 鎖應已釋放（VACUUM 結束/未開始），event_log 可寫
        _log_event_safe(
            severity="warning",
            summary=f"DB 備份失敗：{exc}",
            payload={"phase": "failed", "error": str(exc)[:500]},
        )
        raise RuntimeError("srock.db 備份失敗") from exc
    finally:
        # 不論成功失敗，一律釋放 lock — 否則 UI 會永遠掛著警示
        release_lock()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        run_backup()
    except RuntimeError:
        sys.exit(1)
