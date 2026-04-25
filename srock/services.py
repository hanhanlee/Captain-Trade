from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from srock.config import Config, ROOT
from srock.process import (
    get_pid_on_port,
    is_port_open,
    kill_on_port,
    run_capture,
    start_background,
    wait_port_open,
)


@dataclass
class ServiceStatus:
    name: str
    running: bool
    pid: int | None
    port: int | None
    detail: str = ""


# ── Streamlit ──────────────────────────────────────────────────

class StreamlitService:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    @property
    def name(self) -> str:
        return "Streamlit"

    def status(self) -> ServiceStatus:
        pid = get_pid_on_port(self.cfg.streamlit_port)
        return ServiceStatus(
            name=self.name,
            running=pid is not None,
            pid=pid,
            port=self.cfg.streamlit_port,
            detail=f"http://127.0.0.1:{self.cfg.streamlit_port}",
        )

    def start(self) -> str:
        if is_port_open(self.cfg.streamlit_port):
            return f"Streamlit already running on port {self.cfg.streamlit_port}"
        if not self.cfg.app_py.exists():
            raise FileNotFoundError(f"app.py not found: {self.cfg.app_py}")

        self.cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
        args = [
            sys.executable, "-m", "streamlit", "run", str(self.cfg.app_py),
            "--server.address", "0.0.0.0",
            "--server.port", str(self.cfg.streamlit_port),
            "--server.headless", "true",
        ]
        start_background(
            args=args,
            cwd=ROOT,
            stdout_log=self.cfg.streamlit_out_log,
            stderr_log=self.cfg.streamlit_err_log,
            pid_file=self.cfg.streamlit_pid_file,
        )
        if wait_port_open(self.cfg.streamlit_port, timeout=60):
            pid = get_pid_on_port(self.cfg.streamlit_port)
            return f"Streamlit started — PID {pid}, port {self.cfg.streamlit_port}"
        raise TimeoutError(
            f"Streamlit did not start within 60s. "
            f"Check {self.cfg.streamlit_err_log}"
        )

    def stop(self) -> str:
        if not is_port_open(self.cfg.streamlit_port):
            return "Streamlit is not running"
        pid = get_pid_on_port(self.cfg.streamlit_port)
        if kill_on_port(self.cfg.streamlit_port):
            self.cfg.streamlit_pid_file.unlink(missing_ok=True)
            return f"Streamlit stopped (was PID {pid})"
        raise RuntimeError(f"Failed to stop Streamlit on port {self.cfg.streamlit_port}")

    def restart(self) -> str:
        msg_stop = self.stop() if is_port_open(self.cfg.streamlit_port) else ""
        msg_start = self.start()
        return "\n".join(filter(None, [msg_stop, msg_start]))


# ── Caddy (Auth Proxy) ─────────────────────────────────────────

class CaddyService:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    @property
    def name(self) -> str:
        return "Auth Proxy"

    def status(self) -> ServiceStatus:
        pid = get_pid_on_port(self.cfg.auth_port)
        cred_ok = self.cfg.credential_file.exists()
        return ServiceStatus(
            name=self.name,
            running=pid is not None,
            pid=pid,
            port=self.cfg.auth_port,
            detail=(
                f"http://127.0.0.1:{self.cfg.auth_port}"
                + ("" if cred_ok else "  ⚠ 尚未設定帳號")
            ),
        )

    def start(self, on_wait=None) -> str:
        """Start Caddy auth proxy.

        on_wait: optional callable(elapsed_sec) called every ~5s while polling.
        """
        import psutil as _psutil

        if is_port_open(self.cfg.auth_port):
            return f"Auth proxy already running on port {self.cfg.auth_port}"
        if not self.cfg.caddy_exe.exists():
            raise FileNotFoundError(f"Caddy not found: {self.cfg.caddy_exe}")
        if not self.cfg.caddyfile.exists():
            raise FileNotFoundError(
                f"Caddyfile not found: {self.cfg.caddyfile}\n"
                "Run `srock auth add <user>` first."
            )

        self.cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._kill_stale_pid()
        self._kill_caddy_admin()
        time.sleep(0.3)

        pid = start_background(
            [str(self.cfg.caddy_exe), "run",
             "--config", str(self.cfg.caddyfile),
             "--adapter", "caddyfile"],
            cwd=ROOT,
            stdout_log=self.cfg.runtime_dir / "caddy.out.log",
            stderr_log=self.cfg.runtime_dir / "caddy.err.log",
            pid_file=self.cfg.caddy_pid_file,
        )

        # quick check: did caddy die immediately (config error)?
        time.sleep(0.5)
        try:
            if not _psutil.Process(pid).is_running():
                raise RuntimeError(self._read_caddy_err())
        except _psutil.NoSuchProcess:
            raise RuntimeError(self._read_caddy_err())

        # poll until port open
        timeout = 30
        deadline = time.monotonic() + timeout
        last_report = time.monotonic()
        while time.monotonic() < deadline:
            if is_port_open(self.cfg.auth_port):
                actual_pid = get_pid_on_port(self.cfg.auth_port)
                if actual_pid:
                    self.cfg.caddy_pid_file.write_text(str(actual_pid), encoding="ascii")
                return f"Auth proxy started — PID {actual_pid}, port {self.cfg.auth_port}"
            now = time.monotonic()
            if on_wait and now - last_report >= 3:
                on_wait(now - (deadline - timeout))
                last_report = now
            time.sleep(0.3)

        raise TimeoutError(
            f"Auth proxy did not open port {self.cfg.auth_port} within {timeout}s.\n"
            + self._read_caddy_err()
        )

    def _read_caddy_err(self) -> str:
        log = self.cfg.runtime_dir / "caddy.err.log"
        if log.exists():
            return log.read_text(encoding="utf-8", errors="replace")[-600:]
        return "(no log)"

    _CADDY_ADMIN_PORT = 2019

    def stop(self) -> str:
        if not is_port_open(self.cfg.auth_port):
            self._kill_stale_pid()
            self._kill_caddy_admin()
            return "Auth proxy is not running"
        pid = get_pid_on_port(self.cfg.auth_port)
        kill_on_port(self.cfg.auth_port)
        self._kill_stale_pid()
        self._kill_caddy_admin()
        self.cfg.caddy_pid_file.unlink(missing_ok=True)
        return f"Auth proxy stopped (was PID {pid})"

    def restart(self) -> str:
        msg_stop = self.stop()
        time.sleep(1.0)
        msg_start = self.start()
        return "\n".join([msg_stop, msg_start])

    def _kill_caddy_admin(self) -> None:
        if is_port_open(self._CADDY_ADMIN_PORT):
            kill_on_port(self._CADDY_ADMIN_PORT)

    def _kill_stale_pid(self) -> None:
        if not self.cfg.caddy_pid_file.exists():
            return
        try:
            import psutil
            pid = int(self.cfg.caddy_pid_file.read_text().strip())
            proc = psutil.Process(pid)
            if not proc.is_running():
                raise psutil.NoSuchProcess(pid)
            proc.terminate()
        except Exception:
            pass
        self.cfg.caddy_pid_file.unlink(missing_ok=True)


# ── Tailscale Funnel ───────────────────────────────────────────

class FunnelService:
    """Cloudflare Quick Tunnel — no account needed, URL rotates each restart."""

    _URL_PATTERN = __import__("re").compile(r"https://[a-z0-9-]+\.trycloudflare\.com")

    def __init__(self, cfg: Config):
        self.cfg = cfg

    @property
    def name(self) -> str:
        return "Tunnel"

    def _exe(self) -> Path:
        return ROOT / "tools/cloudflared/cloudflared.exe"

    def _pid_file(self) -> Path:
        return self.cfg.runtime_dir / "cloudflared.pid"

    def _url_file(self) -> Path:
        return self.cfg.runtime_dir / "cloudflared_url.txt"

    def _out_log(self) -> Path:
        return self.cfg.runtime_dir / "cloudflared.out.log"

    def _err_log(self) -> Path:
        return self.cfg.runtime_dir / "cloudflared.err.log"

    def _running_pid(self) -> int | None:
        import psutil as _psutil
        pid_file = self._pid_file()
        if not pid_file.exists():
            return None
        try:
            pid = int(pid_file.read_text().strip())
            proc = _psutil.Process(pid)
            if proc.is_running() and "cloudflared" in proc.name().lower():
                return pid
        except Exception:
            pass
        pid_file.unlink(missing_ok=True)
        return None

    def status(self) -> ServiceStatus:
        pid = self._running_pid()
        url = self.public_url()
        return ServiceStatus(
            name=self.name,
            running=pid is not None,
            pid=pid,
            port=443,
            detail=url or ("running, URL pending…" if pid else ""),
        )

    def start(self, on_wait=None) -> str:
        if self._running_pid():
            url = self.public_url()
            return f"Tunnel already running → {url or '(URL pending)'}"

        exe = self._exe()
        if not exe.exists():
            raise FileNotFoundError(f"cloudflared not found: {exe}")

        self._url_file().unlink(missing_ok=True)
        # truncate old log so URL search doesn't pick up stale URL
        self._err_log().write_text("", encoding="utf-8")

        start_background(
            [str(exe), "tunnel", "--url", f"http://127.0.0.1:{self.cfg.streamlit_port}"],
            cwd=ROOT,
            stdout_log=self._out_log(),
            stderr_log=self._err_log(),
            pid_file=self._pid_file(),
        )

        deadline = time.monotonic() + 30
        last_report = time.monotonic()
        while time.monotonic() < deadline:
            url = self._scan_url_from_logs()
            if url:
                self._url_file().write_text(url, encoding="utf-8")
                return f"Tunnel started → {url}"
            now = time.monotonic()
            if on_wait and now - last_report >= 3:
                on_wait(now - (deadline - 30))
                last_report = now
            time.sleep(0.5)

        return "Tunnel started (URL still pending)"

    def stop(self) -> str:
        pid = self._running_pid()
        if pid is None:
            self._url_file().unlink(missing_ok=True)
            return "Tunnel is not running"
        try:
            import psutil as _psutil
            _psutil.Process(pid).terminate()
        except Exception:
            pass
        self._pid_file().unlink(missing_ok=True)
        self._url_file().unlink(missing_ok=True)
        return f"Tunnel stopped (was PID {pid})"

    def public_url(self) -> str | None:
        url_file = self._url_file()
        if url_file.exists():
            url = url_file.read_text().strip()
            if url:
                return url
        return self._scan_url_from_logs()

    def _scan_url_from_logs(self) -> str | None:
        for log in (self._err_log(), self._out_log()):
            if log.exists():
                try:
                    m = self._URL_PATTERN.search(
                        log.read_text(encoding="utf-8", errors="replace")
                    )
                    if m:
                        return m.group(0)
                except Exception:
                    pass
        return None


# ── Telegram Bot ───────────────────────────────────────────────

class TelegramBotService:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    @property
    def name(self) -> str:
        return "Telegram Bot"

    def _running_pid(self) -> int | None:
        import psutil as _psutil
        pid_file = self.cfg.telegram_bot_pid_file
        if not pid_file.exists():
            return None
        try:
            pid = int(pid_file.read_text().strip())
            proc = _psutil.Process(pid)
            if proc.is_running() and proc.status() != _psutil.STATUS_ZOMBIE:
                return pid
        except Exception:
            pass
        pid_file.unlink(missing_ok=True)
        return None

    def status(self) -> ServiceStatus:
        pid = self._running_pid()
        detail = "long-polling" if pid else ""
        return ServiceStatus(
            name=self.name,
            running=pid is not None,
            pid=pid,
            port=None,
            detail=detail,
        )

    def start(self) -> str:
        if self._running_pid():
            return "Telegram Bot already running"
        from dotenv import load_dotenv
        load_dotenv()
        if not os.getenv("TELEGRAM_BOT_TOKEN", ""):
            raise RuntimeError("TELEGRAM_BOT_TOKEN 未設定，Bot 無法啟動")

        bot_script = ROOT / "telegram_bot.py"
        if not bot_script.exists():
            raise FileNotFoundError(f"telegram_bot.py not found: {bot_script}")

        self.cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
        pid = start_background(
            args=[sys.executable, str(bot_script)],
            cwd=ROOT,
            stdout_log=self.cfg.telegram_bot_out_log,
            stderr_log=self.cfg.telegram_bot_err_log,
            pid_file=self.cfg.telegram_bot_pid_file,
        )
        time.sleep(1.5)
        if not self._running_pid():
            raise RuntimeError(
                f"Telegram Bot 啟動後立即退出，請檢查 {self.cfg.telegram_bot_err_log}"
            )
        return f"Telegram Bot started — PID {pid}"

    def stop(self) -> str:
        pid = self._running_pid()
        if pid is None:
            return "Telegram Bot is not running"
        try:
            import psutil as _psutil
            _psutil.Process(pid).terminate()
        except Exception:
            pass
        self.cfg.telegram_bot_pid_file.unlink(missing_ok=True)
        return f"Telegram Bot stopped (was PID {pid})"

    def restart(self) -> str:
        msg_stop = self.stop()
        time.sleep(0.5)
        msg_start = self.start()
        return "\n".join([msg_stop, msg_start])
