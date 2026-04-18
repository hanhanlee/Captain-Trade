from __future__ import annotations

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

    def start(self) -> str:
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
        # 清除 stale pid
        self._kill_stale_pid()

        rc, out = run_capture(
            [str(self.cfg.caddy_exe), "start",
             "--config", str(self.cfg.caddyfile),
             "--adapter", "caddyfile"],
            cwd=ROOT,
        )
        if rc != 0:
            raise RuntimeError(f"Caddy failed to start:\n{out}")

        if wait_port_open(self.cfg.auth_port, timeout=25):
            pid = get_pid_on_port(self.cfg.auth_port)
            self.cfg.caddy_pid_file.write_text(str(pid), encoding="ascii")
            return f"Auth proxy started — PID {pid}, port {self.cfg.auth_port}"
        raise TimeoutError(
            f"Auth proxy did not start within 25s.\n{out}"
        )

    def stop(self) -> str:
        if not is_port_open(self.cfg.auth_port):
            self._kill_stale_pid()
            return "Auth proxy is not running"
        pid = get_pid_on_port(self.cfg.auth_port)
        kill_on_port(self.cfg.auth_port)
        self._kill_stale_pid()
        self.cfg.caddy_pid_file.unlink(missing_ok=True)
        return f"Auth proxy stopped (was PID {pid})"

    def restart(self) -> str:
        msg_stop = self.stop()
        time.sleep(0.5)
        msg_start = self.start()
        return "\n".join([msg_stop, msg_start])

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
    def __init__(self, cfg: Config):
        self.cfg = cfg

    @property
    def name(self) -> str:
        return "Funnel"

    def _find_tailscale(self) -> str:
        import shutil
        ts = shutil.which("tailscale")
        if ts:
            return ts
        default = r"C:\Program Files\Tailscale\tailscale.exe"
        if Path(default).exists():
            return default
        raise FileNotFoundError(
            "tailscale CLI not found. Install Tailscale or add to PATH."
        )

    def status(self) -> ServiceStatus:
        try:
            ts = self._find_tailscale()
            rc, out = run_capture([ts, "funnel", "status"])
            active = "https" in out.lower() and "off" not in out.lower()
            return ServiceStatus(
                name=self.name,
                running=active,
                pid=None,
                port=self.cfg.funnel_https_port,
                detail=out.split("\n")[0] if out else "",
            )
        except FileNotFoundError as e:
            return ServiceStatus(
                name=self.name, running=False, pid=None,
                port=None, detail=str(e),
            )

    def start(self) -> str:
        ts = self._find_tailscale()
        target = f"http://127.0.0.1:{self.cfg.auth_port}"
        rc, out = run_capture(
            [ts, "funnel", "--bg",
             f"--https={self.cfg.funnel_https_port}", target]
        )
        if rc != 0:
            raise RuntimeError(f"Funnel start failed:\n{out}")
        return f"Funnel started → {target}"

    def stop(self) -> str:
        ts = self._find_tailscale()
        rc, out = run_capture(
            [ts, "funnel", f"--https={self.cfg.funnel_https_port}", "off"]
        )
        if rc != 0 and "handler does not exist" not in out:
            raise RuntimeError(f"Funnel stop failed:\n{out}")
        return "Funnel stopped"

    def public_url(self) -> str | None:
        try:
            import json
            ts = self._find_tailscale()
            rc, out = run_capture([ts, "status", "--json"])
            if rc != 0 or not out:
                return None
            data = json.loads(out)
            dns = data.get("Self", {}).get("DNSName", "").rstrip(".")
            if dns:
                port = self.cfg.funnel_https_port
                suffix = "" if port == 443 else f":{port}"
                return f"https://{dns}{suffix}"
        except Exception:
            pass
        return None
