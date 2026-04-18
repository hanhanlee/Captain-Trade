from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import psutil

_CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0


def get_pid_on_port(port: int) -> int | None:
    try:
        for conn in psutil.net_connections(kind="tcp"):
            if conn.laddr.port == port and conn.status == psutil.CONN_LISTEN:
                return conn.pid
    except (psutil.AccessDenied, PermissionError):
        pass
    return None


def is_port_open(port: int) -> bool:
    return get_pid_on_port(port) is not None


def wait_port_open(port: int, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_port_open(port):
            return True
        time.sleep(0.3)
    return False


def wait_port_closed(port: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_port_open(port):
            return True
        time.sleep(0.3)
    return False


def kill_on_port(port: int, timeout: float = 10.0) -> bool:
    pid = get_pid_on_port(port)
    if pid is None:
        return True
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except psutil.TimeoutExpired:
            proc.kill()
    except psutil.NoSuchProcess:
        pass
    return wait_port_closed(port, timeout)


def start_background(
    args: list[str],
    cwd: Path,
    stdout_log: Path,
    stderr_log: Path,
    pid_file: Path,
) -> int:
    cwd.mkdir(parents=True, exist_ok=True)
    stdout_log.parent.mkdir(parents=True, exist_ok=True)

    with open(stdout_log, "a") as out, open(stderr_log, "a") as err:
        proc = subprocess.Popen(
            args,
            cwd=str(cwd),
            stdout=out,
            stderr=err,
            creationflags=_CREATE_NO_WINDOW,
        )
    pid_file.write_text(str(proc.pid), encoding="ascii")
    return proc.pid


def run_capture(args: list[str], cwd: Path | None = None) -> tuple[int, str]:
    """Run a command, return (returncode, combined output)."""
    result = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        creationflags=_CREATE_NO_WINDOW,
    )
    output = (result.stdout + result.stderr).strip()
    return result.returncode, output
