from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from srock.config import Config
from srock.services import (
    CaddyService,
    FunnelService,
    PrefetchService,
    SchedulerService,
    ServiceStatus,
    StreamlitService,
)

# Windows: 啟用 VT100 + UTF-8（PowerShell / conhost 預設皆未啟用）
# 注意：不可用 os.system("chcp")——pythonw 等無主控台進程（排程跑 srock ensure）
# 會為 cmd.exe 另開黑視窗；一律走 Win32 API，且無主控台時整段跳過。
if sys.platform == "win32":
    import ctypes

    # stdout/stderr 一律轉 UTF-8（含無主控台的 pipe / pythonw 情境）。
    # 這一段必須在 GetConsoleWindow() 判斷「之外」——否則被工具/管線呼叫時
    # stdout 維持 cp950(strict)，Rich 印 ✓ / ⚠ 等字元就 UnicodeEncodeError。
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            try:
                _stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

    # VT100 + 主控台 codepage 只在有真正主控台時設（pythonw 無主控台要跳過）。
    _k32 = ctypes.windll.kernel32
    if _k32.GetConsoleWindow():
        _ENABLE_VT = 0x0004
        for _hid in (-10, -11, -12):
            _h = _k32.GetStdHandle(_hid)
            _m = ctypes.c_ulong()
            if _k32.GetConsoleMode(_h, ctypes.byref(_m)):
                _k32.SetConsoleMode(_h, _m.value | _ENABLE_VT)
        _k32.SetConsoleOutputCP(65001)
        _k32.SetConsoleCP(65001)

console = Console(legacy_windows=False, force_terminal=True)


def _status_badge(running: bool) -> Text:
    if running:
        return Text("● RUNNING", style="bold green")
    return Text("○ STOPPED", style="bold red")


def _build_status_table(
    streamlit: StreamlitService,
    caddy: CaddyService,
    funnel: FunnelService,
    scheduler: SchedulerService,
    prefetch: PrefetchService,
    public_url: str | None = None,
) -> Panel:
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold cyan", width=14)
    table.add_column(min_width=16)
    table.add_column(style="dim", no_wrap=True)

    for svc in [streamlit.status(), caddy.status(), funnel.status(), scheduler.status(), prefetch.status()]:
        pid_str = f"PID {svc.pid}" if svc.pid else ""
        table.add_row(svc.name, _status_badge(svc.running), f"{pid_str}  {svc.detail}")

    table.add_row()

    st_url = f"http://127.0.0.1:{streamlit.cfg.streamlit_port}"
    table.add_row("Local", Text(st_url, style="link " + st_url), "")

    if public_url:
        pub_text = Text(public_url, style="bold yellow", no_wrap=True)
        table.add_row("Public", pub_text, "")

    from datetime import datetime
    title = f"[bold]SROCK[/bold]  [dim]{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]"
    return Panel(table, title=title, border_style="bright_black")


def print_status(cfg: Config) -> None:
    streamlit = StreamlitService(cfg)
    caddy = CaddyService(cfg)
    funnel = FunnelService(cfg)
    scheduler = SchedulerService(cfg)
    prefetch = PrefetchService(cfg)
    public_url = funnel.public_url() if funnel.status().running else None
    console.print(_build_status_table(streamlit, caddy, funnel, scheduler, prefetch, public_url))


def watch_status(cfg: Config) -> None:
    """Live-refresh status every 3 seconds. Ctrl+C to exit."""
    streamlit = StreamlitService(cfg)
    caddy = CaddyService(cfg)
    funnel = FunnelService(cfg)
    scheduler = SchedulerService(cfg)
    prefetch = PrefetchService(cfg)

    try:
        with Live(console=console, refresh_per_second=1, screen=True) as live:
            while True:
                public_url = funnel.public_url() if funnel.status().running else None
                live.update(_build_status_table(streamlit, caddy, funnel, scheduler, prefetch, public_url))
                time.sleep(3)
    except KeyboardInterrupt:
        pass


def tail_log(log_file: Path, follow: bool = False, lines: int = 50) -> None:
    if not log_file.exists():
        console.print(f"[dim]Log file not found: {log_file}[/dim]")
        return

    all_lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
    for line in all_lines[-lines:]:
        console.print(line)

    if follow:
        console.print(f"[dim]--- following {log_file.name} (Ctrl+C to stop) ---[/dim]")
        try:
            with open(log_file, encoding="utf-8", errors="replace") as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        console.print(line, end="")
                    else:
                        time.sleep(0.1)
        except KeyboardInterrupt:
            pass
