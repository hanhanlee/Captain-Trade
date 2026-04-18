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
from srock.services import CaddyService, FunnelService, ServiceStatus, StreamlitService

# Windows: 切換 UTF-8 code page，停用 legacy renderer（避免 cp950 UnicodeEncodeError）
if sys.platform == "win32":
    os.system("chcp 65001 > nul 2>&1")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

console = Console(legacy_windows=False)


def _status_badge(running: bool) -> Text:
    if running:
        return Text("● RUNNING", style="bold green")
    return Text("○ STOPPED", style="bold red")


def _build_status_table(
    streamlit: StreamlitService,
    caddy: CaddyService,
    funnel: FunnelService,
    public_url: str | None = None,
) -> Panel:
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold cyan", width=14)
    table.add_column(min_width=16)
    table.add_column(style="dim")

    for svc in [streamlit.status(), caddy.status(), funnel.status()]:
        pid_str = f"PID {svc.pid}" if svc.pid else ""
        table.add_row(svc.name, _status_badge(svc.running), f"{pid_str}  {svc.detail}")

    table.add_row()

    st_url = f"http://127.0.0.1:{streamlit.cfg.streamlit_port}"
    table.add_row("Local", Text(st_url, style="link " + st_url), "")

    if public_url:
        table.add_row("Public", Text(public_url, style="bold yellow"), "")

    from datetime import datetime
    title = f"[bold]SROCK[/bold]  [dim]{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]"
    return Panel(table, title=title, border_style="bright_black")


def print_status(cfg: Config) -> None:
    streamlit = StreamlitService(cfg)
    caddy = CaddyService(cfg)
    funnel = FunnelService(cfg)
    public_url = funnel.public_url() if funnel.status().running else None
    console.print(_build_status_table(streamlit, caddy, funnel, public_url))


def watch_status(cfg: Config) -> None:
    """Live-refresh status every 3 seconds. Ctrl+C to exit."""
    streamlit = StreamlitService(cfg)
    caddy = CaddyService(cfg)
    funnel = FunnelService(cfg)

    try:
        with Live(console=console, refresh_per_second=0.5, screen=False) as live:
            while True:
                public_url = funnel.public_url() if funnel.status().running else None
                live.update(_build_status_table(streamlit, caddy, funnel, public_url))
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
