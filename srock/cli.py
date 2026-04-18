from __future__ import annotations

import getpass
import webbrowser
from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from srock import auth as auth_mod
from srock.config import load_config
from srock.display import console, print_status, tail_log, watch_status
from srock.services import CaddyService, FunnelService, StreamlitService

app = typer.Typer(
    name="srock",
    help="Srock service manager — 管理 Streamlit + Caddy + Tailscale Funnel",
    no_args_is_help=False,
    invoke_without_command=True,
)
auth_app = typer.Typer(help="Basic Auth 帳號管理")
start_app = typer.Typer(help="啟動個別元件")
stop_app = typer.Typer(help="停止個別元件")
app.add_typer(auth_app, name="auth")
app.add_typer(start_app, name="start")
app.add_typer(stop_app, name="stop")


class Profile(str, Enum):
    full = "full"
    local = "local"
    protected = "protected"


class LogTarget(str, Enum):
    streamlit = "streamlit"
    caddy = "caddy"
    all = "all"


# ── Helpers ────────────────────────────────────────────────────

def _ok(msg: str) -> None:
    console.print(f"[green]✓[/green] {msg}")


def _warn(msg: str) -> None:
    console.print(f"[yellow]![/yellow] {msg}")


def _fail(msg: str) -> None:
    console.print(f"[red]✗[/red] {msg}")
    raise typer.Exit(1)


def _step(msg: str) -> None:
    console.print(f"\n[bold cyan]── {msg}[/bold cyan]")


def _run_step(label: str, fn, *args, **kwargs) -> bool:
    with Progress(
        SpinnerColumn(),
        TextColumn(f"[cyan]{label}[/cyan]"),
        transient=True,
        console=console,
    ) as prog:
        prog.add_task("", total=None)
        try:
            result = fn(*args, **kwargs)
            _ok(result if result else label)
            return True
        except Exception as e:
            _fail(str(e))
            return False


def _ask_password(confirm: bool = True) -> str:
    pw = getpass.getpass("Password: ")
    if confirm:
        pw2 = getpass.getpass("Confirm  : ")
        if pw != pw2:
            _fail("Passwords do not match.")
    return pw


# ── Default command (no subcommand → show status) ─────────────

@app.callback(invoke_without_command=True)
def default(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        cfg = load_config()
        print_status(cfg)


# ── up ─────────────────────────────────────────────────────────

@app.command()
def up(
    profile: Profile = typer.Option(None, "--profile", "-p",
        help="full（預設）| local（僅 Streamlit）| protected（無 Funnel）"),
    local: bool = typer.Option(False, "--local", help="僅啟動 Streamlit（等同 --profile local）"),
    no_funnel: bool = typer.Option(False, "--no-funnel", help="不啟動 Funnel（等同 --profile protected）"),
    no_watch: bool = typer.Option(False, "--no-watch",
        help="啟動完成後不進入 watch 模式"),
):
    """啟動服務（預設 full：Streamlit + Caddy + Funnel）。"""
    cfg = load_config()
    if local:
        profile = Profile.local
    elif no_funnel:
        profile = Profile.protected
    elif profile is None:
        profile = Profile(cfg.default_profile)

    streamlit = StreamlitService(cfg)
    caddy = CaddyService(cfg)
    funnel = FunnelService(cfg)

    console.rule("[bold]srock up[/bold]")

    _step("Streamlit")
    _run_step("啟動 Streamlit...", streamlit.start)

    if profile in (Profile.full, Profile.protected):
        _step("Auth Proxy (Caddy)")
        _run_step("啟動 Caddy...", caddy.start)

    if profile == Profile.full:
        _step("Tailscale Funnel")
        _run_step("啟動 Funnel...", funnel.start)

    console.rule()
    print_status(cfg)

    if cfg.auto_open_browser:
        url = f"http://127.0.0.1:{cfg.streamlit_port}"
        webbrowser.open(url)

    if not no_watch and cfg.watch_after_up:
        console.print("\n[dim]進入監控模式（Ctrl+C 離開，服務仍繼續執行）[/dim]")
        watch_status(cfg)


# ── down ───────────────────────────────────────────────────────

@app.command()
def down():
    """停止所有服務。"""
    cfg = load_config()
    funnel = FunnelService(cfg)
    caddy = CaddyService(cfg)
    streamlit = StreamlitService(cfg)

    console.rule("[bold]srock down[/bold]")
    _run_step("停止 Funnel...", funnel.stop)
    _run_step("停止 Auth Proxy...", caddy.stop)
    _run_step("停止 Streamlit...", streamlit.stop)
    console.rule()
    print_status(cfg)


# ── restart ────────────────────────────────────────────────────

@app.command()
def restart(
    profile: Profile = typer.Option(None, "--profile", "-p"),
    no_watch: bool = typer.Option(False, "--no-watch"),
):
    """重啟所有服務（down → up）。"""
    cfg = load_config()
    if profile is None:
        profile = Profile(cfg.default_profile)

    funnel = FunnelService(cfg)
    caddy = CaddyService(cfg)
    streamlit = StreamlitService(cfg)

    console.rule("[bold]srock restart[/bold]")
    _run_step("停止 Funnel...", funnel.stop)
    _run_step("停止 Auth Proxy...", caddy.stop)
    _run_step("停止 Streamlit...", streamlit.stop)

    _run_step("啟動 Streamlit...", streamlit.start)
    if profile in (Profile.full, Profile.protected):
        _run_step("啟動 Caddy...", caddy.start)
    if profile == Profile.full:
        _run_step("啟動 Funnel...", funnel.start)

    console.rule()
    print_status(cfg)

    if not no_watch and cfg.watch_after_up:
        console.print("\n[dim]進入監控模式（Ctrl+C 離開）[/dim]")
        watch_status(cfg)


# ── status / watch ─────────────────────────────────────────────

@app.command()
def status():
    """顯示各服務目前狀態（靜態）。"""
    cfg = load_config()
    print_status(cfg)


@app.command()
def watch():
    """即時監控各服務狀態（每 3 秒刷新，Ctrl+C 離開）。"""
    cfg = load_config()
    watch_status(cfg)


# ── logs ───────────────────────────────────────────────────────

@app.command()
def logs(
    target: LogTarget = typer.Argument(LogTarget.all),
    follow: bool = typer.Option(False, "-f", "--follow", help="持續追蹤新寫入"),
    lines: int = typer.Option(50, "-n", help="顯示最後 N 行"),
):
    """查看服務 log（預設 all，同時顯示 streamlit 與 caddy）。"""
    cfg = load_config()

    if target == LogTarget.streamlit:
        files = [cfg.streamlit_err_log, cfg.streamlit_out_log]
    elif target == LogTarget.caddy:
        files = [cfg.caddy_err_log, cfg.caddy_out_log]
    else:
        files = [cfg.streamlit_err_log, cfg.caddy_err_log]

    if follow and len(files) > 1:
        _warn("follow 模式下僅顯示 stderr。若要 follow 完整 log 請指定 streamlit 或 caddy。")
        files = files[:1]

    for f in files:
        if f.exists():
            console.rule(f"[dim]{f.name}[/dim]")
            tail_log(f, follow=follow, lines=lines)


# ── open ───────────────────────────────────────────────────────

@app.command(name="open")
def open_browser(
    public: bool = typer.Option(False, "--public", help="開公網 Tailscale URL"),
):
    """在瀏覽器開啟 Srock。"""
    cfg = load_config()
    if public:
        funnel = FunnelService(cfg)
        url = funnel.public_url()
        if not url:
            _fail("無法取得 Tailscale 公網 URL，請確認 Funnel 已啟動。")
    else:
        url = f"http://127.0.0.1:{cfg.streamlit_port}"
    console.print(f"Opening [link={url}]{url}[/link]")
    webbrowser.open(url)


# ── start / stop subcommands ───────────────────────────────────

@start_app.command("streamlit")
def start_streamlit():
    """啟動 Streamlit。"""
    cfg = load_config()
    _run_step("啟動 Streamlit...", StreamlitService(cfg).start)


@start_app.command("caddy")
def start_caddy():
    """啟動 Caddy Auth Proxy。"""
    cfg = load_config()
    _run_step("啟動 Caddy...", CaddyService(cfg).start)


@start_app.command("funnel")
def start_funnel():
    """啟動 Tailscale Funnel。"""
    cfg = load_config()
    _run_step("啟動 Funnel...", FunnelService(cfg).start)


@stop_app.command("streamlit")
def stop_streamlit():
    """停止 Streamlit。"""
    cfg = load_config()
    _run_step("停止 Streamlit...", StreamlitService(cfg).stop)


@stop_app.command("caddy")
def stop_caddy():
    """停止 Caddy Auth Proxy。"""
    cfg = load_config()
    _run_step("停止 Caddy...", CaddyService(cfg).stop)


@stop_app.command("funnel")
def stop_funnel():
    """停止 Tailscale Funnel。"""
    cfg = load_config()
    _run_step("停止 Funnel...", FunnelService(cfg).stop)


# ── auth subcommands ───────────────────────────────────────────

@auth_app.command("add")
def auth_add(
    username: str = typer.Argument(..., help="新增的帳號名稱"),
    password: Optional[str] = typer.Option(None, "--password", "-p",
        help="留空則互動輸入（建議）"),
):
    """新增 Basic Auth 帳號。"""
    cfg = load_config()
    pw = password or _ask_password(confirm=True)
    try:
        msg = auth_mod.add_user(cfg, username, pw)
        _ok(msg)
        _warn("如果 Auth Proxy 正在執行，請執行 `srock restart caddy` 套用新帳號。")
    except ValueError as e:
        _fail(str(e))


@auth_app.command("reset")
def auth_reset(
    username: str = typer.Argument(...),
    password: Optional[str] = typer.Option(None, "--password", "-p"),
):
    """重設帳號密碼。"""
    cfg = load_config()
    pw = password or _ask_password(confirm=True)
    msg = auth_mod.reset_user(cfg, username, pw)
    _ok(msg)
    _warn("如果 Auth Proxy 正在執行，請執行 `srock restart caddy` 套用。")


@auth_app.command("delete")
def auth_delete(
    username: str = typer.Argument(...),
    yes: bool = typer.Option(False, "--yes", "-y", help="略過確認"),
):
    """刪除帳號。"""
    cfg = load_config()
    if not yes:
        typer.confirm(f"確定刪除帳號 '{username}'？", abort=True)
    try:
        msg = auth_mod.delete_user(cfg, username)
        _ok(msg)
    except ValueError as e:
        _fail(str(e))


@auth_app.command("list")
def auth_list():
    """列出所有 Basic Auth 帳號。"""
    cfg = load_config()
    users = auth_mod.list_users(cfg)
    if not users:
        console.print("[dim]尚無帳號。使用 `srock auth add <user>` 新增。[/dim]")
    else:
        for u in users:
            console.print(f"  [cyan]•[/cyan] {u}")
