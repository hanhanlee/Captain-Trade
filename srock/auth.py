from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import bcrypt

from srock.config import Config


def _load_users(cfg: Config) -> dict[str, str]:
    """Load {username: bcrypt_hash} from users JSON file."""
    if not cfg.users_file.exists():
        return {}
    try:
        items = json.loads(cfg.users_file.read_text(encoding="utf-8"))
        return {item["user"]: item["hash"] for item in items if "user" in item and "hash" in item}
    except Exception:
        return {}


def _save_users(cfg: Config, users: dict[str, str]) -> None:
    cfg.secrets_dir.mkdir(parents=True, exist_ok=True)
    items = [
        {"user": u, "hash": h, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        for u, h in users.items()
    ]
    cfg.users_file.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_caddyfile(cfg: Config, users: dict[str, str]) -> None:
    if not users:
        raise ValueError("At least one user is required before writing Caddyfile.")

    auth_lines = "\n".join(f"        {u} {h}" for u, h in users.items())
    content = f"""\
{{
    auto_https off
    admin 127.0.0.1:2019
    persist_config off
    storage file_system {cfg.runtime_dir / "caddy_data"}
}}

http://127.0.0.1:{cfg.auth_port} {{
    bind 127.0.0.1
    encode gzip

    basic_auth {{
{auth_lines}
    }}

    reverse_proxy 127.0.0.1:{cfg.streamlit_port}
}}
"""
    cfg.secrets_dir.mkdir(parents=True, exist_ok=True)
    cfg.caddyfile.write_text(content, encoding="utf-8")


def _save_credential_hint(cfg: Config, username: str, password: str) -> None:
    content = (
        f"Basic Auth 最近更新憑證\n\n"
        f"URL:      http://127.0.0.1:{cfg.auth_port}\n"
        f"Username: {username}\n"
        f"Password: {password}\n\n"
        f"Changed:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"Users file: {cfg.users_file}\n"
        f"Keep this file local. It is ignored by git.\n"
    )
    cfg.credential_file.write_text(content, encoding="utf-8")


def _hash_password(password: str) -> str:
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(prefix=b"2a"))
    return hashed.decode()


# ── Public API ─────────────────────────────────────────────────

def add_user(cfg: Config, username: str, password: str) -> str:
    users = _load_users(cfg)
    if username in users:
        raise ValueError(f"User '{username}' already exists. Use `srock auth reset` to change password.")
    users[username] = _hash_password(password)
    _save_users(cfg, users)
    _write_caddyfile(cfg, users)
    _save_credential_hint(cfg, username, password)
    return f"User '{username}' added."


def reset_user(cfg: Config, username: str, password: str) -> str:
    users = _load_users(cfg)
    existed = username in users
    users[username] = _hash_password(password)
    _save_users(cfg, users)
    _write_caddyfile(cfg, users)
    _save_credential_hint(cfg, username, password)
    verb = "updated" if existed else "created"
    return f"User '{username}' {verb}."


def delete_user(cfg: Config, username: str) -> str:
    users = _load_users(cfg)
    if username not in users:
        raise ValueError(f"User '{username}' not found.")
    del users[username]
    _save_users(cfg, users)
    if users:
        _write_caddyfile(cfg, users)
    else:
        cfg.caddyfile.unlink(missing_ok=True)
    return f"User '{username}' deleted."


def list_users(cfg: Config) -> list[str]:
    return list(_load_users(cfg).keys())
