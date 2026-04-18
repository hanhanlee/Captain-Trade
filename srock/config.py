from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore

ROOT = Path(__file__).resolve().parent.parent
_CONFIG_FILE = ROOT / "config.toml"

_DEFAULTS: dict = {
    "services": {
        "streamlit_port": 8501,
        "auth_port": 8080,
        "funnel_https_port": 443,
    },
    "paths": {
        "caddy_exe": "tools/caddy/caddy.exe",
        "app_py": "app.py",
        "runtime_dir": "runtime",
        "secrets_dir": "secrets",
    },
    "startup": {
        "default_profile": "full",
        "auto_open_browser": False,
        "watch_after_up": True,
    },
    "finmind": {
        "tier": "free",
        "premium_enabled": False,
        "features": {
            "risk_flags": True,
            "broker_branch": True,
            "holding_shares": True,
            "fundamentals_mode": "penalty",
        },
    },
}


def _merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _merge(result[k], v)
        else:
            result[k] = v
    return result


@dataclass
class Config:
    # services
    streamlit_port: int = 8501
    auth_port: int = 8080
    funnel_https_port: int = 443

    # paths (absolute)
    caddy_exe: Path = field(default_factory=lambda: ROOT / "tools/caddy/caddy.exe")
    app_py: Path = field(default_factory=lambda: ROOT / "app.py")
    runtime_dir: Path = field(default_factory=lambda: ROOT / "runtime")
    secrets_dir: Path = field(default_factory=lambda: ROOT / "secrets")

    # derived paths (not in toml)
    @property
    def caddyfile(self) -> Path:
        return self.secrets_dir / "Caddyfile"

    @property
    def users_file(self) -> Path:
        return self.secrets_dir / "basic_auth_users.json"

    @property
    def credential_file(self) -> Path:
        return self.secrets_dir / "basic_auth_credentials.txt"

    @property
    def streamlit_out_log(self) -> Path:
        return self.runtime_dir / "streamlit.out.log"

    @property
    def streamlit_err_log(self) -> Path:
        return self.runtime_dir / "streamlit.err.log"

    @property
    def caddy_out_log(self) -> Path:
        return self.runtime_dir / "caddy.out.log"

    @property
    def caddy_err_log(self) -> Path:
        return self.runtime_dir / "caddy.err.log"

    @property
    def streamlit_pid_file(self) -> Path:
        return self.runtime_dir / "streamlit.pid"

    @property
    def caddy_pid_file(self) -> Path:
        return self.runtime_dir / "caddy.pid"

    # startup
    default_profile: str = "full"
    auto_open_browser: bool = False
    watch_after_up: bool = True

    # finmind premium flags
    finmind_tier: str = "free"
    finmind_premium_enabled: bool = False
    finmind_risk_flags: bool = True
    finmind_broker_branch: bool = True
    finmind_holding_shares: bool = True
    finmind_fundamentals_mode: str = "penalty"


def load_config() -> Config:
    raw: dict = dict(_DEFAULTS)
    if _CONFIG_FILE.exists():
        with open(_CONFIG_FILE, "rb") as f:
            user_cfg = tomllib.load(f)
        raw = _merge(raw, user_cfg)

    svc = raw["services"]
    pth = raw["paths"]
    stup = raw["startup"]
    fin = raw["finmind"]
    fin_features = fin.get("features", {})

    return Config(
        streamlit_port=int(svc["streamlit_port"]),
        auth_port=int(svc["auth_port"]),
        funnel_https_port=int(svc["funnel_https_port"]),
        caddy_exe=ROOT / pth["caddy_exe"],
        app_py=ROOT / pth["app_py"],
        runtime_dir=ROOT / pth["runtime_dir"],
        secrets_dir=ROOT / pth["secrets_dir"],
        default_profile=stup["default_profile"],
        auto_open_browser=bool(stup["auto_open_browser"]),
        watch_after_up=bool(stup["watch_after_up"]),
        finmind_tier=str(fin.get("tier", "free")),
        finmind_premium_enabled=bool(fin.get("premium_enabled", False)),
        finmind_risk_flags=bool(fin_features.get("risk_flags", True)),
        finmind_broker_branch=bool(fin_features.get("broker_branch", True)),
        finmind_holding_shares=bool(fin_features.get("holding_shares", True)),
        finmind_fundamentals_mode=str(fin_features.get("fundamentals_mode", "penalty")),
    )
