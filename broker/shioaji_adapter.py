"""
Shioaji Read-Only Adapter — Singleton, thread-safe.

下單功能完全禁用（place_order / cancel_order / update_order 均 raise RuntimeError）。
行情查詢通過內建 rate limiter，確保不超過 50 次/5 秒上限。
"""
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_MARKET_LIMIT = 50
_MARKET_WINDOW = 5.0  # seconds


class ShioajiAdapter:
    _instance: "ShioajiAdapter | None" = None
    _class_lock = threading.Lock()

    @classmethod
    def get(cls) -> "ShioajiAdapter":
        if cls._instance is None:
            with cls._class_lock:
                if cls._instance is None:
                    inst = object.__new__(cls)
                    inst._state_lock = threading.Lock()
                    inst._rate_lock = threading.Lock()
                    inst._api = None
                    inst._logged_in = False
                    inst._mkt_calls: deque = deque()
                    cls._instance = inst
        return cls._instance

    # ── 禁用下單（硬封鎖）────────────────────────────────────────────────────

    def place_order(self, *args, **kwargs):
        raise RuntimeError("Order placement is disabled in read-only mode.")

    def cancel_order(self, *args, **kwargs):
        raise RuntimeError("Order cancellation is disabled in read-only mode.")

    def update_order(self, *args, **kwargs):
        raise RuntimeError("Order update is disabled in read-only mode.")

    # ── Rate limiter ──────────────────────────────────────────────────────────

    def _throttle(self) -> None:
        """等待直到市場資料呼叫額度空出（50 次/5 秒）。"""
        while True:
            with self._rate_lock:
                now = time.time()
                while self._mkt_calls and now - self._mkt_calls[0] > _MARKET_WINDOW:
                    self._mkt_calls.popleft()
                if len(self._mkt_calls) < _MARKET_LIMIT:
                    self._mkt_calls.append(now)
                    return
                sleep_until = self._mkt_calls[0] + _MARKET_WINDOW + 0.05
            time.sleep(max(0, sleep_until - time.time()))

    # ── Login / Logout ────────────────────────────────────────────────────────

    def login(self) -> bool:
        from db.event_log import log_event

        api_key = os.getenv("SINOTRADE_APIKEY", "").strip()
        secret_key = os.getenv("SINOTRADE_SECRETKEY", "").strip()

        if not api_key or not secret_key:
            log_event(
                "broker_login_failed", module="shioaji_adapter", severity="error",
                summary="SINOTRADE_APIKEY 或 SINOTRADE_SECRETKEY 未設定",
            )
            return False

        try:
            import shioaji as sj
            api = sj.Shioaji(simulation=False)
            api.login(
                api_key=api_key,
                secret_key=secret_key,
                contracts_timeout=10000,
                subscribe_trade=False,
            )
            with self._state_lock:
                self._api = api
                self._logged_in = True
            log_event(
                "broker_login_success", module="shioaji_adapter", severity="info",
                summary="Shioaji 登入成功，商品檔已下載",
            )
            logger.info("Shioaji 登入成功")
            return True
        except Exception as exc:
            log_event(
                "broker_login_failed", module="shioaji_adapter", severity="error",
                summary=f"Shioaji 登入失敗：{exc}",
            )
            logger.error("Shioaji 登入失敗：%s", exc)
            return False

    def logout(self) -> None:
        from db.event_log import log_event
        with self._state_lock:
            if self._api is not None:
                try:
                    self._api.logout()
                except Exception as exc:
                    logger.warning("Shioaji logout error: %s", exc)
                finally:
                    self._api = None
                    self._logged_in = False
        log_event(
            "broker_logout", module="shioaji_adapter", severity="info",
            summary="Shioaji 已登出",
        )

    def is_logged_in(self) -> bool:
        return self._logged_in and self._api is not None

    # ── Usage ─────────────────────────────────────────────────────────────────

    def usage(self) -> dict:
        if not self.is_logged_in():
            return {"connected": False}
        try:
            u = self._api.usage()
            pct = round(u.bytes / u.limit_bytes * 100, 1) if u.limit_bytes else 0
            return {
                "connected": True,
                "connections": u.connections,
                "bytes_used": u.bytes,
                "bytes_limit": u.limit_bytes,
                "bytes_remaining": u.remaining_bytes,
                "usage_pct": pct,
            }
        except Exception as exc:
            logger.warning("Shioaji usage() failed: %s", exc)
            return {"connected": True, "error": str(exc)}

    # ── Contract ──────────────────────────────────────────────────────────────

    def get_contract(self, stock_id: str):
        if not self.is_logged_in():
            return None
        try:
            return self._api.Contracts.Stocks[str(stock_id)]
        except Exception:
            return None

    def get_contract_info(self, stock_id: str) -> dict | None:
        """回傳 contract 的重要欄位，供 UI 顯示。"""
        c = self.get_contract(stock_id)
        if c is None:
            return None
        return {
            "code": getattr(c, "code", stock_id),
            "name": getattr(c, "name", ""),
            "reference": getattr(c, "reference", None),
            "limit_up": getattr(c, "limit_up", None),
            "limit_down": getattr(c, "limit_down", None),
            "unit": getattr(c, "unit", None),
            "day_trade": str(getattr(c, "day_trade", "")),
            "margin_trading_balance": getattr(c, "margin_trading_balance", None),
            "short_selling_balance": getattr(c, "short_selling_balance", None),
            "update_date": str(getattr(c, "update_date", "")),
        }

    # ── Snapshots ─────────────────────────────────────────────────────────────

    def get_snapshots(self, stock_ids: list) -> dict:
        """
        批次取得即時快照。回傳 {stock_id: snapshot_dict}。
        snapshot_dict 合併了 contract 的 limit_up/limit_down/reference/name。
        """
        if not self.is_logged_in() or not stock_ids:
            return {}

        contracts = []
        id_map: dict[str, object] = {}
        for sid in stock_ids:
            c = self.get_contract(str(sid))
            if c is not None:
                contracts.append(c)
                id_map[str(sid)] = c

        if not contracts:
            return {}

        self._throttle()

        try:
            raw = self._api.snapshots(contracts)
        except Exception as exc:
            logger.warning("Shioaji snapshots() failed: %s", exc)
            from db.event_log import log_event
            log_event(
                "broker_snapshot_failed", module="shioaji_adapter", severity="warning",
                summary=f"snapshots 失敗：{exc}",
            )
            return {}

        result: dict[str, dict] = {}
        for snap in raw:
            sid = str(snap.code)
            contract = id_map.get(sid)
            limit_up = getattr(contract, "limit_up", None)
            limit_down = getattr(contract, "limit_down", None)
            reference = getattr(contract, "reference", None)
            name = getattr(contract, "name", "")
            close = snap.close

            dist_up = (
                round((limit_up - close) / close * 100, 2)
                if (limit_up and close) else None
            )
            dist_down = (
                round((close - limit_down) / close * 100, 2)
                if (limit_down and close) else None
            )

            result[sid] = {
                "stock_id": sid,
                "stock_name": name,
                "last_price": close,
                "reference_price": reference,
                "limit_up": limit_up,
                "limit_down": limit_down,
                "open": snap.open,
                "high": snap.high,
                "low": snap.low,
                "total_volume": snap.total_volume,
                "change_price": snap.change_price,
                "change_rate": snap.change_rate,
                "dist_to_limit_up_pct": dist_up,
                "dist_to_limit_down_pct": dist_down,
                "ts": datetime.now().strftime("%H:%M:%S"),
            }

        return result

    def get_snapshot(self, stock_id: str) -> dict | None:
        """查詢單一股票快照，供 UI 互動用。"""
        result = self.get_snapshots([stock_id])
        return result.get(str(stock_id))

    # ── KBars ─────────────────────────────────────────────────────────────────

    def get_kbars(self, stock_id: str, start: str, end: str):
        """回傳 DataFrame（欄位：ts/open/high/low/close/volume），失敗回傳 None。"""
        if not self.is_logged_in():
            return None
        contract = self.get_contract(stock_id)
        if contract is None:
            return None
        self._throttle()
        try:
            import pandas as pd
            kbars = self._api.kbars(contract=contract, start=start, end=end)
            return pd.DataFrame({**kbars})
        except Exception as exc:
            logger.warning("Shioaji kbars(%s) failed: %s", stock_id, exc)
            return None

    # ── Health Check ──────────────────────────────────────────────────────────

    def health_check(self) -> dict:
        if not self.is_logged_in():
            return {"logged_in": False, "connected": False, "contracts_ready": False}
        contracts_ready = False
        try:
            test = self._api.Contracts.Stocks["2330"]
            contracts_ready = test is not None
        except Exception:
            pass
        return {
            "logged_in": True,
            "contracts_ready": contracts_ready,
            **self.usage(),
        }


def get_adapter() -> ShioajiAdapter:
    """全域 accessor，所有模組統一透過此函式取得 adapter 實例。"""
    return ShioajiAdapter.get()
