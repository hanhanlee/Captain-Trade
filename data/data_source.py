"""
資料來源管理器

優先使用 FinMind（本機快取 + API），遇到 429 限額錯誤時自動切換
至 yfinance 備援模式，並停用需要法人資料的條件。

FinMind 免費帳號限制：每小時 600 次（註冊會員）

使用方式：
    from data.data_source import DataSourceManager
    dsm = DataSourceManager()
    df = dsm.get_price("2330", required_days=150)
    if dsm.fallback_mode:
        st.warning(dsm.FALLBACK_WARNING)
"""
import pandas as pd
from datetime import datetime, timedelta


FALLBACK_WARNING = (
    "⚠️ FinMind API 已達每小時限額（600 次），已切換至 yfinance 備援模式。"
    "  \n三大法人條件已自動停用；資料來源為 Yahoo Finance，報價可能有 15 分鐘延遲。"
)


def _is_rate_limit_error(exc: Exception) -> bool:
    """判斷例外是否為 FinMind 429 限額錯誤"""
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg or "rate limit" in msg


def _yf_symbol(stock_id: str, otc_ids: set = None) -> str:
    """將台股代碼轉換為 yfinance 格式（上市 .TW / 上櫃 .TWO）"""
    if otc_ids and stock_id in otc_ids:
        return f"{stock_id}.TWO"
    return f"{stock_id}.TW"


def _fetch_yfinance(stock_id: str, days: int = 150) -> pd.DataFrame:
    """
    透過 yfinance 取得日K資料，欄位名稱對齊系統標準格式：
      date, open, max, min, close, Trading_Volume
    """
    try:
        import yfinance as yf
    except ImportError:
        raise RuntimeError("yfinance 未安裝，請執行：pip install yfinance")

    symbol = _yf_symbol(stock_id)
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    ticker = yf.Ticker(symbol)
    raw = ticker.history(start=start, auto_adjust=True)

    if raw.empty:
        # 嘗試上櫃後綴
        symbol_otc = f"{stock_id}.TWO"
        ticker = yf.Ticker(symbol_otc)
        raw = ticker.history(start=start, auto_adjust=True)

    if raw.empty:
        return pd.DataFrame()

    raw = raw.reset_index()
    df = pd.DataFrame({
        "date": pd.to_datetime(raw["Date"]),
        "open": pd.to_numeric(raw["Open"], errors="coerce"),
        "max":  pd.to_numeric(raw["High"], errors="coerce"),
        "min":  pd.to_numeric(raw["Low"],  errors="coerce"),
        "close": pd.to_numeric(raw["Close"], errors="coerce"),
        "Trading_Volume": pd.to_numeric(raw["Volume"], errors="coerce"),
    })
    df = df.sort_values("date").reset_index(drop=True)
    return df


class DataSourceManager:
    """
    統一資料來源管理器

    屬性：
        fallback_mode (bool)：True 表示已切換至 yfinance 備援
        institutional_available (bool)：False 時應停用法人條件
    """

    def __init__(self):
        self.fallback_mode: bool = False
        self._fallback_triggered_at: datetime | None = None

    @property
    def institutional_available(self) -> bool:
        return not self.fallback_mode

    def get_price(self, stock_id: str, required_days: int = 150) -> pd.DataFrame:
        """
        取得日K資料。

        - 正常模式：呼叫 smart_get_price()（本機快取優先）
        - 備援模式：呼叫 yfinance
        - 若正常模式拋出 402 錯誤，自動切換備援並重試
        """
        if self.fallback_mode:
            return _fetch_yfinance(stock_id, days=required_days)

        try:
            from data.finmind_client import smart_get_price
            return smart_get_price(stock_id, required_days=required_days)
        except Exception as e:
            if _is_rate_limit_error(e):
                self.fallback_mode = True
                self._fallback_triggered_at = datetime.now()
                return _fetch_yfinance(stock_id, days=required_days)
            raise

    def get_stock_list(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        取得股票清單。備援模式下仍嘗試使用 FinMind（清單資料不消耗每日限額）。
        """
        from data.finmind_client import get_stock_list
        return get_stock_list(force_refresh=force_refresh)

    def get_institutional(self, stock_id: str, days: int = 10):
        """
        取得三大法人資料。備援模式下直接回傳空 DataFrame（資料不可用）。
        優先讀本機快取（24 小時 TTL），避免重複消耗 API 額度。
        """
        if self.fallback_mode:
            return pd.DataFrame()
        from data.finmind_client import smart_get_institutional
        return smart_get_institutional(stock_id, days=days)

    def reset_fallback(self):
        """手動重置備援狀態（例如隔天重新嘗試 FinMind）"""
        self.fallback_mode = False
        self._fallback_triggered_at = None
