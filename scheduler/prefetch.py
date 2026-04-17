"""
背景資料預抓取工作器

FinMind 免費帳號限制：每小時 600 次（註冊會員）

策略：
  - 交易時間（09:00–13:30）：每小時上限 100 次，保留額度給手動操作
  - 盤後/非交易時間（13:35+）：每小時上限 600 次，全力更新資料庫
  - 每 5.8 秒抓一檔（≈ 620 次/小時，讓 600 次上限當煞車）
  - 優先順序：① 完全無快取 → ② 快取 > 5 天舊 → ③ 其餘
  - 遇到 429：暫停 20 分鐘後自動恢復；可手動提前恢復

獨立執行：
  python -m scheduler.prefetch
"""
import time
import threading
import logging
from datetime import date, datetime, timedelta
from collections import deque

logger = logging.getLogger(__name__)

# ── 可調整參數 ─────────────────────────────────────────────────
HOURLY_LIMIT_OFFPEAK      = 600   # 盤後/非交易時間每小時上限（用滿 FinMind 全額度）
HOURLY_LIMIT_TRADING      = 100   # 交易時間每小時上限
FETCH_INTERVAL_SEC        = 5.8   # 正常模式每次請求間隔（秒）≈ 620 次/小時，讓上限當煞車
FETCH_INTERVAL_REBUILD    = 2.0   # 重建模式間隔（秒）；讓 429 來當剎車
STALE_DAYS                = 5     # 快取幾天未更新視為過期
RATE_LIMIT_PAUSE_MIN      = 20    # 遇到 429 後暫停幾分鐘（一般模式）
PREFETCH_DAYS             = 400   # 一般預抓天數（涵蓋回測需求：365天 + 60天指標暖身）
BACKTEST_PREFETCH_YEARS   = 10    # 回測資料重建：往前幾年
BACKTEST_PREFETCH_DAYS    = BACKTEST_PREFETCH_YEARS * 365  # ≈ 3650 天
TRADING_END_DEFAULT_HHMM  = (15, 0)    # 尚無學習記錄時的保底開始時間（最晚 15:00）
ADAPTIVE_LEAD_MIN         = 10         # 比學習到的最早更新時間提早幾分鐘開始全速
STALE_DELISTED_DAYS       = 180        # 快取超過此天數仍無新資料 → 視為下市/合併，永久跳過


def _is_429(exc: Exception) -> bool:
    """判斷例外是否為 429 限額"""
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg or "rate limit" in msg


def _latest_trading_day() -> date:
    """
    委派給 finmind_client.resolve_latest_trading_day()（含 2330 基準驗證 + TTL 快取）。
    呼叫失敗時退回保守的週曆計算，確保 Worker 不因此中斷。
    """
    try:
        from data.finmind_client import resolve_latest_trading_day
        return resolve_latest_trading_day()
    except Exception as e:
        logger.warning(f"_latest_trading_day 呼叫失敗，退回週曆計算：{e}")
        today = date.today()
        wd = today.weekday()
        if wd == 5:
            return today - timedelta(days=1)
        if wd == 6:
            return today - timedelta(days=2)
        return today


class PrefetchWorker:
    """
    背景資料預抓取工作器（單例，透過 get_worker() 取得）

    狀態屬性：
        running (bool)              — 背景執行緒是否活著
        hour_fetched (int)          — 本小時已消耗次數
        total_fetched (int)         — 本次啟動累計次數
        last_fetch_at               — 最近一次抓取時間
        current_stock (str)         — 正在抓取的股票代碼
        queue_size (int)            — 待抓取數量（快照）
        paused_for_market (bool)    — 是否因交易時間降速
        paused_until (datetime|None)— 429 暫停到幾點（None = 未暫停）
        rate_limit_count (int)      — 本次啟動共遇到幾次 429
    """

    def __init__(self):
        self.running = False
        self.hour_fetched: int = 0
        self.total_fetched: int = 0
        self._hour_window: deque = deque()
        self.last_fetch_at: datetime | None = None
        self.current_stock: str = ""
        self.queue_size: int = 0
        self.fund_queue_size: int = 0            # 待抓取基本面資料股票數
        self.paused_until: datetime | None = None
        self.rate_limit_count: int = 0
        self.rebuild_mode: bool = False
        self.rebuild_completed_at: datetime | None = None  # 重建完成時間
        self.backtest_rebuild_mode: bool = False            # 回測深度歷史資料重建
        self.backtest_completed_at: datetime | None = None # 回測重建完成時間
        self.backtest_queue_size: int = 0                  # 待補深度歷史的股票數
        self.backtest_initial_queue_size: int = 0          # 進度條分母
        self.initial_queue_size: int = 0         # 本次啟動時的初始待更新數量（進度條分母）
        self._skip_stocks: set = set()           # 無資料或抓了也不更新的股票，永久跳過
        self._error_counts: dict = {}            # 連續 error 次數；超過門檻後暫時跳過
        self._resume_event = threading.Event()
        self._wake_event = threading.Event()     # 模式切換 / 手動操作時喚醒背景迴圈
        self._stop_event   = threading.Event()
        self._thread: threading.Thread | None = None
        # 自適應開始時間：收盤後首筆成功更新的學習記錄
        self._today_first_update_recorded: bool = False   # 當天已記錄過則不重複寫
        self._today_record_date: date | None = None       # 用於每日重置上方旗標
        # Yahoo Bridge：13:45 後補充今日收盤資料
        self._yahoo_bridge_date: date | None = None       # 已執行 Yahoo Bridge 的日期
        self.yahoo_bridge_count: int = 0                  # 今日 Yahoo Bridge 已補充股票數
        self.yahoo_bridge_total: int = 0                  # 待抓總檔數
        self.yahoo_bridge_batch_done: int = 0             # 已完成批次數
        self.yahoo_bridge_batch_total: int = 0            # 總批次數
        self.yahoo_bridge_failed_ids: list = []           # 無 Yahoo 資料的股票清單
        self.yahoo_bridge_in_progress: bool = False       # 是否正在執行
        # 最近嘗試追蹤
        self.last_attempt_at: datetime | None = None
        self.last_attempt_result: str = ""   # normal/cached/suspended/delisted/rate_limit/error/no_update/ok
        self.last_attempt_stock: str = ""    # stock_id 或帶前綴的標籤，如 "[法人] 2330"
        # Supplementary 附加資料（法人 + 融資）
        self.inst_supplementary_total: int = 0
        self.inst_supplementary_done: int = 0
        self.margin_supplementary_total: int = 0
        self.margin_supplementary_done: int = 0
        self.supplementary_completed_at: datetime | None = None
        self._supplementary_date: date | None = None      # 已執行 Supplementary 的日期
        self._inst_no_update: set = set()                 # 本次啟動中法人無資料的股票
        self._margin_no_update: set = set()               # 本次啟動中融資無資料的股票

    # ── 公開控制 ───────────────────────────────────────────────

    def start(self):
        """啟動背景執行緒（已在跑則無操作）"""
        if self._thread and self._thread.is_alive():
            self._wake_event.set()
            return
        self._stop_event.clear()
        self._wake_event.set()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="prefetch-worker"
        )
        self._thread.start()
        self.running = True
        logger.info("PrefetchWorker 背景執行緒已啟動")

    def stop(self):
        """要求背景執行緒停止"""
        self._stop_event.set()
        self._resume_event.set()   # 喚醒可能正在暫停的執行緒
        self._wake_event.set()
        self.running = False
        logger.info("PrefetchWorker 已要求停止")

    def resume(self):
        """手動提前恢復（清除 429 暫停）"""
        self.paused_until = None
        self._resume_event.set()
        self._wake_event.set()
        logger.info("PrefetchWorker 手動恢復")

    def enable_rebuild_mode(self):
        """啟用全速重建模式：額度開放至 600 次/小時，不受交易時間限制"""
        self.rebuild_mode = True
        self.rebuild_completed_at = None
        self.initial_queue_size = 0
        self.current_stock = "[重建] 建立清單中"
        self._wake_event.set()
        logger.info("PrefetchWorker 進入全速重建模式（600次/小時）")

    def disable_rebuild_mode(self):
        """停止重建模式，恢復正常限速"""
        self.rebuild_mode = False
        if self.current_stock.startswith("[重建]"):
            self.current_stock = ""
        self._wake_event.set()
        logger.info("PrefetchWorker 退出重建模式，恢復正常限速")

    def enable_backtest_rebuild_mode(self):
        """啟用回測歷史資料重建模式：補充全市場最多 10 年的深度歷史資料"""
        self.backtest_rebuild_mode = True
        self.backtest_completed_at = None
        self.backtest_initial_queue_size = 0
        self.backtest_queue_size = 0
        self.current_stock = "[回測] 建立清單中"
        self._wake_event.set()
        logger.info(f"PrefetchWorker 進入回測重建模式（{BACKTEST_PREFETCH_YEARS} 年歷史）")

    def disable_backtest_rebuild_mode(self):
        """停止回測重建模式"""
        self.backtest_rebuild_mode = False
        if self.current_stock.startswith("[回測]"):
            self.current_stock = ""
        self._wake_event.set()
        logger.info("PrefetchWorker 退出回測重建模式")

    def _wait_with_wake(self, seconds: float):
        """
        可被模式切換提前喚醒的等待。
        用於一般輪詢 / 間隔等待，避免工作器在 idle 時對新模式無感。
        """
        deadline = time.time() + max(seconds, 0)
        while not self._stop_event.is_set():
            remaining = deadline - time.time()
            if remaining <= 0:
                return
            if self._wake_event.wait(timeout=min(remaining, 1.0)):
                self._wake_event.clear()
                return

    # ── 私有方法 ───────────────────────────────────────────────

    def _get_trading_end_time(self):
        """
        回傳全速更新的開始時間（即「交易時間」的結束點）。
        - 若已有學習記錄：使用歷史最早首筆更新時間 − ADAPTIVE_LEAD_MIN 分鐘
        - 尚無記錄：使用 TRADING_END_DEFAULT_HHMM（保底 15:00）
        """
        from datetime import time as _time, timedelta
        try:
            from db.settings import get_prefetch_optimal_time
            hhmm = get_prefetch_optimal_time()
            if hhmm:
                h, m = map(int, hhmm.split(":"))
                ref = datetime.now().replace(hour=h, minute=m, second=0, microsecond=0)
                ref -= timedelta(minutes=ADAPTIVE_LEAD_MIN)
                return _time(ref.hour, ref.minute)
        except Exception:
            pass
        return _time(*TRADING_END_DEFAULT_HHMM)

    def _within_trading_hours(self) -> bool:
        if self.rebuild_mode or self.backtest_rebuild_mode:
            return False   # 重建模式：不受交易時間限制
        now = datetime.now().time()
        from datetime import time as _time
        return _time(9, 0) <= now <= self._get_trading_end_time()

    def _try_record_first_update_time(self):
        """
        收盤後（13:30 以後）每日首次抓到新資料時，與歷史最早記錄比較：
        - 若比現有記錄更早（或尚無記錄）→ 更新，全速開始時間往前移
        - 否則保留既有的最早記錄不變
        這樣學習值只會越來越早，收斂到 FinMind 最快更新的窗口。
        """
        now = datetime.now()
        today = now.date()

        # 每日重置旗標（每天只取第一筆，避免同日多次比較）
        if self._today_record_date != today:
            self._today_record_date = today
            self._today_first_update_recorded = False

        if self._today_first_update_recorded:
            return

        # 僅在 13:30 之後才記錄（避免盤中資料污染學習值）
        from datetime import time as _time
        if now.time() < _time(13, 30):
            return

        self._today_first_update_recorded = True  # 無論是否更新，今日只比較一次
        hhmm = now.strftime("%H:%M")

        try:
            from db.settings import get_prefetch_optimal_time, set_prefetch_optimal_time
            current = get_prefetch_optimal_time()

            if not current:
                # 首次記錄
                set_prefetch_optimal_time(hhmm)
                end_t = self._get_trading_end_time()
                logger.info(
                    f"[自適應] 首次記錄最早更新時間：{hhmm}，"
                    f"明日全速開始時間：{end_t.strftime('%H:%M')}"
                )
            else:
                cur_h, cur_m = map(int, current.split(":"))
                new_h, new_m = map(int, hhmm.split(":"))
                if (new_h, new_m) < (cur_h, cur_m):
                    set_prefetch_optimal_time(hhmm)
                    end_t = self._get_trading_end_time()
                    logger.info(
                        f"[自適應] 刷新最早更新記錄：{hhmm}（舊：{current}），"
                        f"明日全速開始時間前移至 {end_t.strftime('%H:%M')}"
                    )
                else:
                    logger.info(
                        f"[自適應] 今日首筆：{hhmm}，未早於最早記錄 {current}，保留不變"
                    )
        except Exception as e:
            logger.warning(f"記錄首筆更新時間失敗：{e}")

    def _current_hourly_limit(self) -> int:
        if self.rebuild_mode or self.backtest_rebuild_mode:
            return 600     # 重建模式：全速
        return HOURLY_LIMIT_TRADING if self._within_trading_hours() else HOURLY_LIMIT_OFFPEAK

    def _hour_count(self) -> int:
        cutoff = datetime.now() - timedelta(hours=1)
        while self._hour_window and self._hour_window[0] < cutoff:
            self._hour_window.popleft()
        return len(self._hour_window)

    def _record_request(self):
        self._hour_window.append(datetime.now())
        self.hour_fetched = len(self._hour_window)
        self.total_fetched += 1

    def _next_hour_seconds(self) -> int:
        """計算距離下一個整點還有幾秒（加 61 秒緩衝）"""
        now = datetime.now()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return int((next_hour - now).total_seconds()) + 61

    def _pause_for_rate_limit(self):
        """
        遇到 429：
        - 一般模式：暫停固定 RATE_LIMIT_PAUSE_MIN 分鐘
        - 重建模式：等到下一個整點（FinMind 重置時間）+ 61 秒緩衝
          確保跨整點後立即恢復，不浪費任何配額
        """
        self.rate_limit_count += 1

        if self.rebuild_mode or self.backtest_rebuild_mode:
            wait_sec = self._next_hour_seconds()
            self.paused_until = datetime.now() + timedelta(seconds=wait_sec)
            resume_at_str = self.paused_until.strftime("%H:%M:%S")
            mode_label = "重建模式" if self.rebuild_mode else "回測重建模式"
            logger.warning(
                f"[{mode_label}] 收到 429（第 {self.rate_limit_count} 次），"
                f"等待至下一整點 {resume_at_str}（{wait_sec//60} 分 {wait_sec%60} 秒後）"
            )
        else:
            wait_sec = RATE_LIMIT_PAUSE_MIN * 60
            self.paused_until = datetime.now() + timedelta(seconds=wait_sec)
            resume_at_str = self.paused_until.strftime("%H:%M:%S")
            logger.warning(
                f"收到 429，第 {self.rate_limit_count} 次限額，"
                f"暫停 {RATE_LIMIT_PAUSE_MIN} 分鐘至 {resume_at_str}"
            )

        self._resume_event.clear()
        self._resume_event.wait(timeout=wait_sec)

        self.paused_until = None
        if not self._stop_event.is_set():
            logger.info("429 暫停結束，繼續抓取")

    def _is_market_holiday(self) -> bool:
        """判斷目前是否處於休市壓力真空期（週末 or 手動休市模式）"""
        try:
            from db.settings import is_market_closed
            if is_market_closed():
                return True
        except Exception:
            pass
        return date.today().weekday() >= 5  # 週六/日

    def _try_recover_dead_stocks(self):
        """
        死股回收：只在休市期且 last_attempt_at > 7 天時重試 no_update 股票，
        驗證是否已復牌。平時直接略過以節省 API 額度。
        """
        if not self._is_market_holiday():
            return
        try:
            from db.price_cache import get_no_update_stocks, set_fetch_status, get_cached_dates
            from data.finmind_client import get_daily_price, save_prices
            candidates = get_no_update_stocks(stale_days=7)
            if not candidates:
                return
            logger.info(f"死股回收：嘗試驗證 {len(candidates)} 檔（no_update > 7 天）")
            for sid in candidates:
                if self._stop_event.is_set():
                    break
                if self._hour_count() >= self._current_hourly_limit():
                    break
                try:
                    df = get_daily_price(sid, days=10)
                    if not df.empty:
                        save_prices(sid, df)
                        set_fetch_status(sid, "normal")
                        if sid in self._skip_stocks:
                            self._skip_stocks.discard(sid)
                        logger.info(f"死股回收：{sid} 已復牌，移除跳過名單")
                    else:
                        _, max_before = get_cached_dates(sid)
                        if max_before is None:
                            new_status = "delisted"
                        else:
                            max_d = (max_before if isinstance(max_before, date)
                                     else datetime.strptime(str(max_before), "%Y-%m-%d").date())
                            cache_age = (date.today() - max_d).days
                            # 超過門檻天數仍無新資料 → 已下市或合併，永久跳過
                            new_status = "delisted" if cache_age >= STALE_DELISTED_DAYS else "suspended"
                        set_fetch_status(sid, new_status)
                        logger.debug(f"死股回收：{sid} 仍無資料 → {new_status}")
                    self._record_request()
                    self._wait_with_wake(FETCH_INTERVAL_SEC)
                except Exception as e:
                    if _is_429(e):
                        self._pause_for_rate_limit()
                        break
                    logger.debug(f"死股回收 {sid} 失敗：{e}")
        except Exception as e:
            logger.warning(f"死股回收流程異常：{e}")

    def _get_funds_needing_fetch(self) -> list[str]:
        """回傳尚無新鮮基本面快取的股票清單"""
        try:
            from data.finmind_client import get_stock_list
            from db.fundamental_cache import get_stocks_needing_fundamental
            all_ids = get_stock_list()["stock_id"].tolist()
            return get_stocks_needing_fundamental(all_ids)
        except Exception as e:
            logger.warning(f"取得基本面待抓清單失敗：{e}")
            return []

    def _fetch_one_fundamental(self, stock_id: str) -> str:
        """
        抓取並快取單檔基本面資料。
        回傳值同 _fetch_one：'ok' / 'rate_limit' / 'error'
        """
        try:
            from data.finmind_client import smart_get_fundamentals
            smart_get_fundamentals(stock_id)
            return "ok"
        except Exception as e:
            if _is_429(e):
                return "rate_limit"
            logger.debug(f"抓取基本面 {stock_id} 失敗：{e}")
            return "error"

    def _get_backtest_stale_stocks(self) -> list[str]:
        """
        回傳歷史資料深度不足 BACKTEST_PREFETCH_YEARS 年的股票清單。
        以 price_cache.earliest 判斷；完全無快取的股票也納入。
        """
        try:
            from data.finmind_client import get_stock_list
            from db.price_cache import (
                get_cache_summary,
                get_delisted_stocks,
                get_suspended_stocks,
            )

            all_ids = set(get_stock_list()["stock_id"].tolist())
            target_start = (date.today() - timedelta(days=BACKTEST_PREFETCH_DAYS)).isoformat()

            summary = get_cache_summary()
            if summary.empty:
                return sorted(all_ids - self._skip_stocks)

            cached_ids = set(summary["stock_id"].tolist())
            # 有快取但最早日期不夠早（未達目標年數）
            insufficient = set(
                summary.loc[summary["earliest"] > target_start, "stock_id"].tolist()
            )
            missing = all_ids - cached_ids
            return sorted((missing | insufficient) - self._skip_stocks)
        except Exception as e:
            logger.warning(f"取得回測待抓清單失敗：{e}")
            return []

    def _fetch_one_backtest(self, stock_id: str) -> str:
        """
        補充單檔股票的深度歷史資料（往前 BACKTEST_PREFETCH_YEARS 年）。
        與 _fetch_one 不同：以最早快取日期判斷，而非最新日期；
        直接呼叫 get_daily_price 搭配 start_date，不走 smart_get_price。
        """
        try:
            from db.price_cache import get_cached_dates, save_prices
            from data.finmind_client import get_daily_price

            target_date = date.today() - timedelta(days=BACKTEST_PREFETCH_DAYS)
            target_start_str = target_date.strftime("%Y-%m-%d")

            min_date, _ = get_cached_dates(stock_id)
            if min_date is not None:
                min_d = min_date if isinstance(min_date, date) else \
                    datetime.strptime(str(min_date), "%Y-%m-%d").date()
                if min_d <= target_date:
                    return "cached"  # 已有足夠的歷史深度

            # 從目標起始日補抓（含已有的資料段，save_prices 有 INSERT OR IGNORE 保護）
            new_df = get_daily_price(stock_id, start_date=target_start_str)
            if new_df.empty:
                return "no_update"

            save_prices(stock_id, new_df)
            return "ok"
        except Exception as e:
            if _is_429(e):
                return "rate_limit"
            logger.debug(f"抓取回測歷史 {stock_id} 失敗：{e}")
            return "error"

    def _get_stale_stocks(self) -> tuple[list[str], list[str]]:
        """
        回傳兩個清單：
          needs_update — 真正需要更新的（無快取 + 快取未達最新交易日），用於 queue_size 顯示
          full_queue   — needs_update（優先）+ 新鮮（定期巡迴），用於實際抓取迴圈

        優先級：
          第一優先 (Missing)  — 完全無快取
          第二優先 (Stale)    — 按 max_date ASC（最久未更新的先抓）
          跳過     (Fresh)    — 已同步至最新交易日，不計 API 次數
        """
        try:
            from data.finmind_client import get_stock_list
            from db.price_cache import get_cache_summary, get_delisted_stocks, get_suspended_stocks

            all_stocks_df = get_stock_list()
            if all_stocks_df.empty:
                return [], []
            all_ids = set(all_stocks_df["stock_id"].tolist())

            self._skip_stocks = set(get_delisted_stocks(include_legacy_no_update=True))
            # 只封鎖「3 小時內才失敗」的 suspended 股票；
            # 15:xx 試失敗（FinMind 尚未更新）的股票，18:xx 之後會自動重試
            suspended_today = set(get_suspended_stocks(today_only=True, recent_hours=3))
            summary = get_cache_summary()
            cutoff = _latest_trading_day().isoformat()

            if summary.empty:
                missing = sorted(all_ids - self._skip_stocks)
                return missing, missing

            cached_ids = set(summary["stock_id"].tolist())

            # Stale：按 max_date ASC 排序，確保最久未更新的優先處理
            stale_df = summary[summary["latest"] < cutoff].copy()
            stale_df = stale_df[~stale_df["stock_id"].isin(self._skip_stocks)]
            stale_df = stale_df[~stale_df["stock_id"].isin(suspended_today)]
            stale_df = stale_df.sort_values("latest", ascending=True)
            stale_list = stale_df["stock_id"].tolist()

            missing = sorted(all_ids - cached_ids - self._skip_stocks)

            fresh = sorted(
                all_ids - set(missing) - set(stale_list) - self._skip_stocks
            )

            needs_update = missing + stale_list
            full_queue   = missing + stale_list + fresh
            return needs_update, full_queue
        except Exception as e:
            logger.warning(f"取得待抓清單失敗：{e}")
            return [], []

    def _should_run_yahoo_bridge(self) -> bool:
        """判斷是否需要執行 Yahoo Bridge 補充今日收盤資料"""
        now = datetime.now()
        today = now.date()

        # 已在今日執行過
        if self._yahoo_bridge_date == today:
            return False

        # 13:45 之前不執行（收盤 13:30 + 15 分鐘 yfinance 延遲緩衝）
        from datetime import time as _time
        if now.time() < _time(13, 45):
            return False

        # 週末不執行
        if today.weekday() >= 5:
            return False

        # 手動休市模式不執行
        if self._is_market_holiday():
            return False

        return True

    def _note_attempt(self, stock_label: str, result: str):
        """記錄最近一次嘗試（無論成功與否）"""
        self.last_attempt_at     = datetime.now()
        self.last_attempt_result = result
        self.last_attempt_stock  = stock_label

    def reset_yahoo_bridge(self):
        """重置 Yahoo Bridge，讓 worker 下一輪重新執行（已快取的會自動跳過）"""
        self._yahoo_bridge_date = None
        self.yahoo_bridge_failed_ids = []
        self.yahoo_bridge_total = 0
        self.yahoo_bridge_batch_done = 0
        self.yahoo_bridge_batch_total = 0
        self.yahoo_bridge_in_progress = False
        self._wake_event.set()
        logger.info("Yahoo Bridge 已重置，下一輪將重新執行")

    def _run_yahoo_bridge_phase(self):
        """
        Yahoo Bridge 階段：13:45 之後批次從 Yahoo Finance 補充今日收盤資料。
        使用 INSERT OR IGNORE，確保後續 FinMind 資料可覆蓋，不影響法人/融資欄位。
        """
        today = date.today()
        self._yahoo_bridge_date = today  # 先標記，避免重複執行（即使中途失敗）
        self.yahoo_bridge_in_progress = True
        self.yahoo_bridge_failed_ids = []

        try:
            import yfinance as yf
            from data.finmind_client import get_stock_list
            from db.price_cache import get_delisted_stocks, save_prices
            from data.yahoo_client import (
                _to_yf_ticker, _parse_batch_result,
                BATCH_SIZE, BATCH_SLEEP_SEC, get_today_cached_stock_ids,
            )

            all_stocks_df = get_stock_list()
            if all_stocks_df.empty:
                return

            all_ids = set(all_stocks_df["stock_id"].tolist())
            skip = set(get_delisted_stocks(include_legacy_no_update=True))
            active_ids = sorted(all_ids - skip)

            already_cached = get_today_cached_stock_ids(today)
            to_fetch = [sid for sid in active_ids if sid not in already_cached]

            if not to_fetch:
                logger.info("Yahoo Bridge：今日資料已全數在快取，略過")
                return

            batches = [to_fetch[i:i + BATCH_SIZE] for i in range(0, len(to_fetch), BATCH_SIZE)]
            self.yahoo_bridge_total = len(to_fetch)
            self.yahoo_bridge_batch_total = len(batches)
            self.yahoo_bridge_batch_done = 0

            logger.info(
                f"Yahoo Bridge 開始：{len(to_fetch)} 檔，分 {len(batches)} 批"
                f"（已快取 {len(already_cached)} 檔，跳過 {len(skip)} 檔）"
            )

            # 判斷上櫃股票（.TWO 後綴）
            otc_ids: set | None = None
            try:
                for col in ("type", "market", "市場別"):
                    if col in all_stocks_df.columns:
                        otc_ids = set(
                            all_stocks_df[all_stocks_df[col].str.contains("上櫃", na=False)]["stock_id"].tolist()
                        )
                        break
            except Exception:
                pass

            result_all: dict = {}
            for batch_idx, batch in enumerate(batches):
                if self._stop_event.is_set():
                    break

                self.current_stock = (
                    f"[Yahoo] 批次 {batch_idx + 1}/{len(batches)}"
                )
                tw_map = {_to_yf_ticker(sid, otc_ids): sid for sid in batch}
                tickers = list(tw_map.keys())

                try:
                    raw = yf.download(
                        tickers, period="2d", auto_adjust=True,
                        progress=False, threads=True,
                    )
                    batch_result = _parse_batch_result(raw, batch, today, tw_map)
                    result_all.update(batch_result)
                    ok = len(batch_result)
                    logger.info(
                        f"Yahoo Bridge 批次 {batch_idx+1}/{len(batches)}："
                        f"{ok}/{len(batch)} 檔成功"
                    )
                except Exception as e:
                    logger.warning(f"Yahoo Bridge 批次 {batch_idx+1} 失敗：{e}")

                self.yahoo_bridge_batch_done = batch_idx + 1

                if batch_idx < len(batches) - 1:
                    self._wait_with_wake(BATCH_SLEEP_SEC)

            # 儲存，使用 INSERT OR IGNORE（不覆蓋已有資料）
            count = 0
            for sid, df_row in result_all.items():
                try:
                    save_prices(sid, df_row, replace=False)
                    count += 1
                except Exception as e:
                    logger.debug(f"Yahoo Bridge 儲存 {sid} 失敗：{e}")

            self.yahoo_bridge_failed_ids = [
                sid for sid in to_fetch if sid not in result_all
            ]
            self.yahoo_bridge_count = count
            self.current_stock = ""
            logger.info(
                f"Yahoo Bridge 完成：{count}/{len(to_fetch)} 檔已補充，"
                f"{len(self.yahoo_bridge_failed_ids)} 檔無資料"
            )

        except Exception as e:
            self.current_stock = ""
            logger.warning(f"Yahoo Bridge 執行失敗：{e}")
        finally:
            self.yahoo_bridge_in_progress = False

    def _should_run_supplementary(self) -> bool:
        """判斷是否需要執行 Supplementary 附加資料抓取（法人 + 融資）"""
        today = date.today()
        if self._supplementary_date == today:
            return False
        # 只在 FinMind 已更新今日資料後才執行（確保附加資料與核心資料同日）
        try:
            ltd = _latest_trading_day()
            if ltd < today:
                return False  # FinMind 尚未更新今日資料
        except Exception:
            return False
        # 週末不執行
        if today.weekday() >= 5:
            return False
        return True

    def _fetch_inst_today(self, stock_id: str) -> str:
        """
        抓取並快取單檔今日法人資料。
        回傳：'ok' / 'cached' / 'no_update' / 'rate_limit' / 'error'
        """
        try:
            from db.inst_cache import is_inst_fresh, save_institutional
            from data.finmind_client import get_institutional

            if is_inst_fresh(stock_id):
                return "cached"

            df = get_institutional(stock_id, days=3)
            if df.empty:
                return "no_update"

            save_institutional(stock_id, df)
            return "ok"
        except Exception as e:
            if _is_429(e):
                return "rate_limit"
            logger.debug(f"法人資料 {stock_id} 失敗：{e}")
            return "error"

    def _fetch_margin_today(self, stock_id: str) -> str:
        """
        抓取並快取單檔今日融資融券資料。
        回傳：'ok' / 'cached' / 'no_update' / 'rate_limit' / 'error'
        """
        try:
            from db.margin_cache import get_margin, save_margin
            from data.finmind_client import get_margin_trading

            today = date.today()
            today_str = today.strftime("%Y-%m-%d")

            # 已有今日資料
            existing = get_margin(stock_id, days=1)
            if not existing.empty:
                latest = existing["date"].max()
                if hasattr(latest, "date"):
                    latest = latest.date()
                if str(latest)[:10] >= today_str:
                    return "cached"

            df = get_margin_trading(stock_id, days=3)
            if df.empty:
                return "no_update"

            save_margin(stock_id, df)
            return "ok"
        except Exception as e:
            if _is_429(e):
                return "rate_limit"
            logger.debug(f"融資資料 {stock_id} 失敗：{e}")
            return "error"

    def _run_supplementary_phase(self, active_ids: list[str]):
        """
        Supplementary 附加資料抓取（法人 + 融資融券）。
        在核心價格資料完成後執行，動態分母排除 no_update 的股票。
        """
        today = date.today()
        self._supplementary_date = today

        inst_queue  = [sid for sid in active_ids if sid not in self._inst_no_update]
        margin_queue = [sid for sid in active_ids if sid not in self._margin_no_update]

        self.inst_supplementary_total   = len(inst_queue)
        self.margin_supplementary_total = len(margin_queue)
        self.inst_supplementary_done    = 0
        self.margin_supplementary_done  = 0

        logger.info(
            f"Supplementary 開始：法人 {len(inst_queue)} 檔 / 融資 {len(margin_queue)} 檔"
        )

        # ── 法人資料 ──────────────────────────────────────────────
        hit_rate_limit = False
        for stock_id in inst_queue:
            if self._stop_event.is_set():
                return
            if self._hour_count() >= self._current_hourly_limit():
                break

            self.current_stock = f"[法人] {stock_id}"
            result = self._fetch_inst_today(stock_id)
            self._note_attempt(f"[法人] {stock_id}", result)

            if result in ("ok", "cached"):
                self._record_request()
                self.last_fetch_at = datetime.now()
                self.inst_supplementary_done += 1
                # 動態縮小分母
                if result == "ok":
                    self._wait_with_wake(FETCH_INTERVAL_SEC)
            elif result == "no_update":
                self._inst_no_update.add(stock_id)
                self.inst_supplementary_total = max(0, self.inst_supplementary_total - 1)
            elif result == "rate_limit":
                hit_rate_limit = True
                break
            # 'error': 繼續下一檔

        if hit_rate_limit and not self._stop_event.is_set():
            self.current_stock = ""
            self._pause_for_rate_limit()
            return

        # ── 融資融券資料 ──────────────────────────────────────────
        hit_rate_limit = False
        for stock_id in margin_queue:
            if self._stop_event.is_set():
                return
            if self._hour_count() >= self._current_hourly_limit():
                break

            self.current_stock = f"[融資] {stock_id}"
            result = self._fetch_margin_today(stock_id)
            self._note_attempt(f"[融資] {stock_id}", result)

            if result in ("ok", "cached"):
                self._record_request()
                self.last_fetch_at = datetime.now()
                self.margin_supplementary_done += 1
                if result == "ok":
                    self._wait_with_wake(FETCH_INTERVAL_SEC)
            elif result == "no_update":
                self._margin_no_update.add(stock_id)
                self.margin_supplementary_total = max(0, self.margin_supplementary_total - 1)
            elif result == "rate_limit":
                hit_rate_limit = True
                break

        self.current_stock = ""

        if hit_rate_limit and not self._stop_event.is_set():
            self._pause_for_rate_limit()
            return

        # 兩者均完成
        inst_done   = self.inst_supplementary_done   >= self.inst_supplementary_total   > 0
        margin_done = self.margin_supplementary_done >= self.margin_supplementary_total > 0
        if inst_done and margin_done:
            self.supplementary_completed_at = datetime.now()
            logger.info(
                f"Supplementary 完成：法人 {self.inst_supplementary_done}/{self.inst_supplementary_total}，"
                f"融資 {self.margin_supplementary_done}/{self.margin_supplementary_total}"
            )

    # 回傳值：
    #   'normal'     — 成功抓取並更新快取
    #   'cached'     — 快取仍新鮮，跳過
    #   'suspended'  — 有舊快取，但本次抓不到新資料；同日先暫停重試
    #   'delisted'   — 無舊快取且 API 仍回空值，視為永久跳過
    #   'rate_limit' — 429
    #   'error'      — 其他例外
    def _fetch_one(self, stock_id: str) -> str:
        try:
            from db.price_cache import get_cached_dates, set_fetch_status
            from data.finmind_client import smart_get_price

            _, max_before = get_cached_dates(stock_id)

            if max_before is not None:
                max_d = (max_before if isinstance(max_before, date)
                         else datetime.strptime(str(max_before), "%Y-%m-%d").date())
                if max_d >= _latest_trading_day():
                    return "cached"

            smart_get_price(stock_id, required_days=PREFETCH_DAYS)

            _, max_after = get_cached_dates(stock_id)
            if max_after is None or max_after == max_before:
                if max_before is None:
                    logger.debug(f"{stock_id} 無舊快取且 API 無資料，標記 delisted")
                    set_fetch_status(stock_id, "delisted")
                    return "delisted"

                # 有舊快取但長時間（>STALE_DELISTED_DAYS 天）仍無新資料
                # → 股票已下市或合併，永久標記 delisted 避免無限重試
                max_d = (max_before if isinstance(max_before, date)
                         else datetime.strptime(str(max_before), "%Y-%m-%d").date())
                cache_age = (date.today() - max_d).days
                if cache_age >= STALE_DELISTED_DAYS:
                    logger.info(
                        f"{stock_id} 有舊快取但已 {cache_age} 天無新資料"
                        f"（最新 {max_d}），視為下市/合併，標記 delisted"
                    )
                    set_fetch_status(stock_id, "delisted")
                    return "delisted"

                logger.debug(f"{stock_id} 有舊快取但本次無新資料，標記 suspended")
                set_fetch_status(stock_id, "suspended")
                return "suspended"

            set_fetch_status(stock_id, "normal")
            return "normal"
        except Exception as e:
            if _is_429(e):
                return "rate_limit"
            logger.debug(f"抓取 {stock_id} 失敗：{e}")
            return "error"

    def _run_loop(self):
        logger.info("PrefetchWorker 迴圈開始")
        while not self._stop_event.is_set():

            # 重建模式（含回測）：跳過內部計數器，讓 API 的 429 來當限制
            if not self.rebuild_mode and not self.backtest_rebuild_mode:
                used = self._hour_count()
                limit = self._current_hourly_limit()
                if used >= limit:
                    self._wait_with_wake(60)
                    continue

            # ── 回測深度歷史重建模式：優先於一般價格抓取 ─────────────
            if self.backtest_rebuild_mode:
                bt_queue = self._get_backtest_stale_stocks()
                self.backtest_queue_size = len(bt_queue)
                if self.backtest_initial_queue_size == 0 and self.backtest_queue_size > 0:
                    self.backtest_initial_queue_size = self.backtest_queue_size

                if self.backtest_queue_size == 0:
                    self.backtest_rebuild_mode = False
                    self.backtest_completed_at = datetime.now()
                    logger.info("回測歷史資料重建完成，自動退出回測重建模式")
                    continue

                hit_rate_limit = False
                for stock_id in bt_queue:
                    if self._stop_event.is_set():
                        break
                    if self._hour_count() >= self._current_hourly_limit():
                        break
                    self.current_stock = f"[回測] {stock_id}"
                    result = self._fetch_one_backtest(stock_id)
                    self._note_attempt(f"[回測] {stock_id}", result)
                    if result == "ok":
                        self._record_request()
                        self.last_fetch_at = datetime.now()
                        self.backtest_queue_size = max(0, self.backtest_queue_size - 1)
                        logger.debug(
                            f"回測歷史已抓 {stock_id}，本小時 {self.hour_fetched}，"
                            f"剩餘 {self.backtest_queue_size}"
                        )
                        self._wait_with_wake(FETCH_INTERVAL_REBUILD)
                    elif result == "no_update":
                        self._skip_stocks.add(stock_id)
                        self.backtest_queue_size = max(0, self.backtest_queue_size - 1)
                    elif result == "rate_limit":
                        hit_rate_limit = True
                        break
                    # 'cached' / 'error': 繼續下一檔

                self.current_stock = ""
                if hit_rate_limit and not self._stop_event.is_set():
                    self._pause_for_rate_limit()
                else:
                    self._wait_with_wake(5)
                continue

            # ── Yahoo Bridge：13:45 後補充今日收盤資料（每日一次）─────
            if self._should_run_yahoo_bridge():
                self._run_yahoo_bridge_phase()
                continue

            # ── 一般模式：抓取近期價格資料 ─────────────────────────
            # 取得待抓清單（needs_update 用於顯示，full_queue 用於實際抓取）
            needs_update, full_queue = self._get_stale_stocks()
            needs_update_set = set(needs_update)
            self.queue_size = len(needs_update)
            # 第一次計算時記錄初始值，作為進度條的固定分母
            if self.initial_queue_size == 0 and self.queue_size > 0:
                self.initial_queue_size = self.queue_size

            # 重建模式：待更新清單已清空，自動關閉重建模式
            if self.rebuild_mode and self.queue_size == 0:
                self.rebuild_mode = False
                self.rebuild_completed_at = datetime.now()
                logger.info("全速重建完成，自動退出重建模式，恢復正常限速")

            if not needs_update:
                # 待更新清單為空（needs_update 空）：① Supplementary ② 死股回收 ③ 填充基本面 ④ idle

                # Supplementary：FinMind 已更新今日資料後抓取法人 + 融資
                if self._should_run_supplementary():
                    try:
                        from data.finmind_client import get_stock_list
                        from db.price_cache import get_delisted_stocks
                        all_stocks_df = get_stock_list()
                        skip = set(get_delisted_stocks(include_legacy_no_update=True))
                        active_ids = sorted(
                            set(all_stocks_df["stock_id"].tolist()) - skip
                        )
                    except Exception as e:
                        logger.warning(f"Supplementary 取得股票清單失敗：{e}")
                        active_ids = []
                    if active_ids:
                        self._run_supplementary_phase(active_ids)
                    continue

                self._try_recover_dead_stocks()

                fund_ids = self._get_funds_needing_fetch()
                self.fund_queue_size = len(fund_ids)
                if not fund_ids:
                    logger.info("所有快取（價格+基本面）皆為最新，等待 30 分鐘")
                    self._wait_with_wake(1800)
                else:
                    logger.info(f"價格快取完成，開始填充基本面快取（{len(fund_ids)} 檔）")
                    fund_hit_rate_limit = False
                    for stock_id in fund_ids:
                        if self._stop_event.is_set():
                            break
                        if not self.rebuild_mode and self._hour_count() >= self._current_hourly_limit():
                            break
                        self.current_stock = f"[基本面] {stock_id}"
                        result = self._fetch_one_fundamental(stock_id)
                        self._note_attempt(f"[基本面] {stock_id}", result)
                        if result == "ok":
                            self._record_request()
                            self.last_fetch_at = datetime.now()
                            self.fund_queue_size = max(0, self.fund_queue_size - 1)
                            logger.debug(f"基本面已抓 {stock_id}，本小時 {self.hour_fetched}，剩餘 {self.fund_queue_size}")
                            interval = FETCH_INTERVAL_REBUILD if self.rebuild_mode else FETCH_INTERVAL_SEC
                            self._wait_with_wake(interval)
                        elif result == "rate_limit":
                            fund_hit_rate_limit = True
                            break
                        # 'error': 繼續下一檔
                    self.current_stock = ""
                    if fund_hit_rate_limit and not self._stop_event.is_set():
                        self._pause_for_rate_limit()
                continue

            # 逐檔抓取
            hit_rate_limit = False
            for stock_id in full_queue:
                if self._stop_event.is_set():
                    break
                if not self.rebuild_mode and self._hour_count() >= self._current_hourly_limit():
                    break

                self.current_stock = stock_id
                result = self._fetch_one(stock_id)
                self._note_attempt(stock_id, result)

                if result == "normal":
                    self._record_request()
                    self.last_fetch_at = datetime.now()
                    self._error_counts.pop(stock_id, None)  # 成功後清除 error 計數
                    self._try_record_first_update_time()     # 自適應：收盤後首筆成功則學習時間
                    # 若該股原本在待更新清單，完成後遞減計數
                    if stock_id in needs_update_set:
                        self.queue_size = max(0, self.queue_size - 1)
                    logger.debug(f"已抓 {stock_id}，本小時 {self.hour_fetched}，待更新 {self.queue_size}")
                    interval = FETCH_INTERVAL_REBUILD if self.rebuild_mode else FETCH_INTERVAL_SEC
                    self._wait_with_wake(interval)

                elif result == "suspended":
                    # 同一天先暫停重試，隔天再由 _get_stale_stocks 放回待更新清單
                    if stock_id in needs_update_set:
                        self.queue_size = max(0, self.queue_size - 1)

                elif result == "delisted":
                    # 永久跳過名單
                    self._skip_stocks.add(stock_id)
                    if stock_id in needs_update_set:
                        self.queue_size = max(0, self.queue_size - 1)

                elif result == "error":
                    # 連續 error 超過 3 次：標記 suspended 暫時跳過，避免無限卡在同一批股票
                    self._error_counts[stock_id] = self._error_counts.get(stock_id, 0) + 1
                    if self._error_counts[stock_id] >= 3:
                        from db.price_cache import set_fetch_status
                        set_fetch_status(stock_id, "suspended")
                        self._error_counts.pop(stock_id, None)
                        if stock_id in needs_update_set:
                            self.queue_size = max(0, self.queue_size - 1)
                        logger.debug(f"{stock_id} 連續 3 次 error，標記 suspended 暫時跳過")

                elif result == "rate_limit":
                    hit_rate_limit = True
                    break

                # "cached" 直接繼續，不等待

            self.current_stock = ""

            if hit_rate_limit and not self._stop_event.is_set():
                self._pause_for_rate_limit()
            else:
                self._wait_with_wake(5)

        self.running = False
        logger.info("PrefetchWorker 迴圈結束")

    def status(self) -> dict:
        used  = self._hour_count()
        limit = self._current_hourly_limit()
        now   = datetime.now()

        pause_remaining_sec = 0
        if self.paused_until and self.paused_until > now:
            pause_remaining_sec = int((self.paused_until - now).total_seconds())

        try:
            latest_td = _latest_trading_day()
        except Exception:
            latest_td = None

        return {
            "running":              self.running and bool(self._thread and self._thread.is_alive()),
            "hour_fetched":         used,
            "hourly_limit":         limit,
            "hourly_remaining":     max(limit - used, 0),
            "total_fetched":        self.total_fetched,
            "queue_size":           self.queue_size,
            "fund_queue_size":      self.fund_queue_size,
            "current_stock":        self.current_stock,
            "last_fetch_at":        self.last_fetch_at,
            "paused_for_market":    self._within_trading_hours(),
            "paused_until":         self.paused_until,
            "pause_remaining_sec":  pause_remaining_sec,
            "rate_limit_count":     self.rate_limit_count,
            "rebuild_mode":                self.rebuild_mode,
            "rebuild_completed_at":        self.rebuild_completed_at,
            "initial_queue_size":          self.initial_queue_size,
            "backtest_rebuild_mode":       self.backtest_rebuild_mode,
            "backtest_completed_at":       self.backtest_completed_at,
            "backtest_queue_size":         self.backtest_queue_size,
            "backtest_initial_queue_size": self.backtest_initial_queue_size,
            "skip_count":                  len(self._skip_stocks),
            "latest_trading_day":          latest_td,
            "yahoo_bridge_done":           self._yahoo_bridge_date == date.today(),
            "yahoo_bridge_count":          self.yahoo_bridge_count,
            "yahoo_bridge_total":          self.yahoo_bridge_total,
            "yahoo_bridge_batch_done":     self.yahoo_bridge_batch_done,
            "yahoo_bridge_batch_total":    self.yahoo_bridge_batch_total,
            "yahoo_bridge_failed_ids":     list(self.yahoo_bridge_failed_ids),
            "yahoo_bridge_in_progress":    self.yahoo_bridge_in_progress,
            "last_attempt_at":             self.last_attempt_at,
            "last_attempt_result":         self.last_attempt_result,
            "last_attempt_stock":          self.last_attempt_stock,
            "inst_no_update_count":        len(self._inst_no_update),
            "margin_no_update_count":      len(self._margin_no_update),
            "inst_supplementary_total":    self.inst_supplementary_total,
            "inst_supplementary_done":     self.inst_supplementary_done,
            "margin_supplementary_total":  self.margin_supplementary_total,
            "margin_supplementary_done":   self.margin_supplementary_done,
            "supplementary_completed_at":  self.supplementary_completed_at,
        }


# ── 全域單例 ────────────────────────────────────────────────────
_worker: PrefetchWorker | None = None
_worker_lock = threading.Lock()


def get_worker() -> PrefetchWorker:
    global _worker
    if _worker is None:
        with _worker_lock:
            if _worker is None:
                _worker = PrefetchWorker()
    return _worker


# ── 獨立執行入口 ────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from db.database import init_db
    init_db()

    worker = get_worker()
    worker.start()
    print("背景預抓取已啟動，Ctrl+C 停止")
    try:
        while True:
            s = worker.status()
            pause_str = f"  ⏸ 429暫停剩 {s['pause_remaining_sec']//60}分{s['pause_remaining_sec']%60}秒" \
                        if s["pause_remaining_sec"] > 0 else ""
            print(
                f"\r本小時 {s['hour_fetched']}/{s['hourly_limit']}  "
                f"累計 {s['total_fetched']}  "
                f"待抓 {s['queue_size']}  "
                f"目前：{s['current_stock'] or '閒置'}{pause_str}",
                end="", flush=True,
            )
            time.sleep(2)
    except KeyboardInterrupt:
        worker.stop()
        print("\n已停止")
