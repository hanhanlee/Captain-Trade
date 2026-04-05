"""
背景資料預抓取工作器

FinMind 免費帳號限制：每小時 600 次（註冊會員）

策略：
  - 交易時間（09:00–15:05）：每小時上限 100 次，保留 500 次給手動操作
  - 非交易時間：每小時上限 500 次，快速填滿全市場快取
  - 每 7 秒抓一檔（≈ 514 次/小時，略低於上限）
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
HOURLY_LIMIT_OFFPEAK      = 500   # 非交易時間每小時上限
HOURLY_LIMIT_TRADING      = 100   # 交易時間每小時上限
FETCH_INTERVAL_SEC        = 6.1   # 正常模式每次請求間隔（秒）≈ 590 次/小時
FETCH_INTERVAL_REBUILD    = 2.0   # 重建模式間隔（秒）；讓 429 來當剎車
STALE_DAYS                = 5     # 快取幾天未更新視為過期
RATE_LIMIT_PAUSE_MIN      = 20    # 遇到 429 後暫停幾分鐘（一般模式）
PREFETCH_DAYS             = 400   # 一般預抓天數（涵蓋回測需求：365天 + 60天指標暖身）
BACKTEST_PREFETCH_YEARS   = 10    # 回測資料重建：往前幾年
BACKTEST_PREFETCH_DAYS    = BACKTEST_PREFETCH_YEARS * 365  # ≈ 3650 天


def _is_429(exc: Exception) -> bool:
    """判斷例外是否為 429 限額"""
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg or "rate limit" in msg


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
        self._resume_event = threading.Event()
        self._stop_event   = threading.Event()
        self._thread: threading.Thread | None = None

    # ── 公開控制 ───────────────────────────────────────────────

    def start(self):
        """啟動背景執行緒（已在跑則無操作）"""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
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
        self.running = False
        logger.info("PrefetchWorker 已要求停止")

    def resume(self):
        """手動提前恢復（清除 429 暫停）"""
        self.paused_until = None
        self._resume_event.set()
        logger.info("PrefetchWorker 手動恢復")

    def enable_rebuild_mode(self):
        """啟用全速重建模式：額度開放至 600 次/小時，不受交易時間限制"""
        self.rebuild_mode = True
        logger.info("PrefetchWorker 進入全速重建模式（600次/小時）")

    def disable_rebuild_mode(self):
        """停止重建模式，恢復正常限速"""
        self.rebuild_mode = False
        logger.info("PrefetchWorker 退出重建模式，恢復正常限速")

    def enable_backtest_rebuild_mode(self):
        """啟用回測歷史資料重建模式：補充全市場最多 10 年的深度歷史資料"""
        self.backtest_rebuild_mode = True
        self.backtest_completed_at = None
        self.backtest_initial_queue_size = 0
        logger.info(f"PrefetchWorker 進入回測重建模式（{BACKTEST_PREFETCH_YEARS} 年歷史）")

    def disable_backtest_rebuild_mode(self):
        """停止回測重建模式"""
        self.backtest_rebuild_mode = False
        logger.info("PrefetchWorker 退出回測重建模式")

    # ── 私有方法 ───────────────────────────────────────────────

    def _within_trading_hours(self) -> bool:
        if self.rebuild_mode or self.backtest_rebuild_mode:
            return False   # 重建模式：不受交易時間限制
        now = datetime.now().time()
        from datetime import time as _time
        return _time(9, 0) <= now <= _time(15, 5)

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
            from db.price_cache import get_cache_summary

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
          needs_update — 真正需要更新的（無快取 + 快取過期），用於 queue_size 顯示
          full_queue   — needs_update + 新鮮的（定期巡迴），用於實際抓取迴圈
        """
        try:
            from data.finmind_client import get_stock_list
            from db.price_cache import get_cache_summary

            all_stocks_df = get_stock_list()
            if all_stocks_df.empty:
                return [], []
            all_ids = set(all_stocks_df["stock_id"].tolist())

            summary = get_cache_summary()
            cutoff = (date.today() - timedelta(days=STALE_DAYS)).isoformat()

            if summary.empty:
                cached_ids = set()
                stale_ids  = set()
            else:
                cached_ids = set(summary["stock_id"].tolist())
                stale_ids  = set(
                    summary.loc[summary["latest"] < cutoff, "stock_id"].tolist()
                )

            missing = sorted(all_ids - cached_ids - self._skip_stocks)
            stale   = sorted(stale_ids - set(missing) - self._skip_stocks)
            fresh   = sorted(all_ids - set(missing) - stale_ids - self._skip_stocks)

            needs_update = missing + stale
            full_queue   = missing + stale + fresh
            return needs_update, full_queue
        except Exception as e:
            logger.warning(f"取得待抓清單失敗：{e}")
            return [], []

    # 回傳值：
    #   'ok'         — 成功抓取並更新快取
    #   'cached'     — 快取仍新鮮，跳過
    #   'no_update'  — 呼叫了 API 但快取沒有更新（無資料 / 無新資料），加入永久跳過名單
    #   'rate_limit' — 429
    #   'error'      — 其他例外
    def _fetch_one(self, stock_id: str) -> str:
        try:
            from db.price_cache import get_cached_dates
            from data.finmind_client import smart_get_price

            # 記錄呼叫前的最新日期
            _, max_before = get_cached_dates(stock_id)

            if max_before is not None:
                max_date = max_before if isinstance(max_before, date) else \
                    datetime.strptime(str(max_before), "%Y-%m-%d").date()
                if (date.today() - max_date).days <= STALE_DAYS:
                    return "cached"

            smart_get_price(stock_id, required_days=PREFETCH_DAYS)

            # 驗證快取是否真的有更新
            _, max_after = get_cached_dates(stock_id)
            if max_after is None or max_after == max_before:
                # API 呼叫完成但沒有任何新資料存入（權證、下市股、無資料等）
                logger.debug(f"{stock_id} 無新資料可存，加入跳過名單")
                return "no_update"

            return "ok"
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
                    self._stop_event.wait(60)
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
                    if result == "ok":
                        self._record_request()
                        self.last_fetch_at = datetime.now()
                        self.backtest_queue_size = max(0, self.backtest_queue_size - 1)
                        logger.debug(
                            f"回測歷史已抓 {stock_id}，本小時 {self.hour_fetched}，"
                            f"剩餘 {self.backtest_queue_size}"
                        )
                        self._stop_event.wait(FETCH_INTERVAL_REBUILD)
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
                    self._stop_event.wait(5)
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

            if not full_queue:
                # 價格快取皆為最新，改為嘗試填充基本面快取
                fund_ids = self._get_funds_needing_fetch()
                self.fund_queue_size = len(fund_ids)
                if not fund_ids:
                    logger.info("所有快取（價格+基本面）皆為最新，等待 30 分鐘")
                    self._stop_event.wait(1800)
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
                        if result == "ok":
                            self._record_request()
                            self.last_fetch_at = datetime.now()
                            self.fund_queue_size = max(0, self.fund_queue_size - 1)
                            logger.debug(f"基本面已抓 {stock_id}，本小時 {self.hour_fetched}，剩餘 {self.fund_queue_size}")
                            interval = FETCH_INTERVAL_REBUILD if self.rebuild_mode else FETCH_INTERVAL_SEC
                            self._stop_event.wait(interval)
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

                if result == "ok":
                    self._record_request()
                    self.last_fetch_at = datetime.now()
                    # 若該股原本在待更新清單，完成後遞減計數
                    if stock_id in needs_update_set:
                        self.queue_size = max(0, self.queue_size - 1)
                    logger.debug(f"已抓 {stock_id}，本小時 {self.hour_fetched}，待更新 {self.queue_size}")
                    interval = FETCH_INTERVAL_REBUILD if self.rebuild_mode else FETCH_INTERVAL_SEC
                    self._stop_event.wait(interval)

                elif result == "no_update":
                    # 無資料可存，加入永久跳過名單，不再浪費 API 額度
                    self._skip_stocks.add(stock_id)
                    if stock_id in needs_update_set:
                        self.queue_size = max(0, self.queue_size - 1)

                elif result == "rate_limit":
                    hit_rate_limit = True
                    break

                # "cached" 或 "error" 直接繼續，不等待

            self.current_stock = ""

            if hit_rate_limit and not self._stop_event.is_set():
                self._pause_for_rate_limit()
            else:
                self._stop_event.wait(5)

        self.running = False
        logger.info("PrefetchWorker 迴圈結束")

    def status(self) -> dict:
        used  = self._hour_count()
        limit = self._current_hourly_limit()
        now   = datetime.now()

        pause_remaining_sec = 0
        if self.paused_until and self.paused_until > now:
            pause_remaining_sec = int((self.paused_until - now).total_seconds())

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
