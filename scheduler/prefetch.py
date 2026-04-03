"""
背景資料預抓取工作器

策略：
  - FinMind 免費帳號每日限額 600 次
  - 背景預算上限 500 次/日（保留 100 次給手動掃描）
  - 抓取間隔 6 秒（避免短時間爆量）
  - 優先順序：① 完全無快取 → ② 快取 > 5 天舊 → ③ 其餘
  - 只在非交易時間運行（15:05–08:55），保持盤中速度

獨立執行：
  python -m scheduler.prefetch
"""
import time
import threading
import logging
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

# ── 可調整參數 ─────────────────────────────────────────────────
DAILY_BUDGET = 500          # 每日背景最多消耗幾次 API（留 100 給手動）
FETCH_INTERVAL_SEC = 6      # 每次請求間隔（秒）
BATCH_SIZE = 50             # 每輪最多抓幾檔後暫停，讓手動操作有空間
STALE_DAYS = 5              # 快取幾天未更新視為過期


class PrefetchWorker:
    """
    背景資料預抓取工作器（單例，透過 get_worker() 取得）

    狀態屬性：
        running (bool)         — 背景執行緒是否活著
        today_fetched (int)    — 今日已消耗的 API 次數
        last_fetch_at          — 最近一次抓取時間
        current_stock (str)    — 正在抓取的股票代碼
        queue_size (int)       — 待抓取數量（快照，非即時）
    """

    def __init__(self):
        self.running = False
        self.today_fetched: int = 0
        self._budget_date: date = date.today()
        self.last_fetch_at: datetime | None = None
        self.current_stock: str = ""
        self.queue_size: int = 0
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ── 公開控制 ───────────────────────────────────────────────

    def start(self):
        """啟動背景執行緒（已在跑則無操作）"""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="prefetch-worker")
        self._thread.start()
        self.running = True
        logger.info("PrefetchWorker 背景執行緒已啟動")

    def stop(self):
        """要求背景執行緒停止"""
        self._stop_event.set()
        self.running = False
        logger.info("PrefetchWorker 已要求停止")

    def reset_budget(self):
        """手動重置今日用量計數（換日時自動呼叫，或手動測試用）"""
        self.today_fetched = 0
        self._budget_date = date.today()

    # ── 私有方法 ───────────────────────────────────────────────

    def _check_budget_reset(self):
        """若已跨日，重置計數器"""
        today = date.today()
        if today != self._budget_date:
            self.today_fetched = 0
            self._budget_date = today

    def _within_trading_hours(self) -> bool:
        """是否在交易時間（09:00–15:05），這段時間暫停背景抓取"""
        now = datetime.now().time()
        from datetime import time as _time
        market_open  = _time(9, 0)
        market_close = _time(15, 5)
        return market_open <= now <= market_close

    def _get_stale_stocks(self) -> list[str]:
        """
        回傳需要更新的股票代碼清單，按優先順序排序：
          1. price_cache 完全沒有的
          2. 最新日期 > STALE_DAYS 天前的
        """
        try:
            from data.finmind_client import get_stock_list
            from db.price_cache import get_cache_summary

            all_stocks_df = get_stock_list()
            if all_stocks_df.empty:
                return []
            all_ids = set(all_stocks_df["stock_id"].tolist())

            summary = get_cache_summary()
            cutoff = (date.today() - timedelta(days=STALE_DAYS)).isoformat()

            if summary.empty:
                cached_ids = set()
                stale_ids = set()
            else:
                cached_ids = set(summary["stock_id"].tolist())
                stale_ids = set(
                    summary.loc[summary["latest"] < cutoff, "stock_id"].tolist()
                )

            missing = sorted(all_ids - cached_ids)               # 完全沒有快取
            stale   = sorted(stale_ids - set(missing))           # 有快取但過期
            fresh   = sorted(all_ids - set(missing) - stale_ids) # 還算新鮮

            return missing + stale + fresh
        except Exception as e:
            logger.warning(f"取得待抓清單失敗：{e}")
            return []

    def _fetch_one(self, stock_id: str) -> bool:
        """抓取單一股票並存快取，回傳是否成功消耗了一次 API"""
        try:
            from db.price_cache import get_cached_dates
            from data.finmind_client import get_daily_price, smart_get_price

            _min, _max = get_cached_dates(stock_id)
            if _max is not None:
                max_date = _max if isinstance(_max, date) else \
                    datetime.strptime(str(_max), "%Y-%m-%d").date()
                if (date.today() - max_date).days <= STALE_DAYS:
                    return False  # 快取還新鮮，不消耗額度

            # 只抓缺失區間
            smart_get_price(stock_id, required_days=150)
            return True
        except Exception as e:
            logger.debug(f"抓取 {stock_id} 失敗：{e}")
            return False

    def _run_loop(self):
        """背景主迴圈"""
        logger.info("PrefetchWorker 迴圈開始")
        while not self._stop_event.is_set():
            self._check_budget_reset()

            # 交易時間暫停
            if self._within_trading_hours():
                self._stop_event.wait(60)   # 每分鐘確認一次
                continue

            # 今日預算用完，等到明天
            if self.today_fetched >= DAILY_BUDGET:
                seconds_to_midnight = (
                    datetime.combine(date.today() + timedelta(days=1), datetime.min.time())
                    - datetime.now()
                ).seconds
                logger.info(f"今日預算已用完（{self.today_fetched}/{DAILY_BUDGET}），等待換日")
                self._stop_event.wait(min(seconds_to_midnight + 5, 3600))
                continue

            # 取得待抓清單
            queue = self._get_stale_stocks()
            self.queue_size = len(queue)

            if not queue:
                logger.info("所有股票快取皆為最新，等待 30 分鐘後再確認")
                self._stop_event.wait(1800)
                continue

            # 依批次抓取
            batch = queue[:BATCH_SIZE]
            for stock_id in batch:
                if self._stop_event.is_set():
                    break
                if self.today_fetched >= DAILY_BUDGET:
                    break

                self.current_stock = stock_id
                consumed = self._fetch_one(stock_id)
                if consumed:
                    self.today_fetched += 1
                    self.last_fetch_at = datetime.now()
                    logger.debug(f"已抓 {stock_id}，今日 {self.today_fetched}/{DAILY_BUDGET}")
                    self._stop_event.wait(FETCH_INTERVAL_SEC)
                else:
                    # 快取命中，不需等待
                    pass

            self.current_stock = ""
            # 每批結束後短暫讓出資源
            self._stop_event.wait(10)

        self.running = False
        logger.info("PrefetchWorker 迴圈結束")

    def status(self) -> dict:
        """回傳目前狀態摘要（供 UI 顯示）"""
        self._check_budget_reset()
        return {
            "running": self.running and (self._thread is not None and self._thread.is_alive()),
            "today_fetched": self.today_fetched,
            "daily_budget": DAILY_BUDGET,
            "budget_remaining": DAILY_BUDGET - self.today_fetched,
            "queue_size": self.queue_size,
            "current_stock": self.current_stock,
            "last_fetch_at": self.last_fetch_at,
            "paused_for_market": self._within_trading_hours(),
        }


# ── 全域單例 ────────────────────────────────────────────────────
_worker: PrefetchWorker | None = None
_worker_lock = threading.Lock()


def get_worker() -> PrefetchWorker:
    """取得全域 PrefetchWorker 單例"""
    global _worker
    if _worker is None:
        with _worker_lock:
            if _worker is None:
                _worker = PrefetchWorker()
    return _worker


# ── 獨立執行入口 ────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from db.database import init_db
    init_db()

    worker = get_worker()
    worker.start()
    print("背景預抓取已啟動，Ctrl+C 停止")
    try:
        while True:
            s = worker.status()
            print(f"\r今日已抓 {s['today_fetched']}/{s['daily_budget']}  "
                  f"待抓 {s['queue_size']}  "
                  f"目前：{s['current_stock'] or '閒置'}", end="", flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        worker.stop()
        print("\n已停止")
