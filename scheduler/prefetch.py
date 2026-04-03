"""
背景資料預抓取工作器

FinMind 免費帳號限制：每小時 600 次（註冊會員）

策略：
  - 交易時間（09:00–15:05）：每小時上限 100 次，保留 500 次給手動操作
  - 非交易時間：每小時上限 500 次，快速填滿全市場快取
  - 每 7 秒抓一檔（≈ 514 次/小時，略低於上限）
  - 優先順序：① 完全無快取 → ② 快取 > 5 天舊 → ③ 其餘

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
HOURLY_LIMIT_OFFPEAK  = 500   # 非交易時間每小時上限
HOURLY_LIMIT_TRADING  = 100   # 交易時間每小時上限
FETCH_INTERVAL_SEC    = 7     # 每次請求最短間隔（秒）≈ 514 次/小時
STALE_DAYS            = 5     # 快取幾天未更新視為過期


class PrefetchWorker:
    """
    背景資料預抓取工作器（單例，透過 get_worker() 取得）

    狀態屬性：
        running (bool)           — 背景執行緒是否活著
        hour_fetched (int)       — 本小時已消耗次數
        total_fetched (int)      — 本次啟動累計次數
        last_fetch_at            — 最近一次抓取時間
        current_stock (str)      — 正在抓取的股票代碼
        queue_size (int)         — 待抓取數量（快照）
        paused_for_market (bool) — 是否因交易時間降速
    """

    def __init__(self):
        self.running = False
        self.hour_fetched: int = 0
        self.total_fetched: int = 0
        self._hour_window: deque = deque()   # 儲存最近 1 小時內的請求時間戳
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
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="prefetch-worker"
        )
        self._thread.start()
        self.running = True
        logger.info("PrefetchWorker 背景執行緒已啟動")

    def stop(self):
        """要求背景執行緒停止"""
        self._stop_event.set()
        self.running = False
        logger.info("PrefetchWorker 已要求停止")

    # ── 私有方法 ───────────────────────────────────────────────

    def _within_trading_hours(self) -> bool:
        """是否在交易時間（09:00–15:05）"""
        now = datetime.now().time()
        from datetime import time as _time
        return _time(9, 0) <= now <= _time(15, 5)

    def _current_hourly_limit(self) -> int:
        return HOURLY_LIMIT_TRADING if self._within_trading_hours() else HOURLY_LIMIT_OFFPEAK

    def _hour_count(self) -> int:
        """滑動視窗：計算最近 60 分鐘內的請求數"""
        cutoff = datetime.now() - timedelta(hours=1)
        while self._hour_window and self._hour_window[0] < cutoff:
            self._hour_window.popleft()
        return len(self._hour_window)

    def _record_request(self):
        self._hour_window.append(datetime.now())
        self.hour_fetched = len(self._hour_window)
        self.total_fetched += 1

    def _get_stale_stocks(self) -> list[str]:
        """
        回傳需要更新的股票代碼清單，按優先順序排序：
          1. price_cache 完全沒有的（新股或從未抓過）
          2. 最新快取日期 > STALE_DAYS 天前
          3. 其餘（定期更新）
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
                stale_ids  = set()
            else:
                cached_ids = set(summary["stock_id"].tolist())
                stale_ids  = set(
                    summary.loc[summary["latest"] < cutoff, "stock_id"].tolist()
                )

            missing = sorted(all_ids - cached_ids)
            stale   = sorted(stale_ids - set(missing))
            # 新鮮的也放進去，讓定期巡迴更新
            fresh   = sorted(all_ids - set(missing) - stale_ids)

            return missing + stale + fresh
        except Exception as e:
            logger.warning(f"取得待抓清單失敗：{e}")
            return []

    def _fetch_one(self, stock_id: str) -> bool:
        """
        抓取單一股票並存快取。
        回傳 True = 實際呼叫了 API；False = 快取仍新鮮，跳過
        """
        try:
            from db.price_cache import get_cached_dates
            from data.finmind_client import smart_get_price

            _min, _max = get_cached_dates(stock_id)
            if _max is not None:
                max_date = _max if isinstance(_max, date) else \
                    datetime.strptime(str(_max), "%Y-%m-%d").date()
                if (date.today() - max_date).days <= STALE_DAYS:
                    return False  # 快取還新鮮，跳過

            smart_get_price(stock_id, required_days=150)
            return True
        except Exception as e:
            logger.debug(f"抓取 {stock_id} 失敗：{e}")
            return False

    def _run_loop(self):
        """背景主迴圈"""
        logger.info("PrefetchWorker 迴圈開始")
        while not self._stop_event.is_set():

            # 檢查小時配額
            used = self._hour_count()
            limit = self._current_hourly_limit()
            if used >= limit:
                wait_sec = 60  # 等 1 分鐘後再檢查滑動視窗
                logger.debug(f"本小時配額已用 {used}/{limit}，等待 {wait_sec}s")
                self._stop_event.wait(wait_sec)
                continue

            # 取得待抓清單
            queue = self._get_stale_stocks()
            self.queue_size = len(queue)

            if not queue:
                logger.info("所有股票快取皆為最新，等待 30 分鐘")
                self._stop_event.wait(1800)
                continue

            # 逐檔抓取
            for stock_id in queue:
                if self._stop_event.is_set():
                    break

                # 再次確認配額（滑動視窗，每輪都檢查）
                if self._hour_count() >= self._current_hourly_limit():
                    break

                self.current_stock = stock_id
                consumed = self._fetch_one(stock_id)
                if consumed:
                    self._record_request()
                    self.last_fetch_at = datetime.now()
                    logger.debug(
                        f"已抓 {stock_id}，本小時 {self.hour_fetched}/{limit}"
                    )
                    self._stop_event.wait(FETCH_INTERVAL_SEC)

            self.current_stock = ""
            self._stop_event.wait(5)

        self.running = False
        logger.info("PrefetchWorker 迴圈結束")

    def status(self) -> dict:
        """回傳目前狀態摘要（供 UI 顯示）"""
        used  = self._hour_count()
        limit = self._current_hourly_limit()
        return {
            "running":           self.running and bool(self._thread and self._thread.is_alive()),
            "hour_fetched":      used,
            "hourly_limit":      limit,
            "hourly_remaining":  max(limit - used, 0),
            "total_fetched":     self.total_fetched,
            "queue_size":        self.queue_size,
            "current_stock":     self.current_stock,
            "last_fetch_at":     self.last_fetch_at,
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from db.database import init_db
    init_db()

    worker = get_worker()
    worker.start()
    print("背景預抓取已啟動，Ctrl+C 停止")
    try:
        while True:
            s = worker.status()
            print(
                f"\r本小時 {s['hour_fetched']}/{s['hourly_limit']}  "
                f"累計 {s['total_fetched']}  "
                f"待抓 {s['queue_size']}  "
                f"目前：{s['current_stock'] or '閒置'}",
                end="", flush=True,
            )
            time.sleep(2)
    except KeyboardInterrupt:
        worker.stop()
        print("\n已停止")
