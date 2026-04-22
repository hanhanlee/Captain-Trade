from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, UniqueConstraint, Boolean, Index
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class Portfolio(Base):
    """持股清單"""
    __tablename__ = "portfolio"

    id = Column(Integer, primary_key=True)
    stock_id = Column(String(10), nullable=False)
    stock_name = Column(String(50))
    shares = Column(Integer, nullable=False)       # 持有股數（支援零股）
    cost_price = Column(Float, nullable=False)     # 成本價
    buy_date = Column(Date)
    stop_loss = Column(Float)                      # 停損價
    take_profit = Column(Float)                    # 停利價
    note = Column(Text)                            # 舊版欄位，保留相容
    notes = Column(Text)                           # 新版標準欄位
    intraday_monitor = Column(Boolean, default=False)  # 是否啟用盤中監控
    created_at = Column(DateTime, default=datetime.now)


class TradeJournal(Base):
    """交易日誌"""
    __tablename__ = "trade_journal"

    id = Column(Integer, primary_key=True)
    stock_id = Column(String(10), nullable=False)
    stock_name = Column(String(50))
    action = Column(String(4), nullable=False)     # BUY / SELL
    price = Column(Float, nullable=False)
    shares = Column(Integer, nullable=False)
    trade_date = Column(Date)
    reason = Column(Text)                          # 進出場理由
    emotion = Column(String(20))                   # 情緒標記
    pnl = Column(Float)                            # 損益（賣出時計算）
    created_at = Column(DateTime, default=datetime.now)


class ScanResult(Base):
    """選股雷達掃描結果（快取）"""
    __tablename__ = "scan_result"

    id = Column(Integer, primary_key=True)
    scan_date = Column(Date, nullable=False)
    stock_id = Column(String(10), nullable=False)
    stock_name = Column(String(50))
    close = Column(Float)
    change_pct = Column(Float)
    volume_ratio = Column(Float)
    score = Column(Float)
    signals = Column(Text)                         # JSON 字串，記錄觸發的條件
    created_at = Column(DateTime, default=datetime.now)


class ScanSession(Base):
    """選股雷達掃描歷史紀錄"""
    __tablename__ = "scan_session"

    id = Column(Integer, primary_key=True)
    scanned_at = Column(DateTime, default=datetime.now, nullable=False)
    scan_mode = Column(String(50))           # 快速/小型/全市場
    min_price = Column(Float)
    vol_filter = Column(String(100))         # 量能過濾描述
    sector_filter = Column(Text)             # 產業過濾（選中的產業＋漲幅）
    require_weekly = Column(Boolean, default=False)
    min_rs = Column(Float, default=0)
    include_institutional = Column(Boolean, default=False)
    result_count = Column(Integer, default=0)
    results_json = Column(Text)              # JSON：完整結果列表
    top_sectors_json = Column(Text)          # JSON：產業漲幅排行


class PriceCache(Base):
    """本機歷史價格快取（供回測與掃描使用）"""
    __tablename__ = "price_cache"
    __table_args__ = (
        UniqueConstraint("stock_id", "date", name="uq_stock_date"),
        # 複合索引：幾乎所有查詢都同時用 stock_id + date 過濾
        Index("idx_price_stock_date", "stock_id", "date"),
    )

    id = Column(Integer, primary_key=True)
    stock_id = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    updated_at = Column(DateTime, default=datetime.now)


class StockInfoCache(Base):
    """股票基本資料快取（股票清單，避免每次掃描都呼叫 API）"""
    __tablename__ = "stock_info_cache"

    stock_id = Column(String(10), primary_key=True)
    stock_name = Column(String(50))
    industry_category = Column(String(50))
    updated_at = Column(DateTime, default=datetime.now)


class FundamentalCache(Base):
    """基本面財務指標快取（季頻資料，90 天 TTL）"""
    __tablename__ = "fundamental_cache"

    stock_id             = Column(String(10), primary_key=True)
    eps_ttm              = Column(Float)   # 近 4 季 EPS 合計
    roe                  = Column(Float)   # 近 4 季 ROE (%)
    operating_cf         = Column(Float)   # 近 4 季營業現金流合計（千元）
    debt_ratio           = Column(Float)   # 最新負債比 (%)
    gross_margin_latest  = Column(Float)   # 最新季毛利率 (%)
    gross_margin_yoy     = Column(Float)   # 毛利率 YoY 變化（百分點）
    data_date            = Column(String(10))  # 最新財報基準日
    fetched_at           = Column(DateTime, default=datetime.now)


class AppSettings(Base):
    """應用程式設定（key-value 持久化儲存）"""
    __tablename__ = "app_settings"

    key = Column(String(50), primary_key=True)
    value = Column(Text, nullable=False)
    updated_at = Column(DateTime, default=datetime.now)


class LineSubscriber(Base):
    """LINE 推播訂閱者清單"""
    __tablename__ = "line_subscribers"

    id           = Column(Integer, primary_key=True)
    user_id      = Column(String(50), unique=True, nullable=False)  # LINE User ID (Uxxxx...)
    display_name = Column(String(100), default="")                  # 顯示名稱（方便辨識）
    enabled      = Column(Boolean, default=True)                    # False = 暫停群播
    created_at   = Column(DateTime, default=datetime.now)


class PriceFetchStatus(Base):
    """每檔股票的價格抓取狀態（用於死股追蹤與重試管控）"""
    __tablename__ = "price_fetch_status"

    stock_id        = Column(String(10), primary_key=True)
    status          = Column(String(20), default="unknown")  # ok | no_update | error
    last_attempt_at = Column(DateTime)
    updated_at      = Column(DateTime, default=datetime.now)


class MarginCache(Base):
    """融資融券本機快取（累積式，保留最近 400 天）"""
    __tablename__ = "margin_cache"
    __table_args__ = (
        UniqueConstraint("stock_id", "date", name="uq_margin_stock_date"),
        Index("idx_margin_stock_date", "stock_id", "date"),
    )

    id             = Column(Integer, primary_key=True)
    stock_id       = Column(String(10), nullable=False)
    date           = Column(String(10), nullable=False)   # YYYY-MM-DD
    margin_buy     = Column(Integer)   # MarginPurchaseBuy（融資買進）
    margin_sell    = Column(Integer)   # MarginPurchaseSell（融資賣出）
    margin_balance = Column(Integer)   # MarginPurchaseTodayBalance（融資餘額）
    short_buy      = Column(Integer)   # ShortSaleBuy（融券買進）
    short_sell     = Column(Integer)   # ShortSaleSell（融券賣出）
    short_balance  = Column(Integer)   # ShortSaleTodayBalance（融券餘額）
    fetch_at       = Column(String(30))  # ISO timestamp


class FinmindFetchTiming(Base):
    """FinMind 各資料類型每次批次抓取的時間點與筆數記錄（分析發布規律用）"""
    __tablename__ = "finmind_fetch_timing"
    __table_args__ = (
        Index("idx_fft_type_date", "data_type", "trading_date"),
    )

    id           = Column(Integer, primary_key=True)
    trading_date = Column(String(10), nullable=False)  # 交易日 'YYYY-MM-DD'
    data_type    = Column(String(10), nullable=False)  # 'inst' | 'margin' | 'price'
    fetch_at     = Column(String(30), nullable=False)  # ISO datetime，本次抓取時間點
    stock_count  = Column(Integer,    nullable=False)  # 本次 active 股票中有資料的數量
    active_total = Column(Integer,    nullable=False)  # active 股票總數（分母）


class InstCache(Base):
    """三大法人買賣超本機快取（每日一次，避免重複 API 請求）"""
    __tablename__ = "inst_cache"
    __table_args__ = (
        UniqueConstraint("stock_id", "date", "name", name="uq_inst_stock_date_name"),
    )

    id = Column(Integer, primary_key=True)
    stock_id = Column(String(10), nullable=False)
    date = Column(String(10), nullable=False)   # ISO date string YYYY-MM-DD
    name = Column(String(50), nullable=False)   # Foreign_Investor, Investment_Trust, ...
    buy = Column(Float, default=0.0)
    sell = Column(Float, default=0.0)
    net = Column(Float, default=0.0)
    fetched_at = Column(DateTime, default=datetime.now)


class CacheHealthRun(Base):
    """快取健康度分析任務。"""
    __tablename__ = "cache_health_run"
    __table_args__ = (
        Index("idx_cache_health_run_dataset_requested", "dataset", "requested_at"),
    )

    id = Column(Integer, primary_key=True)
    dataset = Column(String(50), nullable=False)
    date_from = Column(String(10), nullable=False)
    date_to = Column(String(10), nullable=False)
    requested_at = Column(DateTime, default=datetime.now, nullable=False)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    status = Column(String(20), default="queued", nullable=False)
    requested_by = Column(String(50), default="streamlit")
    scan_scope = Column(String(50), default="active_stocks")
    total_expected_units = Column(Integer, default=0)
    total_present_units = Column(Integer, default=0)
    total_missing_units = Column(Integer, default=0)
    completeness_pct = Column(Float, default=0.0)
    earliest_cached_date = Column(String(10))
    latest_cached_date = Column(String(10))
    notes = Column(Text)
    error_message = Column(Text)


class CacheHealthDailySummary(Base):
    """健康度分析每日彙總。"""
    __tablename__ = "cache_health_daily_summary"
    __table_args__ = (
        UniqueConstraint("run_id", "trade_date", name="uq_cache_health_daily_run_date"),
        Index("idx_cache_health_daily_run_date", "run_id", "trade_date"),
    )

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, nullable=False)
    dataset = Column(String(50), nullable=False)
    trade_date = Column(String(10), nullable=False)
    expected_count = Column(Integer, default=0)
    present_count = Column(Integer, default=0)
    missing_count = Column(Integer, default=0)
    completeness_pct = Column(Float, default=0.0)


class CacheHealthGap(Base):
    """健康度分析缺漏明細。"""
    __tablename__ = "cache_health_gap"
    __table_args__ = (
        UniqueConstraint("run_id", "dataset", "trade_date", "stock_id", name="uq_cache_health_gap_unit"),
        Index("idx_cache_health_gap_run_date", "run_id", "trade_date"),
        Index("idx_cache_health_gap_dataset_status", "dataset", "repair_status"),
    )

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, nullable=False)
    dataset = Column(String(50), nullable=False)
    trade_date = Column(String(10), nullable=False)
    stock_id = Column(String(10), nullable=False)
    gap_type = Column(String(20), default="missing")
    severity = Column(String(20), default="normal")
    detail_json = Column(Text)
    repair_status = Column(String(20), default="pending")
    repaired_at = Column(DateTime)
    repair_error = Column(Text)


class CacheHealthRepairJob(Base):
    """健康度缺漏補抓任務。"""
    __tablename__ = "cache_health_repair_job"
    __table_args__ = (
        Index("idx_cache_health_repair_run_requested", "run_id", "requested_at"),
    )

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, nullable=False)
    dataset = Column(String(50), nullable=False)
    status = Column(String(20), default="queued", nullable=False)
    requested_at = Column(DateTime, default=datetime.now, nullable=False)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    target_count = Column(Integer, default=0)
    done_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_error = Column(Text)
