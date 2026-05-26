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


class TelegramChatMember(Base):
    """Telegram 群組成員自動記錄（有發言或加入即寫入）"""
    __tablename__ = "telegram_chat_members"
    __table_args__ = (
        UniqueConstraint("chat_id", "user_id", name="uq_tg_chat_user"),
    )

    id        = Column(Integer, primary_key=True)
    chat_id   = Column(String(30), nullable=False)   # 群組 chat_id
    user_id   = Column(String(30), nullable=False)   # Telegram user_id
    username  = Column(String(100), default="")      # @username（無則空）
    full_name = Column(String(200), default="")      # first_name + last_name
    first_seen_at = Column(DateTime, default=datetime.now)  # 首次紀錄
    last_seen_at  = Column(DateTime, default=datetime.now)  # 最近一次活動
    joined_via    = Column(String(20), default="message")   # "message" | "join"


class EtfHoldingCache(Base):
    """ETF 成分股持股快取（月度重新平衡快照）"""
    __tablename__ = "etf_holding_cache"
    __table_args__ = (
        UniqueConstraint("etf_id", "date", "hold_stock_id", name="uq_etf_holding"),
        Index("idx_etf_holding_etf_date", "etf_id", "date"),
    )

    id            = Column(Integer, primary_key=True)
    etf_id        = Column(String(10), nullable=False)
    date          = Column(String(10), nullable=False)   # YYYY-MM-DD，本次快照日期
    hold_stock_id = Column(String(10), nullable=False)
    hold_stock_name = Column(String(50), default="")
    percentage    = Column(Float, default=0.0)           # 持股權重 %
    shares        = Column(Integer, default=0)           # 持股股數（有資料的 ETF 才非零）
    fetched_at    = Column(DateTime, default=datetime.now)


class TradePlan(Base):
    """交易計畫（進場前制定，含風控自動檢查結果）"""
    __tablename__ = "trade_plan"

    id              = Column(Integer, primary_key=True)
    stock_id        = Column(String(10), nullable=False)
    stock_name      = Column(String(50), default="")
    direction       = Column(String(4), nullable=False)   # BUY / SELL
    entry_price     = Column(Float, nullable=False)
    stop_loss       = Column(Float, nullable=False)
    target_price    = Column(Float)                       # 選填
    shares          = Column(Integer, nullable=False)
    reason          = Column(Text, nullable=False)        # 必填，進場理由
    status          = Column(String(20), default="pending")  # pending / executed / cancelled
    risk_check_json = Column(Text)                        # JSON snapshot of rule results
    has_violation   = Column(Boolean, default=False)      # 是否有任何規則未通過
    created_at      = Column(DateTime, default=datetime.now)
    executed_at     = Column(DateTime)
    journal_id      = Column(Integer)                     # 對應 TradeJournal.id


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


class EventLog(Base):
    """系統事件日誌 — 記錄選股、警示、風控、交易計畫、推播等決策時間線，供 AI 分析使用。"""
    __tablename__ = "event_log"
    __table_args__ = (
        Index("idx_event_log_created_at", "created_at"),
        Index("idx_event_log_event_type", "event_type"),
        Index("idx_event_log_stock_id", "stock_id"),
        Index("idx_event_log_scan_id", "scan_id"),
    )

    id           = Column(Integer, primary_key=True)
    created_at   = Column(String(30), nullable=False)   # YYYY-MM-DD HH:MM:SS
    event_type   = Column(String(50), nullable=False)   # scan_completed / stock_selected / ...
    module       = Column(String(50))                   # scanner / portfolio / trade_plan / scheduler
    scan_id      = Column(String(50))                   # 同批次掃描的共同 key
    stock_id     = Column(String(10))
    stock_name   = Column(String(50))
    severity     = Column(String(10), default="info")   # info / warning / danger
    summary      = Column(Text)
    payload_json = Column(Text)                         # 詳細 JSON，策略快照放這


class NPatternWatchlist(Base):
    """
    盤中 N 字底候選清單（每日早盤 8:50 重建）

    用途：盤前掃描全市場（指定產業）日 K 找出已成形 A→B→C 的股票，
    盤中只需對名單檢查即時價是否突破 B（D 點），即可推 Telegram 警示。
    """
    __tablename__ = "n_pattern_watchlist"
    __table_args__ = (
        UniqueConstraint("stock_id", "scan_date", name="uq_n_pattern_stock_date"),
        Index("idx_n_pattern_scan_date", "scan_date"),
    )

    id                  = Column(Integer, primary_key=True)
    scan_date           = Column(Date, nullable=False)         # 候選清單建立日期（通常 = 當日）
    stock_id            = Column(String(10), nullable=False)
    stock_name          = Column(String(50))
    industry            = Column(String(50))                   # 產業分類
    a_date              = Column(Date)
    a_price             = Column(Float)
    b_date              = Column(Date)
    b_price             = Column(Float, nullable=False)        # 突破目標價（D > B）
    c_date              = Column(Date)
    c_price             = Column(Float)
    b_rise_pct          = Column(Float)                        # B 對 A 漲幅 %
    c_retrace_pct       = Column(Float)                        # C 從 B 回測 %
    vol_a_to_b_increase = Column(Boolean)                      # A→B 量增？
    vol_b_to_c_decrease = Column(Boolean)                      # B→C 量縮？
    avg_volume_b_to_c   = Column(Float)                        # B→C 平均量（盤中算突破量倍數的基準）
    vol_ma5             = Column(Float)                        # 前 5 日均量（張，盤中流動性 gate 用）
    last_close          = Column(Float)                        # 建表當下最新收盤
    distance_to_b_pct   = Column(Float)                        # 現價距 B 還差 %（負值=已突破）
    alerted_today       = Column(Boolean, default=False)       # 今日是否已推播
    created_at          = Column(DateTime, default=datetime.now)


class NPatternAlertHistory(Base):
    """
    N 字底突破推播歷史——跨日去重用。

    唯一鍵：stock_id + b_date（B 點日期代表同一組突破結構）。
    設計取捨：若偵測器因新 C 點而識別出「同 B date 但不同 C date」的結構，
    該 B 點已在 history 中則仍會被擋掉，不會重複通知。
    寧可在 B 點附近多次盤整時保持安靜，也不重複騷擾。

    purge 建議保留 90 天，確保覆蓋 watchlist builder 的最大回看範圍
    （lookback=80 交易日），讓任何仍可能出現在 watchlist 的 B 點都還在歷史中。
    """
    __tablename__ = "n_pattern_alert_history"
    __table_args__ = (
        UniqueConstraint("stock_id", "b_date", name="uq_n_pattern_alert_b"),
        Index("idx_n_pattern_alert_stock", "stock_id"),
    )

    id         = Column(Integer, primary_key=True)
    stock_id   = Column(String(10), nullable=False)
    b_date     = Column(Date, nullable=False)   # B 點日期（結構識別鍵）
    b_price    = Column(Float)
    a_date     = Column(Date)                   # 記錄完整結構供追蹤
    c_date     = Column(Date)
    alerted_at = Column(DateTime, default=datetime.now)


class V3BreakoutWatchlist(Base):
    """
    盤中 V3 三線齊穿候選清單（每日 08:50 重建）

    條件：昨日收盤在 5/10/20MA 全線以下 + 三線糾結度 < 3%。
    盤中以即時價確認是否同時突破三線，且量能超過前五日均量 1.5 倍，即推 Telegram。
    """
    __tablename__ = "v3_breakout_watchlist"
    __table_args__ = (
        UniqueConstraint("stock_id", "scan_date", name="uq_v3_breakout_stock_date"),
        Index("idx_v3_breakout_scan_date", "scan_date"),
    )

    id            = Column(Integer, primary_key=True)
    scan_date     = Column(Date, nullable=False)
    stock_id      = Column(String(10), nullable=False)
    stock_name    = Column(String(50))
    industry      = Column(String(50))
    ma5           = Column(Float, nullable=False)   # 昨日 MA5（盤中突破閾值）
    ma10          = Column(Float, nullable=False)   # 昨日 MA10
    ma20          = Column(Float, nullable=False)   # 昨日 MA20
    vol_ma5       = Column(Float, nullable=False)   # 前五日均量（盤中量能閾值基準）
    last_close    = Column(Float)                   # 昨日收盤（參考）
    ma_spread_pct = Column(Float)                   # 昨日三線糾結度 %（參考）
    alerted_today = Column(Boolean, default=False)
    created_at    = Column(DateTime, default=datetime.now)


class V5SniperWatchlist(Base):
    """
    盤中 v5 狙擊手版候選清單（每日 08:50 重建）

    builder 在盤後跑 v5 共同 gate 的「靜態部分」+ 型態 A/B 任一命中，把候選寫入。
    盤中 intraday_v5_breakout 比對即時報價：
      - last_price ≥ breakout_target （型態 A=近10日high；型態 B=昨日high）
      - last_price > open（紅K）
      - 推估全日量 ≥ prev_volume × 1.5
      - （選用）漲幅 ≤ max_gain_pct
    四項全過 → 推 Telegram + mark_alerted 防重複。
    實體紅K 60% 不放盤中（會抖），由收盤後驗證。
    """
    __tablename__ = "v5_sniper_watchlist"
    __table_args__ = (
        UniqueConstraint("stock_id", "scan_date", name="uq_v5_sniper_stock_date"),
        Index("idx_v5_sniper_scan_date", "scan_date"),
    )

    id              = Column(Integer, primary_key=True)
    scan_date       = Column(Date, nullable=False)
    stock_id        = Column(String(10), nullable=False)
    stock_name      = Column(String(50))
    industry        = Column(String(50))
    pattern_type    = Column(String(1), nullable=False)  # 'A'=糾結突破 / 'B'=回檔再上
    breakout_target = Column(Float, nullable=False)      # 盤中觸發價（A=近10日high；B=昨日high）
    prev_high       = Column(Float, nullable=False)      # 昨日 high（破昨高的基準）
    prev_close      = Column(Float)                      # 昨日收盤（參考、算漲幅用）
    prev_volume     = Column(Float, nullable=False)      # 昨日成交量（張）；盤中 ×1.5 為量能閾值
    vol_ma5         = Column(Float)                      # 五日均量（參考）
    ma5             = Column(Float)
    ma10            = Column(Float)
    ma20            = Column(Float)
    ma60            = Column(Float)
    ma_spread_pct   = Column(Float)                      # 糾結度 %（型態 A 用；型態 B 可 NULL）
    max_gain_pct    = Column(Float)                      # 漲幅上限 %；NULL = 停用 gate
    alerted_today   = Column(Boolean, default=False)
    entry_path      = Column(String(10), default="morning")  # "morning"=08:50 builder / "late"=盤中補抓
    created_at      = Column(DateTime, default=datetime.now)
