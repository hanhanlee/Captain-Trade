import logging
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from .models import Base
import os

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "srock.db")

ENGINE = create_engine(
    f"sqlite:///{DB_PATH}",
    echo=False,
    connect_args={"check_same_thread": False},
)


@event.listens_for(ENGINE, "connect")
def _set_sqlite_pragmas(dbapi_conn, _):
    """每次新連線時套用 SQLite 效能設定"""
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")       # 允許讀寫並行，寫入更快
    cur.execute("PRAGMA synchronous=NORMAL")      # 比 FULL 快，比 OFF 安全
    cur.execute("PRAGMA cache_size=-32000")       # 32 MB 記憶體快取
    cur.execute("PRAGMA temp_store=MEMORY")       # 暫存表放記憶體
    cur.execute("PRAGMA mmap_size=268435456")     # 256 MB memory-mapped I/O
    cur.execute("PRAGMA busy_timeout=5000")       # 寫入衝突時等待最多 5 秒，避免 "database is locked"
    cur.close()


SessionLocal = sessionmaker(bind=ENGINE)


def init_db():
    Base.metadata.create_all(ENGINE)
    _migrate_schema()


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return any(r[1] == column_name for r in rows)


def _table_exists(conn, table_name: str) -> bool:
    rows = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name=:t"),
        {"t": table_name}
    ).fetchall()
    return len(rows) > 0


def _migrate_schema():
    """輕量 schema migration，確保舊 DB 可相容新版欄位與新表。"""
    with ENGINE.begin() as conn:
        # etf_holding_cache shares 欄位（舊 DB 補建）
        if _table_exists(conn, "etf_holding_cache") and not _column_exists(conn, "etf_holding_cache", "shares"):
            conn.execute(text("ALTER TABLE etf_holding_cache ADD COLUMN shares INTEGER DEFAULT 0"))
            logger.info("migration: etf_holding_cache 新增 shares 欄位")

        # portfolio intraday_monitor 欄位（舊 DB 補建）
        if not _column_exists(conn, "portfolio", "intraday_monitor"):
            conn.execute(text("ALTER TABLE portfolio ADD COLUMN intraday_monitor INTEGER DEFAULT 0"))
            logger.info("migration: portfolio 新增 intraday_monitor 欄位")

        # portfolio notes 欄位相容
        if _column_exists(conn, "portfolio", "note") and not _column_exists(conn, "portfolio", "notes"):
            conn.execute(text("ALTER TABLE portfolio ADD COLUMN notes TEXT"))
            conn.execute(text("UPDATE portfolio SET notes = note WHERE notes IS NULL AND note IS NOT NULL"))
        elif _column_exists(conn, "portfolio", "note") and _column_exists(conn, "portfolio", "notes"):
            conn.execute(text("UPDATE portfolio SET notes = note WHERE (notes IS NULL OR notes = '') AND note IS NOT NULL"))

        # price_fetch_status 表（舊 DB 補建）
        if not _table_exists(conn, "price_fetch_status"):
            conn.execute(text("""
                CREATE TABLE price_fetch_status (
                    stock_id        TEXT PRIMARY KEY,
                    status          TEXT DEFAULT 'unknown',
                    last_attempt_at TEXT,
                    updated_at      TEXT
                )
            """))
            logger.info("migration: 建立 price_fetch_status 表")

        # line_subscribers 表（舊 DB 補建）
        if not _table_exists(conn, "line_subscribers"):
            conn.execute(text("""
                CREATE TABLE line_subscribers (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id      TEXT UNIQUE NOT NULL,
                    display_name TEXT DEFAULT '',
                    enabled      INTEGER DEFAULT 1,
                    created_at   TEXT
                )
            """))
            logger.info("migration: 建立 line_subscribers 表")

        # margin_cache 表（舊 DB 補建）
        if not _table_exists(conn, "margin_cache"):
            conn.execute(text("""
                CREATE TABLE margin_cache (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_id       TEXT NOT NULL,
                    date           TEXT NOT NULL,
                    margin_buy     INTEGER,
                    margin_sell    INTEGER,
                    margin_balance INTEGER,
                    short_buy      INTEGER,
                    short_sell     INTEGER,
                    short_balance  INTEGER,
                    fetch_at       TEXT,
                    CONSTRAINT uq_margin_stock_date UNIQUE (stock_id, date)
                )
            """))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_margin_stock_date ON margin_cache (stock_id, date)"
            ))
            logger.info("migration: 建立 margin_cache 表")

        if not _table_exists(conn, "broker_main_force_cache"):
            conn.execute(text("""
                CREATE TABLE broker_main_force_cache (
                    stock_id     TEXT NOT NULL,
                    date         TEXT NOT NULL,
                    buy_top15    REAL DEFAULT 0,
                    sell_top15   REAL DEFAULT 0,
                    net          REAL DEFAULT 0,
                    broker_count INTEGER DEFAULT 0,
                    top5_buy_concentration REAL,
                    consecutive_buy_days INTEGER,
                    reversal_flag INTEGER,
                    fetched_at   TEXT,
                    PRIMARY KEY (stock_id, date)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_broker_main_force_stock_date
                ON broker_main_force_cache (stock_id, date)
            """))
            logger.info("migration: 建立 broker_main_force_cache 表")
        else:
            for col_name, col_type in [
                ("top5_buy_concentration", "REAL"),
                ("consecutive_buy_days", "INTEGER"),
                ("reversal_flag", "INTEGER"),
            ]:
                if not _column_exists(conn, "broker_main_force_cache", col_name):
                    conn.execute(text(
                        f"ALTER TABLE broker_main_force_cache ADD COLUMN {col_name} {col_type}"
                    ))
                    logger.info("migration: broker_main_force_cache 新增欄位 %s", col_name)

        if not _table_exists(conn, "risk_flags_cache"):
            conn.execute(text("""
                CREATE TABLE risk_flags_cache (
                    stock_id    TEXT NOT NULL,
                    date        TEXT NOT NULL,
                    flag_type   TEXT NOT NULL,
                    detail      TEXT,
                    fetched_at  TEXT,
                    PRIMARY KEY (stock_id, date, flag_type)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_risk_flags_stock_date
                ON risk_flags_cache (stock_id, date)
            """))
            logger.info("migration: 建立 risk_flags_cache 表")

        if not _table_exists(conn, "holding_shares_cache"):
            conn.execute(text("""
                CREATE TABLE holding_shares_cache (
                    stock_id       TEXT NOT NULL,
                    date           TEXT NOT NULL,
                    above_400_pct  REAL,
                    above_1000_pct REAL,
                    below_10_pct   REAL,
                    fetched_at     TEXT,
                    PRIMARY KEY (stock_id, date)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_holding_shares_stock_date
                ON holding_shares_cache (stock_id, date)
            """))
            logger.info("migration: 建立 holding_shares_cache 表")

        if not _table_exists(conn, "cache_health_run"):
            conn.execute(text("""
                CREATE TABLE cache_health_run (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset TEXT NOT NULL,
                    date_from TEXT NOT NULL,
                    date_to TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT NOT NULL DEFAULT 'queued',
                    requested_by TEXT DEFAULT 'streamlit',
                    scan_scope TEXT DEFAULT 'active_stocks',
                    total_expected_units INTEGER DEFAULT 0,
                    total_present_units INTEGER DEFAULT 0,
                    total_missing_units INTEGER DEFAULT 0,
                    completeness_pct REAL DEFAULT 0,
                    earliest_cached_date TEXT,
                    latest_cached_date TEXT,
                    notes TEXT,
                    error_message TEXT
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cache_health_run_dataset_requested
                ON cache_health_run (dataset, requested_at)
            """))
            logger.info("migration: 建立 cache_health_run 表")

        if not _table_exists(conn, "cache_health_daily_summary"):
            conn.execute(text("""
                CREATE TABLE cache_health_daily_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    dataset TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    expected_count INTEGER DEFAULT 0,
                    present_count INTEGER DEFAULT 0,
                    missing_count INTEGER DEFAULT 0,
                    completeness_pct REAL DEFAULT 0,
                    CONSTRAINT uq_cache_health_daily_run_date UNIQUE (run_id, trade_date)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cache_health_daily_run_date
                ON cache_health_daily_summary (run_id, trade_date)
            """))
            logger.info("migration: 建立 cache_health_daily_summary 表")

        if not _table_exists(conn, "cache_health_gap"):
            conn.execute(text("""
                CREATE TABLE cache_health_gap (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    dataset TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    stock_id TEXT NOT NULL,
                    gap_type TEXT DEFAULT 'missing',
                    severity TEXT DEFAULT 'normal',
                    detail_json TEXT,
                    repair_status TEXT DEFAULT 'pending',
                    repaired_at TEXT,
                    repair_error TEXT,
                    CONSTRAINT uq_cache_health_gap_unit UNIQUE (run_id, dataset, trade_date, stock_id)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cache_health_gap_run_date
                ON cache_health_gap (run_id, trade_date)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cache_health_gap_dataset_status
                ON cache_health_gap (dataset, repair_status)
            """))
            logger.info("migration: 建立 cache_health_gap 表")

        if not _table_exists(conn, "cache_health_repair_job"):
            conn.execute(text("""
                CREATE TABLE cache_health_repair_job (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    dataset TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    requested_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    target_count INTEGER DEFAULT 0,
                    done_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    last_error TEXT
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cache_health_repair_run_requested
                ON cache_health_repair_job (run_id, requested_at)
            """))
            logger.info("migration: 建立 cache_health_repair_job 表")

        if not _table_exists(conn, "etf_holding_cache"):
            conn.execute(text("""
                CREATE TABLE etf_holding_cache (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    etf_id          TEXT NOT NULL,
                    date            TEXT NOT NULL,
                    hold_stock_id   TEXT NOT NULL,
                    hold_stock_name TEXT DEFAULT '',
                    percentage      REAL DEFAULT 0.0,
                    fetched_at      TEXT,
                    CONSTRAINT uq_etf_holding UNIQUE (etf_id, date, hold_stock_id)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_etf_holding_etf_date
                ON etf_holding_cache (etf_id, date)
            """))
            logger.info("migration: 建立 etf_holding_cache 表")


def vacuum_db():
    """清理資料庫碎片，定期維護用"""
    with ENGINE.connect() as conn:
        conn.execute(text("VACUUM"))


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
