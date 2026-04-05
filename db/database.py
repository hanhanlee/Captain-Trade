from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from .models import Base
import os

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
    cur.close()


SessionLocal = sessionmaker(bind=ENGINE)


def init_db():
    Base.metadata.create_all(ENGINE)
    _migrate_schema()


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return any(r[1] == column_name for r in rows)


def _migrate_schema():
    """輕量 schema migration，確保舊 DB 可相容新版欄位。"""
    with ENGINE.begin() as conn:
        if _column_exists(conn, "portfolio", "note") and not _column_exists(conn, "portfolio", "notes"):
            conn.execute(text("ALTER TABLE portfolio ADD COLUMN notes TEXT"))
            conn.execute(text("UPDATE portfolio SET notes = note WHERE notes IS NULL AND note IS NOT NULL"))
        elif _column_exists(conn, "portfolio", "note") and _column_exists(conn, "portfolio", "notes"):
            conn.execute(text("UPDATE portfolio SET notes = note WHERE (notes IS NULL OR notes = '') AND note IS NOT NULL"))


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
