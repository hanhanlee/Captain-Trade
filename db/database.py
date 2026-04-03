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
