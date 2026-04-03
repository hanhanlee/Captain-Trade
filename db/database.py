from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from .models import Base
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "srock.db")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", echo=False)
SessionLocal = sessionmaker(bind=ENGINE)


def init_db():
    Base.metadata.create_all(ENGINE)


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
