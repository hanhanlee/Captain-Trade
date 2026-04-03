from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()


class Portfolio(Base):
    """持股清單"""
    __tablename__ = "portfolio"

    id = Column(Integer, primary_key=True)
    stock_id = Column(String(10), nullable=False)
    stock_name = Column(String(50))
    shares = Column(Integer, nullable=False)       # 持有張數
    cost_price = Column(Float, nullable=False)     # 成本價
    buy_date = Column(Date)
    stop_loss = Column(Float)                      # 停損價
    take_profit = Column(Float)                    # 停利價
    note = Column(Text)
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


class PriceCache(Base):
    """本機歷史價格快取（供回測使用，避免重複呼叫 API）"""
    __tablename__ = "price_cache"
    __table_args__ = (UniqueConstraint("stock_id", "date", name="uq_stock_date"),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(String(10), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    updated_at = Column(DateTime, default=datetime.now)
