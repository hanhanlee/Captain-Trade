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
