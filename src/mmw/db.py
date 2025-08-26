"""Database models and initialization."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from .config import DB_PATH

engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


class Asset(Base):
    __tablename__ = "assets"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True, nullable=False)
    name = Column(String)

    prices = relationship("Price", back_populates="asset")


class Price(Base):
    __tablename__ = "prices"
    __table_args__ = (UniqueConstraint("asset_id", "date", name="uix_price_asset_date"),)

    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

    asset = relationship("Asset", back_populates="prices")


class Index(Base):
    __tablename__ = "indices"

    id = Column(Integer, primary_key=True)
    code = Column(String, unique=True, nullable=False)
    name = Column(String)

    points = relationship("IndexPoint", back_populates="index")


class IndexPoint(Base):
    __tablename__ = "index_points"

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("indices.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)

    index = relationship("Index", back_populates="points")


class News(Base):
    __tablename__ = "news"

    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, nullable=False)
    title = Column(String, nullable=False)
    summary = Column(Text)
    summary_ai = Column(Text)
    published_at = Column(DateTime)
    source = Column(String)


class Entity(Base):
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, ForeignKey("news.id"), nullable=False)
    type = Column(String, nullable=False)
    value = Column(String, nullable=False)
    score = Column(Float)

    news = relationship("News")


class Link(Base):
    __tablename__ = "links"

    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, ForeignKey("news.id"), nullable=False)
    asset_ticker = Column(String)
    index_code = Column(String)
    score = Column(Float, nullable=False)


class Run(Base):
    __tablename__ = "runs"

    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String)


def init_db() -> None:
    """Initialize the SQLite database and create tables."""

    Base.metadata.create_all(engine)
