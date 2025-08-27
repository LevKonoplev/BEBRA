import pandas as pd
import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mmw import db
from mmw.analytics import compute_daily_returns, news_intensity


def test_compute_daily_returns_percentage_changes():
    engine = create_engine("sqlite:///:memory:", future=True)
    db.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)

    with Session.begin() as session:
        asset1 = db.Asset(ticker="AAA")
        asset2 = db.Asset(ticker="BBB")
        session.add_all([asset1, asset2])
        session.flush()
        session.add_all([
            db.Price(asset_id=asset1.id, date=datetime(2024, 1, 1), close=100.0),
            db.Price(asset_id=asset1.id, date=datetime(2024, 1, 2), close=110.0),
            db.Price(asset_id=asset2.id, date=datetime(2024, 1, 1), close=200.0),
            db.Price(asset_id=asset2.id, date=datetime(2024, 1, 2), close=220.0),
        ])

    df = compute_daily_returns(engine, ["AAA", "BBB"])
    assert len(df) == 2
    assert set(df["ticker"]) == {"AAA", "BBB"}
    assert df.loc[df["ticker"] == "AAA", "ret"].iloc[0] == pytest.approx(0.10)
    assert df.loc[df["ticker"] == "BBB", "ret"].iloc[0] == pytest.approx(0.10)
    assert pd.Timestamp(df.loc[df["ticker"] == "AAA", "date"].iloc[0]) == pd.Timestamp(
        "2024-01-02"
    )


def test_news_intensity_daily_aggregation():
    engine = create_engine("sqlite:///:memory:", future=True)
    db.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)

    with Session.begin() as session:
        session.add_all([
            db.News(
                url="u1",
                title="t1",
                summary="",
                summary_ai="good",
                published_at=datetime(2024, 1, 1, 10, 0, 0),
            ),
            db.News(
                url="u2",
                title="t2",
                summary="",
                summary_ai="betternews",
                published_at=datetime(2024, 1, 1, 12, 0, 0),
            ),
            db.News(
                url="u3",
                title="t3",
                summary="",
                summary_ai="bad",
                published_at=datetime(2024, 1, 2, 9, 0, 0),
            ),
        ])

    df = news_intensity(engine).sort_values("date").reset_index(drop=True)
    assert len(df) == 2
    assert df.loc[0, "date"] == datetime(2024, 1, 1).date()
    assert df.loc[0, "news_count"] == 2
    assert df.loc[0, "avg_sentiment"] == pytest.approx((4 + 10) / 2)
    assert df.loc[1, "date"] == datetime(2024, 1, 2).date()
    assert df.loc[1, "news_count"] == 1
    assert df.loc[1, "avg_sentiment"] == pytest.approx(3)
