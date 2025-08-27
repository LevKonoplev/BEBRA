import pandas as pd

import mmw.news as news
from mmw.news import normalize_news, refresh_news


def test_normalize_news_empty():
    df = normalize_news([])
    assert df.empty
    assert list(df.columns) == ["ts", "source", "url", "title", "summary"]


def test_normalize_news_parses_fields():
    item = {
        "title": "Some title",
        "summary": "Short summary",
        "published": "2024-01-02T03:04:05Z",
        "source": "Example Feed",
        "url": "https://example.com/article",
    }
    df = normalize_news([item])
    assert df.loc[0, "title"] == item["title"]
    assert df.loc[0, "summary"] == item["summary"]
    assert df.loc[0, "source"] == item["source"]
    assert df.loc[0, "url"] == item["url"]
    assert df.loc[0, "ts"] == pd.Timestamp("2024-01-02T03:04:05Z")


def test_refresh_news_drops_duplicates(monkeypatch):
    items = [
        {
            "title": "Title 1",
            "summary": "Summary",
            "published": "2024-01-01T00:00:00Z",
            "source": "Feed",
            "url": "https://example.com/a",
        },
        {
            "title": "Title 2",
            "summary": "Summary 2",
            "published": "2024-01-01T01:00:00Z",
            "source": "Feed",
            "url": "https://example.com/a",
        },
    ]

    monkeypatch.setattr(news, "RSS_FEEDS", ["dummy"])
    monkeypatch.setattr(news, "fetch_rss", lambda feed: items)

    captured = {}

    def fake_upsert(df):
        captured["df"] = df.copy()
        return len(df)

    monkeypatch.setattr(news, "upsert_news", fake_upsert)

    refresh_news()

    assert len(captured["df"]) == 1
    assert captured["df"].iloc[0]["url"] == "https://example.com/a"
