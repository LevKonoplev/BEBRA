import pandas as pd

from mmw.news import normalize_news


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
