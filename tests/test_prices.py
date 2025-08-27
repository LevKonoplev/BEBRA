import logging

import mmw.prices as prices


def test_fetch_prices_yf_handles_error(monkeypatch, caplog):
    def bad_download(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(prices.yf, "download", bad_download)

    with caplog.at_level(logging.WARNING):
        df = prices.fetch_prices_yf(["AAA"], start="2024-01-01", end="2024-01-10")

    assert df.empty
    assert any("Failed to download prices" in r.getMessage() for r in caplog.records)
