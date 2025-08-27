import logging
from datetime import datetime

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from mmw import db
from mmw.indices import import_indices_from_csv


def test_import_indices_missing_csv(tmp_path, caplog):
    path = tmp_path / "missing.csv"
    with caplog.at_level(logging.INFO):
        import_indices_from_csv(path)
    messages = [r.getMessage() for r in caplog.records]
    assert f"CSV {path} not found, skipping" in messages


def test_import_indices_from_csv_inserts_data(tmp_path, monkeypatch):
    csv_path = tmp_path / "indices.csv"
    csv_path.write_text("date,index_code,value,source\n2024-01-01,TEST,123.45,unit\n")

    engine = create_engine("sqlite:///:memory:", future=True)
    db.Base.metadata.create_all(engine)
    import mmw.indices as indices
    monkeypatch.setattr(indices, "engine", engine)

    import_indices_from_csv(csv_path)

    Session = sessionmaker(bind=engine, future=True)
    with Session() as session:
        idx = session.execute(select(db.Index).where(db.Index.code == "TEST")).scalar_one()
        point = (
            session.execute(
                select(db.IndexPoint).where(db.IndexPoint.index_id == idx.id)
            ).scalar_one()
        )
        assert point.value == 123.45
        assert point.date == datetime(2024, 1, 1)
