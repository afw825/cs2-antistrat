from pathlib import Path

import pandas as pd
import pytest
from antistrat.db.base import Base
from antistrat.ingestion import loader
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

pytestmark = pytest.mark.integration


def test_load_demo_data_persists_rows_in_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "integration.db"
    test_engine = create_engine(
        f"sqlite:///{db_path.as_posix()}", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=test_engine)
    test_session_local = sessionmaker(bind=test_engine, autocommit=False, autoflush=False)

    monkeypatch.setattr(loader, "SessionLocal", test_session_local)

    df = pd.DataFrame(
        {
            "tick": [64, 128],
            "round_number": [1, 1],
            "player_name": ["PlayerOne", "PlayerOne"],
            "steamid": ["76561198000000000", "76561198000000000"],
            "team_name": ["CT", "CT"],
            "X": [-3000.0, -2990.0],
            "Y": [1500.0, 1490.0],
            "Z": [0.0, 0.0],
        }
    )

    summary = loader.load_demo_data("sample.dem", "de_mirage", df)

    assert summary["tick_count"] == 2
    assert summary["player_count"] == 1
    assert summary["round_count"] == 1

    with test_engine.connect() as conn:
        ticks = conn.execute(text("SELECT COUNT(*) FROM tick_data")).scalar_one()
        matches = conn.execute(text("SELECT COUNT(*) FROM matches")).scalar_one()

    assert ticks == 2
    assert matches == 1


def test_load_demo_data_replaces_existing_rows_for_same_demo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "replace_same_demo.db"
    test_engine = create_engine(
        f"sqlite:///{db_path.as_posix()}", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(bind=test_engine)
    test_session_local = sessionmaker(bind=test_engine, autocommit=False, autoflush=False)

    monkeypatch.setattr(loader, "SessionLocal", test_session_local)

    first_df = pd.DataFrame(
        {
            "tick": [64, 128],
            "round_number": [1, 1],
            "player_name": ["PlayerOne", "PlayerOne"],
            "steamid": ["76561198000000000", "76561198000000000"],
            "team_name": ["CT", "CT"],
            "X": [-3000.0, -2990.0],
            "Y": [1500.0, 1490.0],
            "Z": [0.0, 0.0],
        }
    )
    second_df = pd.DataFrame(
        {
            "tick": [192],
            "round_number": [1],
            "player_name": ["PlayerOne"],
            "steamid": ["76561198000000000"],
            "team_name": ["CT"],
            "X": [-2980.0],
            "Y": [1480.0],
            "Z": [0.0],
        }
    )

    loader.load_demo_data("sample.dem", "de_mirage", first_df)
    summary = loader.load_demo_data("sample.dem", "de_mirage", second_df)

    assert summary["tick_count"] == 1

    with test_engine.connect() as conn:
        ticks = conn.execute(text("SELECT COUNT(*) FROM tick_data")).scalar_one()
        matches = conn.execute(text("SELECT COUNT(*) FROM matches")).scalar_one()

    assert ticks == 1
    assert matches == 1
