import pandas as pd
import pytest
from antistrat.ingestion import parser

pytestmark = pytest.mark.e2e


class FakeParser:
    def __init__(self, demo_path: str) -> None:
        self.demo_path = demo_path

    def parse_ticks(self, _wanted_fields):
        return pd.DataFrame(
            {
                "tick": [1280, 1344, 1408, 1472, 1536, 1600],
                "X": [-3000.0, -2990.0, -2980.0, -2970.0, -2960.0, -2950.0],
                "Y": [1500.0, 1490.0, 1480.0, 1470.0, 1460.0, 1450.0],
                "Z": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "team_name": ["CT", "CT", "T", "CT", "CT", "CT"],
                "player_name": ["A", "A", "B", "A", "A", "A"],
                "steamid": ["111", "111", "222", "111", "111", "111"],
                "is_alive": [True, True, True, True, True, True],
                "m_bFreezePeriod": [True, False, False, False, False, False],
                "m_bCTTimeOutActive": [False, True, False, False, False, False],
                "m_bRoundInProgress": [True, True, True, False, True, True],
                "m_bWarmupPeriod": [False, False, False, False, True, False],
                "round_num": [0, 0, 0, 0, 0, 0],
                "round_number": [1, 1, 1, 1, 1, 1],
                "total_rounds_played": [0, 0, 0, 0, 0, 0],
                "m_totalRoundsPlayed": [0, 0, 0, 0, 0, 0],
            }
        )

    def parse_events(self, event_name):
        if isinstance(event_name, list):
            event_name = event_name[0]

        if event_name in {"round_freeze_end", "round_start", "round_begin"}:
            return [{"tick": 60}]
        if event_name in {"round_end", "round_officially_ended", "round_prestart"}:
            return [{"tick": 1800}]
        if event_name == "bomb_planted":
            return []
        return []


def test_extract_ct_telemetry_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(parser, "DemoParser", FakeParser)

    out = parser.extract_ct_telemetry(
        demo_path="dummy.dem",
        side_filter="CT",
        min_rounds_present=1,
        opening_seconds_cap=35,
    )

    assert not out.empty
    assert 1280 not in out["tick"].tolist()
    assert 1344 not in out["tick"].tolist()
    assert 1472 not in out["tick"].tolist()
    assert 1536 not in out["tick"].tolist()
    assert sorted(out["team_name"].unique().tolist()) == ["CT"]
    assert out["round_number"].min() == 1
    assert {
        "tick",
        "player_name",
        "steamid",
        "team_name",
        "X",
        "Y",
        "Z",
        "round_economy_type",
    }.issubset(out.columns)
