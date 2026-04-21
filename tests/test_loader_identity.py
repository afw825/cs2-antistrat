import pytest
from antistrat.ingestion.loader import build_player_identity

pytestmark = pytest.mark.unit


def test_build_player_identity_prefers_valid_steamid() -> None:
    steam_key, player_name = build_player_identity("76561198000000000", "PlayerOne")

    assert steam_key == "76561198000000000"
    assert player_name == "PlayerOne"


def test_build_player_identity_falls_back_to_name_when_steamid_missing() -> None:
    steam_key, player_name = build_player_identity(None, "Player Two")

    assert steam_key == "name:player_two"
    assert player_name == "Player Two"


def test_build_player_identity_handles_missing_name() -> None:
    steam_key, player_name = build_player_identity("0", None)

    assert steam_key == "name:unknown_player"
    assert player_name == "unknown_player"
