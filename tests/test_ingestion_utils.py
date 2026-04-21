import pandas as pd
import pytest
from antistrat.ingestion.parser import (
    annotate_round_numbers,
    apply_opening_phase_filter,
    build_active_round_windows,
    build_round_windows_from_ticks,
    classify_rounds_by_economy,
    detect_round_economy_type,
    derive_round_numbers_from_tick_columns,
    derive_round_windows_from_df,
    extract_round_start_ticks,
    extract_tick_values,
    derive_spawn_anchor_points,
    filter_freeze_period_ticks,
    filter_non_playing_state_ticks,
    filter_spawn_proximity_points,
    filter_timeout_ticks,
    filter_by_side,
    filter_high_velocity_points,
    filter_recurring_players,
    parse_event_rows,
)
from antistrat.utils.maps import game_to_pixel, get_map_metadata

pytestmark = pytest.mark.unit


def test_build_active_round_windows_pairs_freeze_and_end_ticks() -> None:
    freeze_ends = pd.DataFrame({"tick": [100, 1000, 2000]})
    round_ends = pd.DataFrame({"tick": [800, 1500, 2600]})

    windows = build_active_round_windows(freeze_ends, round_ends)

    assert windows == [(100, 800, 1), (1000, 1500, 2), (2000, 2600, 3)]


def test_annotate_round_numbers_keeps_only_active_ticks() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 700, 1024, 1300, 4000],
            "player_name": ["p1"] * 6,
            "steamid": ["1"] * 6,
            "X": [0.0] * 6,
            "Y": [0.0] * 6,
            "Z": [0.0] * 6,
        }
    )
    windows = [(100, 800, 1), (1000, 1500, 2)]

    annotated = annotate_round_numbers(ticks, windows)

    assert annotated["tick"].tolist() == [128, 700, 1024, 1300]
    assert annotated["round_number"].tolist() == [1, 1, 2, 2]


def test_game_to_pixel_uses_standard_cs2_formula() -> None:
    pixel_x, pixel_y = game_to_pixel(
        game_x=-3000.0, game_y=1500.0, pos_x=-3230.0, pos_y=1713.0, scale=5.0
    )

    assert pixel_x == 46.0
    assert pixel_y == 42.6


def test_get_map_metadata_returns_known_entry() -> None:
    meta = get_map_metadata("de_inferno")

    assert meta.scale == 4.9
    assert meta.radar_image_path is not None


def test_get_map_metadata_returns_fallback_for_unknown_map() -> None:
    meta = get_map_metadata("de_not_real")

    assert meta.pos_x == 0.0
    assert meta.pos_y == 0.0
    assert meta.scale == 5.0


def test_parse_event_rows_handles_list_output_shape() -> None:
    class FakeParser:
        def parse_events(self, event_name):  # noqa: ANN001
            if isinstance(event_name, list):
                return [{"tick": 128}, {"tick": 256}]
            raise TypeError

    parsed = parse_event_rows(FakeParser(), "round_end")

    assert isinstance(parsed, pd.DataFrame)
    assert not parsed.empty
    assert parsed["tick"].tolist() == [128, 256]


def test_build_active_round_windows_returns_empty_when_tick_missing() -> None:
    freeze_ends = pd.DataFrame({"event_time": [100, 1000]})
    round_ends = pd.DataFrame({"event_time": [800, 1500]})

    windows = build_active_round_windows(freeze_ends, round_ends)

    assert windows == []


def test_extract_tick_values_supports_event_tick_column() -> None:
    events = pd.DataFrame({"event_tick": [100, 200, 200, 350]})

    ticks = extract_tick_values(events)

    assert ticks == [100, 200, 350]


def test_build_active_round_windows_supports_tick_like_columns() -> None:
    freeze_ends = pd.DataFrame({"event_tick": [100, 1000, 2000]})
    round_ends = pd.DataFrame({"event_tick": [800, 1500, 2600]})

    windows = build_active_round_windows(freeze_ends, round_ends)

    assert windows == [(100, 800, 1), (1000, 1500, 2), (2000, 2600, 3)]


def test_extract_tick_values_ignores_non_string_column_names() -> None:
    events = pd.DataFrame({0: [1, 2, 3], "event_tick": [128, 256, 384]})

    ticks = extract_tick_values(events)

    assert ticks == [128, 256, 384]


def test_build_round_windows_from_ticks_pairs_starts_and_ends() -> None:
    windows = build_round_windows_from_ticks(
        round_start_ticks=[100, 1000, 2000],
        round_end_ticks=[800, 1500, 2600],
        max_tick=3000,
    )

    assert windows == [(100, 800, 1), (1000, 1500, 2), (2000, 2600, 3)]


def test_build_round_windows_from_ticks_falls_back_to_start_boundaries() -> None:
    windows = build_round_windows_from_ticks(
        round_start_ticks=[100, 1000, 2000],
        round_end_ticks=[],
        max_tick=2700,
    )

    assert windows == [(100, 999, 1), (1000, 1999, 2), (2000, 2700, 3)]


def test_extract_round_start_ticks_prefers_round_freeze_end() -> None:
    class FakeParser:
        def parse_events(self, event_name):  # noqa: ANN001
            if isinstance(event_name, list):
                event_name = event_name[0]

            if event_name == "round_freeze_end":
                return [{"tick": 200}, {"tick": 1000}]
            if event_name == "round_start":
                return [{"tick": 100}, {"tick": 900}]
            if event_name == "round_begin":
                return [{"tick": 120}, {"tick": 920}]
            return []

    starts = extract_round_start_ticks(FakeParser())

    assert starts == [200, 1000]


def test_derive_round_numbers_from_total_rounds_played() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 640, 704],
            "total_rounds_played": [0, 0, 1, 1],
        }
    )

    derived = derive_round_numbers_from_tick_columns(ticks)

    assert derived["round_number"].tolist() == [1, 1, 2, 2]


def test_derive_round_numbers_defaults_to_one_when_missing() -> None:
    ticks = pd.DataFrame({"tick": [64, 128, 192]})

    derived = derive_round_numbers_from_tick_columns(ticks)

    assert derived["round_number"].tolist() == [1, 1, 1]


def test_filter_by_side_both_keeps_ct_and_t() -> None:
    ticks = pd.DataFrame({"team_name": ["CT", "T", "SPEC"]})

    filtered = filter_by_side(ticks, "Both")

    assert filtered["team_name"].tolist() == ["CT", "T"]


def test_filter_freeze_period_ticks_removes_frozen_rows() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 192],
            "m_bFreezePeriod": [True, False, 1],
            "team_name": ["CT", "CT", "CT"],
        }
    )

    filtered = filter_freeze_period_ticks(ticks)

    assert filtered["tick"].tolist() == [128]


def test_filter_freeze_period_ticks_is_noop_without_column() -> None:
    ticks = pd.DataFrame({"tick": [64, 128], "team_name": ["CT", "CT"]})

    filtered = filter_freeze_period_ticks(ticks)

    assert filtered["tick"].tolist() == [64, 128]


def test_filter_timeout_ticks_removes_timeout_rows() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 192, 256, 320],
            "m_bCTTimeOutActive": [False, True, False, False, False],
            "m_bTerroristTimeOutActive": [0, 0, 1, 0, 0],
            "m_bTechnicalTimeOutActive": [False, False, False, True, False],
            "team_name": ["CT", "CT", "CT", "CT", "CT"],
        }
    )

    filtered = filter_timeout_ticks(ticks)

    assert filtered["tick"].tolist() == [64, 320]


def test_filter_timeout_ticks_is_noop_without_timeout_columns() -> None:
    ticks = pd.DataFrame({"tick": [64, 128], "team_name": ["CT", "CT"]})

    filtered = filter_timeout_ticks(ticks)

    assert filtered["tick"].tolist() == [64, 128]


def test_filter_non_playing_state_ticks_keeps_only_in_round_rows() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 192],
            "m_bRoundInProgress": [False, True, False],
            "team_name": ["CT", "CT", "CT"],
        }
    )

    filtered = filter_non_playing_state_ticks(ticks)

    assert filtered["tick"].tolist() == [128]


def test_filter_non_playing_state_ticks_removes_warmup_and_pause_rows() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 192],
            "m_bRoundInProgress": [True, True, True],
            "m_bWarmupPeriod": [False, True, False],
            "m_bGamePaused": [False, False, True],
            "team_name": ["CT", "CT", "CT"],
        }
    )

    filtered = filter_non_playing_state_ticks(ticks)

    assert filtered["tick"].tolist() == [64]


def test_derive_spawn_anchor_points_uses_first_early_player_positions() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [100, 164, 228, 100, 164, 228],
            "round_number": [1, 1, 1, 1, 1, 1],
            "player_name": ["A", "A", "A", "B", "B", "B"],
            "steamid": ["1", "1", "1", "2", "2", "2"],
            "X": [0.0, 100.0, 200.0, 1000.0, 900.0, 800.0],
            "Y": [0.0, 0.0, 0.0, 1000.0, 1000.0, 1000.0],
        }
    )
    windows = [(100, 500, 1)]

    anchors = derive_spawn_anchor_points(ticks, windows=windows, anchor_window_seconds=2)

    assert len(anchors) == 2
    assert sorted(anchors["X"].tolist()) == [0.0, 1000.0]


def test_filter_spawn_proximity_points_drops_points_near_anchors() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [100, 164, 228],
            "X": [0.0, 50.0, 1000.0],
            "Y": [0.0, 0.0, 1000.0],
        }
    )
    anchors = pd.DataFrame({"X": [0.0], "Y": [0.0]})

    filtered = filter_spawn_proximity_points(ticks, spawn_anchors_df=anchors, radius_units=100.0)

    assert filtered["tick"].tolist() == [228]


def test_filter_recurring_players_min_rounds() -> None:
    ticks = pd.DataFrame(
        {
            "steamid": ["1", "1", "2", "2", "2"],
            "player_name": ["A", "A", "B", "B", "B"],
            "round_number": [1, 2, 1, 2, 3],
            "tick": [64, 128, 64, 128, 192],
            "team_name": ["CT", "CT", "CT", "CT", "CT"],
            "X": [0.0] * 5,
            "Y": [0.0] * 5,
            "Z": [0.0] * 5,
        }
    )

    filtered = filter_recurring_players(ticks, min_rounds_present=3)

    assert sorted(filtered["player_name"].unique().tolist()) == ["B"]


def test_apply_opening_phase_filter_respects_bomb_plant_tick() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [100, 164, 228, 292],
            "round_number": [1, 1, 1, 1],
            "player_name": ["A"] * 4,
            "steamid": ["1"] * 4,
            "team_name": ["CT"] * 4,
            "X": [0.0] * 4,
            "Y": [0.0] * 4,
            "Z": [0.0] * 4,
        }
    )
    windows = [(100, 400, 1)]

    filtered = apply_opening_phase_filter(
        ticks,
        windows=windows,
        bomb_plant_ticks=[230],
        opening_seconds_cap=35,
        opening_start_seconds_after_round_start=0,
    )

    assert filtered["tick"].tolist() == [100, 164, 228]


def test_apply_opening_phase_filter_respects_seconds_cap_without_plant() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [100, 164, 228, 292],
            "round_number": [1, 1, 1, 1],
            "player_name": ["A"] * 4,
            "steamid": ["1"] * 4,
            "team_name": ["CT"] * 4,
            "X": [0.0] * 4,
            "Y": [0.0] * 4,
            "Z": [0.0] * 4,
        }
    )
    windows = [(100, 400, 1)]

    filtered = apply_opening_phase_filter(
        ticks,
        windows=windows,
        bomb_plant_ticks=[],
        opening_seconds_cap=2,
        opening_start_seconds_after_round_start=0,
    )

    assert filtered["tick"].tolist() == [100, 164, 228]


def test_detect_round_economy_type_marks_pistol_rounds() -> None:
    assert detect_round_economy_type(1) == "pistol"
    assert detect_round_economy_type(13) == "pistol"
    assert detect_round_economy_type(7) == "gun"


def test_classify_rounds_by_economy_adds_column() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 192],
            "round_number": [1, 13, 5],
            "player_name": ["A", "A", "A"],
            "steamid": ["1", "1", "1"],
            "team_name": ["CT", "CT", "CT"],
            "X": [0.0, 0.0, 0.0],
            "Y": [0.0, 0.0, 0.0],
            "Z": [0.0, 0.0, 0.0],
        }
    )

    out = classify_rounds_by_economy(ticks)

    assert out["round_economy_type"].tolist() == ["pistol", "pistol", "gun"]


def test_apply_opening_phase_filter_excludes_initial_round_seconds() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [100, 164, 228, 292, 356],
            "round_number": [1, 1, 1, 1, 1],
            "player_name": ["A"] * 5,
            "steamid": ["1"] * 5,
            "team_name": ["CT"] * 5,
            "X": [0.0] * 5,
            "Y": [0.0] * 5,
            "Z": [0.0] * 5,
        }
    )
    windows = [(100, 400, 1)]

    filtered = apply_opening_phase_filter(
        ticks,
        windows=windows,
        bomb_plant_ticks=[],
        opening_seconds_cap=35,
        opening_start_seconds_after_round_start=3,
    )

    assert filtered["tick"].tolist() == [292, 356]


def test_apply_opening_phase_filter_clamps_cap_when_start_exceeds_cap() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [100, 164, 228, 292, 356, 420, 484],
            "round_number": [1, 1, 1, 1, 1, 1, 1],
            "player_name": ["A"] * 7,
            "steamid": ["1"] * 7,
            "team_name": ["CT"] * 7,
            "X": [0.0] * 7,
            "Y": [0.0] * 7,
            "Z": [0.0] * 7,
        }
    )
    windows = [(100, 500, 1)]

    filtered = apply_opening_phase_filter(
        ticks,
        windows=windows,
        bomb_plant_ticks=[],
        opening_seconds_cap=2,
        opening_start_seconds_after_round_start=4,
    )

    assert filtered["tick"].tolist() == [356, 420]


def test_filter_high_velocity_points_drops_fast_movement_ticks() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 192],
            "round_number": [1, 1, 1],
            "player_name": ["A", "A", "A"],
            "steamid": ["1", "1", "1"],
            "team_name": ["CT", "CT", "CT"],
            "X": [0.0, 100.0, 500.0],
            "Y": [0.0, 0.0, 0.0],
            "Z": [0.0, 0.0, 0.0],
        }
    )

    filtered = filter_high_velocity_points(ticks, max_velocity_units_per_second=250.0)

    assert filtered["tick"].tolist() == [64, 128]


def test_filter_high_velocity_points_disabled_when_threshold_zero() -> None:
    ticks = pd.DataFrame(
        {
            "tick": [64, 128, 192],
            "round_number": [1, 1, 1],
            "player_name": ["A", "A", "A"],
            "steamid": ["1", "1", "1"],
            "team_name": ["CT", "CT", "CT"],
            "X": [0.0, 100.0, 500.0],
            "Y": [0.0, 0.0, 0.0],
            "Z": [0.0, 0.0, 0.0],
        }
    )

    filtered = filter_high_velocity_points(ticks, max_velocity_units_per_second=0)

    assert filtered["tick"].tolist() == [64, 128, 192]


def test_derive_round_windows_from_df_works_without_event_windows() -> None:
    ticks = pd.DataFrame({"tick": [100, 164, 1000, 1064], "round_number": [1, 1, 2, 2]})

    windows = derive_round_windows_from_df(ticks)

    assert windows == [(100, 164, 1), (1000, 1064, 2)]
