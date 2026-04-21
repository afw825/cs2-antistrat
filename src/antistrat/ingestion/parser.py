# src/antistrat/ingestion/parser.py
import logging
import re
from typing import Any

import pandas as pd
from demoparser2 import DemoParser

TICKRATE = 64
logger = logging.getLogger(__name__)
PRIMARY_ROUND_START_EVENTS = ["round_freeze_end"]
FALLBACK_ROUND_START_EVENTS = ["round_start", "round_begin"]
ROUND_END_EVENTS = ["round_end", "round_officially_ended", "round_prestart"]
BOMB_PLANT_EVENTS = ["bomb_planted"]
ROUND_TICK_COLUMNS = [
    "round_num",
    "round_number",
    "total_rounds_played",
    "m_totalRoundsPlayed",
]


def normalize_map_name(raw_map_name: object) -> str | None:
    """Normalize map names from demo header payloads to canonical values."""
    if raw_map_name is None:
        return None

    text = str(raw_map_name).strip().lower()
    if not text:
        return None

    # Handle paths like workshop/123456/de_mirage.vpk or C:\maps\de_nuke.bsp.
    if "/" in text or "\\" in text:
        text = re.split(r"[\\/]", text)[-1]

    if "." in text:
        text = text.split(".", 1)[0]

    match = re.search(r"\b(de_[a-z0-9_]+)\b", text)
    if match:
        return match.group(1)

    return text if text.startswith(("de_", "cs_")) else None


def extract_map_name_from_header(header_payload: Any) -> str | None:
    """Walk parser header payloads and return the first normalized map name."""
    map_field_names = {
        "map",
        "map_name",
        "mapname",
        "level",
        "level_name",
        "levelname",
    }

    def _walk(value: Any) -> str | None:
        if isinstance(value, dict):
            # Prefer direct map fields, but also recurse into nested structures.
            for key, raw_value in value.items():
                if str(key).strip().lower() in map_field_names:
                    normalized = normalize_map_name(raw_value)
                    if normalized:
                        return normalized

            for nested in value.values():
                nested_match = _walk(nested)
                if nested_match:
                    return nested_match
            return None

        if isinstance(value, (list, tuple, set)):
            for item in value:
                nested_match = _walk(item)
                if nested_match:
                    return nested_match
            return None

        return normalize_map_name(value)

    return _walk(header_payload)


def detect_demo_map_name(demo_path: str) -> str | None:
    """Read map name from demo header metadata when available."""
    try:
        header_payload = DemoParser(demo_path).parse_header()
    except Exception:
        logger.exception("Failed to parse demo header for map autodetection demo=%s", demo_path)
        return None

    map_name = extract_map_name_from_header(header_payload)
    if map_name:
        logger.info("Autodetected map=%s for demo=%s", map_name, demo_path)
    else:
        logger.warning("Could not autodetect map for demo=%s", demo_path)
    return map_name


def extract_tick_values(events_df: pd.DataFrame) -> list[int]:
    """Extract event tick values from known or inferred tick-like columns."""
    if events_df.empty:
        return []

    preferred_columns = [
        "tick",
        "event_tick",
        "game_tick",
        "tick_count",
        "tick_num",
    ]

    tick_col = None
    for col in preferred_columns:
        if col in events_df.columns:
            tick_col = col
            break

    if tick_col is None:
        inferred = [c for c in events_df.columns if "tick" in str(c).lower()]
        if inferred:
            tick_col = inferred[0]

    if tick_col is None:
        return []

    ticks = pd.to_numeric(events_df[tick_col], errors="coerce").dropna().astype(int)
    if ticks.empty:
        return []

    return sorted(ticks.unique().tolist())


def extract_ticks_for_events(parser: DemoParser, event_names: list[str]) -> list[int]:
    """Parse multiple event types and combine all detected tick values."""
    all_ticks: set[int] = set()
    for event_name in event_names:
        event_rows = parse_event_rows(parser, event_name)
        all_ticks.update(extract_tick_values(event_rows))
    return sorted(all_ticks)


def extract_round_start_ticks(parser: DemoParser) -> list[int]:
    """Prefer freeze-end ticks so round windows start after buy/freeze time."""
    freeze_end_ticks = extract_ticks_for_events(parser, PRIMARY_ROUND_START_EVENTS)
    if freeze_end_ticks:
        return freeze_end_ticks
    return extract_ticks_for_events(parser, FALLBACK_ROUND_START_EVENTS)


def parse_event_rows(parser: DemoParser, event_name: str) -> pd.DataFrame:
    """Parse one event name with compatibility across demoparser2 builds."""
    raw_result = None
    try:
        # Newer bindings accept a vector of event names.
        raw_result = parser.parse_events([event_name])
    except TypeError:
        # Older bindings may still accept a single string.
        raw_result = parser.parse_events(event_name)

    if isinstance(raw_result, pd.DataFrame):
        return raw_result

    if isinstance(raw_result, list):
        if not raw_result:
            return pd.DataFrame(columns=["tick"])
        return pd.DataFrame(raw_result)

    if raw_result is None:
        return pd.DataFrame(columns=["tick"])

    try:
        return pd.DataFrame(raw_result)
    except Exception:
        return pd.DataFrame(columns=["tick"])


def build_active_round_windows(
    freeze_ends: pd.DataFrame,
    round_ends: pd.DataFrame,
) -> list[tuple[int, int, int]]:
    """Build inclusive [start_tick, end_tick] windows for active rounds."""
    if freeze_ends.empty or round_ends.empty:
        return []

    freeze_ticks = extract_tick_values(freeze_ends)
    round_end_ticks = extract_tick_values(round_ends)
    if not freeze_ticks or not round_end_ticks:
        return []

    windows: list[tuple[int, int, int]] = []
    used_end_idx = 0

    for round_number, start_tick in enumerate(freeze_ticks, start=1):
        while used_end_idx < len(round_end_ticks) and round_end_ticks[used_end_idx] <= start_tick:
            used_end_idx += 1

        if used_end_idx >= len(round_end_ticks):
            break

        end_tick = round_end_ticks[used_end_idx]
        windows.append((start_tick, end_tick, round_number))
        used_end_idx += 1

    return windows


def build_round_windows_from_ticks(
    round_start_ticks: list[int],
    round_end_ticks: list[int],
    max_tick: int,
) -> list[tuple[int, int, int]]:
    """Build windows from tick lists with graceful fallback when end events are missing."""
    if not round_start_ticks:
        return []

    if round_end_ticks:
        windows: list[tuple[int, int, int]] = []
        used_end_idx = 0
        for round_number, start_tick in enumerate(round_start_ticks, start=1):
            while (
                used_end_idx < len(round_end_ticks) and round_end_ticks[used_end_idx] <= start_tick
            ):
                used_end_idx += 1

            if used_end_idx >= len(round_end_ticks):
                break

            end_tick = round_end_ticks[used_end_idx]
            windows.append((start_tick, end_tick, round_number))
            used_end_idx += 1
        if windows:
            return windows

    windows = []
    for idx, start_tick in enumerate(round_start_ticks):
        end_tick = round_start_ticks[idx + 1] - 1 if idx + 1 < len(round_start_ticks) else max_tick
        if end_tick >= start_tick:
            windows.append((start_tick, end_tick, idx + 1))
    return windows


def annotate_round_numbers(
    ticks_df: pd.DataFrame,
    windows: list[tuple[int, int, int]],
) -> pd.DataFrame:
    """Annotate each tick row with a round number using active-round windows."""
    if ticks_df.empty:
        result = ticks_df.copy()
        result["round_number"] = pd.Series(dtype="int64")
        return result

    if not windows:
        result = ticks_df.copy()
        result["round_number"] = 1
        return result

    slices: list[pd.DataFrame] = []
    for start_tick, end_tick, round_number in windows:
        in_window = ticks_df[(ticks_df["tick"] >= start_tick) & (ticks_df["tick"] <= end_tick)]
        if in_window.empty:
            continue
        in_window = in_window.copy()
        in_window["round_number"] = round_number
        slices.append(in_window)

    if not slices:
        result = ticks_df.iloc[0:0].copy()
        result["round_number"] = pd.Series(dtype="int64")
        return result

    return pd.concat(slices, ignore_index=True)


def derive_round_numbers_from_tick_columns(ticks_df: pd.DataFrame) -> pd.DataFrame:
    """Infer round numbers directly from tick properties when available."""
    for col in ROUND_TICK_COLUMNS:
        if col not in ticks_df.columns:
            continue

        numeric = pd.to_numeric(ticks_df[col], errors="coerce")
        if numeric.dropna().empty:
            continue

        # Common schema uses 0-indexed completed rounds; normalize to 1-based.
        if numeric.min(skipna=True) == 0:
            numeric = numeric + 1

        numeric = numeric.ffill().bfill()
        if numeric.dropna().empty:
            continue

        result = ticks_df.copy()
        result["round_number"] = numeric.astype(int)
        return result

    result = ticks_df.copy()
    result["round_number"] = 1
    return result


def filter_by_side(ticks_df: pd.DataFrame, side_filter: str) -> pd.DataFrame:
    """Filter telemetry by side: CT, T, or Both."""
    normalized = (side_filter or "CT").strip().upper()
    if normalized == "BOTH":
        return ticks_df[ticks_df["team_name"].isin(["CT", "T"])].copy()
    if normalized in {"CT", "T"}:
        return ticks_df[ticks_df["team_name"] == normalized].copy()
    return ticks_df[ticks_df["team_name"] == "CT"].copy()


def filter_freeze_period_ticks(ticks_df: pd.DataFrame) -> pd.DataFrame:
    """Drop ticks flagged as freeze period when parser provides m_bFreezePeriod."""
    if ticks_df.empty or "m_bFreezePeriod" not in ticks_df.columns:
        return ticks_df

    freeze_col = ticks_df["m_bFreezePeriod"]
    if pd.api.types.is_bool_dtype(freeze_col):
        freeze_mask = freeze_col.fillna(False)
    else:
        numeric = pd.to_numeric(freeze_col, errors="coerce")
        normalized = freeze_col.astype(str).str.strip().str.lower()
        freeze_mask = (numeric == 1) | normalized.isin({"true", "t", "yes", "y", "1"})

    filtered = ticks_df[~freeze_mask].copy()
    dropped = int(len(ticks_df) - len(filtered))
    if dropped > 0:
        logger.info("Dropped freeze-period ticks rows=%s", dropped)
    return filtered


def filter_timeout_ticks(ticks_df: pd.DataFrame) -> pd.DataFrame:
    """Drop ticks flagged as timeout when parser provides timeout bool fields."""
    if ticks_df.empty:
        return ticks_df

    timeout_columns = [
        col
        for col in ticks_df.columns
        if "timeout" in str(col).strip().lower() and "time" in str(col).strip().lower()
    ]
    if not timeout_columns:
        return ticks_df

    timeout_mask = pd.Series(False, index=ticks_df.index)
    for col in timeout_columns:
        state_col = ticks_df[col]
        if pd.api.types.is_bool_dtype(state_col):
            col_mask = state_col.fillna(False)
        else:
            numeric = pd.to_numeric(state_col, errors="coerce")
            normalized = state_col.astype(str).str.strip().str.lower()
            col_mask = (numeric == 1) | normalized.isin({"true", "t", "yes", "y", "1"})
        timeout_mask = timeout_mask | col_mask

    filtered = ticks_df[~timeout_mask].copy()
    dropped = int(len(ticks_df) - len(filtered))
    if dropped > 0:
        logger.info("Dropped timeout ticks rows=%s columns=%s", dropped, timeout_columns)
    return filtered


def filter_non_playing_state_ticks(ticks_df: pd.DataFrame) -> pd.DataFrame:
    """Drop ticks that occur in non-playing states (warmup, pause, not-in-round)."""
    if ticks_df.empty:
        return ticks_df

    # If this flag is present, only retain ticks while rounds are actively in progress.
    round_in_progress_col = "m_bRoundInProgress"
    work = ticks_df.copy()
    dropped_total = 0

    if round_in_progress_col in work.columns:
        state_col = work[round_in_progress_col]
        if pd.api.types.is_bool_dtype(state_col):
            in_round = state_col.fillna(False)
        else:
            numeric = pd.to_numeric(state_col, errors="coerce")
            normalized = state_col.astype(str).str.strip().str.lower()
            in_round = (numeric == 1) | normalized.isin({"true", "t", "yes", "y", "1"})

        before = len(work)
        work = work[in_round].copy()
        dropped_total += before - len(work)

    # Additional break-state flags that should never be tracked as positional data.
    break_state_columns = [
        col
        for col in [
            "m_bWarmupPeriod",
            "m_bGamePaused",
            "m_bMatchWaitingForResume",
        ]
        if col in work.columns
    ]
    for col in break_state_columns:
        state_col = work[col]
        if pd.api.types.is_bool_dtype(state_col):
            break_mask = state_col.fillna(False)
        else:
            numeric = pd.to_numeric(state_col, errors="coerce")
            normalized = state_col.astype(str).str.strip().str.lower()
            break_mask = (numeric == 1) | normalized.isin({"true", "t", "yes", "y", "1"})

        before = len(work)
        work = work[~break_mask].copy()
        dropped_total += before - len(work)

    if dropped_total > 0:
        logger.info("Dropped non-playing-state ticks rows=%s", int(dropped_total))

    return work


def filter_recurring_players(ticks_df: pd.DataFrame, min_rounds_present: int) -> pd.DataFrame:
    """Keep players that appear in at least N rounds."""
    if ticks_df.empty or min_rounds_present <= 1:
        return ticks_df

    work = ticks_df.copy()
    work["steam_key"] = work["steamid"].astype(str)
    invalid_mask = work["steam_key"].str.lower().isin(["nan", "none", "null", "0", "0.0"])
    if invalid_mask.any():
        work.loc[invalid_mask, "steam_key"] = "name:" + work.loc[
            invalid_mask, "player_name"
        ].astype(str)

    rounds_per_player = work.groupby("steam_key")["round_number"].nunique()
    keep_keys = rounds_per_player[rounds_per_player >= int(min_rounds_present)].index
    filtered = work[work["steam_key"].isin(keep_keys)].copy()
    filtered.drop(columns=["steam_key"], inplace=True)
    return filtered


def filter_high_velocity_points(
    ticks_df: pd.DataFrame,
    max_velocity_units_per_second: float,
) -> pd.DataFrame:
    """Drop ticks where player movement speed exceeds the configured threshold."""
    if ticks_df.empty or max_velocity_units_per_second <= 0:
        return ticks_df

    work = ticks_df.copy()
    work["movement_key"] = work["steamid"].astype(str)
    invalid_mask = work["movement_key"].str.lower().isin(["nan", "none", "null", "0", "0.0"])
    if invalid_mask.any():
        work.loc[invalid_mask, "movement_key"] = "name:" + work.loc[
            invalid_mask, "player_name"
        ].astype(str)

    group_cols = ["round_number", "movement_key"]
    work = work.sort_values(group_cols + ["tick"])

    dx = work.groupby(group_cols)["X"].diff()
    dy = work.groupby(group_cols)["Y"].diff()
    dz = work.groupby(group_cols)["Z"].diff()
    dt_seconds = work.groupby(group_cols)["tick"].diff() / TICKRATE

    distance = (dx.pow(2) + dy.pow(2) + dz.pow(2)).pow(0.5)
    speed = distance / dt_seconds
    speed = speed.replace([float("inf"), -float("inf")], pd.NA).fillna(0.0)

    filtered = work[speed <= float(max_velocity_units_per_second)].copy()
    filtered.drop(columns=["movement_key"], inplace=True)
    return filtered


def derive_spawn_anchor_points(
    ticks_df: pd.DataFrame,
    windows: list[tuple[int, int, int]],
    anchor_window_seconds: int = 12,
) -> pd.DataFrame:
    """Derive likely spawn anchors from each player's first early-round position."""
    if ticks_df.empty or not windows:
        return pd.DataFrame(columns=["X", "Y"])

    round_start_by_number = {int(rn): int(start) for start, _end, rn in windows}
    work = ticks_df.copy()
    work["movement_key"] = work["steamid"].astype(str)
    invalid_mask = work["movement_key"].str.lower().isin(["nan", "none", "null", "0", "0.0"])
    if invalid_mask.any():
        work.loc[invalid_mask, "movement_key"] = "name:" + work.loc[
            invalid_mask, "player_name"
        ].astype(str)

    start_ticks = work["round_number"].map(round_start_by_number)
    anchor_window_ticks = max(int(anchor_window_seconds), 1) * TICKRATE
    in_anchor_window = (work["tick"] >= start_ticks) & (work["tick"] <= (start_ticks + anchor_window_ticks))
    early = work[in_anchor_window].copy()
    if early.empty:
        return pd.DataFrame(columns=["X", "Y"])

    early = early.sort_values(["round_number", "movement_key", "tick"])
    anchors = early.groupby(["round_number", "movement_key"], as_index=False).first()
    return anchors[["X", "Y"]].reset_index(drop=True)


def filter_spawn_proximity_points(
    ticks_df: pd.DataFrame,
    spawn_anchors_df: pd.DataFrame,
    radius_units: float = 320.0,
) -> pd.DataFrame:
    """Drop points that fall within configured radius of any inferred spawn anchor."""
    if ticks_df.empty or spawn_anchors_df.empty or radius_units <= 0:
        return ticks_df

    radius_sq = float(radius_units) ** 2
    mask = pd.Series(False, index=ticks_df.index)
    for anchor in spawn_anchors_df.itertuples(index=False):
        dist_sq = (ticks_df["X"] - float(anchor.X)).pow(2) + (ticks_df["Y"] - float(anchor.Y)).pow(2)
        mask = mask | (dist_sq <= radius_sq)

    filtered = ticks_df[~mask].copy()
    dropped = int(len(ticks_df) - len(filtered))
    if dropped > 0:
        logger.info("Dropped spawn-proximity ticks rows=%s radius=%s", dropped, radius_units)
    return filtered





def derive_round_windows_from_df(ticks_df: pd.DataFrame) -> list[tuple[int, int, int]]:
    """Derive per-round start/end tick windows from labeled telemetry."""
    if ticks_df.empty or "round_number" not in ticks_df.columns:
        return []

    windows: list[tuple[int, int, int]] = []
    grouped = ticks_df.groupby("round_number")["tick"].agg(["min", "max"]).reset_index()
    for row in grouped.itertuples(index=False):
        windows.append((int(row.min), int(row.max), int(row.round_number)))
    return windows


def detect_round_economy_type(
    round_number: int,
) -> str:
    """Classify round type: pistol or gun.

    Pistol rounds are rounds 1 and 13 (1-indexed) in a match.
    """
    if round_number in (1, 13):
        return "pistol"
    return "gun"


def classify_rounds_by_economy(
    ticks_df: pd.DataFrame,
) -> pd.DataFrame:
    """Add round_economy_type column to telemetry DataFrame."""
    if ticks_df.empty or "round_number" not in ticks_df.columns:
        return ticks_df

    result = ticks_df.copy()
    result["round_economy_type"] = result["round_number"].apply(
        lambda rn: detect_round_economy_type(int(rn))
    )
    return result


def apply_opening_phase_filter(
    ticks_df: pd.DataFrame,
    windows: list[tuple[int, int, int]],
    bomb_plant_ticks: list[int],
    opening_seconds_cap: int,
    opening_start_seconds_after_round_start: int = 20,
) -> pd.DataFrame:
    """Keep only opening-round positioning up to plant or configured early-round cap.

    If start offset is greater than or equal to the cap, clamp the effective cap
    to at least one second after start so the window remains valid and predictable.
    """
    if ticks_df.empty:
        return ticks_df

    if not windows:
        windows = derive_round_windows_from_df(ticks_df)
        if not windows:
            return ticks_df

    round_rows: list[dict[str, int]] = []
    for start_tick, end_tick, round_number in windows:
        first_plant_tick = None
        for plant_tick in bomb_plant_ticks:
            if start_tick <= plant_tick <= end_tick:
                first_plant_tick = plant_tick
                break

        start_offset_seconds = max(int(opening_start_seconds_after_round_start), 0)
        cap_offset_seconds = max(int(opening_seconds_cap), 1)
        # Keep cap semantics stable: it is always an absolute second offset from round start.
        effective_cap_seconds = max(cap_offset_seconds, start_offset_seconds + 1)

        opening_start_tick = start_tick + start_offset_seconds * TICKRATE
        early_cap_tick = start_tick + effective_cap_seconds * TICKRATE
        cutoff_tick = min(end_tick, early_cap_tick)

        if first_plant_tick is not None:
            cutoff_tick = min(cutoff_tick, first_plant_tick - 1)

        round_rows.append(
            {
                "round_number": int(round_number),
                "opening_start_tick": int(opening_start_tick),
                "opening_cutoff_tick": int(cutoff_tick),
            }
        )

    cutoff_df = pd.DataFrame(round_rows)
    merged = ticks_df.merge(cutoff_df, on="round_number", how="left")
    filtered = merged[
        (merged["tick"] >= merged["opening_start_tick"])
        & (merged["tick"] <= merged["opening_cutoff_tick"])
    ].copy()
    return filtered.drop(columns=["opening_start_tick", "opening_cutoff_tick"])


def extract_ct_telemetry(
    demo_path: str,
    side_filter: str = "CT",
    min_rounds_present: int = 1,
    opening_seconds_cap: int = 35,
    opening_start_seconds_after_round_start: int = 20,
    max_velocity_units_per_second: float = 200.0,
    exclude_spawn_locations: bool = True,
    spawn_exclusion_radius_units: float = 320.0,
    spawn_anchor_window_seconds: int = 12,
    round_economy_filter: str = "all",
) -> pd.DataFrame:
    """
    Parses a CS2 demo file and returns a downsampled DataFrame of active CT player positions.

    Args:
        round_economy_filter: Filter rounds by economy type.
            'all' = include all rounds
            'pistol' = rounds 1 and 13 only
            'gun' = all rounds except pistol
            'pistol,gun' = comma-separated for multiple types
    """
    parser = DemoParser(demo_path)
    logger.info(
        "Parsing telemetry demo=%s side_filter=%s min_rounds_present=%s opening_seconds_cap=%s max_velocity=%s",
        demo_path,
        side_filter,
        min_rounds_present,
        opening_seconds_cap,
        max_velocity_units_per_second,
    )

    # 1. Parse the tick data we need
    # We want Coordinates (X, Y, Z), Player Info, whether they are alive,
    # and optional round indicators for fallback labeling.
    core_fields = [
        "X",
        "Y",
        "Z",
        "team_name",
        "team_clan_name",
        "clan_name",
        "player_name",
        "steamid",
        "is_alive",
        "round_num",
        "round_number",
        "total_rounds_played",
        "m_totalRoundsPlayed",
    ]
    optional_state_fields = [
        "m_bFreezePeriod",
        "m_bTerroristTimeOutActive",
        "m_bCTTimeOutActive",
        "m_bTechnicalTimeOutActive",
        "m_bTechnicalTimeoutActive",
        "m_bTechnicalTimeOut",
        "m_bTechnicalTimeout",
        "m_bRoundInProgress",
        "m_bWarmupPeriod",
        "m_bGamePaused",
        "m_bMatchWaitingForResume",
    ]
    wanted_fields = core_fields + optional_state_fields
    try:
        df = parser.parse_ticks(wanted_fields)
    except Exception:
        logger.warning(
            "Optional state fields unsupported for demo=%s; retrying with core fields only",
            demo_path,
        )
        df = parser.parse_ticks(core_fields)

    required_columns = {"tick", "X", "Y", "Z", "team_name", "player_name", "steamid", "is_alive"}
    missing = required_columns.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns from parsed ticks: {sorted(missing)}")

    # 2. Filter for active players and requested side(s)
    df = df[df["is_alive"] == True].copy()
    df = filter_by_side(df, side_filter)
    df = filter_freeze_period_ticks(df)
    df = filter_timeout_ticks(df)
    df = filter_non_playing_state_ticks(df)

    if df.empty:
        logger.warning("No telemetry rows remain after state-based filtering demo=%s", demo_path)
        return pd.DataFrame(
            columns=[
                "tick",
                "round_number",
                "player_name",
                "steamid",
                "team_name",
                "X",
                "Y",
                "Z",
                "round_economy_type",
            ]
        )

    # 3. Downsample to 1 tick per second (CRITICAL)
    # CS2 servers run at 64 ticks per second. Modulo 64 == 0 grabs exactly 1 frame per second.
    df = df[df["tick"] % TICKRATE == 0]

    # 4. Build active-round windows using robust multi-event extraction.
    round_start_ticks = extract_round_start_ticks(parser)
    round_end_ticks = extract_ticks_for_events(parser, ROUND_END_EVENTS)
    windows = build_round_windows_from_ticks(
        round_start_ticks, round_end_ticks, int(df["tick"].max())
    )

    # Compatibility path for older flow/tests using round_freeze_end + round_end payloads.
    if not windows:
        freeze_ends = parse_event_rows(parser, "round_freeze_end")
        round_ends = parse_event_rows(parser, "round_end")
        windows = build_active_round_windows(freeze_ends, round_ends)

    if windows:
        df = annotate_round_numbers(df, windows)
    else:
        df = derive_round_numbers_from_tick_columns(df)
    # Classify rounds by economy type (pistol vs gun)
    df = classify_rounds_by_economy(df)


    round_windows = windows if windows else derive_round_windows_from_df(df)
    if exclude_spawn_locations:
        spawn_anchors = derive_spawn_anchor_points(
            df,
            windows=round_windows,
            anchor_window_seconds=spawn_anchor_window_seconds,
        )
        df = filter_spawn_proximity_points(
            df,
            spawn_anchors_df=spawn_anchors,
            radius_units=spawn_exclusion_radius_units,
        )

    bomb_plant_ticks = extract_ticks_for_events(parser, BOMB_PLANT_EVENTS)
    df = apply_opening_phase_filter(
        df,
        windows=round_windows,
        bomb_plant_ticks=bomb_plant_ticks,
        opening_seconds_cap=opening_seconds_cap,
        opening_start_seconds_after_round_start=opening_start_seconds_after_round_start,
    )

    effective_speed_cap = min(max(float(max_velocity_units_per_second), 0.0), 200.0)
    if effective_speed_cap <= 0:
        effective_speed_cap = 200.0

    df = filter_high_velocity_points(
        df,
        max_velocity_units_per_second=effective_speed_cap,
    )

    # Filter by round economy type
    if round_economy_filter and round_economy_filter.lower() != "all":
        allowed_types = [t.strip() for t in round_economy_filter.lower().split(",")]
        df = df[df["round_economy_type"].isin(allowed_types)].copy()

    df = filter_recurring_players(df, min_rounds_present=min_rounds_present)

    selected_columns = [
        "tick",
        "round_number",
        "player_name",
        "steamid",
        "team_name",
        "team_clan_name",
        "clan_name",
        "X",
        "Y",
        "Z",
        "round_economy_type",
    ]
    if df.empty:
        logger.warning("No telemetry rows remained after filtering for demo=%s", demo_path)
        return pd.DataFrame(columns=selected_columns)

    logger.info("Parsed telemetry rows=%s for demo=%s", len(df), demo_path)
    existing_columns = [column for column in selected_columns if column in df.columns]
    return df[existing_columns].reset_index(drop=True)
