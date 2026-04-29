# src/antistrat/api/main.py
import inspect
import logging
import os
import re
import sys
from pathlib import Path
from uuid import uuid4

import pandas as pd
import streamlit as st
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
src_root_str = str(SRC_ROOT)
if src_root_str in sys.path:
    sys.path.remove(src_root_str)
sys.path.insert(0, src_root_str)

from antistrat.db.session import engine, init_db, reset_db
from antistrat.ingestion.loader import load_demo_data
from antistrat.ingestion.parser import detect_demo_map_name, extract_ct_telemetry
from antistrat.utils.maps import get_map_analysis_profile
from antistrat.utils.logging_config import configure_logging, configure_sentry
from antistrat.viz.radar import plot_radar_positions

configure_sentry()
configure_logging()
logger = logging.getLogger(__name__)

# Initialize the database tables if they don't exist yet
init_db()
logger.info("Database initialized")

st.set_page_config(page_title="CS2 Anti-Strat Tool", layout="wide")

st.title("🛡️ CS2 Anti-Strat Tool")
st.markdown("Upload a demo file to extract and visualize CT setups.")

if "last_ingestion_summary" not in st.session_state:
    st.session_state["last_ingestion_summary"] = None
if "selected_map_name" not in st.session_state:
    st.session_state["selected_map_name"] = None
if "selected_team_name" not in st.session_state:
    st.session_state["selected_team_name"] = None
if "selected_team_identity_source" not in st.session_state:
    st.session_state["selected_team_identity_source"] = None
if "selected_team_signature" not in st.session_state:
    st.session_state["selected_team_signature"] = None
if "pending_ingestion" not in st.session_state:
    st.session_state["pending_ingestion"] = None
if "pending_team_choice" not in st.session_state:
    st.session_state["pending_team_choice"] = None
if st.session_state.pop("clear_map_override_input", False):
    st.session_state.pop("map_override_input", None)

RAW_DEMO_DIR = PROJECT_ROOT / "data" / "raw_demos"
RAW_DEMO_DIR.mkdir(parents=True, exist_ok=True)


def build_safe_demo_filename(raw_name: str) -> str:
    """Return a Windows-safe .dem filename for temporary upload storage."""
    base_name = Path(str(raw_name or "")).name.strip()
    stem = Path(base_name).stem
    safe_stem = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", stem).strip(" .")
    if not safe_stem:
        safe_stem = f"uploaded_demo_{uuid4().hex[:8]}"
    return f"{safe_stem}.dem"


def _build_player_identity_key(raw_steamid: object, raw_player_name: object) -> str:
    steam = str(raw_steamid).strip() if pd.notna(raw_steamid) else ""
    if steam and steam.lower() not in {"nan", "none", "null"} and steam not in {"0", "0.0"}:
        return steam

    name = str(raw_player_name).strip().lower() if pd.notna(raw_player_name) else ""
    slug = "".join(ch if ch.isalnum() else "_" for ch in name).strip("_")
    if not slug:
        slug = "unknown_player"
    return f"name:{slug}"


def build_team_signature(parsed_df: pd.DataFrame, top_n: int = 6) -> list[str]:
    """Build a stable fallback roster signature from most-seen player identities."""
    if parsed_df.empty:
        return []

    identities = [
        _build_player_identity_key(sid, pname)
        for sid, pname in zip(parsed_df["steamid"], parsed_df["player_name"])
    ]
    counts = pd.Series(identities).value_counts()
    return counts.head(max(int(top_n), 1)).index.astype(str).tolist()


def _normalize_team_name_value(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.lower() in {"nan", "none", "null", "ct", "t", "both"}:
        return None

    return text


def _canonical_team_name(value: object) -> str | None:
    """Normalize team names so common variants compare equal.

    Examples:
    - "Team Falcons" -> "falcons"
    - "Falcons" -> "falcons"
    - "Falcons Esports" -> "falcons"
    """
    normalized = _normalize_team_name_value(value)
    if not normalized:
        return None

    text = normalized.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [token for token in text.split() if token]
    stopwords = {
        "team",
        "gaming",
        "esports",
        "esport",
        "club",
        "cs",
        "cs2",
        "the",
    }
    filtered_tokens = [token for token in tokens if token not in stopwords]
    if not filtered_tokens:
        filtered_tokens = tokens
    return " ".join(filtered_tokens) if filtered_tokens else None


def extract_team_name_candidates(parsed_df: pd.DataFrame) -> list[str]:
    """Extract likely team names from demoparser team-name fields."""
    candidates: list[str] = []
    for column in ("team_clan_name", "clan_name", "team_name"):
        if column not in parsed_df.columns:
            continue

        values = parsed_df[column].map(_normalize_team_name_value).dropna().astype(str).tolist()
        for value in values:
            if value not in candidates:
                candidates.append(value)

    return candidates


def build_team_identity(parsed_df: pd.DataFrame) -> tuple[str, str]:
    """Derive a team identity from demoparser team-name fields when available."""
    for column in ("team_clan_name", "clan_name", "team_name"):
        if column not in parsed_df.columns:
            continue

        values = parsed_df[column].map(_normalize_team_name_value).dropna()
        if values.empty:
            continue

        counts = values.value_counts()
        if not counts.empty:
            return str(counts.index[0]), column

    return ",".join(build_team_signature(parsed_df)), "player_signature"


def team_identities_match(locked: str, candidate: str) -> bool:
    locked_key = _canonical_team_name(locked)
    candidate_key = _canonical_team_name(candidate)
    if not locked_key or not candidate_key:
        return False
    return locked_key == candidate_key


def team_signatures_match(locked: list[str], candidate: list[str]) -> tuple[bool, list[str], int]:
    """Return whether candidate roster is compatible with locked fallback roster signature."""
    locked_set = {str(x) for x in locked}
    candidate_set = {str(x) for x in candidate}
    overlap = sorted(locked_set.intersection(candidate_set))
    min_size = min(len(locked_set), len(candidate_set))
    required_overlap = min(3, max(min_size, 1))
    return len(overlap) >= required_overlap, overlap, required_overlap


def filter_rows_to_selected_team(parsed_df: pd.DataFrame, selected_team_name: str) -> pd.DataFrame:
    """Keep only telemetry rows that match the selected team identity."""
    if parsed_df.empty:
        return parsed_df

    canonical_target = _canonical_team_name(selected_team_name)
    if not canonical_target:
        return parsed_df

    team_columns = [
        col for col in ("team_clan_name", "clan_name", "team_name") if col in parsed_df.columns
    ]
    if not team_columns:
        return parsed_df

    mask = pd.Series(False, index=parsed_df.index)
    for column in team_columns:
        canonical_values = parsed_df[column].map(_canonical_team_name)
        mask = mask | (canonical_values == canonical_target)

    filtered = parsed_df[mask].copy()
    dropped = int(len(parsed_df) - len(filtered))
    if dropped > 0:
        logger.info(
            "Dropped non-selected-team rows=%s selected_team=%s", dropped, selected_team_name
        )
    return filtered


# --- Sidebar: File Upload & Ingestion ---
with st.sidebar:
    st.header("1. Upload Demo")
    uploaded_file = st.file_uploader("Choose a .dem file", type="dem")

    selected_map_name = st.session_state.get("selected_map_name")
    if selected_map_name:
        st.info(f"Selected map for this session: {selected_map_name}")
    selected_team_name = st.session_state.get("selected_team_name")
    if selected_team_name:
        st.info(f"Selected team for this session: {selected_team_name}")

    pending_ingestion = st.session_state.get("pending_ingestion")
    if pending_ingestion:
        st.subheader("1a. Confirm Team")
        team_options = pending_ingestion.get("team_candidates") or pending_ingestion.get(
            "fallback_team_signature", []
        )
        if not team_options:
            team_options = ["unknown_team"]

        default_team = st.session_state.get("pending_team_choice")
        if default_team not in team_options:
            default_team = team_options[0]

        pending_choice = st.selectbox(
            "Team to track for the pending demo",
            options=team_options,
            index=team_options.index(default_team),
            key="pending_team_choice",
        )

        if st.button("Confirm Team & Load Pending Demo"):
            chosen_team = (pending_choice or "").strip()
            if not chosen_team:
                st.error("Choose a team before continuing.")
                st.stop()

            pending_ingestion = st.session_state.pop("pending_ingestion")
            st.session_state["selected_team_name"] = chosen_team
            st.session_state["selected_team_identity_source"] = pending_ingestion.get(
                "team_identity_source"
            )
            st.session_state["selected_team_signature"] = pending_ingestion.get(
                "fallback_team_signature", []
            )

            selected_map_name = st.session_state.get("selected_map_name")
            resolved_map_name = pending_ingestion["resolved_map_name"]
            if selected_map_name is None:
                st.session_state["selected_map_name"] = resolved_map_name
                selected_map_name = resolved_map_name
            elif resolved_map_name != selected_map_name:
                raise ValueError(
                    "All ingested demos must be the same map. "
                    f"Selected map is '{selected_map_name}', but this demo resolved to '{resolved_map_name}'."
                )

            load_summary = load_demo_data(
                demo_file_name=pending_ingestion["demo_file_name"],
                map_name=resolved_map_name,
                df=pending_ingestion["df_telemetry"],
            )
            parsed_rounds = pending_ingestion["parsed_rounds"]
            parsed_players = pending_ingestion["parsed_players"]
            parsed_sides = pending_ingestion["parsed_sides"]
            if not isinstance(load_summary, dict):
                df_pending = pending_ingestion["df_telemetry"]
                load_summary = {
                    "tick_count": int(len(df_pending)),
                    "player_count": int(df_pending["player_name"].nunique())
                    if not df_pending.empty
                    else 0,
                    "round_count": int(df_pending["round_number"].nunique())
                    if not df_pending.empty
                    else 0,
                    "players": parsed_players,
                }

            st.caption(
                "Loaded "
                f"{load_summary.get('tick_count', 0)} ticks, "
                f"{load_summary.get('player_count', 0)} players, "
                f"{load_summary.get('round_count', 0)} rounds."
            )
            st.session_state["last_ingestion_summary"] = {
                "demo_file_name": pending_ingestion["demo_file_name"],
                "map_name": resolved_map_name,
                "detected_map_name": pending_ingestion.get("detected_map_name"),
                "map_override_name": pending_ingestion.get("map_override_name"),
                "side_filter": pending_ingestion["side_filter"],
                "team_name": chosen_team,
                "team_identity_source": pending_ingestion.get("team_identity_source"),
                "min_rounds_present": pending_ingestion["min_rounds_present"],
                "opening_start_seconds_after_round_start": pending_ingestion[
                    "opening_start_seconds_after_round_start"
                ],
                "opening_seconds_cap": pending_ingestion["opening_seconds_cap"],
                "max_velocity_units_per_second": pending_ingestion["max_velocity_units_per_second"],
                "round_economy_filter": pending_ingestion["round_economy_filter"],
                "parsed_tick_count": len(pending_ingestion["df_telemetry"]),
                "parsed_rounds": parsed_rounds,
                "parsed_players": parsed_players,
                "parsed_sides": parsed_sides,
                "loaded_tick_count": load_summary.get("tick_count", 0),
                "loaded_player_count": load_summary.get("player_count", 0),
                "loaded_round_count": load_summary.get("round_count", 0),
                "loaded_players": load_summary.get("players", parsed_players),
            }
            st.session_state["pending_ingestion"] = None
            st.success("Pending demo loaded with confirmed team selection.")

    map_override_input = st.text_input(
        "Map Name Override (optional)",
        value=selected_map_name or "",
        key="map_override_input",
        help="Leave blank to autodetect map from demo header metadata.",
    )

    controls_map_name = (
        (map_override_input or "").strip().lower()
        or str(selected_map_name or "").strip().lower()
        or "de_mirage"
    )
    map_profile = get_map_analysis_profile(controls_map_name)
    st.caption(f"Map control profile: {controls_map_name}")

    side_filter = st.selectbox("Side filter", options=["CT", "T", "Both"], index=0)
    round_economy_filter = st.multiselect(
        "Round type filter",
        options=["pistol", "gun"],
        default=["pistol", "gun"],
        help="Pistol rounds are rounds 1 & 13. Gun rounds are all others.",
    )
    round_economy_filter_str = ",".join(round_economy_filter) if round_economy_filter else "all"

    min_rounds_present = st.slider(
        "Keep players seen in at least N rounds",
        min_value=1,
        max_value=15,
        value=1,
        help="Use values >1 to focus on recurring players and reduce one-off noise.",
    )
    opening_start_seconds_after_round_start = int(
        st.number_input(
            "Ignore first seconds after round start",
            min_value=0,
            value=int(getattr(map_profile, "opening_start_seconds_after_round_start", 20)),
            step=1,
            help="Skip the freeze-time / spawn transition window at the start of each round. Type any value for debugging.",
        )
    )
    opening_seconds_cap = int(
        st.number_input(
            "Opening phase window (seconds after round start)",
            min_value=1,
            value=int(map_profile.opening_seconds_cap),
            step=1,
            help="Only keep positioning up to this time cap (or earlier if bomb is planted). Type any value for debugging. If start exclusion is >= cap, effective cap is auto-clamped to start+1s.",
        )
    )
    max_velocity_units_per_second = st.slider(
        "Max movement speed (units/second)",
        min_value=0,
        max_value=400,
        value=map_profile.max_velocity_units_per_second,
        help="Drop points where a player is moving faster than this (0 disables the filter).",
    )

    if st.button("Parse & Load Demo") and uploaded_file is not None:
        with st.spinner("Parsing demo file... This might take a minute."):
            # Streamlit uploads are held in memory, so persist under data/raw_demos first.
            tmp_file_name = build_safe_demo_filename(uploaded_file.name)
            tmp_file_path = RAW_DEMO_DIR / tmp_file_name
            tmp_file_path.write_bytes(uploaded_file.getvalue())
            logger.info("Saved uploaded demo to %s", tmp_file_path)

            try:
                # 1. Parse the telemetry
                parser_kwargs = {
                    "side_filter": side_filter,
                    "min_rounds_present": min_rounds_present,
                    "opening_seconds_cap": opening_seconds_cap,
                    "opening_start_seconds_after_round_start": opening_start_seconds_after_round_start,
                }
                optional_kwargs = {
                    "max_velocity_units_per_second": max_velocity_units_per_second,
                    "round_economy_filter": round_economy_filter_str,
                }
                signature_params = inspect.signature(extract_ct_telemetry).parameters
                for key, value in optional_kwargs.items():
                    if key in signature_params:
                        parser_kwargs[key] = value
                    else:
                        logger.warning(
                            "Parser signature does not support %s; continuing without it",
                            key,
                        )

                df_telemetry = extract_ct_telemetry(str(tmp_file_path), **parser_kwargs)
                if df_telemetry.empty:
                    st.warning(
                        "No telemetry remained after applying the freeze-time filter. "
                        "Try lowering the start exclusion or use a demo with movement after freeze time."
                    )
                    st.stop()
                parsed_rounds = (
                    sorted(int(x) for x in df_telemetry["round_number"].dropna().unique().tolist())
                    if not df_telemetry.empty
                    else []
                )
                parsed_players = (
                    sorted(df_telemetry["player_name"].dropna().astype(str).unique().tolist())
                    if not df_telemetry.empty
                    else []
                )
                parsed_sides = (
                    sorted(df_telemetry["team_name"].dropna().astype(str).unique().tolist())
                    if not df_telemetry.empty
                    else []
                )
                st.info(
                    f"Parsed {len(df_telemetry)} ticks across {len(parsed_rounds)} round(s): {parsed_rounds[:12]}"
                )
                st.caption(f"Parsed players ({len(parsed_players)}): {parsed_players[:12]}")
                st.caption(f"Parsed side(s): {parsed_sides}")

                # 2. Resolve map name (autodetect first, optional manual override)
                detected_map_name = detect_demo_map_name(str(tmp_file_path))
                map_override = map_override_input.strip().lower()
                resolved_map_name = map_override or detected_map_name
                if not resolved_map_name:
                    raise ValueError(
                        "Could not autodetect map from demo header. "
                        "Provide a Map Name Override (e.g., de_mirage)."
                    )

                if map_override:
                    st.caption(
                        f"Map override applied: {resolved_map_name} "
                        f"(autodetected: {detected_map_name or 'n/a'})"
                    )
                else:
                    st.caption(f"Autodetected map: {resolved_map_name}")

                team_candidates = extract_team_name_candidates(df_telemetry)
                fallback_team_signature = build_team_signature(df_telemetry)
                team_identity_source = "team_name"
                candidate_team_name = team_candidates[0] if team_candidates else ""
                locked_team_name = st.session_state.get("selected_team_name")
                need_team_selection = False

                if locked_team_name is None:
                    need_team_selection = True
                else:
                    if team_candidates:
                        if not any(
                            team_identities_match(locked_team_name, candidate)
                            for candidate in team_candidates
                        ):
                            raise ValueError(
                                "All ingested demos must be from the same team. "
                                f"Locked team is '{locked_team_name}', but this demo exposed team names {team_candidates[:6]}."
                            )
                    elif not team_identities_match(
                        locked_team_name, ",".join(fallback_team_signature)
                    ):
                        raise ValueError(
                            "All ingested demos must be from the same team. "
                            f"Locked team is '{locked_team_name}', but this demo could not resolve a matching team name."
                        )

                locked_map_name = st.session_state.get("selected_map_name")
                if locked_map_name is None:
                    st.session_state["selected_map_name"] = resolved_map_name
                    locked_map_name = resolved_map_name
                    st.caption(f"Selected map set to: {locked_map_name}")
                elif resolved_map_name != locked_map_name:
                    raise ValueError(
                        "All ingested demos must be the same map. "
                        f"Selected map is '{locked_map_name}', but this demo resolved to '{resolved_map_name}'."
                    )

                if need_team_selection:
                    st.session_state["pending_ingestion"] = {
                        "demo_file_name": uploaded_file.name,
                        "resolved_map_name": resolved_map_name,
                        "detected_map_name": detected_map_name,
                        "map_override_name": map_override or None,
                        "df_telemetry": df_telemetry,
                        "side_filter": side_filter,
                        "team_candidates": team_candidates,
                        "fallback_team_signature": fallback_team_signature,
                        "team_identity_source": team_identity_source,
                        "min_rounds_present": min_rounds_present,
                        "opening_start_seconds_after_round_start": opening_start_seconds_after_round_start,
                        "opening_seconds_cap": opening_seconds_cap,
                        "max_velocity_units_per_second": max_velocity_units_per_second,
                        "round_economy_filter": round_economy_filter_str,
                        "parsed_rounds": parsed_rounds,
                        "parsed_players": parsed_players,
                        "parsed_sides": parsed_sides,
                    }
                    st.info(
                        "Team detection is available for this demo. Choose the team to track in the sidebar, then confirm."
                    )
                    st.rerun()

                # 3. Load it into the database
                load_summary = load_demo_data(
                    demo_file_name=uploaded_file.name,
                    map_name=resolved_map_name,
                    df=df_telemetry,
                )
                logger.info(
                    "Loaded demo=%s map=%s ticks=%s",
                    uploaded_file.name,
                    resolved_map_name,
                    len(df_telemetry),
                )

                if not isinstance(load_summary, dict):
                    load_summary = {
                        "tick_count": int(len(df_telemetry)),
                        "player_count": int(df_telemetry["player_name"].nunique())
                        if not df_telemetry.empty
                        else 0,
                        "round_count": int(df_telemetry["round_number"].nunique())
                        if not df_telemetry.empty
                        else 0,
                        "players": parsed_players,
                    }

                st.caption(
                    "Loaded "
                    f"{load_summary.get('tick_count', 0)} ticks, "
                    f"{load_summary.get('player_count', 0)} players, "
                    f"{load_summary.get('round_count', 0)} rounds."
                )
                st.session_state["last_ingestion_summary"] = {
                    "demo_file_name": uploaded_file.name,
                    "map_name": resolved_map_name,
                    "detected_map_name": detected_map_name,
                    "map_override_name": map_override or None,
                    "side_filter": side_filter,
                    "team_name": st.session_state.get("selected_team_name"),
                    "team_identity_source": st.session_state.get("selected_team_identity_source"),
                    "min_rounds_present": min_rounds_present,
                    "opening_start_seconds_after_round_start": opening_start_seconds_after_round_start,
                    "opening_seconds_cap": opening_seconds_cap,
                    "max_velocity_units_per_second": max_velocity_units_per_second,
                    "round_economy_filter": round_economy_filter_str,
                    "parsed_tick_count": len(df_telemetry),
                    "parsed_rounds": parsed_rounds,
                    "parsed_players": parsed_players,
                    "parsed_sides": parsed_sides,
                    "loaded_tick_count": load_summary.get("tick_count", 0),
                    "loaded_player_count": load_summary.get("player_count", 0),
                    "loaded_round_count": load_summary.get("round_count", 0),
                    "loaded_players": load_summary.get("players", parsed_players),
                }
                st.success("Demo successfully parsed and loaded into the database!")
            except Exception as e:
                logger.exception("Parse/load failed for %s", uploaded_file.name)
                st.error(f"An error occurred: {e}")
            finally:
                # Clean up the temporary file
                tmp_file_path.unlink(missing_ok=True)
                logger.info("Removed temporary file %s", tmp_file_path)

    if st.session_state["last_ingestion_summary"] is not None:
        summary = st.session_state["last_ingestion_summary"]
        with st.expander("Last Ingestion Summary", expanded=False):
            st.write(f"Demo: {summary['demo_file_name']}")
            st.write(f"Map: {summary['map_name']}")
            st.write(f"Autodetected map: {summary.get('detected_map_name')}")
            st.write(f"Map override: {summary.get('map_override_name')}")
            st.write(f"Side filter: {summary.get('side_filter', 'CT')}")
            st.write(f"Team name: {summary.get('team_name')}")
            st.write(f"Team identity source: {summary.get('team_identity_source')}")
            st.write(f"Min recurring rounds: {summary.get('min_rounds_present', 1)}")
            st.write(
                "Opening start exclusion: "
                f"{summary.get('opening_start_seconds_after_round_start', 20)} seconds"
            )
            st.write(f"Opening window cap: {summary.get('opening_seconds_cap', 35)} seconds")
            st.write(
                "Max movement speed: "
                f"{summary.get('max_velocity_units_per_second', 150)} units/second"
            )
            st.write(f"Round type filter: {summary.get('round_economy_filter', 'all')}")
            st.write(
                "Parsed: "
                f"{summary['parsed_tick_count']} ticks, "
                f"{len(summary['parsed_rounds'])} rounds"
            )
            st.write(f"Parsed side(s): {summary.get('parsed_sides', [])}")
            st.write(
                "Loaded: "
                f"{summary['loaded_tick_count']} ticks, "
                f"{summary['loaded_player_count']} players, "
                f"{summary['loaded_round_count']} rounds"
            )
            st.write(f"Parsed players ({len(summary['parsed_players'])}):")
            st.write(summary["parsed_players"])
            st.write(f"Loaded players ({len(summary['loaded_players'])}):")
            st.write(summary["loaded_players"])

    st.divider()
    st.header("Testing Tools")
    with st.expander("Danger Zone: Reset Database"):
        confirm_reset = st.checkbox("I understand this permanently deletes all ingested data.")
        if st.button("Reset DB", type="primary"):
            if not confirm_reset:
                st.warning("Check the confirmation box before resetting the database.")
            else:
                try:
                    reset_db()
                    st.session_state["selected_map_name"] = None
                    st.session_state["selected_team_name"] = None
                    st.session_state["selected_team_identity_source"] = None
                    st.session_state["selected_team_signature"] = None
                    st.session_state["last_ingestion_summary"] = None
                    st.session_state["clear_map_override_input"] = True
                    logger.warning("Database reset requested by UI user")
                    st.success(
                        "Database reset complete. Map lock cleared; you can ingest demos for a different map now."
                    )
                    st.rerun()
                except Exception as e:
                    logger.exception("Database reset failed")
                    st.error(f"Failed to reset database: {e}")

# --- Main View: Data Querying & Visualization ---
st.header("2. Analyze CT Positions")

if st.session_state["last_ingestion_summary"] is not None:
    summary = st.session_state["last_ingestion_summary"]
    with st.expander("Most Recent Parsed Players", expanded=False):
        st.write(f"Demo: {summary['demo_file_name']}")
        st.write(summary["parsed_players"])

# We use pandas.read_sql to query our SQLite database directly
query = text("""
    SELECT
        td.pixel_x,
        td.pixel_y,
        p.player_name,
        p.steam_id,
        m.map_name,
        r.round_number,
        mt.demo_file_name
    FROM tick_data td
    JOIN players p ON td.player_id = p.player_id
    JOIN rounds r ON td.round_id = r.round_id
    JOIN matches mt ON r.match_id = mt.match_id
    JOIN maps m ON mt.map_id = m.map_id
    WHERE m.map_name = :map_name
""")

analysis_map_name = str(st.session_state.get("selected_map_name") or "").strip().lower()
if not analysis_map_name:
    analysis_map_name = map_override_input.strip().lower()
if not analysis_map_name and st.session_state["last_ingestion_summary"] is not None:
    analysis_map_name = str(st.session_state["last_ingestion_summary"].get("map_name", "")).strip()
if not analysis_map_name:
    analysis_map_name = "de_mirage"

try:
    # Read data directly from the SQLAlchemy engine into a DataFrame
    df_results = pd.read_sql(query, con=engine, params={"map_name": analysis_map_name})

    if not df_results.empty:
        st.write(f"Found {len(df_results)} position data points for {analysis_map_name}.")

        demos = sorted(df_results["demo_file_name"].dropna().unique().tolist())
        selected_demos = st.multiselect("Filter by Demo(s)", options=demos, default=demos)

        rounds = sorted(int(x) for x in df_results["round_number"].dropna().unique().tolist())
        testing_mode = st.checkbox("Testing mode: enable round filter", value=False)
        if testing_mode:
            selected_rounds = st.multiselect("Filter by Round(s)", options=rounds, default=rounds)
            if len(rounds) == 1:
                st.caption(
                    "Only one round is currently present for this map/demo selection. "
                    "If you recently updated parser logic, use Reset DB and re-ingest to refresh round labels."
                )
        else:
            selected_rounds = rounds

        # Optional: Add a filter for specific players
        players = sorted(df_results["player_name"].dropna().unique().tolist())
        selected_players = st.multiselect("Filter by Player(s)", options=players, default=players)

        active_profile = get_map_analysis_profile(analysis_map_name)
        use_density_heatmap = st.checkbox(
            "Render as density heatmap",
            value=active_profile.use_density_heatmap,
            help="Hotter colors indicate spots where players appear more often.",
        )
        heatmap_grid_size = st.slider(
            "Heatmap detail",
            min_value=40,
            max_value=180,
            value=active_profile.heatmap_grid_size,
            step=5,
            help="Higher values show finer cell detail; lower values smooth the map.",
        )
        heatmap_palette = st.selectbox(
            "Heatmap palette",
            options=["jet", "turbo", "inferno", "hot"],
            index=["jet", "turbo", "inferno", "hot"].index(active_profile.heatmap_color_map)
            if active_profile.heatmap_color_map in ["jet", "turbo", "inferno", "hot"]
            else 0,
            help="jet gives a classic blue->yellow->red heatmap ramp.",
        )

        filtered_df = df_results[
            df_results["demo_file_name"].isin(selected_demos)
            & df_results["round_number"].isin(selected_rounds)
            & df_results["player_name"].isin(selected_players)
        ]

        if filtered_df.empty:
            st.info("No points match your current filters.")
        else:
            st.write(f"Showing {len(filtered_df)} filtered points.")
            # Generate and display the plot
            fig = plot_radar_positions(
                filtered_df,
                analysis_map_name,
                dot_size=active_profile.dot_size,
                dot_alpha=active_profile.dot_alpha,
                dot_quantization_px=active_profile.dot_quantization_px,
                dot_color=active_profile.dot_color,
                use_density_heatmap=use_density_heatmap,
                heatmap_grid_size=heatmap_grid_size,
                heatmap_alpha=active_profile.heatmap_alpha,
                heatmap_color_map=heatmap_palette or active_profile.heatmap_color_map,
            )
            st.pyplot(fig)

    else:
        st.info("No data found in the database for this map. Please upload and parse a demo first.")

except Exception as e:
    logger.exception("Database query/render failed")
    st.error(
        f"Could not load data from database. Ensure the tables are set up correctly. Error: {e}"
    )
