# src/antistrat/ingestion/loader.py
import logging

import pandas as pd
from sqlalchemy import delete
from sqlalchemy.orm import Session

from ..db.models import Map, Match, Player, Round, Team, TickData
from ..db.session import SessionLocal
from ..utils.maps import game_to_pixel, get_map_metadata

logger = logging.getLogger(__name__)


def build_player_identity(raw_steamid: object, raw_player_name: object) -> tuple[str, str]:
    """Build stable player identity keys, even when steamid is missing."""
    name = str(raw_player_name).strip() if pd.notna(raw_player_name) else ""
    if not name or name.lower() in {"nan", "none", "null"}:
        name = "unknown_player"

    steam = str(raw_steamid).strip() if pd.notna(raw_steamid) else ""
    if steam and steam.lower() not in {"nan", "none", "null"} and steam not in {"0", "0.0"}:
        return steam, name

    slug = "".join(ch if ch.isalnum() else "_" for ch in name.lower()).strip("_")
    if not slug:
        slug = "unknown_player"
    return f"name:{slug}", name


def load_demo_data(demo_file_name: str, map_name: str, df: pd.DataFrame):
    """
    Takes the parsed DataFrame, calculates pixel coordinates, and bulk inserts into SQLite.
    """
    if df.empty:
        raise ValueError("Parsed telemetry is empty. Nothing to load.")

    required_columns = {"tick", "round_number", "player_name", "steamid", "X", "Y", "Z"}
    missing = required_columns.difference(df.columns)
    if missing:
        raise ValueError(f"Telemetry DataFrame is missing required columns: {sorted(missing)}")

    df_work = df.copy()
    identities = [
        build_player_identity(sid, pname)
        for sid, pname in zip(df_work["steamid"], df_work["player_name"])
    ]
    df_work["steam_key"] = [sid for sid, _ in identities]
    df_work["player_label"] = [pname for _, pname in identities]

    db: Session = SessionLocal()
    logger.info(
        "Starting DB load for demo=%s map=%s rows=%s", demo_file_name, map_name, len(df_work)
    )

    try:
        # 1. Ensure Map exists
        db_map = db.query(Map).filter(Map.map_name == map_name).first()
        if not db_map:
            map_meta = get_map_metadata(map_name)
            db_map = Map(
                map_name=map_name,
                pos_x=map_meta.pos_x,
                pos_y=map_meta.pos_y,
                scale=map_meta.scale,
                radar_image_path=map_meta.radar_image_path,
            )
            db.add(db_map)
            db.flush()

        # Remove previous loads for the same demo+map so re-parsing replaces stale data.
        existing_matches = (
            db.query(Match)
            .filter(Match.map_id == db_map.map_id, Match.demo_file_name == demo_file_name)
            .all()
        )
        if existing_matches:
            match_ids = [m.match_id for m in existing_matches]
            round_ids = [
                rid
                for (rid,) in db.query(Round.round_id)
                .filter(Round.match_id.in_(match_ids))
                .all()
            ]

            if round_ids:
                db.execute(delete(TickData).where(TickData.round_id.in_(round_ids)))
                db.execute(delete(Round).where(Round.round_id.in_(round_ids)))

            db.execute(delete(Match).where(Match.match_id.in_(match_ids)))
            db.flush()
            logger.info(
                "Replaced prior load for demo=%s map=%s prior_matches=%s",
                demo_file_name,
                map_name,
                len(match_ids),
            )

        # 2. Create the Match record
        db_match = Match(demo_file_name=demo_file_name, map_id=db_map.map_id)
        db.add(db_match)
        db.flush()

        # 3. Ensure side teams exist
        available_sides = sorted(
            side
            for side in df_work.get("team_name", pd.Series(dtype="object"))
            .dropna()
            .astype(str)
            .unique()
            .tolist()
            if side in {"CT", "T"}
        )
        if not available_sides:
            available_sides = ["CT"]

        existing_teams = db.query(Team).filter(Team.team_name.in_(available_sides)).all()
        team_by_name = {t.team_name: t for t in existing_teams}
        for side in available_sides:
            if side not in team_by_name:
                team = Team(team_name=side)
                db.add(team)
                db.flush()
                team_by_name[side] = team

        # 4. Create rounds based on parsed round numbers
        round_numbers = sorted(int(n) for n in df_work["round_number"].dropna().unique())
        round_rows = [
            Round(match_id=db_match.match_id, round_number=n, winner_side=None)
            for n in round_numbers
        ]
        db.add_all(round_rows)
        db.flush()
        round_id_by_number = {row.round_number: row.round_id for row in round_rows}

        # 5. Ensure players exist in a single pass
        steam_ids = sorted(df_work["steam_key"].astype(str).unique().tolist())
        existing_players = (
            db.query(Player).filter(Player.steam_id.in_(steam_ids)).all() if steam_ids else []
        )
        player_by_steam = {p.steam_id: p for p in existing_players}

        new_players: list[Player] = []
        for steam_id, player_name in (
            df_work[["steam_key", "player_label"]]
            .astype({"steam_key": str})
            .drop_duplicates(subset=["steam_key"])
            .itertuples(index=False, name=None)
        ):
            if steam_id not in player_by_steam:
                player_rows = df_work[df_work["steam_key"] == steam_id]
                preferred_side = None
                if "team_name" in player_rows.columns:
                    side_counts = player_rows["team_name"].dropna().astype(str).value_counts()
                    if not side_counts.empty:
                        preferred_side = side_counts.index[0]
                preferred_team = team_by_name.get(preferred_side) if preferred_side else None
                new_players.append(
                    Player(
                        steam_id=steam_id,
                        player_name=player_name,
                        team_id=preferred_team.team_id if preferred_team else None,
                    )
                )

        if new_players:
            db.add_all(new_players)
            db.flush()
            for player in new_players:
                player_by_steam[player.steam_id] = player

        # 6. Bulk build tick rows
        tick_objects: list[TickData] = []
        for row in df_work.itertuples(index=False):
            steam_id = str(row.steam_key)
            player = player_by_steam.get(steam_id)
            round_id = round_id_by_number.get(int(row.round_number))
            if player is None or round_id is None:
                continue

            px_x, px_y = game_to_pixel(
                game_x=float(row.X),
                game_y=float(row.Y),
                pos_x=float(db_map.pos_x),
                pos_y=float(db_map.pos_y),
                scale=float(db_map.scale),
            )

            tick_objects.append(
                TickData(
                    round_id=round_id,
                    player_id=player.player_id,
                    tick=int(row.tick),
                    pos_x=float(row.X),
                    pos_y=float(row.Y),
                    pos_z=float(row.Z),
                    pixel_x=px_x,
                    pixel_y=px_y,
                )
            )

        if tick_objects:
            db.bulk_save_objects(tick_objects)

        # Bulk insert is exponentially faster for thousands of rows!
        db.commit()
        logger.info(
            "DB load complete demo=%s map=%s rounds=%s players=%s ticks=%s",
            demo_file_name,
            map_name,
            len(round_numbers),
            int(df_work["steam_key"].nunique()),
            len(tick_objects),
        )
        return {
            "match_id": db_match.match_id,
            "round_count": len(round_numbers),
            "player_count": int(df_work["steam_key"].nunique()),
            "tick_count": len(tick_objects),
            "players": sorted(df_work["player_label"].dropna().astype(str).unique().tolist()),
        }

    except Exception as e:
        db.rollback()
        logger.exception("DB load failed for demo=%s", demo_file_name)
        raise RuntimeError(f"Error during data load: {e}") from e
    finally:
        db.close()
