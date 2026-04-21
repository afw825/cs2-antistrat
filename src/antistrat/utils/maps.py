from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MapMetadata:
    pos_x: float
    pos_y: float
    scale: float
    radar_image_path: str | None = None


@dataclass(frozen=True)
class MapAnalysisProfile:
    opening_start_seconds_after_round_start: int = 25
    opening_seconds_cap: int = 30
    max_velocity_units_per_second: int = 150
    dot_size: int = 15
    dot_alpha: float = 0.4
    dot_quantization_px: float = 2.0
    dot_color: str = "cyan"
    use_density_heatmap: bool = True
    heatmap_grid_size: int = 90
    heatmap_alpha: float = 0.7
    heatmap_color_map: str = "jet"


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RADAR_DIR = PROJECT_ROOT / "data" / "radar_images"

KNOWN_MAPS: dict[str, MapMetadata] = {
    # Coordinates/scales are practical defaults and may need map-image-specific tuning.
    "de_anubis": MapMetadata(
        pos_x=-2796.0,
        pos_y=3328.0,
        scale=5.2,
        radar_image_path=(RADAR_DIR / "de_anubis.png").as_posix(),
    ),
    "de_ancient": MapMetadata(
        pos_x=-2953.0,
        pos_y=2164.0,
        scale=5.0,
        radar_image_path=(RADAR_DIR / "de_ancient.png").as_posix(),
    ),
    "de_dust2": MapMetadata(
        pos_x=-2476.0,
        pos_y=3239.0,
        scale=4.4,
        radar_image_path=(RADAR_DIR / "de_dust2.png").as_posix(),
    ),
    "de_inferno": MapMetadata(
        pos_x=-2087.0,
        pos_y=3870.0,
        scale=4.9,
        radar_image_path=(RADAR_DIR / "de_inferno.png").as_posix(),
    ),
    "de_mirage": MapMetadata(
        pos_x=-3230.0,
        pos_y=1713.0,
        scale=5.0,
        radar_image_path=(RADAR_DIR / "de_mirage.png").as_posix(),
    ),
    "de_nuke": MapMetadata(
        pos_x=-3453.0,
        pos_y=2887.0,
        scale=7.0,
        radar_image_path=(RADAR_DIR / "de_nuke.png").as_posix(),
    ),
    "de_overpass": MapMetadata(
        pos_x=-4831.0,
        pos_y=1781.0,
        scale=5.2,
        radar_image_path=(RADAR_DIR / "de_overpass.png").as_posix(),
    ),
    "de_train": MapMetadata(
        pos_x=-2477.0,
        pos_y=2392.0,
        scale=4.7,
        radar_image_path=(RADAR_DIR / "de_train.png").as_posix(),
    ),
    "de_vertigo": MapMetadata(
        pos_x=-3168.0,
        pos_y=1762.0,
        scale=4.0,
        radar_image_path=(RADAR_DIR / "de_vertigo.png").as_posix(),
    ),
}


DEFAULT_ANALYSIS_PROFILE = MapAnalysisProfile()


MAP_ANALYSIS_PROFILES: dict[str, MapAnalysisProfile] = {
    # These are baseline presets tuned for opening CT setup analysis.
    "de_anubis": MapAnalysisProfile(opening_seconds_cap=30, max_velocity_units_per_second=150),
    "de_ancient": MapAnalysisProfile(opening_seconds_cap=34, max_velocity_units_per_second=150),
    "de_dust2": MapAnalysisProfile(opening_seconds_cap=28, max_velocity_units_per_second=150),
    "de_inferno": MapAnalysisProfile(opening_seconds_cap=36, max_velocity_units_per_second=150),
    "de_mirage": MapAnalysisProfile(opening_seconds_cap=32, max_velocity_units_per_second=150),
    "de_nuke": MapAnalysisProfile(
        opening_seconds_cap=38,
        max_velocity_units_per_second=150,
        dot_size=14,
        dot_alpha=0.35,
        dot_quantization_px=2.5,
    ),
    "de_overpass": MapAnalysisProfile(
        opening_seconds_cap=35,
        max_velocity_units_per_second=150,
        dot_quantization_px=2.5,
    ),
    "de_train": MapAnalysisProfile(opening_seconds_cap=31, max_velocity_units_per_second=150),
    "de_vertigo": MapAnalysisProfile(
        opening_seconds_cap=33,
        max_velocity_units_per_second=150,
    ),
}


def get_map_metadata(map_name: str) -> MapMetadata:
    """Return known map metadata or conservative defaults if unknown."""
    return KNOWN_MAPS.get(map_name, MapMetadata(pos_x=0.0, pos_y=0.0, scale=5.0))


def get_map_analysis_profile(map_name: str) -> MapAnalysisProfile:
    """Return map-level parser and heatmap defaults used by the UI."""
    return MAP_ANALYSIS_PROFILES.get(map_name, DEFAULT_ANALYSIS_PROFILE)


def game_to_pixel(
    game_x: float, game_y: float, pos_x: float, pos_y: float, scale: float
) -> tuple[float, float]:
    """Translate CS2 world coordinates to radar pixel coordinates."""
    pixel_x = (game_x - pos_x) / scale
    pixel_y = (pos_y - game_y) / scale
    return pixel_x, pixel_y
