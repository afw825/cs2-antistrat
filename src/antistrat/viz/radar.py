# src/antistrat/viz/radar.py
import os

import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image


def _quantize_series(series: pd.Series, step: float) -> pd.Series:
    if step <= 0:
        return series
    return (series / step).round() * step


def plot_radar_positions(
    df: pd.DataFrame,
    map_name: str,
    dot_size: int = 15,
    dot_alpha: float = 0.4,
    dot_quantization_px: float = 0.0,
    dot_color: str = "cyan",
    use_density_heatmap: bool = True,
    heatmap_grid_size: int = 90,
    heatmap_alpha: float = 0.7,
    heatmap_color_map: str = "inferno",
) -> plt.Figure:
    """
    Takes a DataFrame of pixel coordinates and overlays them on the corresponding map radar image.
    """
    # Create the Matplotlib figure
    fig, ax = plt.subplots(figsize=(10, 10), dpi=150)

    # Path to your radar images (Make sure you have a map_name.png in this folder!)
    image_path = os.path.join(os.getcwd(), "data", "radar_images", f"{map_name}.png")

    try:
        # Load and display the map image
        img = Image.open(image_path)
        ax.imshow(img)

        plot_df = df.copy()
        if dot_quantization_px > 0:
            # Snap points to a map-level grid for consistent, repeatable placement.
            plot_df["pixel_x"] = _quantize_series(plot_df["pixel_x"], dot_quantization_px)
            plot_df["pixel_y"] = _quantize_series(plot_df["pixel_y"], dot_quantization_px)

        # Render a true heatmap: repeated positions accumulate into hotter cells.
        if use_density_heatmap:
            hb = ax.hexbin(
                plot_df["pixel_x"],
                plot_df["pixel_y"],
                gridsize=heatmap_grid_size,
                cmap=heatmap_color_map,
                mincnt=1,
                bins="log",
                alpha=heatmap_alpha,
                linewidths=0,
            )
            cbar = fig.colorbar(hb, ax=ax, fraction=0.035, pad=0.02)
            cbar.set_label("Position Frequency (log)", color="white")
            cbar.ax.yaxis.set_tick_params(color="white")
            plt.setp(cbar.ax.get_yticklabels(), color="white")
        else:
            ax.scatter(
                plot_df["pixel_x"],
                plot_df["pixel_y"],
                c=dot_color,
                s=dot_size,
                alpha=dot_alpha,
                edgecolors="none",
            )

        ax.set_title(f"CT Holding Positions: {map_name}", fontsize=16, color="white")

    except FileNotFoundError:
        # Fallback if the user hasn't downloaded the radar image yet
        ax.text(
            0.5,
            0.5,
            f"Radar image not found at:\n{image_path}",
            ha="center",
            va="center",
            color="red",
            fontsize=12,
        )
        ax.scatter(df["pixel_x"], df["pixel_y"], c=dot_color, s=dot_size, alpha=dot_alpha)

    # Clean up the axes so it looks like a clean game map, not a math graph
    ax.axis("off")
    fig.patch.set_facecolor("#1e1e1e")  # Dark background to match Streamlit's dark mode

    return fig
