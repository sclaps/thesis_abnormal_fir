"""
Run weather data extraction pipeline.

Mirrors the structure of run_extraction.py so that both runners can be merged
into a single entry-point in the future.

Usage:
    python src/pipelines/run_weather_extraction.py

The script:
  1. Loads the cleaned flight data from data/processed/.
  2. Derives the extraction date range from the cleaned data.
  3. Extracts KLM METAR data (BigQuery) and IEM METAR data (ASOS API).
  4. Extracts KLM TAF data (BigQuery).
  5. Saves the combined METAR and TAF datasets to data/processed/.
"""

from __future__ import annotations

import time
from pathlib import Path
import sys

# Allow running as a top-level script from the project root
sys.path.append(str(Path(__file__).resolve().parents[2]))

import pandas as pd
import polars as pl

from src.data.extract.extract_weather_data import build_metar_dataset, build_taf_dataset


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed"
CLEANED_DATA_FILE = PROCESSED_DATA_PATH / "klm_cleaned_data_2023_2026(0).parquet"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_cleaned_data(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Cleaned flight data not found at {path}. "
            "Run the preprocessing pipeline first."
        )
    print(f"Loading cleaned flight data from {path}...")
    df = pl.from_pandas(pd.read_parquet(path))
    print(f"  → {df.height:,} rows loaded.")
    return df


def _derive_date_range(cleaned_df: pl.DataFrame) -> tuple[str, str]:
    start_date = "2025-01-01"
    end_date = "2025-12-31"
    # start_date = str(cleaned_df["fl_scheduled_departure_date_utc"].min())[:10]
    # end_date = str(cleaned_df["fl_scheduled_departure_date_utc"].max())[:10]
    return start_date, end_date


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_weather_extraction(
    include_iem: bool = False,  # TODO: check
    output_dir: Path | None = None,
) -> None:
    """
    Run the full weather extraction pipeline and save outputs.

    Args:
        include_iem: Whether to include IEM METAR data (requires internet access).
        output_dir:  Directory to save output files. Defaults to
                     ``data/processed/``.
    """
    t_start = time.time()
    out_dir = Path(output_dir) if output_dir is not None else PROCESSED_DATA_PATH
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load cleaned data
    cleaned_df = _load_cleaned_data(CLEANED_DATA_FILE)
    start_date, end_date = _derive_date_range(cleaned_df)
    print(f"Date range: {start_date} to {end_date}")

    # 2. Build METAR dataset
    print("\n--- Extracting METAR data ---")
    t0 = time.time()
    metar_df = build_metar_dataset(
        cleaned_df=cleaned_df,
        start_date=start_date,
        end_date=end_date,
        include_iem=include_iem,
    )
    metar_out = out_dir / "metar_data.parquet"
    metar_df.write_parquet(metar_out)
    print(f"METAR data saved to {metar_out} ({time.time() - t0:.1f}s, {metar_df.height:,} rows)")

    # 3. Build TAF dataset
    print("\n--- Extracting TAF data ---")
    t0 = time.time()
    taf_df = build_taf_dataset(
        cleaned_df=cleaned_df,
        start_date=start_date,
        end_date=end_date,
    )
    taf_out = out_dir / "taf_data.parquet"
    taf_df.write_parquet(taf_out)
    print(f"TAF data saved to {taf_out} ({time.time() - t0:.1f}s, {taf_df.height:,} rows)")

    print(f"\nWeather extraction complete in {time.time() - t_start:.1f}s total.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    run_weather_extraction(include_iem=False)


if __name__ == "__main__":
    main()
