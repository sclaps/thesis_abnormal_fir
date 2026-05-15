from datetime import timedelta
from pathlib import Path

import polars as pl
from tqdm.auto import tqdm

import logging
from pathlib import Path as _Path

from weather.iem_metar.fetch import fetch_iem_airport_metar_data

logger = logging.getLogger(__name__)

# Default cache directory: <project_root>/data/interim/
WEATHER_DATA_PATH: str = str(_Path(__file__).resolve().parents[3] / "data" / "interim")


def _normalize_validity_timestamp(iem_metar_df: pl.DataFrame) -> pl.DataFrame:
    """Parse the `valid` string column (from IEM CSV response) to a UTC datetime and sort."""
    return iem_metar_df.with_columns(
        pl.col("valid")
        .str.to_datetime(format="%Y-%m-%d %H:%M", strict=False)
        .dt.replace_time_zone("UTC")
    ).sort("valid")


def build_iem_metar_cache_for_airports(
    airport_date_ranges: dict[str, tuple[pl.Datetime, pl.Datetime]],
    cache_dir: str | Path | None = None,
    force_refresh: bool = False,
) -> dict[str, pl.DataFrame]:
    """
    Build a parquet-backed cache of IEM METAR data for relevant airports.

    Arguments:
        airport_date_ranges: Mapping of airport ICAO code to requested timestamp range.
        cache_dir: Directory to store/read parquet cache files.
        force_refresh: If True, always fetch from the API and overwrite cache.
    """
    cache_root = Path(cache_dir) if cache_dir is not None else _Path(WEATHER_DATA_PATH) / "iem_metar_cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    iem_metar_cache: dict[str, pl.DataFrame] = {}

    with tqdm(total=len(airport_date_ranges), desc="Building IEM METAR cache") as pbar:
        for airport, (min_date, max_date) in airport_date_ranges.items():
            t_start = (min_date - timedelta(days=1)).replace(hour=0, minute=0, second=0)
            t_end = (max_date + timedelta(days=1)).replace(hour=23, minute=59, second=59)

            target_name = f"{airport}_{t_start:%Y-%m-%d}_{t_end:%Y-%m-%d}.parquet"
            target_path = cache_root / target_name

            if target_path.exists() and not force_refresh:
                pbar.set_postfix(airport=airport, status="cache")
                iem_metar_df = pl.read_parquet(target_path)
            else:
                pbar.set_postfix(airport=airport, status="fetching")
                iem_metar_df = fetch_iem_airport_metar_data(airport, t_start, t_end)

                if iem_metar_df is not None:
                    iem_metar_df = _normalize_validity_timestamp(iem_metar_df)
                    iem_metar_df.write_parquet(target_path)

            if iem_metar_df is not None:
                iem_metar_cache[airport] = iem_metar_df

            pbar.update(1)

    return iem_metar_cache
