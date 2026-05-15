"""
Weather data extraction helpers.

Mirrors the structure of extract_sql_data.py so that both can be merged into a
single extraction module in the future. Provides functions to:

  - Extract KLM METAR data from BigQuery (extract_klm_metar_data)
  - Extract KLM TAF data from BigQuery  (extract_klm_taf_data)
  - Extract IEM METAR data via the public ASOS API (extract_iem_metar_data)
  - Combine KLM and IEM METAR data into a single dataframe (build_metar_dataset)
  - Combine TAF data with cleaned flight data (build_taf_dataset)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from google.cloud import bigquery
import polars as pl

if TYPE_CHECKING:
    pass

# Path to the sql/ directory (two levels up: src/data/extract/ → src/ → project root)
SQL_PATH = Path(__file__).resolve().parents[3] / "sql"


def _load_sql_query(query_file_name: str, **kwargs: str) -> str:
    """Load a SQL query file and substitute keyword placeholders."""
    sql_path = SQL_PATH / f"{query_file_name}.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL query file not found: {sql_path}")
    query = sql_path.read_text()
    for key, value in kwargs.items():
        query = query.replace(f"{{{key}}}", value)
    return query


def _run_bq_query(query_file_name: str, **kwargs: str) -> pl.DataFrame:
    """Run a named SQL file against BigQuery and return a Polars DataFrame."""
    import pandas as pd
    query = _load_sql_query(query_file_name, **kwargs)
    client = bigquery.Client()
    print(f"Running BigQuery query '{query_file_name}'...")
    df = pl.from_pandas(client.query(query).to_dataframe())
    print(f"  → {df.height:,} rows extracted.")
    return df


# ---------------------------------------------------------------------------
# KLM BigQuery extractors
# ---------------------------------------------------------------------------

def extract_klm_metar_data(start_date: str, end_date: str) -> pl.DataFrame:
    """
    Extract KLM METAR observations from BigQuery for the given date range.

    The query returns one row per flight with the closest METAR report to
    actual arrival time (T0) and 30 minutes before arrival (T30).

    Args:
        start_date: Inclusive start date in 'YYYY-MM-DD' format.
        end_date:   Inclusive end date in 'YYYY-MM-DD' format.

    Returns:
        Polars DataFrame with columns prefixed ``klm_t0_metar_*`` and
        ``klm_t30_metar_*`` plus flight-level identifiers.
    """
    return _run_bq_query(
        "extract_metar_data",
        start_date=start_date,
        end_date=end_date,
    )


def extract_klm_taf_data(start_date: str, end_date: str) -> pl.DataFrame:
    """
    Extract KLM TAF forecasts from BigQuery for the given date range.

    The query returns one row per flight with the TAF report that was most
    recently issued before the flight plan creation timestamp.

    Args:
        start_date: Inclusive start date in 'YYYY-MM-DD' format.
        end_date:   Inclusive end date in 'YYYY-MM-DD' format.

    Returns:
        Polars DataFrame with ``taf_report``, ``taf_timestamp``,
        ``has_taf_report`` and flight-level identifiers.
    """
    return _run_bq_query(
        "extract_taf_data",
        start_date=start_date,
        end_date=end_date,
    )


# ---------------------------------------------------------------------------
# IEM METAR extractor
# ---------------------------------------------------------------------------

def extract_iem_metar_data(flight_details_df: pl.DataFrame) -> pl.DataFrame:
    """
    Fetch IEM METAR data for all arrival airports in *flight_details_df* and
    match each flight to its nearest observation.

    The input DataFrame must contain:
      - ``flight_key_id``              – unique flight identifier
      - ``lkpa_arrival_airport_icao_code`` – ICAO code of the arrival airport
      - ``fl_actual_arrival_time_utc`` – actual in-block time (UTC)

    Returns a Polars DataFrame with columns prefixed ``iem_t0_metar_*`` and
    ``iem_t30_metar_*`` joined by ``flight_key_id``.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

    from weather.iem_metar.extract import (
        get_airport_weather_date_ranges,
        extract_iem_metar_data as _extract,
    )

    required = {"flight_key_id", "lkpa_arrival_airport_icao_code", "fl_actual_arrival_time_utc"}
    missing = required - set(flight_details_df.columns)
    if missing:
        raise ValueError(
            f"flight_details_df is missing required columns: {', '.join(sorted(missing))}"
        )

    print("Extracting IEM METAR data...")
    iem_df = _extract(flight_details_df)
    print(f"  → {iem_df.height:,} matched records.")
    return iem_df


# ---------------------------------------------------------------------------
# Dataset builders (join + filter against cleaned flight data)
# ---------------------------------------------------------------------------

def build_metar_dataset(
    cleaned_df: pl.DataFrame,
    start_date: str,
    end_date: str,
    include_iem: bool = False,
) -> pl.DataFrame:
    """
    Build a combined METAR dataset from KLM BigQuery and (optionally) IEM ASOS.

    Steps:
      1. Extract KLM METAR data from BigQuery.
      2. Inner-join with *cleaned_df* to keep only cleaned, filtered flights.
      3. Optionally extract and join IEM METAR data.

    Args:
        cleaned_df:  Cleaned flight DataFrame produced by the preprocessing
                     pipeline. Must contain ``flight_leg_key``,
                     ``fl_actual_in_block_time_utc``,
                     ``fl_arrival_airport_icao_code``, and
                     ``fl_actual_arrival_airport``.
        start_date:  Inclusive start date ('YYYY-MM-DD').
        end_date:    Inclusive end date ('YYYY-MM-DD').
        include_iem: Whether to also fetch and join IEM METAR data.

    Returns:
        Combined Polars DataFrame with one row per cleaned flight and all
        available METAR columns.
    """
    # 1. KLM METAR
    klm_metar_df = extract_klm_metar_data(start_date, end_date)

    # 2. Join all flight columns from cleaned_df, skipping any that already
    #    exist in the weather data to avoid duplicate column errors.
    cleaned_for_join = cleaned_df.rename({"flight_leg_key": "flight_key_id"})
    existing_weather_cols = set(klm_metar_df.columns) - {"flight_key_id"}
    flight_cols = [c for c in cleaned_for_join.columns if c not in existing_weather_cols]

    metar_df = klm_metar_df.join(
        cleaned_for_join.select(flight_cols),
        on="flight_key_id",
        how="inner",
    )
    n_before = metar_df.height
    metar_df = metar_df.unique(subset=["flight_key_id"], keep="first")
    print(f"After join with cleaned data: {metar_df.height:,} flights retained "
          f"({n_before - metar_df.height:,} duplicate rows removed).")

    # 3. IEM METAR (optional)
    if include_iem:
        flight_details_df = metar_df.select([
            pl.col("flight_key_id"),
            pl.col("fl_actual_arrival_time_utc"),
            pl.col("lkpa_arrival_airport_icao_code"),
        ]).filter(
            pl.col("fl_actual_arrival_time_utc").is_not_null()
            & pl.col("lkpa_arrival_airport_icao_code").is_not_null()
        )

        iem_t0_df, iem_t30_df = _extract_iem_t0_t30(flight_details_df)
        metar_df = metar_df.join(iem_t0_df, on="flight_key_id", how="left")
        metar_df = metar_df.join(iem_t30_df, on="flight_key_id", how="left")
        print(f"IEM METAR joined. Final shape: {metar_df.height:,} rows, {len(metar_df.columns)} columns.")

    return metar_df


def build_taf_dataset(
    cleaned_df: pl.DataFrame,
    start_date: str,
    end_date: str,
) -> pl.DataFrame:
    """
    Build a TAF dataset from KLM BigQuery, joined against the cleaned flight data.

    Args:
        cleaned_df:  Cleaned flight DataFrame (must contain ``flight_leg_key``
                     and ``fl_touchdown_delay_min``).
        start_date:  Inclusive start date ('YYYY-MM-DD').
        end_date:    Inclusive end date ('YYYY-MM-DD').

    Returns:
        Polars DataFrame with TAF columns joined to the cleaned flight features.
    """
    klm_taf_df = extract_klm_taf_data(start_date, end_date)

    # Join all flight columns from cleaned_df, skipping any that already
    # exist in the weather data to avoid duplicate column errors.
    cleaned_for_join = cleaned_df.rename({"flight_leg_key": "flight_key_id"})
    existing_weather_cols = set(klm_taf_df.columns) - {"flight_key_id"}
    flight_cols = [c for c in cleaned_for_join.columns if c not in existing_weather_cols]

    taf_df = klm_taf_df.join(
        cleaned_for_join.select(flight_cols),
        on="flight_key_id",
        how="inner",
    )
    n_before = taf_df.height
    taf_df = taf_df.unique(subset=["flight_key_id"], keep="first")
    print(f"TAF dataset built: {taf_df.height:,} flights "
          f"({n_before - taf_df.height:,} duplicate rows removed).")
    return taf_df


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _extract_iem_t0_t30(
    flight_details_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (iem_t0_df, iem_t30_df) for the given flight details."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

    from weather.iem_metar.extract import get_airport_weather_date_ranges
    from weather.iem_metar.cache import build_iem_metar_cache_for_airports
    from weather.iem_metar.match import match_flight_details_with_iem_metar

    airport_date_ranges = get_airport_weather_date_ranges(
        flight_details_df=flight_details_df,
        airport_col="lkpa_arrival_airport_icao_code",
        timestamp_col="fl_actual_arrival_time_utc",
    )
    iem_cache = build_iem_metar_cache_for_airports(airport_date_ranges)

    iem_t0 = match_flight_details_with_iem_metar(
        flight_details_df, iem_cache, target_offset=0, time_window_minutes=60
    )
    iem_t30 = match_flight_details_with_iem_metar(
        flight_details_df, iem_cache, target_offset=30, time_window_minutes=60
    )
    return iem_t0, iem_t30
