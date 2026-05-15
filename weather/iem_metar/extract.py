import polars as pl

import logging

from weather.iem_metar.cache import build_iem_metar_cache_for_airports
from weather.iem_metar.match import match_flight_details_with_iem_metar

logger = logging.getLogger(__name__)


def extract_iem_metar_data(flight_details_df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract IEM METAR data for all relevant airports and match it to flights.

    Arguments:
        flight_details_df (pl.DataFrame): Polars DataFrame containing flight details, including
            arrival airport ICAO codes and actual arrival timestamps.
    """
    logger.info("Determining relevant airports and date ranges from flight details...")
    airport_date_ranges = get_airport_weather_date_ranges(
        flight_details_df=flight_details_df,
        airport_col="lkpa_arrival_airport_icao_code",
        timestamp_col="fl_actual_arrival_time_utc",
    )
    logger.info(f"Found {len(airport_date_ranges)} unique arrival airports in flight details.")

    iem_metar_cache = build_iem_metar_cache_for_airports(
        airport_date_ranges=airport_date_ranges
    )
    logger.info(f"Built IEM METAR cache for {len(iem_metar_cache)} airports.")

    logger.info("Matching flight details with IEM METAR data close to actual arrival time...")
    iem_t0_metar_df = match_flight_details_with_iem_metar(
        flight_details_df=flight_details_df,
        iem_metar_cache=iem_metar_cache,
        target_offset=0,
        time_window_minutes=60,
    )
    logger.info(
        f"Matched flight details with IEM METAR data, resulting in {iem_t0_metar_df.height} matched records."
    )

    logger.info("Matching flight details with IEM METAR data close to ATA-30...")
    iem_t30_metar_df = match_flight_details_with_iem_metar(
        flight_details_df=flight_details_df,
        iem_metar_cache=iem_metar_cache,
        target_offset=30,
        time_window_minutes=60,
    )
    logger.info(
        f"Matched flight details with IEM METAR data for ATA-30, resulting in {iem_t30_metar_df.height} matched records."
    )

    iem_metar_df = iem_t0_metar_df.join(iem_t30_metar_df, on="flight_key_id", how="left")
    logger.info(
        f"Combined T0 and T30 matched dataframes, resulting in {iem_metar_df.height} combined records."
    )

    return iem_metar_df


def get_airport_weather_date_ranges(
    flight_details_df: pl.DataFrame,
    airport_col: str,
    timestamp_col: str,
) -> dict[str, tuple[pl.Datetime, pl.Datetime]]:
    """
    Return a mapping of airport to min/max weather timestamps required.

    Arguments:
        flight_details_df: Polars DataFrame containing flight details.
        airport_col: Name of the column containing airport identifiers.
        timestamp_col: Name of the column containing timestamps.
    """
    if airport_col not in flight_details_df.columns:
        raise ValueError(f"Airport column '{airport_col}' not found in the DataFrame.")

    if timestamp_col not in flight_details_df.columns:
        raise ValueError(f"Timestamp column '{timestamp_col}' not found in the DataFrame.")

    scoped_flights_df = flight_details_df.filter(
        pl.col(airport_col).is_not_null() & pl.col(timestamp_col).is_not_null()
    )

    airport_date_ranges = scoped_flights_df.group_by(airport_col).agg(
        [
            pl.col(timestamp_col).min().alias("min_timestamp"),
            pl.col(timestamp_col).max().alias("max_timestamp"),
        ]
    )

    return {
        row[airport_col]: (row["min_timestamp"], row["max_timestamp"])
        for row in airport_date_ranges.to_dicts()
    }
