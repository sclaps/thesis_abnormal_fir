from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, timezone
import os
from typing import Any

import polars as pl
from tqdm.auto import tqdm

import logging

logger = logging.getLogger(__name__)


def build_iem_match_record(
    flight_key_id: str,
    tag: str,
    report: str | None,
    timestamp: Any,
    obs_timestamp: Any,
    target_diff_minutes: int | None,
    ata_diff_minutes: int | None,
    has_weather_report: int,
) -> dict[str, Any]:
    """Create a stable output record for one matched IEM METAR target, with matching fields to internal SQL extraction."""
    return {
        "flight_key_id": flight_key_id,
        f"iem_{tag}_metar_report": report,
        f"iem_{tag}_metar_timestamp": timestamp,
        f"iem_{tag}_metar_obs_timestamp": obs_timestamp,
        f"iem_{tag}_metar_target_diff_minutes": target_diff_minutes,
        f"iem_{tag}_metar_ata_diff_minutes": ata_diff_minutes,
        f"has_iem_{tag}_metar_report": has_weather_report,
    }


def _match_chunk_worker(
    args: tuple[list[dict], dict[str, pl.DataFrame], int, int],
) -> list[dict[str, Any]]:
    """Module-level worker so it is picklable by ProcessPoolExecutor."""
    chunk_rows, iem_metar_cache, target_offset, time_window_minutes = args

    tag = f"t{target_offset}"
    matched_records: list[dict[str, Any]] = []

    for row in chunk_rows:
        flight_key_id = row["flight_key_id"]
        arrival_airport = row["lkpa_arrival_airport_icao_code"]
        actual_arrival_time = row["fl_actual_arrival_time_utc"]

        if arrival_airport is None or actual_arrival_time is None:
            continue

        if arrival_airport not in iem_metar_cache:
            continue

        # Ensure actual_arrival_time is UTC-aware so it matches the Datetime(UTC)
        # column in the IEM cache; Polars raises SchemaError on naive vs aware comparisons.
        if actual_arrival_time.tzinfo is None:
            actual_arrival_time = actual_arrival_time.replace(tzinfo=timezone.utc)

        iem_metar_df = iem_metar_cache[arrival_airport]
        window_end = actual_arrival_time
        window_start = actual_arrival_time - timedelta(minutes=time_window_minutes)
        target_timestamp = actual_arrival_time - timedelta(minutes=target_offset)

        candidates = iem_metar_df.filter(
            (pl.col("valid") >= window_start) & (pl.col("valid") <= window_end)
        )

        if candidates.height == 0:
            matched_records.append(
                build_iem_match_record(
                    flight_key_id=flight_key_id,
                    tag=tag,
                    report=None,
                    timestamp=None,
                    obs_timestamp=None,
                    target_diff_minutes=None,
                    ata_diff_minutes=None,
                    has_weather_report=0,
                )
            )
            continue

        closest = (
            candidates.with_columns(
                (pl.col("valid") - target_timestamp).abs().alias("_target_time_diff")
            )
            .sort("_target_time_diff")
            .row(0, named=True)
        )

        ata_diff_minutes = int(
            round((actual_arrival_time - closest["valid"]).total_seconds() / 60)
        )
        target_diff_minutes = int(round(closest["_target_time_diff"].total_seconds() / 60))
        metar_report = closest.get("metar_report", closest.get("metar"))
        metar_timestamp = closest.get("metar_timestamp", closest.get("valid"))

        matched_records.append(
            build_iem_match_record(
                flight_key_id=flight_key_id,
                tag=tag,
                report=metar_report,
                timestamp=metar_timestamp,
                obs_timestamp=closest["valid"],
                target_diff_minutes=target_diff_minutes,
                ata_diff_minutes=ata_diff_minutes,
                has_weather_report=1,
            )
        )

    return matched_records


def match_flight_details_with_iem_metar(
    flight_details_df: pl.DataFrame,
    iem_metar_cache: dict[str, pl.DataFrame],
    target_offset: int = 0,
    time_window_minutes: int = 60,
) -> pl.DataFrame:
    """
    Matches flight details with IEM METAR data based on arrival airport and
    timestamp proximity using an actual-arrival anchored lookback window.

    Arguments:
        flight_details_df:    Polars DataFrame with flight details (arrival airport,
                              actual arrival time, flight_key_id, ...).
        iem_metar_cache:      Dict mapping ICAO code -> sorted Polars METAR DataFrame.
        target_offset:        Target offset before actual arrival to match against
                              (0 = ATA-T0, 30 = ATA-T30).
        time_window_minutes:  Lookback window size in minutes before actual arrival time.

    Returns:
        Polars DataFrame with one row per matched flight, using iem_metar_t{n}_*
        column naming.
    """
    total_rows = flight_details_df.height
    row_list = flight_details_df.to_dicts()

    n_workers = min(os.cpu_count() or 4, total_rows)
    chunk_size = max(1, total_rows // (n_workers * 4))
    chunks = [row_list[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]

    ordered_results: list[list | None] = [None] * len(chunks)
    futures_map = {}
    logger.info(f"Using {n_workers} parallel workers to match IEM METAR reports in {len(chunks)} chunks of ~{chunk_size} records each.")
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        for i, chunk in enumerate(chunks):
            futures_map[
                executor.submit(_match_chunk_worker, (chunk, iem_metar_cache, target_offset, time_window_minutes))
            ] = (i, len(chunk))

        with tqdm(total=total_rows, desc=f"Matching IEM METAR ATA-{target_offset}", unit="flight") as pbar:
            for future in as_completed(futures_map):
                idx, chunk_len = futures_map[future]
                ordered_results[idx] = future.result()
                pbar.update(chunk_len)

    matched_records = [record for chunk in ordered_results if chunk for record in chunk]
    return pl.DataFrame(matched_records)
