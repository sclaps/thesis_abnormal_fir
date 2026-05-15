from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from typing import Literal

from metafora.engineering import process_metars, process_tafs, reports_to_dataframe
import polars as pl
from tqdm.auto import tqdm

import logging

logger = logging.getLogger(__name__)


def _parse_records_chunk(args: tuple[list, str]) -> list:
    """Module-level worker so it is picklable by ProcessPoolExecutor."""
    chunk, report_type = args
    parse_fn = process_metars if report_type == "METAR" else process_tafs
    return parse_fn(chunk)


def _feature_prefix_from_report_col(report_col: str) -> str:
    if report_col.endswith("_report"):
        return report_col[: -len("_report")]
    return report_col


def extract_ml_features_from_weather_report(
    weather_df: pl.DataFrame,
    report_col: str,
    timestamp_col: str,
    flight_key_col: str = "flight_key_id",
    insert_before_col: str | None = None,
    report_type: Literal["METAR", "TAF"] = "METAR",
) -> tuple[pl.DataFrame, list[str]]:
    """
    Extract machine learning features from a single report column and merge back into the dataframe.

    NOTE: For the METAR the associated timestamp is the observation time of the report, while for the TAF it is the scheduled time of arrival.

    Arguments:
        weather_df (pl.DataFrame): Source dataframe containing one row per flight and a weather report column.
        report_col (str): Report column name to parse.
        timestamp_col (str): Timestamp column name corresponding to the report column.
        flight_key_col (str): Primary key used to merge features back.
        insert_before_col (str | None): Optional column name where extracted features are inserted before.
            If not provided, new features are appended after existing columns.
        report_type (str): Type of report being parsed, used for logging and prefixing features.

    Returns:
        A tuple with:
        - dataframe enriched with extracted machine learning features
        - list of newly created feature column names
    """
    # validate input columns
    required_cols = {flight_key_col, report_col, timestamp_col}
    missing_cols = [col for col in required_cols if col not in weather_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required column(s): {', '.join(sorted(missing_cols))}")

    # extract records with non-null timestamps and reports
    records_df = weather_df.select(
        pl.col(flight_key_col),
        pl.col(timestamp_col).alias("_time"),
        pl.col(report_col).alias("_report"),
    ).filter(pl.col("_time").is_not_null() & pl.col("_report").is_not_null())

    if records_df.height == 0:
        raise ValueError(
            f"No {report_type} records found to process after filtering null timestamps/reports "
            f"in columns {report_col}/{timestamp_col}."
        )

    records_df = records_df.with_row_index("row_id")

    # parse reports using Metafora one record at a time so progress is visible
    parse_input = records_df.select(
        pl.col("_time").cast(pl.Utf8).alias("time"), pl.col("_report").alias("report")
    ).to_dicts()

    logger.info(f"Parsing {len(parse_input)} {report_type} reports from column '{report_col}'...")

    n_workers = min(os.cpu_count() or 4, len(parse_input))
    chunk_size = max(1, len(parse_input) // (n_workers * 4))
    chunks = [parse_input[i : i + chunk_size] for i in range(0, len(parse_input), chunk_size)]

    parsed_reports: list = [None] * len(chunks)
    futures_map = {}
    logger.info(
        f"Using {n_workers} parallel workers to parse {report_type} reports in {len(chunks)} chunks of ~{chunk_size} records each."
    )
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        for i, chunk in enumerate(chunks):
            futures_map[executor.submit(_parse_records_chunk, (chunk, report_type))] = i

        with tqdm(
            total=len(parse_input), desc=f"Parsing {report_type} ({report_col})", unit="report"
        ) as pbar:
            for future in as_completed(futures_map):
                idx = futures_map[future]
                parsed_reports[idx] = future.result()
                pbar.update(len(chunks[idx]))

    flat_reports = [record for chunk_result in parsed_reports for record in chunk_result]

    parsed_pd = reports_to_dataframe(flat_reports)
    parsed_pd["row_id"] = range(len(parsed_pd))
    parsed_df = pl.from_pandas(parsed_pd)

    records_with_features = records_df.join(parsed_df, on="row_id", how="left")

    # extract feature columns (exclude metadata)
    metadata_cols = {"row_id", flight_key_col, "_time", "_report"}
    feature_cols = [col for col in records_with_features.columns if col not in metadata_cols]

    # get feature prefix from report column name
    feature_prefix = _feature_prefix_from_report_col(report_col)

    # add prefix to feature columns and rename
    feature_df = records_with_features.select([flight_key_col, *feature_cols])
    rename_map = {col: f"{feature_prefix}_{col}" for col in feature_cols}
    feature_df = feature_df.rename(rename_map)

    # merge features back to original dataframe
    new_feature_cols = list(rename_map.values())
    processed_weather_df = weather_df.join(feature_df, on=flight_key_col, how="left")
    new_feature_cols = [col for col in new_feature_cols if col in processed_weather_df.columns]
    logger.info(
        f"Extracted {len(new_feature_cols)} {report_type} feature columns from {report_col}: {', '.join(new_feature_cols)}"
    )

    # insert new feature columns before specified column if provided
    if not insert_before_col:
        return processed_weather_df, new_feature_cols

    if insert_before_col not in processed_weather_df.columns:
        logger.warning(
            f"insert_before_col '{insert_before_col}' not found in dataframe columns. "
            "New feature columns will be appended at the end."
        )
        return processed_weather_df, new_feature_cols

    new_feature_cols_set = set(new_feature_cols)
    base_cols = [col for col in processed_weather_df.columns if col not in new_feature_cols_set]

    insert_idx = base_cols.index(insert_before_col)
    ordered_cols = base_cols[:insert_idx] + new_feature_cols + base_cols[insert_idx:]

    return processed_weather_df.select(ordered_cols), new_feature_cols
