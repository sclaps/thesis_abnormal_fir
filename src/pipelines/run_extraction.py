from __future__ import annotations

from datetime import datetime, timezone
import time
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.data.extract.extract_sql_data import extract_flight_details

def main():
    print("Extracting data from BigQuery...")

    df = extract_flight_details(
        start_date="2023-10-01",
        end_date="2026-03-31"
    )

    print(f"Extracted {len(df)} rows")

    # -----------------------------
    # Save locally
    # -----------------------------
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"klm_raw_2023_2026.parquet"
    df.to_parquet(file_path)

    print(f"Saved to {file_path}")


if __name__ == "__main__":
    main()










'''
from src.settings.dataset import RAW_DATASET_VERSION
from src.config.paths import CONFIGS_PATH
from src.utils.config import (
    initialize_raw_data_directory,
    load_yaml_config,
    save_df_csv,
    save_df_parquet,
    save_json,
    save_yaml,
)
from src.utils.logger import add_file_handler, get_logger, remove_file_handler
from src.utils.metadata import create_extraction_metadata

logger = get_logger(__name__, buffer_logs=True)


def run_extract_flight_details(start_date: str, end_date: str) -> None:
    """
    Extract raw flight details from the KLM SQL database based on the provided date range.
    """
    t_start = time.time()
    # validate input dates
    validate_date_range(start_date, end_date)

    # create output folder
    logger.info("Resolving output directory for raw flight details...")
    output_dir = initialize_raw_data_directory(
        dataset_version=RAW_DATASET_VERSION,
    )

    # add file handler to save ALL logs to output directory
    file_handler = add_file_handler(output_dir / "extraction.log")

    # --- 1. Extract Flight Details ----------------------------------------
    logger.info("Extracting raw flight details from KLM SQL database...")
    extract_config_path = CONFIGS_PATH / "extract_flight_details.yaml"
    extraction_config = load_yaml_config(config_path=extract_config_path)
    raw_flight_details_df = extract_flight_details(
        start_date=start_date, end_date=end_date
    )
    t_extraction = time.time() - t_start
    logger.info(f"Extracted {len(raw_flight_details_df)} records in {t_extraction:.2f} seconds.")

    # --- 2. Save ----------------------------------------------------------
    logger.info("Saving extracted raw flight details...")
    save_df_csv(raw_flight_details_df, output_dir, "flight_details")
    save_df_parquet(raw_flight_details_df, output_dir, "flight_details")
    save_yaml(extraction_config, output_dir, "extraction_config")

    # create and save extraction metadata
    metadata = create_extraction_metadata(
        raw_dataset_version=RAW_DATASET_VERSION,
        extraction_time=round(t_extraction, 2),
        start_date=start_date,
        end_date=end_date,
        raw_flight_details_df=raw_flight_details_df,
    )
    save_json(metadata, output_dir, "extraction_metadata")
    
    t_end = time.time() - t_start
    logger.info(f"Extraction pipeline finished in {t_end:.2f} seconds.")
    # remove logger file handler
    remove_file_handler(file_handler)


def validate_date_range(start_date: str, end_date: str) -> None:
    """
    Validate input date range and return normalized date objects.

    Raises ValueError if:
        - Dates are not in 'YYYY-MM-DD' format.
        - Start date is after end date.
        - End date is in the future.
    """
    try:
        start_date_fmt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_fmt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError("Dates must be in 'YYYY-MM-DD' format.") from e

    if start_date_fmt > end_date_fmt:
        raise ValueError("Start date must be earlier than or equal to the end date.")

    today = datetime.now(timezone.utc).date()
    if end_date_fmt > today:
        raise ValueError("End date cannot be in the future.")
'''