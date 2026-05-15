from datetime import date
from pathlib import Path

from google.cloud import bigquery
import pandas as pd


# Define path to SQL folder
SQL_PATH = Path(__file__).resolve().parents[3] / "sql"


def load_sql_query(query_file_name: str, **kwargs) -> str:
    sql_path = SQL_PATH / f"{query_file_name}.sql"

    if not sql_path.exists():
        raise FileNotFoundError(f"{sql_path} not found")

    query = sql_path.read_text()

    if kwargs:
        query = query.format(**kwargs)

    return query


def run_query(query_file_name: str, **kwargs) -> pd.DataFrame:
    client = bigquery.Client()
    query = load_sql_query(query_file_name, **kwargs)

    print("Running query...")

    df = client.query(query).to_dataframe()

    return df


def extract_flight_details(
    start_date: str,
    end_date: str                          #str(date.today()),
) -> pd.DataFrame:
    return run_query(
        query_file_name="klm_raw_2023_2026",
        start_date=start_date,
        end_date=end_date,
    )

'''
from src.config.paths import SQL_QUERIES_PATH
from src.utils.logger import get_logger    #need to create this logger in utils/logger.py?

logger = get_logger(__name__)


def load_sql_query(query_file_name: str, **kwargs) -> str:
    """
    Load a SQL query from the sql_queries directory.

    Args:
        query_name (str): The name of the SQL query file (without extension).
        **kwargs: Additional keyword arguments to format the SQL query.
                e.g. start_date='2023-01-01', end_date='2026-03-31'.

    Returns:
        query (str): The SQL query as a string.
    """
    sql_path = SQL_QUERIES_PATH / f"{query_file_name}.sql"  
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL query file {sql_path} does not exist.")
    logger.info(f"Loading SQL query from {sql_path}...")

    query = sql_path.read_text()

    if kwargs:
        query = query.format(**kwargs)

    logger.info(f"SQL query from '{query_file_name}':\n{query}")  

    return query


def run_query(query_file_name: str, **kwargs) -> pd.DataFrame:
    """
    Run a SQL query from BigQuery.

    Args:
        query (str): The SQL query to execute.
        **kwargs: Parameters to substitute into the query.
                    e.g. start_date='2023-01-01', end_date='2026-03-31'.
    Returns:
        df (pd.DataFrame): The result of the query as a pandas DataFrame.
    """
    try:
        logger.info(f"Loading query from '{query_file_name}'...")
        query = load_sql_query(query_file_name, **kwargs)

        logger.info(f"Executing query from '{query_file_name}'...")
        client = bigquery.Client()
        df = client.query(query).to_dataframe()

        logger.info(f"Successfully extracted {len(df)} records from '{query_file_name}'.")
        return df
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise


def extract_flight_details(
    query_file_name: str = "extract_flight_details",
    start_date: str = "2025-01-01",
    end_date: str =  "2025-12-31"                          #str(date.today()),
) -> pd.DataFrame:
    """
    Extract flight details from the KLM SQL database based on the provided date range.

    Args:
        start_date (str): Start date in the format 'yyyy-mm-dd'.
        end_date (str): End date in the format 'yyyy-mm-dd'.

    Returns:
        df (pd.DataFrame): Extracted flight details as a pandas DataFrame.
    """
    return run_query(
        query_file_name=query_file_name, start_date=start_date, end_date=end_date
    )
    '''