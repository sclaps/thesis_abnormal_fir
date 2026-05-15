from io import StringIO
import time
import warnings

import polars as pl
import requests
import urllib3

import logging

# suppress InsecureRequestWarning raised when verify=False is used
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
DEFAULT_REQUEST_DELAY_SECONDS = 1
MAX_FETCH_ATTEMPTS = 3


def fetch_iem_airport_metar_data(
    airport: str,
    start_date: pl.Datetime,
    end_date: pl.Datetime,
) -> pl.DataFrame | None:
    """
    Fetch METAR data from the IEM ASOS API for a given airport and date range.

    Arguments:
        airport: ICAO code of the airport to fetch data for.
        start_date: Start timestamp of the requested period.
        end_date: End timestamp of the requested period.
    """
    params = {
        "station": airport,
        "data": "metar",
        "year1": start_date.year,
        "month1": start_date.month,
        "day1": start_date.day,
        "hour1": start_date.hour,
        "minute1": start_date.minute,
        "year2": end_date.year,
        "month2": end_date.month,
        "day2": end_date.day,
        "hour2": end_date.hour,
        "minute2": end_date.minute,
        "tz": "UTC",
        "format": "onlycomma",
        "latlon": "no",
        "missing": "M",
        "trace": "T",
        "direct": "no",
        "report_type": "1,2",
    }

    response = None
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            time.sleep(DEFAULT_REQUEST_DELAY_SECONDS)
            response = requests.get(IEM_ASOS_URL, params=params, timeout=60, verify=False)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logger.warning("[%s] Rate limited - sleeping %ss", airport, retry_after)
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                logger.warning(
                    "[%s] IEM server error %s on attempt %s/%s.",
                    airport,
                    response.status_code,
                    attempt,
                    MAX_FETCH_ATTEMPTS,
                )
                continue

            response.raise_for_status()
            break
        except requests.RequestException as exc:
            logger.warning(
                "[%s] Request failed on attempt %s/%s: %s",
                airport,
                attempt,
                MAX_FETCH_ATTEMPTS,
                exc,
            )
            response = None

    if response is None or response.status_code >= 400:
        logger.error("[%s] Failed to fetch IEM METAR data after %s attempts; skipping.", airport, MAX_FETCH_ATTEMPTS)
        return None

    lines = [line for line in response.text.strip().splitlines() if not line.startswith("#")]
    if len(lines) <= 1:
        logger.warning("[%s] No METAR data returned.", airport)
        return None

    return pl.read_csv(
        StringIO("\n".join(lines)),
        truncate_ragged_lines=True,
        quote_char=None,  # disable quote handling so embedded quotes don't split rows
    )
