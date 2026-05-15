import polars as pl

import logging

logger = logging.getLogger(__name__)


def _format_pct(value: float | None) -> str:
    """Format ratio values as percentage strings for logs."""
    return f"{value:.2%}" if value is not None else "n/a"


def normalize_metar_report_expr(report_col: str) -> pl.Expr:
    """Normalize METAR report strings so format differences do not cause false mismatches."""
    return (
        pl.col(report_col)
        .cast(pl.Utf8)
        .str.replace(r"\b(\d{6})Z\b", r"${1}")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_uppercase()
    )


def extract_metar_quality_stats(metar_df: pl.DataFrame, target_offset: int) -> pl.DataFrame:
    """Enrich per-flight METAR records with KLM vs IEM availability and equality columns."""
    target_tag = f"t{target_offset}"
    klm_has_col = f"has_klm_{target_tag}_metar_report"
    iem_has_col = f"has_iem_{target_tag}_metar_report"
    klm_report_col = f"klm_{target_tag}_metar_report"
    iem_report_col = f"iem_{target_tag}_metar_report"

    required_columns = [
        "flight_key_id",
        "lkpa_arrival_airport_icao_code",
        klm_has_col,
        iem_has_col,
        klm_report_col,
        iem_report_col,
    ]
    missing_columns = [col for col in required_columns if col not in metar_df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing expected column(s) for {target_tag}: {', '.join(missing_columns)}"
        )

    return metar_df.with_columns(
        normalize_metar_report_expr(klm_report_col).alias(f"{klm_report_col}_norm"),
        normalize_metar_report_expr(iem_report_col).alias(f"{iem_report_col}_norm"),
        ((pl.col(klm_has_col) == 1) & pl.col(klm_report_col).is_not_null()).alias(
            "has_klm_report"
        ),
        ((pl.col(iem_has_col) == 1) & pl.col(iem_report_col).is_not_null()).alias(
            "has_iem_report"
        ),
    ).with_columns(
        pl.when(pl.col("has_klm_report") & pl.col("has_iem_report"))
        .then(pl.lit("both_present"))
        .when(pl.col("has_klm_report") & (~pl.col("has_iem_report")))
        .then(pl.lit("only_klm"))
        .when((~pl.col("has_klm_report")) & pl.col("has_iem_report"))
        .then(pl.lit("only_iem"))
        .otherwise(pl.lit("both_missing"))
        .alias("presence_status"),
        pl.when(pl.col("has_klm_report") & pl.col("has_iem_report"))
        .then(pl.col(f"{klm_report_col}_norm") == pl.col(f"{iem_report_col}_norm"))
        .otherwise(None)
        .alias("reports_equal_norm"),
    )


def summarize_metar_quality(quality_df: pl.DataFrame, target_offset: int) -> pl.DataFrame:
    """Build one-row global summary statistics for METAR data quality."""
    target_tag = f"t{target_offset}"

    n_total = quality_df.height
    n_klm_available = quality_df.filter(pl.col("has_klm_report") == 1).height
    n_iem_available = quality_df.filter(pl.col("has_iem_report") == 1).height
    n_both_present = quality_df.filter(pl.col("presence_status") == "both_present").height
    n_only_klm = quality_df.filter(pl.col("presence_status") == "only_klm").height
    n_only_iem = quality_df.filter(pl.col("presence_status") == "only_iem").height
    n_both_missing = quality_df.filter(pl.col("presence_status") == "both_missing").height
    n_reports_equal = quality_df.filter(pl.col("reports_equal_norm").fill_null(False)).height

    summary_stats = {
        "target": target_tag,
        "num_records_extracted": n_total,
        "num_klm_metar_available": n_klm_available,
        "num_iem_metar_available": n_iem_available,
        "num_both_present": n_both_present,
        "num_only_klm": n_only_klm,
        "num_only_iem": n_only_iem,
        "num_both_missing": n_both_missing,
        "num_reports_equal": n_reports_equal,
        "pct_klm_metar_available": (n_klm_available / n_total) if n_total else None,
        "pct_iem_metar_available": (n_iem_available / n_total) if n_total else None,
        "pct_reports_equal_of_both_present": (n_reports_equal / n_both_present)
        if n_both_present
        else None,
    }

    return pl.DataFrame([summary_stats])


def summarize_metar_quality_by_airport(
    quality_df: pl.DataFrame,
    target_offset: int,
) -> pl.DataFrame:
    """Build per-airport METAR quality summary sorted by most missing records."""
    target_tag = f"t{target_offset}"
    grouped_df = quality_df.with_columns(
        pl.when(
            pl.col("lkpa_arrival_airport_icao_code").cast(pl.Utf8, strict=False).is_null()
            | (
                pl.col("lkpa_arrival_airport_icao_code")
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                == ""
            )
        )
        .then(pl.lit("MISSING"))
        .otherwise(pl.col("lkpa_arrival_airport_icao_code").cast(pl.Utf8, strict=False))
        .alias("lkpa_arrival_airport_icao_code"),
        pl.when(
            pl.col("fl_arrival_airport").cast(pl.Utf8, strict=False).is_null()
            | pl.col("lkpa_arrival_airport_icao_code").cast(pl.Utf8, strict=False).is_null()
            | (pl.col("fl_arrival_airport").cast(pl.Utf8, strict=False).str.strip_chars() == "")
        )
        .then(pl.lit("MISSING"))
        .otherwise(pl.col("fl_arrival_airport").cast(pl.Utf8, strict=False))
        .alias("fl_arrival_airport"),
    )

    return (
        grouped_df.group_by("lkpa_arrival_airport_icao_code")
        .agg(
            pl.col("fl_arrival_airport").first().alias("fl_arrival_airport"),
            pl.len().alias("num_records_extracted"),
            pl.col("has_klm_report").sum().alias("num_klm_metar_available"),
            pl.col("has_iem_report").sum().alias("num_iem_metar_available"),
            (pl.col("presence_status") == "both_present").sum().alias("num_both_present"),
            (pl.col("presence_status") == "only_klm").sum().alias("num_only_klm"),
            (pl.col("presence_status") == "only_iem").sum().alias("num_only_iem"),
            (pl.col("presence_status") == "both_missing").sum().alias("num_both_missing"),
            pl.col("reports_equal_norm").fill_null(False).sum().alias("num_reports_equal"),
        )
        .with_columns(
            pl.lit(target_tag).alias("target"),
            (pl.col("num_klm_metar_available") / pl.col("num_records_extracted")).alias(
                "pct_klm_metar_available"
            ),
            (pl.col("num_iem_metar_available") / pl.col("num_records_extracted")).alias(
                "pct_iem_metar_available"
            ),
            pl.when(pl.col("num_both_present") > 0)
            .then(pl.col("num_reports_equal") / pl.col("num_both_present"))
            .otherwise(None)
            .alias("pct_reports_equal_of_both_present"),
        )
        .sort(
            ["num_records_extracted", "lkpa_arrival_airport_icao_code"],
            descending=[True, False],
        )
    )


def format_global_metar_summary(global_metar_summary_df: pl.DataFrame) -> str:
    """Format global T0/T30 METAR quality summary for logs and text-file export."""
    lines = ["Global METAR data quality summary:"]

    for row in global_metar_summary_df.sort("target").iter_rows(named=True):
        target = str(row.get("target", "?")).upper()
        lines.append(f"[{target}] total records: {row.get('num_records_extracted', 0)}")
        lines.append(
            f"[{target}] availability - KLM: {row.get('num_klm_metar_available', 0)} "
            f"({_format_pct(row.get('pct_klm_metar_available'))}), "
            f"IEM: {row.get('num_iem_metar_available', 0)} "
            f"({_format_pct(row.get('pct_iem_metar_available'))})"
        )
        lines.append(
            f"[{target}] presence - both: {row.get('num_both_present', 0)}, "
            f"only_klm: {row.get('num_only_klm', 0)}, "
            f"only_iem: {row.get('num_only_iem', 0)}, "
            f"both_missing: {row.get('num_both_missing', 0)}"
        )
        lines.append(
            f"[{target}] agreement - equal reports: {row.get('num_reports_equal', 0)} / "
            f"{row.get('num_both_present', 0)} "
            f"({_format_pct(row.get('pct_reports_equal_of_both_present'))})"
        )

    return "\n".join(lines)


def log_weather_summary(weather_summary_df: pl.DataFrame, type: str) -> None:
    """Log METAR and TAF quality summaries."""
    if type == "METAR":
        logger.info("METAR data quality summary:")
        for line in format_global_metar_summary(weather_summary_df).splitlines():
            logger.info(line)
    elif type == "TAF":
        logger.info("TAF data quality summary:")
        for row in weather_summary_df.iter_rows(named=True):
            logger.info(
                f"Total records: {row.get('num_records_extracted', 0)}, "
                f"TAF available: {row.get('num_taf_available', 0)} "
                f"({_format_pct(row.get('num_taf_available') / row.get('num_records_extracted', 0) if row.get('num_records_extracted', 0) else None)})"
            )


def evaluate_metar_data_quality(
    metar_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Evaluate METAR comparison quality globally and per-airport for T0 and T30.

    Arguments:
        metar_df (pl.DataFrame): DataFrame containing joined KLM/IEM METAR records with presence and equality columns.

    Returns:
        global_metar_summary_df (pl.DataFrame): One-row summary of METAR data quality across all records for T0 and T30.
        airport_metar_summary_df (pl.DataFrame): Per-airport summary of METAR data quality for T0 and T30, sorted by most missing records.
    """
    metar_quality_t0_df = extract_metar_quality_stats(metar_df, target_offset=0)
    metar_quality_t30_df = extract_metar_quality_stats(metar_df, target_offset=30)

    global_t0_df = summarize_metar_quality(metar_quality_t0_df, target_offset=0)
    global_t30_df = summarize_metar_quality(metar_quality_t30_df, target_offset=30)
    global_metar_summary_df = pl.concat([global_t0_df, global_t30_df])

    airport_t0_df = summarize_metar_quality_by_airport(metar_quality_t0_df, target_offset=0)
    airport_t30_df = summarize_metar_quality_by_airport(metar_quality_t30_df, target_offset=30)
    airport_metar_summary_df = pl.concat([airport_t0_df, airport_t30_df]).sort(
        ["target", "num_records_extracted", "lkpa_arrival_airport_icao_code"],
        descending=[False, True, False],
    )

    log_weather_summary(global_metar_summary_df, type="METAR")

    return global_metar_summary_df, airport_metar_summary_df


def evaluate_taf_data_quality(taf_df: pl.DataFrame) -> pl.DataFrame:
    """Evaluate TAF data quality."""
    summary_stats = {
        "num_records_extracted": taf_df.height,
        "num_taf_available": taf_df.filter(pl.col("has_taf_report") == 1).height,
    }
    taf_summary_df = pl.DataFrame([summary_stats])

    log_weather_summary(taf_summary_df, type="TAF")
    return taf_summary_df