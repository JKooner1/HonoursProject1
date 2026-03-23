from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from app.ingestion.cleaners import clean_sales_chunk
from app.ingestion.readers import inspect_csv_file, list_csv_files, read_csv_in_chunks
from app.ingestion.schemas import REQUIRED_CANONICAL_COLUMNS, SourceType
from app.logging_config import setup_logging
from app.utils.paths import ensure_project_dirs


@dataclass
class ETLRunResult:
    files_processed: int
    rows_in: int
    rows_out: int
    issue_count: int
    summary_by_file: list[dict[str, Any]]
    cleaned_data: pd.DataFrame
    issue_log: pd.DataFrame


def _empty_cleaned_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ts",
            "sale_date",
            "sku",
            "product_name",
            "category",
            "qty",
            "unit_price_gbp",
            "line_total_gbp",
            "transaction_id",
            "source",
        ]
    )


def _empty_issue_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "file_name",
            "issue_type",
            "issue_detail",
            "ts_raw",
            "ts",
            "sku",
            "product_name",
            "category",
            "qty",
            "unit_price_gbp",
            "line_total_gbp",
            "transaction_id",
            "source",
        ]
    )


def run_etl_for_source(source: SourceType) -> ETLRunResult:
    ensure_project_dirs()
    logger = setup_logging()

    files = list_csv_files(source)
    cleaned_chunks: list[pd.DataFrame] = []
    all_issue_rows: list[dict[str, Any]] = []
    summary_by_file: list[dict[str, Any]] = []

    total_rows_in = 0
    total_rows_out = 0

    for file_path in files:
        inspection = inspect_csv_file(file_path=file_path, source=source)

        file_summary: dict[str, Any] = {
            "file_name": file_path.name,
            "source": source.value,
            "rows_read": inspection.rows_read,
            "missing_required_columns": inspection.missing_required_columns,
            "rows_out": 0,
            "issue_count": 0,
            "status": "processed",
        }

        if inspection.missing_required_columns:
            logger.warning(
                "Skipping file %s due to missing required columns: %s",
                file_path.name,
                ", ".join(inspection.missing_required_columns),
            )
            all_issue_rows.append(
                {
                    "file_name": file_path.name,
                    "issue_type": "missing_required_columns",
                    "issue_detail": (
                        "File skipped because required canonical columns were not mapped: "
                        + ", ".join(inspection.missing_required_columns)
                    ),
                    "ts_raw": None,
                    "ts": None,
                    "sku": None,
                    "product_name": None,
                    "category": None,
                    "qty": None,
                    "unit_price_gbp": None,
                    "line_total_gbp": None,
                    "transaction_id": None,
                    "source": source.value,
                }
            )
            file_summary["status"] = "skipped"
            file_summary["issue_count"] = 1
            summary_by_file.append(file_summary)
            continue

        logger.info("Processing file: %s", file_path.name)

        file_rows_out = 0
        file_issue_count = 0

        for chunk in read_csv_in_chunks(file_path=file_path, source=source):
            total_rows_in += len(chunk)

            cleaned_result = clean_sales_chunk(df=chunk, file_name=file_path.name)

            cleaned_chunks.append(cleaned_result.cleaned_df)
            all_issue_rows.extend(cleaned_result.issue_rows)

            file_rows_out += cleaned_result.metrics["rows_out"]
            file_issue_count += len(cleaned_result.issue_rows)
            total_rows_out += cleaned_result.metrics["rows_out"]

        file_summary["rows_out"] = file_rows_out
        file_summary["issue_count"] = file_issue_count
        summary_by_file.append(file_summary)

        logger.info(
            "Finished file %s | rows_read=%s | rows_out=%s | issues=%s",
            file_path.name,
            inspection.rows_read,
            file_rows_out,
            file_issue_count,
        )

    cleaned_data = (
        pd.concat(cleaned_chunks, ignore_index=True)
        if cleaned_chunks
        else _empty_cleaned_dataframe()
    )

    issue_log = (
        pd.DataFrame(all_issue_rows)
        if all_issue_rows
        else _empty_issue_dataframe()
    )

    logger.info(
        "ETL complete for source=%s | files=%s | rows_in=%s | rows_out=%s | issues=%s",
        source.value,
        len(files),
        total_rows_in,
        total_rows_out,
        len(issue_log),
    )

    return ETLRunResult(
        files_processed=len(files),
        rows_in=total_rows_in,
        rows_out=total_rows_out,
        issue_count=int(len(issue_log)),
        summary_by_file=summary_by_file,
        cleaned_data=cleaned_data,
        issue_log=issue_log,
    )


def run_etl_all_sources() -> ETLRunResult:
    pos_result = run_etl_for_source(SourceType.POS)
    delivery_result = run_etl_for_source(SourceType.DELIVERY)

    cleaned_frames = [
        df for df in [pos_result.cleaned_data, delivery_result.cleaned_data] if not df.empty
    ]
    issue_frames = [
        df for df in [pos_result.issue_log, delivery_result.issue_log] if not df.empty
    ]

    combined_cleaned = (
        pd.concat(cleaned_frames, ignore_index=True)
        if cleaned_frames
        else _empty_cleaned_dataframe()
    )

    combined_issues = (
        pd.concat(issue_frames, ignore_index=True)
        if issue_frames
        else _empty_issue_dataframe()
    )

    combined_before_dedup = len(combined_cleaned)
    if not combined_cleaned.empty:
        combined_cleaned = combined_cleaned.drop_duplicates(
            subset=["ts", "sku", "source"],
            keep="first",
        ).reset_index(drop=True)

    cross_file_duplicates_removed = combined_before_dedup - len(combined_cleaned)

    if cross_file_duplicates_removed > 0:
        combined_issues = pd.concat(
            [
                combined_issues,
                pd.DataFrame(
                    [
                        {
                            "file_name": "ALL_SOURCES",
                            "issue_type": "cross_file_duplicates_removed",
                            "issue_detail": (
                                f"Removed {cross_file_duplicates_removed} duplicate rows "
                                "across combined sources using (ts, sku, source)."
                            ),
                            "ts_raw": None,
                            "ts": None,
                            "sku": None,
                            "product_name": None,
                            "category": None,
                            "qty": None,
                            "unit_price_gbp": None,
                            "line_total_gbp": None,
                            "transaction_id": None,
                            "source": None,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

    return ETLRunResult(
        files_processed=pos_result.files_processed + delivery_result.files_processed,
        rows_in=pos_result.rows_in + delivery_result.rows_in,
        rows_out=len(combined_cleaned),
        issue_count=int(len(combined_issues)),
        summary_by_file=pos_result.summary_by_file + delivery_result.summary_by_file,
        cleaned_data=combined_cleaned,
        issue_log=combined_issues,
    )