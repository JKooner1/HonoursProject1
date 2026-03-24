from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from app.ingestion.report_transformer import (
    _empty_issue_dataframe,
    _empty_transformed_dataframe,
    list_report_csv_files,
    transform_weekly_report_file,
)
from app.logging_config import setup_logging
from app.utils.paths import ensure_project_dirs


@dataclass
class ETLRunResult:
    files_processed: int
    rows_out: int
    issue_count: int
    summary_by_file: list[dict[str, Any]]
    cleaned_data: pd.DataFrame
    issue_log: pd.DataFrame


def run_etl_weekly_reports() -> ETLRunResult:
    ensure_project_dirs()
    logger = setup_logging()

    files = list_report_csv_files()
    summary_by_file: list[dict[str, Any]] = []
    transformed_frames: list[pd.DataFrame] = []
    issue_frames: list[pd.DataFrame] = []

    for file_path in files:
        result = transform_weekly_report_file(file_path)

        summary_by_file.append(result.metrics)

        if not result.transformed_df.empty:
            transformed_frames.append(result.transformed_df)

        if not result.issue_log_df.empty:
            issue_frames.append(result.issue_log_df)

    cleaned_data = (
        pd.concat(transformed_frames, ignore_index=True)
        if transformed_frames
        else _empty_transformed_dataframe()
    )

    issue_log = (
        pd.concat(issue_frames, ignore_index=True)
        if issue_frames
        else _empty_issue_dataframe()
    )

    logger.info(
        "Weekly report ETL complete | files=%s | rows_out=%s | issues=%s",
        len(files),
        len(cleaned_data),
        len(issue_log),
    )

    return ETLRunResult(
        files_processed=len(files),
        rows_out=len(cleaned_data),
        issue_count=len(issue_log),
        summary_by_file=summary_by_file,
        cleaned_data=cleaned_data,
        issue_log=issue_log,
    )