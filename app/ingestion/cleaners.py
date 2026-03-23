from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from app.ingestion.schemas import UNKNOWN_CATEGORY_LABEL


@dataclass
class ChunkCleaningResult:
    cleaned_df: pd.DataFrame
    metrics: dict[str, int]
    issue_rows: list[dict[str, Any]]


def _clean_string_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def _normalise_missing_strings(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    return cleaned.replace(
        {
            "": pd.NA,
            "nan": pd.NA,
            "none": pd.NA,
            "null": pd.NA,
            "nat": pd.NA,
            "<na>": pd.NA,
        }
    )


def _parse_timestamps(series: pd.Series) -> tuple[pd.Series, int]:
    cleaned = _normalise_missing_strings(series)

    parsed = pd.to_datetime(
        cleaned,
        errors="coerce",
        dayfirst=True,
        format="mixed",
    )

    invalid_count = int(parsed.isna().sum())
    return parsed, invalid_count


def _to_numeric(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace("£", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace("(", "-", regex=False)
        .str.replace(")", "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _build_issue_row(
    row: pd.Series,
    file_name: str,
    issue_type: str,
    issue_detail: str,
) -> dict[str, Any]:
    return {
        "file_name": file_name,
        "issue_type": issue_type,
        "issue_detail": issue_detail,
        "ts_raw": row.get("ts_raw"),
        "ts": row.get("ts"),
        "sku": row.get("sku"),
        "product_name": row.get("product_name"),
        "category": row.get("category"),
        "qty": row.get("qty"),
        "unit_price_gbp": row.get("unit_price_gbp"),
        "line_total_gbp": row.get("line_total_gbp"),
        "transaction_id": row.get("transaction_id"),
        "source": row.get("source"),
    }


def clean_sales_chunk(df: pd.DataFrame, file_name: str) -> ChunkCleaningResult:
    working_df = df.copy()

    metrics = {
        "rows_in": int(len(working_df)),
        "rows_out": 0,
        "missing_ts": 0,
        "invalid_ts": 0,
        "missing_sku": 0,
        "invalid_qty": 0,
        "invalid_line_total": 0,
        "unknown_category_mapped": 0,
        "duplicate_rows_removed": 0,
        "refund_inconsistency_flagged": 0,
    }

    issue_rows: list[dict[str, Any]] = []

    expected_columns = [
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

    for column in expected_columns:
        if column not in working_df.columns:
            working_df[column] = pd.NA

    working_df = working_df[expected_columns].copy()

    for column in ["sku", "product_name", "category", "transaction_id", "source"]:
        working_df[column] = _normalise_missing_strings(working_df[column])

    working_df["ts_raw"] = working_df["ts"]
    ts_missing_before = _normalise_missing_strings(working_df["ts"]).isna()
    working_df["ts"], invalid_ts_total = _parse_timestamps(working_df["ts"])

    metrics["missing_ts"] = int(ts_missing_before.sum())
    metrics["invalid_ts"] = max(0, invalid_ts_total - metrics["missing_ts"])

    working_df["qty"] = _to_numeric(working_df["qty"])
    working_df["unit_price_gbp"] = _to_numeric(working_df["unit_price_gbp"])
    working_df["line_total_gbp"] = _to_numeric(working_df["line_total_gbp"])

    working_df["sku"] = _clean_string_series(working_df["sku"]).replace(
        {"": pd.NA, "nan": pd.NA, "None": pd.NA}
    )

    category_before_missing = working_df["category"].isna()
    metrics["unknown_category_mapped"] = int(category_before_missing.sum())
    working_df["category"] = working_df["category"].fillna(UNKNOWN_CATEGORY_LABEL)

    source_missing_mask = working_df["source"].isna()
    if source_missing_mask.any():
        for _, row in working_df[source_missing_mask].iterrows():
            issue_rows.append(
                _build_issue_row(
                    row=row,
                    file_name=file_name,
                    issue_type="missing_source",
                    issue_detail="Source was missing after ingestion mapping.",
                )
            )

    missing_ts_mask = working_df["ts"].isna()
    for _, row in working_df[missing_ts_mask].iterrows():
        issue_rows.append(
            _build_issue_row(
                row=row,
                file_name=file_name,
                issue_type="invalid_timestamp",
                issue_detail="Timestamp missing or failed parsing.",
            )
        )

    missing_sku_mask = working_df["sku"].isna()
    metrics["missing_sku"] = int(missing_sku_mask.sum())
    for _, row in working_df[missing_sku_mask].iterrows():
        issue_rows.append(
            _build_issue_row(
                row=row,
                file_name=file_name,
                issue_type="missing_sku",
                issue_detail="SKU missing after normalisation.",
            )
        )

    invalid_qty_mask = working_df["qty"].isna()
    metrics["invalid_qty"] = int(invalid_qty_mask.sum())
    for _, row in working_df[invalid_qty_mask].iterrows():
        issue_rows.append(
            _build_issue_row(
                row=row,
                file_name=file_name,
                issue_type="invalid_qty",
                issue_detail="Quantity missing or non-numeric.",
            )
        )

    invalid_total_mask = working_df["line_total_gbp"].isna()
    metrics["invalid_line_total"] = int(invalid_total_mask.sum())
    for _, row in working_df[invalid_total_mask].iterrows():
        issue_rows.append(
            _build_issue_row(
                row=row,
                file_name=file_name,
                issue_type="invalid_line_total",
                issue_detail="Line total missing or non-numeric.",
            )
        )

    refund_inconsistency_mask = (
        working_df["qty"].notna()
        & working_df["line_total_gbp"].notna()
        & (
            ((working_df["qty"] < 0) & (working_df["line_total_gbp"] > 0))
            | ((working_df["qty"] > 0) & (working_df["line_total_gbp"] < 0))
        )
    )
    metrics["refund_inconsistency_flagged"] = int(refund_inconsistency_mask.sum())

    for _, row in working_df[refund_inconsistency_mask].iterrows():
        issue_rows.append(
            _build_issue_row(
                row=row,
                file_name=file_name,
                issue_type="refund_sign_inconsistency",
                issue_detail="Quantity and line total signs conflict. Raw values preserved.",
            )
        )

    valid_mask = (
        working_df["ts"].notna()
        & working_df["sku"].notna()
        & working_df["qty"].notna()
        & working_df["line_total_gbp"].notna()
        & working_df["source"].notna()
    )

    cleaned_df = working_df.loc[valid_mask].copy()

    before_dedup = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates(subset=["ts", "sku", "source"], keep="first")
    metrics["duplicate_rows_removed"] = int(before_dedup - len(cleaned_df))

    metrics["rows_out"] = int(len(cleaned_df))

    cleaned_df["sku"] = cleaned_df["sku"].astype(str)
    cleaned_df["product_name"] = cleaned_df["product_name"].astype("string")
    cleaned_df["category"] = cleaned_df["category"].astype(str)
    cleaned_df["transaction_id"] = cleaned_df["transaction_id"].astype("string")
    cleaned_df["source"] = cleaned_df["source"].astype(str)
    cleaned_df["qty"] = cleaned_df["qty"].astype(float)
    cleaned_df["unit_price_gbp"] = cleaned_df["unit_price_gbp"].astype(float)
    cleaned_df["line_total_gbp"] = cleaned_df["line_total_gbp"].astype(float)

    cleaned_df["sale_date"] = cleaned_df["ts"].dt.date

    cleaned_df = cleaned_df[
        [
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
    ].reset_index(drop=True)

    return ChunkCleaningResult(
        cleaned_df=cleaned_df,
        metrics=metrics,
        issue_rows=issue_rows,
    )