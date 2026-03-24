from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from app.config import RAW_REPORTS_DIR, WEEKDAY_ORDER
from app.ingestion.schemas import (
    UNKNOWN_CATEGORY_LABEL,
    UNKNOWN_SUBCATEGORY_LABEL,
    WEEKLY_REPORT_DAILY_COLUMNS,
)
from app.logging_config import setup_logging


@dataclass
class WeeklyReportTransformResult:
    transformed_df: pd.DataFrame
    issue_log_df: pd.DataFrame
    metrics: dict[str, int | str]


HEADER_PRODUCT_INDEX = 8
HEADER_VARIANT_INDEX = 16
HEADER_DAY_INDEXES = {
    "SUN": 19,
    "MON": 23,
    "TUE": 26,
    "WED": 28,
    "THU": 31,
    "FRI": 33,
    "SAT": 37,
}
HEADER_TOTAL_INDEX = 42
HEADER_VALUE_INDEX = 46
HEADER_COST_INDEX = 50
HEADER_PROFIT_INDEX = 53
HEADER_STOCK_INDEX = 56
HEADER_ON_ORDER_INDEX = 59
HEADER_MARGIN_INDEX = 61
HEADER_SKU_INDEX = 0


def list_report_csv_files() -> list[Path]:
    if not RAW_REPORTS_DIR.exists():
        return []
    return sorted(RAW_REPORTS_DIR.glob("*.csv"))


def _empty_transformed_dataframe() -> pd.DataFrame:
    return pd.DataFrame(columns=WEEKLY_REPORT_DAILY_COLUMNS)


def _empty_issue_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "source_file",
            "row_number",
            "issue_type",
            "issue_detail",
            "department",
            "sub_department",
            "product_name",
            "raw_total",
            "computed_total",
            "raw_value",
        ]
    )


def _safe_cell(row: list[str], index: int) -> str:
    if 0 <= index < len(row):
        return str(row[index]).strip()
    return ""


def _normalise_text(value: str) -> Optional[str]:
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned


def _to_float(value: str) -> Optional[float]:
    cleaned = (
        str(value)
        .strip()
        .replace(",", "")
        .replace("£", "")
        .replace("%", "")
        .replace("(", "-")
        .replace(")", "")
    )
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _strip_code_prefix(value: Optional[str]) -> str:
    if not value:
        return UNKNOWN_CATEGORY_LABEL
    cleaned = value.strip()
    if "-" in cleaned:
        _, rhs = cleaned.split("-", 1)
        rhs = rhs.strip()
        return rhs if rhs else cleaned
    return cleaned


def _detect_header_row(rows: list[list[str]]) -> int:
    for index, row in enumerate(rows):
        row_text = " | ".join(cell.strip() for cell in row if cell.strip())
        if "Product Description" in row_text and "SUN" in row_text and "Value" in row_text:
            return index
    raise ValueError("Could not detect weekly report header row.")


def _parse_report_week_info(rows: list[list[str]]) -> tuple[Optional[str], datetime, datetime]:
    joined_text = "\n".join(" ".join(cell for cell in row if cell) for row in rows[:10])

    week_match = re.search(r"For Week\s+(\d+)", joined_text, flags=re.IGNORECASE)
    date_match = re.search(
        r"(\d{1,2}-[A-Za-z]{3}-\d{4})\s+to\s+(\d{1,2}-[A-Za-z]{3}-\d{4})",
        joined_text,
        flags=re.IGNORECASE,
    )

    if not date_match:
        raise ValueError("Could not parse week start and end dates from report metadata.")

    week_id = week_match.group(1) if week_match else None
    week_start = datetime.strptime(date_match.group(1), "%d-%b-%Y")
    week_end = datetime.strptime(date_match.group(2), "%d-%b-%Y")

    return week_id, week_start, week_end


def _is_department_row(row: list[str]) -> bool:
    return _safe_cell(row, 0).upper() == "DEPT:" or _safe_cell(row, 0).startswith("Dept:")


def _is_sub_department_row(row: list[str]) -> bool:
    return _safe_cell(row, 5).upper() == "SUB DEPT:" or _safe_cell(row, 5).startswith("Sub Dept:")


def _is_grand_total_row(row: list[str]) -> bool:
    return _safe_cell(row, 0).startswith("Grand Total:")


def _build_product_name(row: list[str]) -> Optional[str]:
    base_name = _normalise_text(_safe_cell(row, HEADER_PRODUCT_INDEX))
    variant = _normalise_text(_safe_cell(row, HEADER_VARIANT_INDEX))

    if not base_name:
        return None

    if variant:
        return f"{base_name} {variant}".strip()

    return base_name


def _has_numeric_sales_content(row: list[str]) -> bool:
    daily_values = [_to_float(_safe_cell(row, idx)) for idx in HEADER_DAY_INDEXES.values()]
    weekly_total = _to_float(_safe_cell(row, HEADER_TOTAL_INDEX))
    weekly_value = _to_float(_safe_cell(row, HEADER_VALUE_INDEX))

    return any(value is not None for value in daily_values) or weekly_total is not None or weekly_value is not None


def _is_product_row(row: list[str]) -> bool:
    if _is_department_row(row) or _is_sub_department_row(row) or _is_grand_total_row(row):
        return False

    product_name = _build_product_name(row)
    if not product_name:
        return False

    return _has_numeric_sales_content(row)


def _build_issue(
    source_file: str,
    row_number: int,
    issue_type: str,
    issue_detail: str,
    department: Optional[str],
    sub_department: Optional[str],
    product_name: Optional[str],
    raw_total: Optional[float] = None,
    computed_total: Optional[float] = None,
    raw_value: Optional[float] = None,
) -> dict[str, Any]:
    return {
        "source_file": source_file,
        "row_number": row_number,
        "issue_type": issue_type,
        "issue_detail": issue_detail,
        "department": department,
        "sub_department": sub_department,
        "product_name": product_name,
        "raw_total": raw_total,
        "computed_total": computed_total,
        "raw_value": raw_value,
    }


def transform_weekly_report_file(file_path: Path) -> WeeklyReportTransformResult:
    logger = setup_logging()

    with open(file_path, "r", encoding="utf-8-sig", errors="replace", newline="") as file_handle:
        reader = csv.reader(file_handle)
        rows = list(reader)

    if not rows:
        return WeeklyReportTransformResult(
            transformed_df=_empty_transformed_dataframe(),
            issue_log_df=_empty_issue_dataframe(),
            metrics={
                "file_name": file_path.name,
                "rows_read": 0,
                "product_rows_detected": 0,
                "daily_rows_output": 0,
                "issue_count": 0,
                "status": "empty",
            },
        )

    header_index = _detect_header_row(rows)
    report_week, week_start, week_end = _parse_report_week_info(rows)

    current_department = UNKNOWN_CATEGORY_LABEL
    current_sub_department = UNKNOWN_SUBCATEGORY_LABEL

    output_rows: list[dict[str, Any]] = []
    issue_rows: list[dict[str, Any]] = []
    product_rows_detected = 0

    for row_number, row in enumerate(rows[header_index + 1 :], start=header_index + 2):
        if not any(cell.strip() for cell in row):
            continue

        if _is_department_row(row):
            current_department = _strip_code_prefix(_safe_cell(row, 4))
            continue

        if _is_sub_department_row(row):
            current_sub_department = _strip_code_prefix(_safe_cell(row, 12))
            continue

        if _is_grand_total_row(row):
            continue

        if not _is_product_row(row):
            continue

        product_rows_detected += 1

        sku = _normalise_text(_safe_cell(row, HEADER_SKU_INDEX))
        product_name = _build_product_name(row)
        if not product_name:
            continue

        daily_units_map: dict[str, float] = {}
        for day_name, day_index in HEADER_DAY_INDEXES.items():
            day_value = _to_float(_safe_cell(row, day_index))
            daily_units_map[day_name] = 0.0 if day_value is None else float(day_value)

        computed_total_units = float(sum(daily_units_map.values()))
        weekly_units = _to_float(_safe_cell(row, HEADER_TOTAL_INDEX))
        weekly_value = _to_float(_safe_cell(row, HEADER_VALUE_INDEX))
        weekly_cost = _to_float(_safe_cell(row, HEADER_COST_INDEX))
        weekly_profit = _to_float(_safe_cell(row, HEADER_PROFIT_INDEX))
        stock_on_hand = _to_float(_safe_cell(row, HEADER_STOCK_INDEX))
        on_order = _to_float(_safe_cell(row, HEADER_ON_ORDER_INDEX))
        margin_pct = _to_float(_safe_cell(row, HEADER_MARGIN_INDEX))

        if weekly_units is None:
            weekly_units = computed_total_units

        if abs(computed_total_units - weekly_units) > 0.001:
            issue_rows.append(
                _build_issue(
                    source_file=file_path.name,
                    row_number=row_number,
                    issue_type="weekly_total_mismatch",
                    issue_detail=(
                        "Sum of SUN-SAT units does not match the report Total column. "
                        "Computed daily sum retained for row-level daily data."
                    ),
                    department=current_department,
                    sub_department=current_sub_department,
                    product_name=product_name,
                    raw_total=weekly_units,
                    computed_total=computed_total_units,
                    raw_value=weekly_value,
                )
            )

        estimated_unit_price = None
        if weekly_value is not None and computed_total_units > 0:
            estimated_unit_price = weekly_value / computed_total_units

        if weekly_value is not None and computed_total_units == 0:
            issue_rows.append(
                _build_issue(
                    source_file=file_path.name,
                    row_number=row_number,
                    issue_type="weekly_value_without_units",
                    issue_detail=(
                        "Weekly value exists but total daily units are zero. "
                        "Daily value could not be proportionally estimated."
                    ),
                    department=current_department,
                    sub_department=current_sub_department,
                    product_name=product_name,
                    raw_total=weekly_units,
                    computed_total=computed_total_units,
                    raw_value=weekly_value,
                )
            )

        for offset, day_name in enumerate(WEEKDAY_ORDER):
            sale_date = (week_start + timedelta(days=offset)).date()
            units_sold = daily_units_map[day_name]

            estimated_daily_value = None
            if estimated_unit_price is not None:
                estimated_daily_value = units_sold * estimated_unit_price

            output_rows.append(
                {
                    "source_file": file_path.name,
                    "report_week": report_week,
                    "week_start": week_start.date(),
                    "week_end": week_end.date(),
                    "sale_date": sale_date,
                    "department": current_department or UNKNOWN_CATEGORY_LABEL,
                    "sub_department": current_sub_department or UNKNOWN_SUBCATEGORY_LABEL,
                    "sku": sku,
                    "product_name": product_name,
                    "units_sold": float(units_sold),
                    "weekly_units": float(weekly_units),
                    "weekly_value_gbp": weekly_value,
                    "weekly_cost_gbp": weekly_cost,
                    "weekly_profit_gbp": weekly_profit,
                    "estimated_unit_price_gbp": estimated_unit_price,
                    "estimated_daily_value_gbp": estimated_daily_value,
                    "stock_on_hand": stock_on_hand,
                    "on_order": on_order,
                    "margin_pct": margin_pct,
                }
            )

    transformed_df = (
        pd.DataFrame(output_rows, columns=WEEKLY_REPORT_DAILY_COLUMNS)
        if output_rows
        else _empty_transformed_dataframe()
    )
    issue_log_df = pd.DataFrame(issue_rows) if issue_rows else _empty_issue_dataframe()

    logger.info(
        "Transformed weekly report %s | product_rows=%s | daily_rows=%s | issues=%s",
        file_path.name,
        product_rows_detected,
        len(transformed_df),
        len(issue_log_df),
    )

    return WeeklyReportTransformResult(
        transformed_df=transformed_df,
        issue_log_df=issue_log_df,
        metrics={
            "file_name": file_path.name,
            "rows_read": len(rows),
            "product_rows_detected": product_rows_detected,
            "daily_rows_output": len(transformed_df),
            "issue_count": len(issue_log_df),
            "status": "processed",
        },
    )