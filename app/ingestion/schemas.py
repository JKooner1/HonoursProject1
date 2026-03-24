from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


UNKNOWN_CATEGORY_LABEL = "Other"
UNKNOWN_SUBCATEGORY_LABEL = "Other"

WEEKLY_REPORT_DAILY_COLUMNS = [
    "source_file",
    "report_week",
    "week_start",
    "week_end",
    "sale_date",
    "department",
    "sub_department",
    "sku",
    "product_name",
    "units_sold",
    "weekly_units",
    "weekly_value_gbp",
    "weekly_cost_gbp",
    "weekly_profit_gbp",
    "estimated_unit_price_gbp",
    "estimated_daily_value_gbp",
    "stock_on_hand",
    "on_order",
    "margin_pct",
]


class WeeklyReportDailyRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_file: str
    report_week: Optional[str] = None
    week_start: date
    week_end: date
    sale_date: date

    department: str = Field(default=UNKNOWN_CATEGORY_LABEL)
    sub_department: str = Field(default=UNKNOWN_SUBCATEGORY_LABEL)

    sku: Optional[str] = None
    product_name: str

    units_sold: float
    weekly_units: float
    weekly_value_gbp: Optional[float] = None
    weekly_cost_gbp: Optional[float] = None
    weekly_profit_gbp: Optional[float] = None
    estimated_unit_price_gbp: Optional[float] = None
    estimated_daily_value_gbp: Optional[float] = None
    stock_on_hand: Optional[float] = None
    on_order: Optional[float] = None
    margin_pct: Optional[float] = None


class WeeklyReportFileSummary(BaseModel):
    file_name: str
    rows_read: int
    product_rows_detected: int
    daily_rows_output: int
    issue_count: int
    status: str