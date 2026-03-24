from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Date, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SalesDaily(Base):
    __tablename__ = "sales_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    source_file: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    report_week: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    week_start: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    week_end: Mapped[date] = mapped_column(Date, nullable=False)
    sale_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    department: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    sub_department: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    sku: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    units_sold: Mapped[float] = mapped_column(Float, nullable=False)
    weekly_units: Mapped[float] = mapped_column(Float, nullable=False)

    weekly_value_gbp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weekly_cost_gbp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    weekly_profit_gbp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    estimated_unit_price_gbp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    estimated_daily_value_gbp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    stock_on_hand: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    on_order: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    margin_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)


class ETLRunLog(Base):
    __tablename__ = "etl_runlog"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    run_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    files_processed: Mapped[int] = mapped_column(Integer, nullable=False)
    rows_out: Mapped[int] = mapped_column(Integer, nullable=False)
    issue_count: Mapped[int] = mapped_column(Integer, nullable=False)

    status: Mapped[str] = mapped_column(String(50), nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)