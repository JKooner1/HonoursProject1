from __future__ import annotations

from datetime import datetime, UTC
from typing import Optional

import pandas as pd
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.db.models import ETLRunLog, SalesDaily


def reset_sales_daily_table(db: Session) -> None:
    db.execute(delete(SalesDaily))


def insert_sales_daily_dataframe(db: Session, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    records = df.to_dict(orient="records")
    objects = [SalesDaily(**record) for record in records]
    db.add_all(objects)
    return len(objects)


def insert_etl_runlog(
    db: Session,
    *,
    files_processed: int,
    rows_out: int,
    issue_count: int,
    status: str,
    notes: Optional[str] = None,
) -> ETLRunLog:
    runlog = ETLRunLog(
        run_timestamp=datetime.now(UTC),
        files_processed=files_processed,
        rows_out=rows_out,
        issue_count=issue_count,
        status=status,
        notes=notes,
    )
    db.add(runlog)
    return runlog


def get_sales_daily_count(db: Session) -> int:
    return len(db.scalars(select(SalesDaily)).all())


def get_latest_etl_runlog(db: Session) -> Optional[ETLRunLog]:
    return db.scalars(
        select(ETLRunLog).order_by(ETLRunLog.run_timestamp.desc())
    ).first()