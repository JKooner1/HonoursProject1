from app.config import PROCESSED_DIR
from app.db.repo import insert_etl_runlog, insert_sales_daily_dataframe, reset_sales_daily_table
from app.db.session import SessionLocal
from app.ingestion.pipeline import run_etl_weekly_reports
from app.utils.paths import ensure_project_dirs
from scripts.init_db import main as init_db_main


def main() -> None:
    ensure_project_dirs()
    init_db_main()

    result = run_etl_weekly_reports()

    print("Weekly report ETL run complete")
    print(f"Files processed: {result.files_processed}")
    print(f"Daily rows output: {result.rows_out}")
    print(f"Issues logged: {result.issue_count}")

    if result.summary_by_file:
        print("\nPer-file summary:")
        for item in result.summary_by_file:
            print(
                f"- {item['file_name']} | status={item['status']} | "
                f"rows_read={item['rows_read']} | "
                f"product_rows={item['product_rows_detected']} | "
                f"daily_rows={item['daily_rows_output']} | "
                f"issues={item['issue_count']}"
            )
    else:
        print("\nNo report CSV files found in data/raw/reports.")

    if not result.cleaned_data.empty:
        cleaned_output_path = PROCESSED_DIR / "sales_daily_transformed.csv"
        result.cleaned_data.to_csv(cleaned_output_path, index=False)
        print(f"\nSaved transformed daily data to: {cleaned_output_path}")

    if not result.issue_log.empty:
        issues_output_path = PROCESSED_DIR / "etl_issue_log.csv"
        result.issue_log.to_csv(issues_output_path, index=False)
        print(f"Saved ETL issue log to: {issues_output_path}")

    with SessionLocal() as db:
        reset_sales_daily_table(db)
        inserted_rows = insert_sales_daily_dataframe(db, result.cleaned_data)

        status = "success"
        notes = f"Inserted {inserted_rows} rows into sales_daily."

        if result.files_processed == 0:
            status = "no_files"
            notes = "No weekly report CSV files found in data/raw/reports."

        insert_etl_runlog(
            db,
            files_processed=result.files_processed,
            rows_out=result.rows_out,
            issue_count=result.issue_count,
            status=status,
            notes=notes,
        )
        db.commit()

    print("\nDatabase load complete")
    print(f"Rows inserted into sales_daily: {result.rows_out}")
    print("ETL run logged in etl_runlog")
    print("SQLite database path: db/retail_analytics.db")


if __name__ == "__main__":
    main()