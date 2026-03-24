from app.config import PROCESSED_DIR
from app.ingestion.pipeline import run_etl_weekly_reports
from app.utils.paths import ensure_project_dirs


def main() -> None:
    ensure_project_dirs()

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


if __name__ == "__main__":
    main()