from app.ingestion.pipeline import run_etl_all_sources
from app.utils.paths import ensure_project_dirs


def main() -> None:
    ensure_project_dirs()

    result = run_etl_all_sources()

    print("ETL run complete")
    print(f"Files processed: {result.files_processed}")
    print(f"Rows in: {result.rows_in}")
    print(f"Rows out: {result.rows_out}")
    print(f"Issues logged: {result.issue_count}")

    if result.summary_by_file:
        print("\nPer-file summary:")
        for item in result.summary_by_file:
            print(
                f"- {item['file_name']} | source={item['source']} | "
                f"status={item['status']} | rows_read={item['rows_read']} | "
                f"rows_out={item['rows_out']} | issues={item['issue_count']}"
            )
    else:
        print("\nNo CSV files found in data/raw/pos or data/raw/delivery.")


if __name__ == "__main__":
    main()