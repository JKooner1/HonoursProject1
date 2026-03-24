from app.config import (
    DB_DIR,
    LOG_DIR,
    PROCESSED_DIR,
    RAW_DELIVERY_DIR,
    RAW_POS_DIR,
    RAW_REPORTS_DIR,
)


def ensure_project_dirs() -> None:
    for directory in [
        RAW_POS_DIR,
        RAW_DELIVERY_DIR,
        RAW_REPORTS_DIR,
        PROCESSED_DIR,
        LOG_DIR,
        DB_DIR,
    ]:
        directory.mkdir(parents=True, exist_ok=True)