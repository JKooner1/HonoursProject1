from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_POS_DIR = RAW_DIR / "pos"
RAW_DELIVERY_DIR = RAW_DIR / "delivery"
RAW_REPORTS_DIR = RAW_DIR / "reports"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = DATA_DIR / "logs"

DB_DIR = BASE_DIR / "db"
DB_PATH = DB_DIR / "retail_analytics.db"
DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

APP_NAME = "Retail Analytics Dashboard"
APP_VERSION = "0.1.0"

CHUNK_SIZE = 10000
LOG_FILE_NAME = "etl.log"

WEEKDAY_ORDER = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]