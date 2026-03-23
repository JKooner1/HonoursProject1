from datetime import datetime
from typing import Optional

from app.config import DEFAULT_DATE_FORMATS


def parse_datetime_multi_format(value: str) -> Optional[datetime]:
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    for fmt in DEFAULT_DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None