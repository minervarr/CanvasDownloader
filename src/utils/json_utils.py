import json
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, time):
            return obj.isoformat()
        if isinstance(obj, timedelta):
            return obj.total_seconds()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, set):
            return list(obj)
        try:
            return str(obj)
        except Exception:
            return f"<non-serializable: {type(obj).__name__}>"