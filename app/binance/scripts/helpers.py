from datetime import datetime, timezone
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

def open_time_ms_to_ist(open_time_ms: int):
    dt_utc = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
    return dt_utc.astimezone(IST)
