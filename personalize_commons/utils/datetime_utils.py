from datetime import datetime
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")
IST = ZoneInfo("Asia/Kolkata")


# for strorin the date tin to DB
def utc_now_iso() -> str:
    """Get current UTC time in ISO format (for DB store)."""
    return datetime.now(UTC).isoformat()

# return to ui
def to_ist_iso(utc_iso: str) -> str:
    """Convert UTC ISO string to IST ISO string (for UI)."""
    dt = datetime.fromisoformat(utc_iso).replace(tzinfo=UTC)
    return dt.astimezone(IST).isoformat()

# ist_utc to utc_iso
def ist_to_utc_iso(ist_iso: str) -> str:
    dt = datetime.fromisoformat(ist_iso).replace(tzinfo=IST)
    return dt.astimezone(UTC).isoformat()

