from datetime import datetime
from zoneinfo import ZoneInfo

# Define Indian Standard Time
IST = ZoneInfo("Asia/Kolkata")

def ist_now() -> datetime:
    """
    Get current datetime in IST timezone.
    """
    return datetime.now(IST)

def ist_now_iso() -> str:
    """
    Get current IST time in ISO format (useful for storing in DB or sending to frontend).
    Example: "2025-07-23T12:34:56+05:30"
    """
    return ist_now().isoformat()

def ist_now_human_readable() -> str:
    """
    Return IST time in human-friendly format (e.g., for logging or UI).
    Example: "Jul 23, 2025 12:34 PM"
    """
    return ist_now().strftime("%b %d, %Y %I:%M %p")  # 12-hour format with AM/PM

