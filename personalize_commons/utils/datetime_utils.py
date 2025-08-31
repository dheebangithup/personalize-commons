from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Define Indian Standard Time
IST = ZoneInfo("Asia/Kolkata")

def ist_now() -> datetime:
    """
    Get current datetime in IST timezone.
    """
    return datetime.now(IST)

def  ist_now_month(date_time:datetime=None)->str:
    return date_time.strftime("%Y-%m") if date_time else ist_now().strftime("%Y-%m")

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

def get_month_start_end_dates(date: datetime = None) -> tuple[datetime, datetime]:
    """
    Get the start and end dates of the month for the given date.
    If no date is provided, uses current date in IST.
    
    Args:
        date: The reference date. If None, uses current IST date.
        
    Returns:
        tuple: (first_day_of_month, last_day_of_month) as datetime objects in IST
    """
    if date is None:
        date = ist_now()
    elif date.tzinfo is None:
        # If naive datetime, assume it's in IST
        date = date.replace(tzinfo=IST)
    
    # First day of month
    first_day = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Calculate last day of month
    if date.month == 12:
        last_day = date.replace(year=date.year + 1, month=1, day=1)
    else:
        last_day = date.replace(month=date.month + 1, day=1)
    last_day = last_day - timedelta(days=1)
    last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return first_day, last_day


if __name__ == "__main__":
    # Example usage
    # Get current month's start and end
    start, end = get_month_start_end_dates()
    print(f"Current month - Start: {start}, End: {end}")

    # For a specific date
    test_date = datetime(2023, 2, 15)  # February 15, 2023
    start, end = get_month_start_end_dates(test_date)
    print(f"\nFor Feb 15, 2023 - Start: {start}, End: {end}")
