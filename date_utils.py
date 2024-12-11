from datetime import datetime, timedelta
from typing import List
import re

def get_effective_dates(num_months: int = 6) -> List[str]:
    """
    Generate a list of effective dates starting from the first of next month
    for the specified number of months.
    
    Args:
        num_months (int): Number of months to generate dates for (default: 6)
    
    Returns:
        List[str]: List of dates in YYYY-MM-DD format
    """
    # Start with first day of next month
    today = datetime.now()
    if today.day == 1:
        start_date = today
    else:
        start_date = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    
    dates = []
    current_date = start_date
    for _ in range(num_months):
        dates.append(current_date.strftime('%Y-%m-%d'))
        # Move to first day of next month
        current_date = (current_date + timedelta(days=32)).replace(day=1)
    
    return dates

def validate_effective_date(date_str: str) -> bool:
    """
    Validate that a date string is in YYYY-MM-DD format and is the first day of a month.
    
    Args:
        date_str (str): Date string to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return False
        
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        return date.day == 1
    except ValueError:
        return False

def copy_effective_date_data(db, from_date: str, to_date: str) -> None:
    """
    Copy rate store data from one effective date to another.
    
    Args:
        db: Database connection object
        from_date (str): Source effective date in YYYY-MM-DD format
        to_date (str): Target effective date in YYYY-MM-DD format
    """
    if not validate_effective_date(from_date) or not validate_effective_date(to_date):
        raise ValueError("Both dates must be in YYYY-MM-DD format and be the first day of a month")
        
    cursor = db.connect_turso()
    
    # Copy data from source date to target date
    cursor.execute("""
        INSERT OR REPLACE INTO rate_store (key, effective_date, value)
        SELECT key, ?, value
        FROM rate_store
        WHERE effective_date = ?
    """, (to_date, from_date))
    
    db.conn.commit()

