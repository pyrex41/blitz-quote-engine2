from build_db_new import MedicareSupplementRateDB

import asyncio
from pprint import pprint

import logging
import time

from datetime import datetime, timedelta
ar = asyncio.run

async def init(db):
    await db.cr.async_init()
    await db.cr.fetch_token()

state_list = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC"
        ]

def get_default_effective_date(months_ahead: int = 0) -> str:
    """Get the effective date (first of next month + optional months ahead)"""
    today = datetime.now()
    if today.day == 1 and months_ahead == 0:
        return today.strftime('%Y-%m-%d')
    next_month = today.replace(day=1) + timedelta(days=32)
    target_date = next_month.replace(day=1)
    if months_ahead > 0:
        for _ in range(months_ahead):
            target_date = (target_date + timedelta(days=32)).replace(day=1)
    return target_date.strftime('%Y-%m-%d')

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch quotes for random zip codes")
    parser.add_argument("-d", "--db", type=str, help="Database to use")
    parser.add_argument("-m", "--months", type=int, default=3, help="Number of months to copy forward")
    parser.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format to copy rates from")
    args = parser.parse_args()
    db = MedicareSupplementRateDB(args.db)

    # Initialize database connection and token inside the async context
    await init(db)
    tasks = []
    target_dates_all = [get_default_effective_date(i) for i in range(100)]
    # Find index of start_date in target_dates_all
    try:
        start_idx = target_dates_all.index(args.start_date)
    except ValueError:
        raise ValueError(f"Start date {args.start_date} not found in target dates. Must be first of month within next 100 months.")
    # Get next m dates after start_date
    target_dates = target_dates_all[start_idx+1:start_idx+1+args.months]
    logging.info(f"Copying rates from {args.start_date} to dates: {target_dates}")
    for state in state_list:
        naics = db.get_existing_naics(state)
        for naic in naics:
            for td in target_dates:
                tasks.append(db.copy_rates(state, naic, args.start_date, td))
                                           
    logging.info(f"Tasks: {len(tasks)}. Running sequentially.")
    start_time = time.time()
    for t in tasks:
        await t
    end_time = time.time()
    logging.info(f"Time taken: {end_time - start_time} seconds to run {len(tasks)} tasks")
    return db
    
if __name__ == "__main__":
    db = asyncio.run(main())

    