import argparse
import logging
from datetime import datetime
from build_db_new import MedicareSupplementRateDB
import asyncio
from typing import List, Optional
from date_utils import get_effective_dates
from filter_utils import filter_quote_fields

def setup_logging(quiet: bool) -> None:
    log_filename = 'map_all.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    file_handler = logging.FileHandler(log_filename, mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)

    if not quiet:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(log_format))
        root_logger.addHandler(console_handler)

def get_previous_month(date_str: str) -> str:
    """Get the first day of the previous month"""
    date = datetime.strptime(date_str, '%Y-%m-%d')
    if date.month == 1:
        return f"{date.year-1}-12-01"
    return f"{date.year}-{date.month-1:02d}-01"

async def process_state_naics(db, state: str, effective_date: str, dic: dict) -> set:
    available_naics = await db.get_available_naics(state, effective_date)
    selected_naics = set([x['naic'] for x in db.get_selected_carriers()])
    ls = available_naics.intersection(selected_naics)
    dic[state] = ls

async def main() -> None:
    parser = argparse.ArgumentParser(description="Process Medicare Supplement Rate data for states.")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("-f", "--file", type=str, required=True, help="JSON file from check_script.py containing states and dates to process")
    parser.add_argument("-d", "--db", type=str, required=True, help="Database file name")
    parser.add_argument("-m", "--months", type=int, default=6, help="Number of months to process")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")

    args = parser.parse_args()
    logging.info(f"args: {args}")
    setup_logging(args.quiet)

    if not args.dry_run:
        db = MedicareSupplementRateDB(db_path=args.db)
        await db.cr.async_init()
        await db.cr.fetch_token()

    # Create a list of (state, effective_date) tuples to process
    effective_dates = get_effective_dates(args.months)
    state_date_naic_tuples = []

    # Load state list
    state_list = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC"
    ]

    # Load changes from input file
    import json
    with open(args.file, 'r') as f:
        data = json.load(f)

    # Get available NAICs for each state
    state_available = {}
    state_available_tasks = []
    for state in state_list:
        for effective_date in effective_dates:
            state_available_tasks.append(process_state_naics(db, state, effective_date, state_available))
    await asyncio.gather(*state_available_tasks)

    # Extract state/date combinations where changes were detected
    for effective_date, date_entry in data.items():
        assert effective_date == date_entry['effective_date']
        for state, dic in date_entry['changes'].items():
            for naic, changed in dic.items():
                if changed:
                    state_date_naic_tuples.append((state, effective_date, naic))

    if state_date_naic_tuples:
        logging.info(f"Found state/date pairs to process: {state_date_naic_tuples}")
    else:
        logging.info("No changes detected in input file")
        return

    # Sort pairs by date to ensure proper processing order
    state_date_naic_tuples.sort(key=lambda x: x[1])

    if args.dry_run:
        print("\nDRY RUN - Would update the following state/date combinations:\n")
        current_date = None
        for state, effective_date, naic in state_date_naic_tuples:
            if effective_date != current_date:
                current_date = effective_date
                print(f"Effective Date: {effective_date}")
            print(f"    - {state} {naic}")
        return

    # Process state map tasks
    tasks = []
    for state, effective_date, naic in state_date_naic_tuples:
        tasks.append(db.set_state_map_naic(naic, state))
    logging.info(f"Processing {len(tasks)} state map tasks")
    await asyncio.gather(*tasks)

    # Process rate tasks
    rate_tasks = []
    for state, effective_date, naic in state_date_naic_tuples:
        rate_tasks.extend(db.get_rate_tasks(state, naic, effective_date))

    logging.info(f"Processing {len(rate_tasks)} rate tasks")
    results = await asyncio.gather(*rate_tasks)
    logging.info(f"Completed {len(results)} rate tasks")

if __name__ == "__main__":
    asyncio.run(main()) 