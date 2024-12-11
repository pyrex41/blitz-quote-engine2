import argparse
import logging
from datetime import datetime
from build_db_new import MedicareSupplementRateDB
import asyncio
from typing import List, Dict, Set, Tuple
from check_script import process_rate_changes
from collections import defaultdict
from pprint import pprint
import json
from datetime import timedelta

state_list = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC"
        ]

def setup_logging(quiet: bool) -> None:
    log_filename = 'map_sequential.log'
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

def get_previous_month(date_str: str) -> str:
    date = datetime.strptime(date_str, '%Y-%m-%d')
    if date.month == 1:
        return f"{date.year-1}-12-01"
    return f"{date.year}-{date.month-1:02d}-01"

async def process_state_naics(db, state: str, effective_date: str, dic: dict):
    available_naics = await db.get_available_naics(state, effective_date)
    selected_naics = set([x['naic'] for x in db.get_selected_carriers()])
    ls = available_naics.intersection(selected_naics)
    dic[state] = ls
 

async def process_check_task(db, state: str, effective_date: str, dic: dict, available_naics: set, retry = 3):
    try:
            # Create local results first
        _, _, v = await db.check_rate_changes(state, None, effective_date, available_naics)
        vfilt = {k: v for k, v in v.items() if v}
        
        if sum(1 for x in v.values() if x) > 1:
            # Return results instead of modifying shared dict
            return {
                'state': state,
                'changes': vfilt
            }
        return None
    except Exception as e:
        if retry > 0:
            logging.error(f"Error processing {state} at {effective_date}: {e}. Retrying...")
            return await process_check_task(db, state, effective_date, dic, available_naics, retry - 1)
        logging.error(f"Error processing {state} at {effective_date}: {e}. Giving up.")
        return { 'state': state, 'changes': {}, 'error': str(e) }
    

async def main():
    parser = argparse.ArgumentParser(description="Sequentially process and map Medicare Supplement Rate data")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("-d", "--db", type=str, required=True, help="Database file name")
    parser.add_argument("-m", "--months", type=int, default=3, help="Number of months to process")
    parser.add_argument("-n", "--nzips", type=int, default=1, help="Number of ZIP codes to process per state during check")
    parser.add_argument("-s", "--state", nargs="+", help="Process a single or multiple states")
    parser.add_argument("--dry-run", action="store_true", help="Dry run the script without copying rates")
    parser.add_argument("-o", "--output", type=str, help="Output file name")
    parser.add_argument("--remap", action="store_true", help="Remap the rates if applicable before moving forward")
    parser.add_argument("--log-file", type=str, help="Custom log file for database operations")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    logger = logging.getLogger(__name__)

    logger.info("Connecting to database...")
    db = MedicareSupplementRateDB(db_path=args.db, log_file=args.log_file)
    await db.cr.async_init()
    await db.cr.fetch_token()

    # Load data from check script results
    dates = [get_default_effective_date(i) for i in range(args.months)]
    logger.info(f"Processing dates: {dates}")

    # Sort dates in ascending order
    states_to_process = set(args.state) if args.state else set(state_list)
    logger.info(f"Processing states: {states_to_process}")

    state_available = {}
    state_available_tasks = []

    # this lets us only run the check once, not every month
    for state in states_to_process: #, effective_date in state_date_pairs:
        state_available_tasks.append(process_state_naics(db, state, dates[0], state_available))
    await asyncio.gather(*state_available_tasks)

    # Add semaphore to limit concurrent tasks
    semaphore = asyncio.Semaphore(100)  # Adjust number based on your needs
    
    async def bounded_process_check_task(*args, **kwargs):
        async with semaphore:
            return await process_check_task(*args, **kwargs)
    
    # Collect all states and NAICs
    out = {}
    for date in dates:
        check_tasks = []
        for state in states_to_process:
            available_naics = state_available.get(state)
            for _ in range(args.nzips):
                check_tasks.append(bounded_process_check_task(db, state, date, out, available_naics))
        
        logger.info(f"Checking rate changes for {len(states_to_process)} states in {len(check_tasks)} tasks...")
        if args.dry_run:
            logging.info(f"Dry run, skipping check tasks")
            results = []
        else:
            results = await asyncio.gather(*check_tasks)
        
        # Combine results after all tasks complete
        states_with_changes = []
        changes = {}
        errors = {}
        for result in results:
            if result:
                states_with_changes.append(result['state'])
                changes[result['state']] = result['changes']
            if result and 'error' in result:
                errors[result['state']] = result['error']
        
        date_entry = {
            'effective_date': date,
            'states_with_changes': states_with_changes,
            'changes': changes, 
            'errors': errors
        }
        out[date] = date_entry

        if args.remap and changes and not args.dry_run:
            # Create semaphore for map tasks
            map_semaphore = asyncio.Semaphore(100)

            async def bounded_set_map_task(naic, state):
                async with map_semaphore:
                    return await db.set_state_map_naic(naic, state)

            set_map_tasks = []
            for state, dic in changes.items():
                for naic, bool_ in dic.items():
                    if bool_:
                        set_map_tasks.append(bounded_set_map_task(naic, state))
            logging.info(f"Setting state map for {len(set_map_tasks)} states in {len(set_map_tasks)} tasks...")
            await asyncio.gather(*set_map_tasks)

            # Create semaphore for rate tasks
            rate_semaphore = asyncio.Semaphore(100)

            async def bounded_rate_task(state, naic, date):
                async with rate_semaphore:
                    return await db.get_rate_tasks(state, naic, date)

            rate_tasks = []
            for state, dic in changes.items():
                for naic, bool_ in dic.items():
                    if bool_:
                        rate_tasks.append(bounded_rate_task(state, naic, date))
            logging.info(f"Processing {len(rate_tasks)} rate tasks")
            results = await asyncio.gather(*rate_tasks)
            logging.info(f"Processed {len(results)} rate tasks for {state}")


    logger.info("Processing complete")
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(out, f, indent=2)

    if args.dry_run:
        pprint(out)
        return out
    

    

if __name__ == "__main__":
    r = asyncio.run(main())