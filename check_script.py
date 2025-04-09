import argparse
import logging
import random
from datetime import datetime, timedelta
from build_db_new import MedicareSupplementRateDB
import asyncio
from aiolimiter import AsyncLimiter
from zips import zipHolder
import json
import os
import sqlite3
import traceback
from collections import defaultdict
from pprint import pprint

async def sync_turso():
    url = os.getenv("NEW_QUOTE_DB_URL")
    key = os.getenv("NEW_QUOTE_DB_KEY")
    conn = sqlite3.connect("replica.db")
    return conn

def validate_effective_date(date_str: str) -> datetime:
    """Validate and parse effective date string"""
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        if date.day != 1:
            raise ValueError("Effective date must be the first day of a month")
        return date
    except ValueError as e:
        raise ValueError(f"Invalid effective date format: {e}")

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

async def process_state_tasks(db, state, num_zips, rate_limiter, effective_date):
    zip_holder = zipHolder("static/uszips.csv")
    state_zips = [k for k, v in zip_holder.zip_states.items() if v == state]
    
    if not state_zips:
        logging.warning(f"No ZIP codes found for state: {state}")
        return None

    async def process_zip(random_zip):
        try:
            logging.info(f"Processing state: {state}, zip: {random_zip}, effective date: {effective_date}")
            async with rate_limiter:
                r, s, v = await db.check_rate_changes(state, random_zip, effective_date)
            return {
                "zip": random_zip,
                "state": state,
                "effective_date": effective_date,
                #"current_rates": r,
                #"stored_rates": s,
                "changes": v
            }
        except Exception as e:
            logging.error(f"Error processing state: {state}, zip: {random_zip}. Error: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "zip": random_zip,
                "state": state,
                "effective_date": effective_date,
                #"current_rates": None,
                #"stored_rates": None,
                "changes": None,
                "error": str(e)
            }

    selected_zips = random.sample(state_zips, min(num_zips, len(state_zips)))
    tasks = [process_zip(zip_code) for zip_code in selected_zips]
    return tasks


def print_changes(changes):
    """Print formatted rate changes"""
    if not changes:
        logging.info("No changes found in any state or NAIC code.")
        return

    for state, naic_data in changes.items():
        logging.info(f"\nState: {state}")
        for naic, change_types in naic_data.items():
            logging.info(f"  NAIC: {naic}")
            if 'modified' in change_types:
                logging.info("    Modified rates")
            if 'new' in change_types:
                logging.info("    New rates")

async def process_rate_changes(states_to_process, dates_to_process, num_zips=1, db_path=None, 
                             output_file=None, no_sync=False):
    """
    Process rate changes for given states and dates.
    
    Args:
        states_to_process (list): List of state codes to process
        dates_to_process (list): List of effective dates to check
        num_zips (int): Number of random ZIP codes to use per state
        db_path (str): Path to database file
        output_file (str): Path to output file
        no_sync (bool): Whether to skip Turso sync
    
    Returns:
        dict: Results of rate changes by date
    """
    date_results = {date: {
        'effective_date': date,
        'states_with_changes': set(),
        'changes': {}
    } for date in dates_to_process}

    try:
        if not no_sync:
            await sync_turso()

        db = MedicareSupplementRateDB(db_path=db_path)
        logging.info(f"Using database: {db_path}")
        await db.cr.async_init()
        await db.cr.fetch_token()

        rate_limiter = AsyncLimiter(20, 1)

        # Process each state
        tasks = []
        task_index = {}
        task_count = 0
        for state in states_to_process:
            for effective_date in dates_to_process:
                state_tasks = await process_state_tasks(db, state, num_zips, rate_limiter, effective_date)
                if state_tasks:
                    ii = task_count
                    task_count += len(state_tasks)
                    task_index[(state, effective_date)] = (ii,task_count)
                    tasks.extend(state_tasks)

        results_all = await asyncio.gather(*tasks)
        
        if results_all:
            for result in results_all:
                effective_date = result['effective_date']
                dic_to_extend = date_results.get(effective_date, {})
                state = result['state']
                change_dic = dic_to_extend.get('changes',{}).get(state,{})
                any_changes = False
                if result['changes'] is not None:
                    for naic, bool_ in result['changes'].items():
                        flag = bool_ or change_dic.get(naic, False)
                        change_dic[naic] = flag
                        any_changes = any_changes or flag
                    if any_changes:
                        dic_to_extend['states_with_changes'].add(state)
                    dic_to_extend['changes'][state] = change_dic
                else:
                    logging.warning(f"No changes data for state: {state}, effective date: {effective_date}")
                date_results[effective_date] = dic_to_extend

        for k, v in date_results.items():
            v['states_with_changes'] = list(v['states_with_changes'])
            date_results[k] = v
                
        if date_results and output_file:
            with open(output_file, 'w') as f:
                json.dump(date_results, f, indent=2)
            logging.info(f"Results written to {output_file}")

        return date_results

    except Exception as e:
        logging.error(f"Error in processing: {str(e)}")
        logging.error(traceback.format_exc())
        raise

async def main():
    parser = argparse.ArgumentParser(description="Test Medicare Supplement Rate changes.")
    parser.add_argument("-a", "--all", action="store_true", help="Process all states")
    parser.add_argument("--multiple", nargs="+", help="Process multiple specified states")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("-n", "--num_zips", type=int, default=1, help="Number of random ZIP codes to use per state")
    parser.add_argument("-o", "--output", type=str, help="Output file name")
    parser.add_argument("-d", "--db", type=str, help="Database file name")
    parser.add_argument("-e", "--effective-date", type=str, help="Effective date (YYYY-MM-DD)")
    parser.add_argument("-f", "--full-changes", action="store_true", default=False, help="Output full changes")
    parser.add_argument("--no-sync", action="store_true", help="Do not sync Turso replica")
    parser.add_argument("state", nargs="?", help="Process a single state")
    parser.add_argument("-m", "--months", type=int, help="Number of months ahead to check")
    parser.add_argument("-g", "--group", type=int, help="Process group of states")
    parser.add_argument("--naic", type=str, help="Process a specific carrier NAIC")

    state_list = [
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        'DC'
    ]
    
    args = parser.parse_args()
    
    # Get list of dates to process
    dates_to_process = []
    if args.effective_date:
        dates_to_process = [args.effective_date]
    elif args.months:
        for i in range(args.months-1, -1, -1):
            dates_to_process.append(get_default_effective_date(i))
    else:
        dates_to_process = [get_default_effective_date()]

    # Define states to process
    states_to_process = []
    if args.all:
        states_to_process = state_list
    elif args.multiple:
        states_to_process = [s for s in args.multiple if s in state_list]
    elif args.group:
        if args.group in range(1, 6):
            i = (args.group - 1) * 10
            j = i + 10 if i + 10 < 51 else 51
            states_to_process = state_list[i:j]
        else:
            logging.error(f"Invalid group number: {args.group}. Must be between 1 and 5")
            return
    elif args.state and args.state in state_list:
        states_to_process = [args.state]

    if not states_to_process:
        logging.error("No valid states to process")
        return

    results = await process_rate_changes(
        states_to_process=states_to_process,
        dates_to_process=dates_to_process,
        num_zips=args.num_zips,
        db_path=args.db,
        output_file=args.output,
        no_sync=args.no_sync
    )

    if results and not args.output:
        print(json.dumps(results, indent=2))

if __name__ == "__main__":
    asyncio.run(main())