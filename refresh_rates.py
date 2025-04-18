#!/usr/bin/env python3
import argparse
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
import duckdb
from async_csg import AsyncCSGRequest as csg
from build_duckdb import DuckDBMedicareBuilder
from spot_check import RateSpotChecker
from config import Config
from zips import zipHolder

def setup_logging(quiet: bool) -> None:
    """Set up logging to file and console."""
    log_filename = f'refresh_rates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )
    
    # Log important information about the new rate handling approach
    logging.info("=" * 80)
    logging.info("IMPORTANT: Using natural effective dates from API responses")
    logging.info("Rates will be stored with their actual effective dates from the API,")
    logging.info("not the date used in the API query.")
    logging.info("This allows for proper historical tracking and rate lookups.")
    logging.info("=" * 80)

def calculate_default_check_date() -> tuple:
    """Calculate default check dates - current date and 6 months ahead (first day of month)."""
    today = datetime.now()
    current_date = today.strftime("%Y-%m-%d")
    
    # Calculate date 6 months ahead (first day of that month)
    month = today.month + 6
    year = today.year
    # Adjust if we go into next year
    while month > 12:
        month -= 12
        year += 1
    
    # Use first day of the target month
    target_date = datetime(year, month, 1).strftime("%Y-%m-%d")
    
    return current_date, target_date

async def get_existing_carriers(db_path: str, state: str, previous_date: str = None) -> List[str]:
    """Get list of carriers that exist in the database for a given state, optionally filtered by date."""
    conn = duckdb.connect(db_path)
    try:
        if previous_date:
            # If a specific date is provided, use it
            result = conn.execute("""
                SELECT DISTINCT naic FROM processed_data 
                WHERE state = ? AND effective_date = ? AND success = true
            """, (state, previous_date)).fetchall()
        else:
            # If no date provided, get all carriers for this state from rate_store
            result = conn.execute("""
                SELECT DISTINCT r.naic 
                FROM rate_store r
                JOIN region_mapping m ON r.region_id = m.region_id
                JOIN rate_regions rr ON r.region_id = rr.region_id
                WHERE rr.state = ?
            """, (state,)).fetchall()
            
            if not result:
                # Fallback to checking processed_data without date filter
                result = conn.execute("""
                    SELECT DISTINCT naic FROM processed_data 
                    WHERE state = ? AND success = true
                """, (state,)).fetchall()
                
        return [row[0] for row in result]
    finally:
        conn.close()

async def get_existing_states(db_path: str, previous_date: str = None) -> List[str]:
    """Get list of states that exist in the database, optionally filtered by date."""
    conn = duckdb.connect(db_path)
    try:
        if previous_date:
            # If a specific date is provided, use it
            result = conn.execute("""
                SELECT DISTINCT state FROM processed_data 
                WHERE effective_date = ? AND success = true
            """, (previous_date,)).fetchall()
        else:
            # If no date provided, get all states from rate_regions
            result = conn.execute("""
                SELECT DISTINCT state FROM rate_regions
            """).fetchall()
            
            if not result:
                # Fallback to checking processed_data without date filter
                result = conn.execute("""
                    SELECT DISTINCT state FROM processed_data 
                    WHERE success = true
                """).fetchall()
                
        return [row[0] for row in result]
    finally:
        conn.close()

async def sequential_check_range(args, start_date, end_date):
    """Check rates at 3-month intervals, updating only when changes are detected."""
    logging.info(f"Optimized check from {start_date} to {end_date}")
    
    # Generate list of dates at 3-month intervals between start and end
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    
    date_sequence = [current_date.strftime("%Y-%m-%d")]
    
    # Generate 3-month interval dates
    while current_date < end_date_obj:
        # Add 3 months
        month = current_date.month + 3
        year = current_date.year
        while month > 12:
            month -= 12
            year += 1
        current_date = current_date.replace(year=year, month=month, day=1)
        
        # Stop if we've passed the end date
        if current_date > end_date_obj:
            # Add the actual end date as the final check point
            date_sequence.append(end_date)
            break
            
        date_sequence.append(current_date.strftime("%Y-%m-%d"))
    
    # Ensure the end date is included if it's not already
    if date_sequence[-1] != end_date:
        date_sequence.append(end_date)
    
    logging.info(f"Processing at these check points: {date_sequence}")
    
    # Process each pair of dates in sequence
    for i in range(len(date_sequence) - 1):
        prev_date = date_sequence[i]
        next_date = date_sequence[i + 1]
        
        logging.info(f"Processing {prev_date} -> {next_date}")
        
        # Create modified args for this specific date pair
        current_args = argparse.Namespace(**vars(args))
        current_args.previous_date = prev_date
        current_args.new_date = next_date
        
        # Process this pair of dates
        await process_check_pair(current_args)
        
        logging.info(f"Completed {prev_date} -> {next_date}")
        logging.info("-" * 40)

async def process_check_pair(args):
    """Process a pair of dates, checking for rate changes and updating as needed."""
    # Initialize spot checker
    spot_checker = RateSpotChecker(args.db)
    await spot_checker.init()
    
    # Create notification file
    notification_file = f'rate_change_notifications_{datetime.now().strftime("%Y%m%d")}.md'
    with open(notification_file, 'w') as f:
        f.write(f"# Medicare Rate Change Notifications\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Date range: {args.previous_date} → {args.new_date}\n\n")
        f.write("## Important Notes\n\n")
        f.write("* Rates are stored using the API's natural effective dates, not the requested dates\n")
        f.write("* A carrier might have multiple rates with different effective dates in the database\n")
        f.write("* Rate changes are detected by comparing rates returned by the API, not the dates\n\n")
        f.write("## Rate Changes Detected\n\n")
    
    try:
        # Get states to process
        states_to_process = args.states
        if not states_to_process:
            # Try to get states for the given previous date, then fall back to all states
            states_to_process = await get_existing_states(args.db, args.previous_date)
            if not states_to_process:
                # No states found for previous date, check all states in the database
                states_to_process = await get_existing_states(args.db)
            logging.info(f"Found {len(states_to_process)} states with existing data")
        
        if not states_to_process:
            logging.error(f"No states found in the database")
            return
        
        # Process each state
        total_carriers_checked = 0
        total_carriers_updated = 0
        rate_changes_detected = 0
        
        for state in states_to_process:
            # Get carriers for this state
            carriers_to_check = args.naics
            if not carriers_to_check:
                # Try to get carriers for the given previous date, then fall back to all carriers for the state
                carriers_to_check = await get_existing_carriers(args.db, state, args.previous_date)
                if not carriers_to_check:
                    # No carriers found for previous date, get all carriers for this state
                    carriers_to_check = await get_existing_carriers(args.db, state)
                logging.info(f"Found {len(carriers_to_check)} carriers for {state}")
            
            if not carriers_to_check:
                logging.warning(f"No carriers found for {state} in the database")
                continue
            
            # Track API effective dates for this run
            api_effective_dates = {}
            
            # Custom process function to call when changes are detected
            async def process_carrier(state: str, naic: str, effective_date: str) -> bool:
                nonlocal total_carriers_updated, rate_changes_detected
                if args.dry_run:
                    logging.info(f"DRY RUN: Would process {state}/{naic}/{effective_date}")
                    total_carriers_updated += 1
                    return True
                
                # Get carrier info for notification
                carrier_name = await get_carrier_name(args.db, naic)
                
                # Store the key for updating api_effective_dates later
                carrier_key = f"{state}_{naic}"
                
                builder = DuckDBMedicareBuilder(args.db)
                try:
                    await builder.cr.async_init()
                    await builder.cr.fetch_token()
                    
                    # Process the carrier for the new effective date - this will use
                    # actual effective dates from the API response
                    result = await builder.process_carrier_state(state, naic, effective_date, False)
                    
                    # Get the API effective dates from the processed_data table
                    conn = duckdb.connect(args.db)
                    try:
                        api_date_result = conn.execute("""
                            SELECT api_effective_date FROM processed_data 
                            WHERE state = ? AND naic = ? AND effective_date = ? AND success = true
                        """, (state, naic, effective_date)).fetchone()
                        
                        if api_date_result and api_date_result[0]:
                            api_effective_dates[carrier_key] = api_date_result[0]
                            logging.info(f"API returned effective dates for {carrier_key}: {api_date_result[0]}")
                    finally:
                        conn.close()
                    
                    # Record notification
                    with open(notification_file, 'a') as f:
                        f.write(f"### {carrier_name} ({naic}) - {state}\n\n")
                        f.write(f"- Requested query date: {effective_date}\n")
                        
                        # Include API effective dates if available
                        if carrier_key in api_effective_dates:
                            f.write(f"- API returned effective date(s): {api_effective_dates[carrier_key]}\n")
                            
                        f.write(f"- Status: {'Successful' if result else 'Failed'} - Full refresh triggered due to rate changes\n\n")
                    
                    if result:
                        total_carriers_updated += 1
                        rate_changes_detected += 1
                    
                    return result
                finally:
                    builder.close()
            
            # Check each carrier for rate changes
            carrier_tasks = []
            for naic in carriers_to_check:
                total_carriers_checked += 1
                
                # If force update is enabled, process all carriers without checking
                if args.force_update:
                    task = asyncio.create_task(process_carrier(state, naic, args.new_date))
                    carrier_tasks.append(task)
                    continue
                
                # Check if this carrier-state-date has already been processed
                if not args.force_recheck:
                    conn = duckdb.connect(args.db)
                    already_processed = False
                    try:
                        result = conn.execute("""
                            SELECT success FROM processed_data 
                            WHERE state = ? AND naic = ? AND effective_date = ?
                        """, (state, naic, args.new_date)).fetchone()
                        already_processed = result is not None and result[0]
                    except Exception as e:
                        logging.error(f"Error checking processed status: {str(e)}")
                    finally:
                        conn.close()
                    
                    if already_processed:
                        logging.info(f"Already processed {state}/{naic}/{args.new_date}")
                        continue
                else:
                    logging.info(f"Force recheck enabled, checking {state}/{naic}/{args.new_date} even if already processed")
                
                # Create task to check for rate changes and process if needed
                task = asyncio.create_task(_check_and_process(
                    state, naic, args.previous_date, args.new_date, process_carrier, args.db
                ))
                carrier_tasks.append(task)
            
            # Wait for all carrier checks to complete for this state
            if carrier_tasks:
                await asyncio.gather(*carrier_tasks)
        
        # Print summary
        logging.info(f"Check complete: {args.previous_date} -> {args.new_date}")
        logging.info(f"Checked {total_carriers_checked} carriers, updated {total_carriers_updated} with changes")
        
        # Add summary to notification file
        with open(notification_file, 'a') as f:
            f.write(f"\n## Summary\n\n")
            f.write(f"- Total carriers checked: {total_carriers_checked}\n")
            f.write(f"- Carriers with rate changes: {rate_changes_detected}\n")
            f.write(f"- Carriers successfully updated: {total_carriers_updated}\n")
            
        # Add information about how to verify effective dates
        with open(notification_file, 'a') as f:
            f.write(f"\n## Verification Query\n\n")
            f.write("To verify that rates with their natural effective dates are stored correctly, run this query:\n\n")
            f.write("```sql\n")
            f.write(f'SELECT c.company_name, r.naic, r.plan, r.rate, r.discount_rate, r.effective_date\n')
            f.write(f'FROM rate_store r\n')
            f.write(f'JOIN region_mapping m ON r.region_id = m.region_id AND r.naic = m.naic\n')
            f.write(f'LEFT JOIN carrier_info c ON r.naic = c.naic\n')
            f.write(f'WHERE m.zip_code = \'64105\' AND r.plan = \'G\' AND r.gender = \'M\'\n')
            f.write(f'AND r.tobacco = 0 AND r.age = 65 AND r.naic = \'82538\'\n')
            f.write(f'ORDER BY r.effective_date;\n')
            f.write("```\n\n")
            f.write("This should show entries with their natural effective dates as provided by the API.\n")
    
    finally:
        # Close connections
        spot_checker.close()

# Add new helper function to check and process carriers
async def _check_and_process(state: str, naic: str, previous_date: str, 
                          current_date: str, process_function, db_path: str):
    """Check if rates have changed and process if needed."""
    try:
        # Initialize spot checker just for this check
        spot_checker = RateSpotChecker(db_path)
        await spot_checker.init()
        
        try:
            # Check if rates have changed
            has_changes = await spot_checker.spot_check_carrier(state, naic, previous_date, current_date)
            
            # Make multiple API calls to get a comprehensive set of API effective dates
            # Sometimes different plans or demographics might return different effective dates
            api_effective_dates = []
            try:
                # Make minimal API calls to get the effective dates
                zip_holder = zipHolder("static/uszips.csv")
                state_zips = zip_holder.lookup_zips_by_state(state)
                if state_zips:
                    # Use CSG directly to avoid creating a full builder
                    cr = csg(Config.API_KEY)
                    await cr.async_init()
                    await cr.fetch_token()
                    
                    # Try both G and N plans to catch all possible effective dates
                    plans_to_check = []
                    if state == 'MA':
                        plans_to_check = ['MA_CORE', 'MA_SUPP1']
                    elif state == 'MN':
                        plans_to_check = ['MN_BASIC', 'MN_EXTB']
                    elif state == 'WI':
                        plans_to_check = ['WIR_A50%']
                    else:
                        plans_to_check = ['G', 'N']
                    
                    # Check both age 65 and 75 to ensure we catch effective dates that might vary by age
                    ages_to_check = [65, 75]
                    
                    # Make multiple API calls to get a comprehensive set of dates
                    for plan in plans_to_check:
                        for age in ages_to_check:
                            params = {
                                "zip5": state_zips[0],
                                "naic": naic,
                                "gender": "M",
                                "tobacco": 0,
                                "age": age,
                                "plan": plan,
                                "effective_date": current_date
                            }
                            
                            try:
                                response = await cr.fetch_quote(**params)
                                for quote in response:
                                    if quote.get('company_base', {}).get('naic') == naic:
                                        api_date = quote.get('effective_date')
                                        if api_date and api_date not in api_effective_dates:
                                            api_effective_dates.append(api_date)
                            except Exception as api_error:
                                logging.error(f"Error fetching quote for {plan}, age {age}: {str(api_error)}")
                                continue
            except Exception as e:
                logging.error(f"Error getting API effective dates: {str(e)}")
            
            if has_changes:
                # Before processing, check if we already have data for these API effective dates
                need_processing = True
                
                if api_effective_dates:
                    logging.info(f"API returned effective dates: {api_effective_dates} for {state}/{naic}")
                    
                    # Check if all API effective dates are already in the database
                    conn = duckdb.connect(db_path)
                    try:
                        missing_dates = []
                        for api_date in api_effective_dates:
                            result = conn.execute("""
                                SELECT COUNT(*) FROM rate_store 
                                WHERE naic = ? AND state = ? AND effective_date = ?
                            """, (naic, state, api_date)).fetchone()
                            
                            if result[0] == 0:
                                missing_dates.append(api_date)
                        
                        if not missing_dates:
                            logging.info(f"All API effective dates already have data in the database for {state}/{naic}")
                            need_processing = False
                        else:
                            logging.info(f"Missing data for API effective dates: {missing_dates} for {state}/{naic}")
                    finally:
                        conn.close()
                
                if need_processing:
                    # Process this carrier fully for the new date
                    logging.info(f"Processing {state}/{naic}/{current_date} due to rate changes")
                    await process_function(state, naic, current_date)
                else:
                    # We already have all the data, just mark as processed
                    logging.info(f"Rate changes detected but data already exists for {state}/{naic}")
                    
                    # Convert API effective dates to string for storage
                    api_dates_str = ','.join(api_effective_dates) if api_effective_dates else current_date
                    
                    # Mark as processed
                    conn = duckdb.connect(db_path)
                    try:
                        conn.execute("""
                            INSERT OR REPLACE INTO processed_data 
                            (state, naic, effective_date, api_effective_date, processed_at, success)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                        """, (state, naic, current_date, api_dates_str, True))
                    finally:
                        conn.close()
            else:
                # No changes - just mark as processed without adding new data
                logging.info(f"No rate changes for {state}/{naic} between {previous_date} and {current_date}")
                
                # Get the API effective date from spot_checker's last check
                api_effective_date = None
                try:
                    # First try to get it from the previous record
                    conn = duckdb.connect(db_path)
                    result = conn.execute("""
                        SELECT api_effective_date FROM processed_data 
                        WHERE state = ? AND naic = ? AND effective_date = ? AND success = true
                    """, (state, naic, previous_date)).fetchone()
                    
                    if result and result[0]:
                        api_effective_date = result[0]
                        logging.info(f"Using API effective date from previous record: {api_effective_date}")
                    else:
                        # Make a minimal API call to get the effective date
                        zip_holder = zipHolder("static/uszips.csv")
                        state_zips = zip_holder.lookup_zips_by_state(state)
                        if state_zips:
                            # Use CSG directly to avoid creating a full builder
                            cr = csg(Config.API_KEY)
                            await cr.async_init()
                            await cr.fetch_token()
                            
                            params = {
                                "zip5": state_zips[0],
                                "naic": naic,
                                "gender": "M",
                                "tobacco": 0,
                                "age": 65,
                                "plan": "G",
                                "effective_date": current_date
                            }
                            
                            # Adjust plan based on state
                            if state == 'MA':
                                params['plan'] = 'MA_CORE'
                            elif state == 'MN':
                                params['plan'] = 'MN_BASIC'
                            elif state == 'WI':
                                params['plan'] = 'WIR_A50%'
                            
                            try:
                                response = await cr.fetch_quote(**params)
                                if response and len(response) > 0:
                                    api_effective_date = response[0].get('effective_date')
                                    logging.info(f"Got API effective date from new API call: {api_effective_date}")
                            except Exception as api_error:
                                logging.error(f"Error getting API effective date: {str(api_error)}")
                finally:
                    if not api_effective_date:
                        # Fall back to the requested date if we couldn't get the API date
                        api_effective_date = current_date
                        logging.warning(f"Using requested date as fallback for API date: {api_effective_date}")
                
                # Mark as processed
                conn = duckdb.connect(db_path)
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO processed_data 
                        (state, naic, effective_date, api_effective_date, processed_at, success)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """, (state, naic, current_date, api_effective_date, True))
                finally:
                    conn.close()
        finally:
            spot_checker.close()
    except Exception as e:
        logging.error(f"Error in _check_and_process for {state}/{naic}: {str(e)}")

async def get_carrier_name(db_path: str, naic: str) -> str:
    """Get carrier name from the database."""
    conn = duckdb.connect(db_path)
    try:
        result = conn.execute("""
            SELECT company_name FROM carrier_info 
            WHERE naic = ?
        """, (naic,)).fetchone()
        
        return result[0] if result else f"Carrier {naic}"
    finally:
        conn.close()

async def main():
    parser = argparse.ArgumentParser(description="Spot check Medicare rates and update carriers with changes")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="DuckDB database file path")
    parser.add_argument("--previous-date", type=str, help="Previous date with existing data (YYYY-MM-DD)")
    parser.add_argument("--new-date", type=str, help="New date to check for changes (YYYY-MM-DD)")
    parser.add_argument("--base-date", type=str, help="Starting date for sequential checking")
    parser.add_argument("--end-date", type=str, help="Ending date for sequential checking")
    parser.add_argument("--interval-check", action="store_true", help="Check at 3-month intervals from base-date to end-date")
    parser.add_argument("--states", nargs="+", help="List of states to process (e.g., TX CA)")
    parser.add_argument("--naics", nargs="+", help="List of NAIC codes to process")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")
    parser.add_argument("--force-update", action="store_true", help="Force update all carriers without checking rates")
    parser.add_argument("--force-recheck", action="store_true", help="Force recheck carriers even if they've already been processed for the given date")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    # Validate database exists
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} does not exist. Please create it first.")
        sys.exit(1)
    
    # Check for interval mode (3-month intervals)
    if args.interval_check:
        if not args.base_date or not args.end_date:
            logging.error("Interval check requires both --base-date and --end-date")
            sys.exit(1)
        
        await sequential_check_range(args, args.base_date, args.end_date)
        logging.info("Interval check complete")
        
    else:
        # Regular single date pair mode
        if not args.previous_date or not args.new_date:
            # Calculate default dates if not specified
            current_date, target_date = calculate_default_check_date()
            
            if not args.previous_date:
                args.previous_date = current_date
                logging.info(f"Using current date as previous date: {current_date}")
            
            if not args.new_date:
                args.new_date = target_date
                logging.info(f"Using default target date (6 months ahead): {target_date}")
        
        await process_check_pair(args)

if __name__ == "__main__":
    asyncio.run(main()) 