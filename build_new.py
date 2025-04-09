#!/usr/bin/env python3
import argparse
import asyncio
import logging
from datetime import datetime, timedelta
from build_db_new import MedicareSupplementRateDB
from typing import List, Dict, Set
import random
from zips import zipHolder
from async_csg import AsyncCSGRequest as csg
from config import Config
import itertools
from copy import copy
import json
import sys

def setup_logging(quiet: bool) -> None:
    log_filename = f'build_new_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

def load_carrier_selections() -> Dict[str, bool]:
    """Load carrier selections from config file."""
    try:
        with open('carrier_selections.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("carrier_selections.json not found. Please run select_carriers.py first.")
        sys.exit(1)

async def get_available_naics(db: MedicareSupplementRateDB, state: str, effective_date: str, selected_naics: Set[str]) -> Set[str]:
    """Get available NAICs for a state on a specific date."""
    available_naics = await db.get_available_naics(state, effective_date)
    return available_naics.intersection(selected_naics)

async def setup_state_mapping(db: MedicareSupplementRateDB, state: str, naic: str) -> bool:
    """Set up mapping for a specific NAIC in a state."""
    try:
        result = await db.set_state_map_naic(naic, state)
        if result:
            logging.info(f"Successfully set up mapping for {naic} in {state}")
            return True
        else:
            logging.warning(f"Failed to set up mapping for {naic} in {state}")
            return False
    except Exception as e:
        logging.error(f"Error setting up mapping for {naic} in {state}: {str(e)}")
        return False

def build_rate_requests(db: MedicareSupplementRateDB, state: str, naic: str, effective_date: str) -> List[Dict]:
    """Build requests for all combinations of rates for a state/NAIC combination."""
    # Get zip codes for the state
    state_zips = [k for k, v in db.zip_holder.zip_states.items() if v == state]
    if not state_zips:
        logging.warning(f"No zip codes found for state: {state}")
        return []
    
    # Get multiple zip codes for fallback
    random.shuffle(state_zips)  # Randomize the list
    zip_codes = state_zips[:10]  # Take first 10 zips for fallback
    if not zip_codes:
        logging.warning(f"No valid zip codes found for state: {state}")
        return []

    primary_zip = zip_codes[0]
    fallback_zips = zip_codes[1:]
    matching_county = db.zip_holder.lookup_county(primary_zip)[0]
    
    # Define all combinations
    tobacco_options = [0, 1]
    age_options = [65, 70, 75, 80, 85, 90, 95]
    gender_options = ["M", "F"]
    
    if state == 'MA':
        plan_options = ['MA_CORE', 'MA_SUPP1']
    elif state == 'MN':
        plan_options = ['MN_BASIC', 'MN_EXTB']
    elif state == 'WI':
        plan_options = ['WIR_A50%']
    else:
        plan_options = ['N', 'G', 'F']

    # Build base arguments
    base_args = {
        "select": 0,
        "naic": naic,
        "effective_date": effective_date,
        "zip5": primary_zip,
        "zip5_fallback": fallback_zips,
        "county": matching_county
    }

    # Generate all combinations
    combinations = [
        dict(zip(["tobacco", "age", "gender", "plan"], values))
        for values in itertools.product(tobacco_options, age_options, gender_options, plan_options)
    ]

    # Create requests for each combination
    requests = []
    for combination in combinations:
        args = copy(base_args)
        args.update(combination)
        requests.append(args)

    return requests

async def process_rates(db: MedicareSupplementRateDB, state: str, naic: str, effective_date: str):
    """Process all rate combinations for a state/NAIC combination."""
    try:
        requests = build_rate_requests(db, state, naic, effective_date)
        if not requests:
            logging.warning(f"No rate requests generated for {state} {naic} on {effective_date}")
            return False

        # Process requests in chunks to avoid overwhelming the API
        chunk_size = 10
        for i in range(0, len(requests), chunk_size):
            try:
                chunk = requests[i:i + chunk_size]
                tasks = []
                for args in chunk:
                    tasks.append(db.fetch_and_process_and_save(args, retry=10))
                await asyncio.gather(*tasks, return_exceptions=True)  # Allow individual tasks to fail
                await asyncio.sleep(1)  # Rate limiting
            except Exception as chunk_error:
                logging.error(f"Error processing chunk for {state} {naic}: {chunk_error}")
                continue  # Continue with next chunk even if this one failed

        logging.info(f"Processed {len(requests)} rate combinations for {state} {naic} on {effective_date}")
        return True
    except Exception as e:
        logging.error(f"Error processing rates for {state} {naic} on {effective_date}: {str(e)}")
        return False

async def build_database(db_path: str, months: int = 6, dry_run: bool = False):
    """Build a new database from scratch."""
    try:
        db = MedicareSupplementRateDB(db_path=db_path)
        await db.cr.async_init()
        await db.cr.fetch_token()

        # Load carrier selections
        carrier_selections = load_carrier_selections()
        selected_naics = {naic for naic, selected in carrier_selections.items() if selected}
        
        if not selected_naics:
            logging.error("No carriers selected. Please run select_carriers.py first.")
            return

        logging.info(f"Processing {len(selected_naics)} selected carriers")

        # Get effective dates
        today = datetime.now()
        if today.day == 1:
            start_date = today
        else:
            start_date = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
        
        effective_dates = []
        current_date = start_date
        for _ in range(months):
            effective_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date = (current_date + timedelta(days=32)).replace(day=1)

        # State list
        state_list = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC"
        ]

        if dry_run:
            logging.info("DRY RUN - Would process:")
            for date in effective_dates:
                logging.info(f"Date: {date}")
                for state in state_list:
                    logging.info(f"  - State: {state}")
            return

        # Process each state and date
        for effective_date in effective_dates:
            logging.info(f"Processing date: {effective_date}")
            
            for state in state_list:
                try:
                    logging.info(f"Processing state: {state}")
                    
                    # Get available NAICs for this state
                    naics = await get_available_naics(db, state, effective_date, selected_naics)
                    logging.info(f"Found {len(naics)} selected NAICs for {state}")
                    
                    for naic in naics:
                        try:
                            logging.info(f"Processing NAIC: {naic} for {state}")
                            
                            # Set up mapping
                            mapping_success = await setup_state_mapping(db, state, naic)
                            if not mapping_success:
                                logging.warning(f"Skipping {naic} in {state} due to mapping failure")
                                continue

                            # Process rates
                            rate_success = await process_rates(db, state, naic, effective_date)
                            if not rate_success:
                                logging.warning(f"Failed to process rates for {naic} in {state}")
                        except asyncio.CancelledError:
                            raise  # Re-raise cancellation
                        except Exception as naic_error:
                            logging.error(f"Error processing NAIC {naic} in {state}: {str(naic_error)}")
                            continue  # Continue with next NAIC
                except asyncio.CancelledError:
                    raise  # Re-raise cancellation
                except Exception as state_error:
                    logging.error(f"Error processing state {state}: {str(state_error)}")
                    continue  # Continue with next state

    except asyncio.CancelledError:
        logging.info("Build process cancelled by user")
        raise
    except Exception as e:
        logging.error(f"Error building database: {str(e)}")
        raise
    finally:
        logging.info("Database build completed or interrupted")

async def main():
    parser = argparse.ArgumentParser(description="Build a new Medicare Supplement Rate database from scratch")
    parser.add_argument("-d", "--db", type=str, required=True, help="Database file path")
    parser.add_argument("-m", "--months", type=int, default=6, help="Number of months to process")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    if args.dry_run:
        logging.info("DRY RUN MODE - No changes will be made to the database")
    
    try:
        await build_database(args.db, args.months, args.dry_run)
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 