#!/usr/bin/env python3
import argparse
import asyncio
import logging
import json
import sys
from typing import List, Dict, Set, Optional
from datetime import datetime, timedelta
from build_db_new import MedicareSupplementRateDB
from zips import zipHolder
from config import Config

def setup_logging(log_file: str = None, quiet: bool = False) -> None:
    """Set up logging to file and console."""
    if log_file is None:
        log_file = f'build_carrier_db_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    handlers = [logging.FileHandler(log_file)]
    if not quiet:
        handlers.append(logging.StreamHandler())
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    return logging.getLogger(__name__)

def load_carrier_selections(file_path: str = 'carrier_selections.json') -> List[str]:
    """Load carrier selections from config file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return [naic for naic, selected in data.items() if selected]
    except FileNotFoundError:
        logging.error(f"{file_path} not found. Please run select_carriers.py first.")
        sys.exit(1)

def generate_effective_dates(months: int = 6) -> List[str]:
    """Generate a list of effective dates (first of the next N months)."""
    today = datetime.now()
    
    # Start with the first day of next month
    if today.day == 1:
        start_date = today
    else:
        start_date = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    
    effective_dates = []
    current_date = start_date
    for _ in range(months):
        effective_dates.append(current_date.strftime('%Y-%m-%d'))
        current_date = (current_date + timedelta(days=32)).replace(day=1)
    
    return effective_dates

def get_all_states() -> List[str]:
    """Return a list of all US states and DC."""
    return [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC"
    ]

def get_sample_zip(db: MedicareSupplementRateDB, state: str) -> str:
    """Get a representative ZIP code for a state."""
    state_zips = db.zip_holder.lookup_zips_by_state(state)
    if not state_zips:
        raise ValueError(f"No ZIP codes found for state {state}")
    return state_zips[0]  # Return the first ZIP code

async def check_carrier_availability(db: MedicareSupplementRateDB, state: str, naic: str, effective_date: str) -> bool:
    """Check if a carrier is available in a state for a specific date."""
    try:
        # Get a sample ZIP code for the state
        sample_zip = get_sample_zip(db, state)
        
        # Prepare parameters for a sample quote
        params = {
            "zip5": sample_zip,
            "naic": naic,
            "effective_date": effective_date,
            "age": 65,
            "gender": "M",
            "tobacco": 0,
            "plan": "G"
        }
        
        # Special plan handling for certain states
        if state == 'MN':
            params['plan'] = 'MN_BASIC'
        elif state == 'WI':
            params['plan'] = 'WIR_A50%'
        elif state == 'MA':
            params['plan'] = 'MA_CORE'
        
        # Make the API call
        response = await db.cr.fetch_quote(**params)
        
        # Check if we got a valid response
        if not response:
            logging.info(f"Carrier {naic} not available in {state} for {effective_date}")
            return False
            
        # Check if this specific carrier is in the response
        for quote in response:
            if quote.get('company_base', {}).get('naic') == naic:
                logging.info(f"Carrier {naic} is available in {state} for {effective_date}")
                return True
        
        logging.info(f"Carrier {naic} not found in response for {state} on {effective_date}")
        return False
        
    except Exception as e:
        logging.error(f"Error checking carrier availability for {naic} in {state}: {str(e)}")
        return False

async def process_carrier_state(
    db: MedicareSupplementRateDB, 
    state: str, 
    naic: str, 
    effective_dates: List[str],
    dry_run: bool,
    skip_availability_check: bool = False
) -> bool:
    """Process a single carrier-state combination for all effective dates."""
    try:
        logging.info(f"Processing carrier {naic} in state {state}")
        
        # Check availability if needed
        if not skip_availability_check:
            first_date = effective_dates[0]
            is_available = await check_carrier_availability(db, state, naic, first_date)
            
            if not is_available:
                logging.info(f"Skipping carrier {naic} in {state} - not available")
                return False
        
        if dry_run:
            logging.info(f"DRY RUN: Would process carrier {naic} in {state} for dates: {effective_dates}")
            return True
        
        # Set up mapping for this carrier in this state
        mapping_success = await db.set_state_map_naic(naic, state)
        if not mapping_success:
            logging.warning(f"Failed to set up mapping for {naic} in {state}")
            return False
        
        logging.info(f"Successfully set up mapping for {naic} in {state}")
        
        # Process rates for each effective date
        for effective_date in effective_dates:
            logging.info(f"Processing rates for {naic} in {state} for {effective_date}")
            
            # Get rate tasks for this carrier-state-date combination
            tasks = await db.get_rate_tasks(state, naic, effective_date)
            
            if not tasks:
                logging.warning(f"No rate tasks generated for {naic} in {state} for {effective_date}")
                continue
            
            # Process the tasks in chunks to avoid overwhelming the API
            chunk_size = 10
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i:i + chunk_size]
                await asyncio.gather(*chunk, return_exceptions=True)
                await asyncio.sleep(1)  # Rate limiting
            
            logging.info(f"Completed processing rates for {naic} in {state} for {effective_date}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing carrier {naic} in state {state}: {str(e)}")
        return False

async def get_available_carriers_for_state(db: MedicareSupplementRateDB, state: str, effective_date: str) -> Set[str]:
    """Get all available carriers in a state with a single API call."""
    try:
        # Get a sample ZIP code for the state
        sample_zip = get_sample_zip(db, state)
        
        # Prepare parameters for a sample quote (without specifying NAIC)
        params = {
            "zip5": sample_zip,
            "effective_date": effective_date,
            "age": 65,
            "gender": "M",
            "tobacco": 0,
            "plan": "G"
        }
        
        # Special plan handling for certain states
        if state == 'MN':
            params['plan'] = 'MN_BASIC'
        elif state == 'WI':
            params['plan'] = 'WIR_A50%'
        elif state == 'MA':
            params['plan'] = 'MA_CORE'
        
        # Make the API call without specifying any NAIC
        response = await db.cr.fetch_quote(**params)
        
        # Extract all unique NAIC codes from the response
        available_naics = set()
        for quote in response:
            naic = quote.get('company_base', {}).get('naic')
            if naic:
                available_naics.add(naic)
        
        logging.info(f"Found {len(available_naics)} available carriers in {state}: {available_naics}")
        return available_naics
        
    except Exception as e:
        logging.error(f"Error getting available carriers for state {state}: {str(e)}")
        return set()

async def build_database(
    db_path: str,
    states: Optional[List[str]] = None,
    naics: Optional[List[str]] = None,
    months: int = 6,
    dry_run: bool = False
):
    """Build the Medicare Supplement database using a carrier-centric approach."""
    try:
        # Initialize database and API client
        db = MedicareSupplementRateDB(db_path=db_path)
        await db.cr.async_init()
        await db.cr.fetch_token()
        
        # Load carriers and states
        all_selected_carriers = load_carrier_selections()
        states_to_process = states if states else get_all_states()
        effective_dates = generate_effective_dates(months)
        
        # Log what we're going to do
        logging.info(f"Selected carriers from config: {len(all_selected_carriers)}")
        logging.info(f"Will process {len(states_to_process)} states")
        logging.info(f"Effective dates: {effective_dates}")
        
        if dry_run:
            logging.info("DRY RUN MODE - No changes will be made to the database")
        
        # Main processing loop - state first, then get available carriers
        for state in states_to_process:
            logging.info(f"Processing state: {state}")
            
            # Get all available carriers for this state with a single API call
            available_carriers = await get_available_carriers_for_state(db, state, effective_dates[0])
            
            if not available_carriers:
                logging.warning(f"No available carriers found for state {state}")
                continue
            
            # Filter available carriers based on our selections
            if naics:
                # If specific NAICs were provided, use those (if available in this state)
                carriers_to_process = [n for n in naics if n in available_carriers]
                logging.info(f"Using {len(carriers_to_process)}/{len(naics)} specified carriers available in {state}")
            else:
                # Otherwise, use carriers from our selection that are available in this state
                carriers_to_process = [n for n in all_selected_carriers if n in available_carriers]
                logging.info(f"Using {len(carriers_to_process)}/{len(all_selected_carriers)} selected carriers available in {state}")
            
            if not carriers_to_process:
                logging.warning(f"No carriers to process for state {state}")
                continue
                
            # Process each carrier in this state
            for naic in carriers_to_process:
                logging.info(f"Processing carrier {naic} in state {state}")
                
                # We already know the carrier is available, so skip availability check
                if dry_run:
                    logging.info(f"DRY RUN: Would process carrier {naic} in state {state}")
                    continue
                
                success = await process_carrier_state(db, state, naic, effective_dates, dry_run=False, skip_availability_check=True)
                
                if success:
                    logging.info(f"Successfully processed carrier {naic} in state {state}")
                else:
                    logging.warning(f"Failed to process carrier {naic} in state {state}")
        
        logging.info("Database build completed successfully")
        
    except Exception as e:
        logging.error(f"Error building database: {str(e)}")
        raise

async def main():
    """Parse command line arguments and run the build process."""
    parser = argparse.ArgumentParser(description="Build Medicare Supplement database carrier by carrier")
    parser.add_argument("-d", "--db-path", default="medicare.db", help="Path to SQLite database")
    parser.add_argument("--states", nargs="+", help="List of states to process (e.g., TX CA)")
    parser.add_argument("--naics", nargs="+", help="List of NAIC codes to process")
    parser.add_argument("-m", "--months", type=int, default=6, help="Number of months to process")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without database changes")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    
    args = parser.parse_args()
    logger = setup_logging(quiet=args.quiet)
    
    try:
        logging.info("Starting database build process")
        await build_database(
            db_path=args.db_path,
            states=args.states,
            naics=args.naics,
            months=args.months,
            dry_run=args.dry_run
        )
        logging.info("Database build process completed")
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())