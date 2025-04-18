#!/usr/bin/env python3
import asyncio
import logging
import json
import random
import os
from filter_utils import filter_quote
from typing import List, Dict, Set, Tuple, Any, Optional
from datetime import datetime, timedelta
import duckdb
from async_csg import AsyncCSGRequest as csg
from zips import zipHolder
from config import Config
import httpx
import resource

# Global connection pool limits
MAX_CONNECTIONS = 50
GLOBAL_SEMAPHORE = asyncio.Semaphore(MAX_CONNECTIONS)

class RateSpotChecker:
    """
    Spot checks Medicare rates for changes across months and identifies
    carriers that need to be fully reprocessed for specific months.
    """
    
    def __init__(self, db_path: str):
        """Initialize the spot checker."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.cr = csg(Config.API_KEY)
        self.zip_holder = zipHolder("static/uszips.csv")
        self.month_semaphore = asyncio.Semaphore(5)  # Limit concurrent months being processed
        self.latest_api_dates = set()  # Store API effective dates from the most recent check
        
    async def init(self):
        """Initialize async components."""
        await self.cr.async_init()
        await self.cr.fetch_token()
    
    def get_carrier_list(self, state: str) -> List[str]:
        """Get list of carriers that exist in the database for a state."""
        try:
            result = self.conn.execute("""
                SELECT DISTINCT naic 
                FROM rate_store r
                JOIN rate_regions reg ON r.region_id = reg.region_id
                WHERE reg.state = ?
                ORDER BY r.naic
            """, [state]).fetchall()
            return [r[0] for r in result]
        except Exception as e:
            logging.error(f"Error getting carrier list for {state}: {str(e)}")
            return []
    
    def get_sample_demographics(self) -> List[Dict]:
        """Generate a small representative set of demographic combinations for spot checking."""
        demographics = []
        
        # Male/Female, Smoker/Non-smoker, ages 65/70/75/80
        for gender in ["M", "F"]:
            for tobacco in [0, 1]:
                for age in [65, 70, 75, 80]:
                    demographics.append({
                        "gender": gender,
                        "tobacco": tobacco,
                        "age": age
                    })
        
        # Select random subset to keep API calls manageable
        random.shuffle(demographics)
        return demographics[:6]  # Use 6 demographic combinations for spot checking
    
    def get_representative_regions(self, state: str, naic: str, limit: int = 2) -> List[Dict]:
        """Get representative regions for a carrier in a state."""
        try:
            # Select regions from the database
            result = self.conn.execute("""
                SELECT region_id, mapping_type, region_data
                FROM rate_regions
                WHERE state = ? AND naic = ?
                ORDER BY region_id
                LIMIT ?
            """, [state, naic, limit]).fetchall()
            
            if not result:
                logging.warning(f"No regions found for {naic} in {state}")
                return []
            
            regions = []
            for region_id, mapping_type, region_data in result:
                # Parse region data (JSON array of locations)
                locations = json.loads(region_data)
                
                # Get a representative ZIP code for this region
                rep_zip = None
                if mapping_type == "zip5":
                    if locations:
                        rep_zip = locations[0]
                else:  # county
                    for county in locations:
                        zips = self.zip_holder.lookup_zip_by_county(state, county)
                        if zips:
                            rep_zip = zips[0]
                            break
                
                if rep_zip:
                    regions.append({
                        "region_id": region_id,
                        "rep_zip": rep_zip,
                        "mapping_type": mapping_type
                    })
            
            return regions
            
        except Exception as e:
            logging.error(f"Error getting representative regions for {naic} in {state}: {str(e)}")
            return []
    
    def get_latest_api_dates(self) -> List[str]:
        """Get the API effective dates from the most recent check."""
        return list(self.latest_api_dates)
        
    async def spot_check_carrier(self, state: str, naic: str, source_date: str, 
                                target_date: str) -> bool:
        # Clear previous API dates before starting a new check
        self.latest_api_dates.clear()
        """
        Spot check if rates changed between source_date and target_date.
        Returns True if rates changed and carrier needs updating.
        """
        try:
            # Get representative regions
            regions = self.get_representative_regions(state, naic)
            if not regions:
                logging.error(f"No regions found for {naic} in {state}")
                return False
            
            # Get sample demographics
            demographics = self.get_sample_demographics()
            
            # Get plans for this state
            if state == 'MA':
                plans = ['MA_CORE']
            elif state == 'MN':
                plans = ['MN_BASIC']
            elif state == 'WI':
                plans = ['WIR_A50%']
            else:
                plans = ['G', 'N']
            
            # Check a sample of quotes across demographics and plans for each region
            async with GLOBAL_SEMAPHORE:
                rates_changed = False
                
                for region in regions:
                    region_id = region["region_id"]
                    rep_zip = region["rep_zip"]
                    
                    for demographic in demographics:
                        for plan in plans:
                            # Skip if we already know rates changed
                            if rates_changed:
                                continue
                                
                            # Check if rates differ between source and target date
                            changed = await self._compare_rates(
                                state, naic, region_id, rep_zip, demographic, plan,
                                source_date, target_date
                            )
                            
                            if changed:
                                logging.info(f"Rate change detected for {naic} in {state} between {source_date} and {target_date}")
                                rates_changed = True
                                break  # No need to check more combinations
            
            return rates_changed
            
        except Exception as e:
            logging.error(f"Error spot checking {naic} in {state}: {str(e)}")
            return False  # Assume no changes if error occurs
    
    async def _compare_rates(self, state: str, naic: str, region_id: str, rep_zip: str, 
                           demographic: Dict, plan: str, source_date: str, 
                           target_date: str) -> bool:
        """Compare rates between two dates and return True if they changed."""
        try:
            # Get rates from database for source date
            db_rate, db_effective_date = self._get_db_rate(
                region_id, demographic["gender"], demographic["tobacco"],
                demographic["age"], naic, plan, source_date
            )
            
            if db_rate is None:
                logging.warning(f"No rate in database for {naic} in {state} for region {region_id} for {source_date}")
                return True  # Assume change needed if source rate missing
            
            # Fetch rate from API for target date
            params = {
                "zip5": rep_zip,
                "naic": naic,
                "gender": demographic["gender"],
                "tobacco": demographic["tobacco"],
                "age": demographic["age"],
                "plan": plan,
                "effective_date": target_date
            }
            
            response = await self.cr.fetch_quote(**params)
            
            # Extract rate and effective date from response
            api_rates = []
            for quote in response:
                if quote.get('company_base', {}).get('naic') == naic:
                    filtered_quote = filter_quote(quote)
                    if filtered_quote and 'rate' in filtered_quote:
                        api_effective_date = quote.get('effective_date')
                        api_rates.append({
                            'rate': filtered_quote['rate'],
                            'effective_date': api_effective_date
                        })
                        
                        # Store API effective date for later access
                        if api_effective_date:
                            self.latest_api_dates.add(api_effective_date)
            
            if not api_rates:
                logging.warning(f"No API rate found for {naic} in {state} at {rep_zip} for {target_date}")
                return True  # Assume change needed if API rate missing
            
            for api_rate_info in api_rates:
                api_rate = api_rate_info['rate']
                api_effective_date = api_rate_info['effective_date']
                if db_rate != api_rate or db_effective_date != api_effective_date:
                    logging.info(
                        f"Rate changed from {db_rate} to {api_rate} or effective date changed "
                        f"from {db_effective_date} to {api_effective_date} for {naic} at region {region_id}"
                    )
                    return True
            return False
            
        except Exception as e:
            logging.error(f"Error comparing rates: {str(e)}")
            return True  # Assume change needed if error occurs
    
    def _get_db_rate(self, region_id: str, gender: str, tobacco: int, 
                   age: int, naic: str, plan: str, effective_date: str) -> Tuple[Optional[float], Optional[str]]:
        """Get rate and effective date from database."""
        try:
            result = self.conn.execute("""
                SELECT rate, effective_date 
                FROM rate_store 
                WHERE region_id = ? 
                  AND gender = ? 
                  AND tobacco = ? 
                  AND age = ? 
                  AND naic = ? 
                  AND plan = ? 
                  AND effective_date <= ?
                ORDER BY effective_date DESC
                LIMIT 1
            """, [region_id, gender, tobacco, age, naic, plan, effective_date]).fetchone()
            
            return (result[0], result[1]) if result else (None, None)
            
        except Exception as e:
            logging.error(f"Database error getting rate: {str(e)}")
            return (None, None)
    
    async def process_carrier_month(self, state: str, naic: str, source_date: str, 
                                  target_date: str, process_function) -> bool:
        """
        Process a carrier for a specific month.
        If spot check shows changes, call the provided process_function.
        """
        try:
            # Check if target month already has data
            has_data = self._check_month_data(state, naic, target_date)
            
            if has_data:
                logging.info(f"{naic} in {state} already has data for {target_date}")
                return True
                
            # Spot check to see if rates changed
            rates_changed = await self.spot_check_carrier(state, naic, source_date, target_date)
            
            if rates_changed:
                # Process carrier with full rate refresh
                logging.info(f"Processing {naic} in {state} for {target_date} due to rate changes")
                success = await process_function(state, naic, target_date)
                return success
            else:
                # No changes detected - no need to update the database
                logging.info(f"No rate changes for {naic} in {state} between {source_date} and {target_date} - skipping")
                # Mark as processed in the database to avoid rechecking
                self.mark_processed(state, naic, target_date)
                return True
                
        except Exception as e:
            logging.error(f"Error processing {naic} in {state} for {target_date}: {str(e)}")
            return False
    
    def _check_month_data(self, state: str, naic: str, effective_date: str) -> bool:
        """Check if data already exists for a month."""
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) 
                FROM rate_store r
                JOIN rate_regions reg ON r.region_id = reg.region_id
                WHERE reg.state = ? 
                  AND r.naic = ? 
                  AND r.effective_date = ?
                LIMIT 1
            """, [state, naic, effective_date]).fetchone()
            
            return result[0] > 0 if result else False
            
        except Exception as e:
            logging.error(f"Error checking month data: {str(e)}")
            return False
    
    def mark_processed(self, state: str, naic: str, effective_date: str, api_effective_date: str = None):
        """Mark a carrier-state-date combination as processed without adding new data."""
        try:
            # Check if api_effective_date column exists
            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO processed_data 
                    (state, naic, effective_date, api_effective_date, processed_at, success)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """, [state, naic, effective_date, api_effective_date or effective_date, True])
            except Exception as col_error:
                if "api_effective_date" in str(col_error):
                    # Insert without the api_effective_date column
                    self.conn.execute("""
                        INSERT OR REPLACE INTO processed_data 
                        (state, naic, effective_date, processed_at, success)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """, [state, naic, effective_date, True])
                else:
                    raise col_error
        except Exception as e:
            logging.error(f"Error marking processed: {str(e)}")
    
    async def process_state_months(self, state: str, months: List[str], 
                                 process_carrier_func) -> Dict[str, Dict[str, bool]]:
        """
        Process all carriers for a state across months.
        For each month after the first, spot check and copy forward when possible.
        """
        if not months:
            return {}
            
        # Get carriers for this state
        carriers = self.get_carrier_list(state)
        if not carriers:
            logging.warning(f"No carriers found for {state}")
            return {}
            
        results = {month: {} for month in months}
        source_date = months[0]  # First month is already fully processed
        
        # Process each subsequent month
        for i in range(1, len(months)):
            target_date = months[i]
            month_tasks = []
            
            # Create tasks for all carriers
            for naic in carriers:
                task = self.process_carrier_month(
                    state, naic, source_date, target_date, process_carrier_func
                )
                month_tasks.append((naic, task))
            
            # Wait for all carrier tasks to complete for this month
            for naic, task in month_tasks:
                try:
                    success = await task
                    results[target_date][naic] = success
                except Exception as e:
                    logging.error(f"Error in task for {naic} in {state} for {target_date}: {str(e)}")
                    results[target_date][naic] = False
            
            # Update source date for next iteration
            source_date = target_date
        
        return results
    
    async def process_all_states_months(self, states: List[str], months: List[str], 
                                      process_carrier_func) -> Dict[str, Dict[str, Dict[str, bool]]]:
        """Process all states across all months using spot checking and copy forward."""
        results = {}
        
        for state in states:
            async with self.month_semaphore:
                try:
                    logging.info(f"Processing {state} across {len(months)} months")
                    state_results = await self.process_state_months(state, months, process_carrier_func)
                    results[state] = state_results
                except Exception as e:
                    logging.error(f"Error processing {state} across months: {str(e)}")
                    results[state] = {month: {} for month in months[1:]}
        
        return results
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


# Command-line interface for standalone usage
async def main():
    import argparse
    from build_duckdb import get_all_states, generate_effective_dates, load_carrier_selections
    
    parser = argparse.ArgumentParser(description="Spot check Medicare rates across months")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="DuckDB database file path")
    parser.add_argument("-m", "--months", type=int, default=6, help="Number of months to process")
    parser.add_argument("--states", nargs="+", help="List of states to process (e.g., TX CA)")
    parser.add_argument("--naics", nargs="+", help="List of NAIC codes to process")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    
    args = parser.parse_args()
    
    # Set up logging
    log_filename = f'spot_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not args.quiet else logging.NullHandler()
        ]
    )
    
    try:
        # Initialize spot checker
        checker = RateSpotChecker(args.db)
        await checker.init()
        
        # Get carriers to process
        selected_naics = args.naics if args.naics else load_carrier_selections()
        logging.info(f"Selected carriers: {len(selected_naics)}")
        
        # Get states to process
        states_to_process = args.states if args.states else get_all_states()
        logging.info(f"States to process: {len(states_to_process)}")
        
        # Generate effective dates
        effective_dates = generate_effective_dates(args.months)
        logging.info(f"Effective dates: {effective_dates}")
        
        # For standalone testing, we need a dummy process function
        async def dummy_process(state, naic, effective_date):
            logging.info(f"Would process {naic} in {state} for {effective_date}")
            return True
        
        # Process all states across months
        results = await checker.process_all_states_months(
            states_to_process, effective_dates, dummy_process
        )
        
        # Summarize results
        for state, state_results in results.items():
            logging.info(f"State {state} results:")
            for month, month_results in state_results.items():
                changes = sum(1 for success in month_results.values() if success)
                logging.info(f"  {month}: {changes}/{len(month_results)} carriers processed")
        
        # Close connections
        checker.close()
        logging.info("Spot check completed")
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 