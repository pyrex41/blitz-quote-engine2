#!/usr/bin/env python3
import argparse
import asyncio
import logging
import json
import sys
from typing import List, Dict, Set, Optional, Tuple, Any
from datetime import datetime, timedelta
from build_db_new import MedicareSupplementRateDB
from zips import zipHolder
from config import Config

def setup_logging(log_file: str = None, quiet: bool = False) -> None:
    """Set up logging to file and console."""
    if log_file is None:
        log_file = f'build_optimized_db_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    handlers = [logging.FileHandler(log_file)]
    if not quiet:
        handlers.append(logging.StreamHandler())
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    
    logger = logging.getLogger(__name__)
    return logger

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

class RegionMapBuilder:
    """Class to build optimized region maps for Medicare Supplement carriers."""
    
    def __init__(self, db: MedicareSupplementRateDB, batch_size: int = 5):
        self.db = db
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
    
    def get_all_zips(self, state: str) -> Set[str]:
        """Get all ZIP codes in a state."""
        return set(self.db.zip_holder.lookup_zips_by_state(state))
    
    def get_zips_by_counties(self, state: str, counties: List[str]) -> Set[str]:
        """Get all ZIP codes for a list of counties in a state."""
        zips = set()
        for county in counties:
            county_zips = self.db.zip_holder.lookup_zip_by_county(state, county)
            zips.update(county_zips)
        return zips
    
    async def fetch_batch_quotes(self, zip_code: str, naics: List[str], effective_date: str) -> List[Dict]:
        """Fetch quotes for a batch of carriers at one location."""
        params = {
            "zip5": zip_code,
            "effective_date": effective_date,
            "age": 65,
            "gender": "M",
            "tobacco": 0,
            "plan": "G",
            "select": 0
        }
        
        # Special plan handling for certain states
        state = self.db.zip_holder.lookup_state2(zip_code)
        if state == 'MN':
            params['plan'] = 'MN_BASIC'
        elif state == 'WI':
            params['plan'] = 'WIR_A50%'
        elif state == 'MA':
            params['plan'] = 'MA_CORE'
        
        # Process carriers in batches to avoid API limits
        results = []
        for i in range(0, len(naics), self.batch_size):
            batch = naics[i:i + self.batch_size]
            # For states like NY or MA where naic isn't specified directly
            state = self.db.zip_holder.lookup_state2(zip_code)
            if state not in ['NY', 'MA']:
                params_with_naics = {**params, "naic": batch}
                batch_results = await self.db.cr.fetch_quote(**params_with_naics)
            else:
                # For NY/MA, fetch quotes without specifying NAIC and filter afterward
                batch_results = await self.db.cr.fetch_quote(**params)
                batch_results = [q for q in batch_results if q.get('company_base',{}).get('naic') in batch]
            
            results.extend(batch_results)
            await asyncio.sleep(0.5)  # Rate limiting
        
        return results
    
    def extract_region_zips(self, quote: Dict) -> Tuple[str, Set[str], bool]:
        """Extract region information (set of ZIP codes) from a quote response."""
        naic = quote.get('company_base', {}).get('naic')
        if not naic:
            return None, set(), False
        
        location_base = quote.get('location_base', {})
        
        # Check if ZIP-based rating
        if 'zip5' in location_base:
            return naic, set(location_base['zip5']), True
        
        # Check if county-based rating
        elif 'county' in location_base:
            counties = location_base['county']
            state = self.db.zip_holder.lookup_state2(quote.get('zip5', ''))
            if not state:
                self.logger.error(f"Could not determine state for ZIP in quote: {quote}")
                return naic, set(), False
            
            county_zips = self.get_zips_by_counties(state, counties)
            return naic, county_zips, False
        
        self.logger.error(f"Unknown location base in quote: {quote}")
        return naic, set(), False
    
    async def build_optimized_map(self, state: str, naics: List[str], effective_date: str, dry_run: bool = False) -> Dict[str, List[Set[str]]]:
        """Build optimized region mappings using a greedy approach."""
        self.logger.info(f"Building optimized region map for state {state} with {len(naics)} carriers")
        
        # Get all ZIP codes in the state
        all_zips = self.get_all_zips(state)
        if not all_zips:
            self.logger.error(f"No ZIP codes found for state {state}")
            return {}
            
        if dry_run:
            self.logger.info(f"DRY RUN: Would build optimized map for state {state} with {len(naics)} carriers")
            return {}
        
        # Initialize tracking structures
        covered_zips: Dict[str, Set[str]] = {naic: set() for naic in naics}  # ZIPs covered per carrier
        known_regions: Dict[str, List[Set[str]]] = {naic: [] for naic in naics}  # Regions per carrier
        region_types: Dict[str, bool] = {naic: None for naic in naics}  # True for ZIP, False for county
        selected_locations: Set[str] = set()  # Selected ZIP codes
        
        max_iterations = min(100, len(all_zips))  # Safety limit to prevent infinite loops
        iteration = 0
        
        # Greedy selection of ZIP codes
        while iteration < max_iterations:
            iteration += 1
            
            # Identify carriers with incomplete coverage
            incomplete_carriers = [naic for naic in naics if covered_zips[naic] != all_zips]
            if not incomplete_carriers:
                self.logger.info(f"All carriers fully covered after {iteration} iterations")
                break
                
            # Calculate coverage percentage for progress tracking
            if iteration % 5 == 0:
                coverage_stats = {naic: f"{len(covered_zips[naic])*100/len(all_zips):.1f}%" for naic in naics}
                self.logger.info(f"Coverage after {iteration} iterations: {coverage_stats}")
            
            # Candidates: ZIPs not yet covered by at least one incomplete carrier
            candidates = [z for z in all_zips if any(z not in covered_zips[naic] for naic in incomplete_carriers)]
            if not candidates:
                self.logger.warning(f"Cannot find candidates to cover remaining regions in {state}")
                break
            
            # Greedy choice: ZIP that is uncovered for the most incomplete carriers
            # Weighted by number of uncovered zips per carrier to prioritize carriers with less coverage
            zip_scores = {}
            for z in candidates:
                score = 0
                for naic in incomplete_carriers:
                    if z not in covered_zips[naic]:
                        # Weight by inverse of current coverage (prioritize carriers with less coverage)
                        carrier_coverage = len(covered_zips[naic]) / len(all_zips)
                        score += 1 * (1.0 - carrier_coverage)
                zip_scores[z] = score
                
            next_zip = max(zip_scores, key=zip_scores.get)
            selected_locations.add(next_zip)
            self.logger.info(f"Selected ZIP {next_zip} with score {zip_scores[next_zip]:.2f}")
            
            try:
                # Query API for this ZIP with all incomplete carriers
                quotes = await self.fetch_batch_quotes(next_zip, incomplete_carriers, effective_date)
                
                # Process response to update regions
                for quote in quotes:
                    naic, region_zips, is_zip_based = self.extract_region_zips(quote)
                    if not naic or not region_zips:
                        continue
                        
                    # Store region type (ZIP or county based)
                    if region_types[naic] is None:
                        region_types[naic] = is_zip_based
                    
                    # Check if this is a new region
                    is_new_region = True
                    for existing_region in known_regions[naic]:
                        # If there's significant overlap, consider it the same region
                        overlap = len(existing_region.intersection(region_zips)) / len(region_zips)
                        if overlap > 0.8:  # 80% overlap threshold
                            is_new_region = False
                            # Update existing region with any new ZIPs
                            existing_region.update(region_zips)
                            break
                    
                    # Add new region if not matching any existing one
                    if is_new_region:
                        known_regions[naic].append(region_zips)
                        self.logger.info(f"Discovered new region for {naic} with {len(region_zips)} ZIPs")
                    
                    # Update covered ZIPs
                    covered_zips[naic].update(region_zips)
                
            except Exception as e:
                self.logger.error(f"API query failed for {next_zip}: {str(e)}")
                continue  # Try with a different ZIP
        
        # Final coverage check
        incomplete_coverage = False
        for naic in naics:
            coverage_pct = len(covered_zips[naic]) * 100 / len(all_zips)
            self.logger.info(f"Final coverage for NAIC {naic}: {coverage_pct:.1f}% with {len(known_regions[naic])} regions")
            if coverage_pct < 95:
                incomplete_coverage = True
                self.logger.warning(f"NAIC {naic} has low coverage ({coverage_pct:.1f}%) in {state}")
        
        if incomplete_coverage:
            self.logger.warning(f"Some carriers have incomplete coverage in {state}")
        else:
            self.logger.info(f"Successfully mapped all carriers in {state} using {len(selected_locations)} locations")
        
        return known_regions, region_types
    
    async def store_mappings(self, state: str, known_regions: Dict[str, List[Set[str]]], region_types: Dict[str, bool]):
        """Store the discovered region mappings in the database."""
        self.logger.info(f"Storing region mappings for state {state}")
        
        for naic, regions in known_regions.items():
            is_zip_based = region_types[naic]
            
            # Remove any existing mappings for this carrier/state
            self.db.remove_naic(naic, state)
            
            # Insert the group type
            self.db.conn.cursor().execute(
                "INSERT OR REPLACE INTO group_type (naic, state, group_zip) VALUES (?, ?, ?)",
                (naic, state, 1 if is_zip_based else 0)
            )
            
            # Insert the group mappings
            mapping_data = []
            for group_id, region in enumerate(regions, 1):
                for location in region:
                    mapping_data.append((naic, state, location, group_id))
            
            if mapping_data:
                self.db.conn.cursor().executemany(
                    "INSERT OR REPLACE INTO group_mapping (naic, state, location, naic_group) VALUES (?, ?, ?, ?)",
                    mapping_data
                )
            
            self.db.conn.commit()
            self.logger.info(f"Stored {len(mapping_data)} mapping entries for NAIC {naic} in {state}")
    
    async def fetch_rates(self, state: str, known_regions: Dict[str, List[Set[str]]], effective_dates: List[str]):
        """Fetch rates for all discovered regions."""
        self.logger.info(f"Fetching rates for state {state} for {len(effective_dates)} effective dates")
        
        rate_tasks = []
        
        for naic, regions in known_regions.items():
            for effective_date in effective_dates:
                # Get tasks for this carrier/state/date combination
                tasks = await self.db.get_rate_tasks(state, naic, effective_date)
                rate_tasks.extend(tasks)
                
                self.logger.info(f"Added {len(tasks)} rate tasks for NAIC {naic} on {effective_date}")
        
        # Process rate tasks in chunks to avoid overwhelming the API
        chunk_size = 10
        for i in range(0, len(rate_tasks), chunk_size):
            chunk = rate_tasks[i:i + chunk_size]
            try:
                results = await asyncio.gather(*chunk, return_exceptions=True)
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        self.logger.error(f"Error in rate task {i+j}: {str(result)}")
                await asyncio.sleep(1)  # Rate limiting
            except Exception as e:
                self.logger.error(f"Error processing rate chunk: {str(e)}")
        
        self.logger.info(f"Completed fetching rates for state {state}")

async def build_optimized_database(
    db_path: str,
    states: Optional[List[str]] = None,
    naics: Optional[List[str]] = None,
    months: int = 6,
    dry_run: bool = False,
    batch_size: int = 5
):
    """Build the Medicare Supplement database using an optimized combinatorial approach."""
    try:
        # Initialize database and API client
        db = MedicareSupplementRateDB(db_path=db_path)
        await db.cr.async_init()
        await db.cr.fetch_token()
        
        # Initialize the region map builder
        builder = RegionMapBuilder(db, batch_size=batch_size)
        
        # Load carriers and states
        all_carriers = load_carrier_selections()
        carriers_to_process = naics if naics else all_carriers
        
        states_to_process = states if states else get_all_states()
        effective_dates = generate_effective_dates(months)
        
        # Log what we're going to do
        logging.info(f"Will process {len(carriers_to_process)} carriers in {len(states_to_process)} states")
        logging.info(f"Effective dates: {effective_dates}")
        
        if dry_run:
            logging.info("DRY RUN MODE - No changes will be made to the database")
        
        # Process each state
        for state in states_to_process:
            try:
                logging.info(f"Building optimized database for state {state}")
                
                # Build the region map
                regions_result = await builder.build_optimized_map(
                    state, carriers_to_process, effective_dates[0], dry_run=dry_run
                )
                
                if dry_run:
                    logging.info(f"DRY RUN: Would have built optimized maps for {state}")
                    continue
                    
                if not regions_result:
                    logging.warning(f"Failed to build region map for state {state}")
                    continue
                    
                known_regions, region_types = regions_result
                
                # Store the mappings
                await builder.store_mappings(state, known_regions, region_types)
                
                # Fetch rates for all effective dates
                await builder.fetch_rates(state, known_regions, effective_dates)
                
                logging.info(f"Completed processing state {state}")
                
            except Exception as e:
                logging.error(f"Error processing state {state}: {str(e)}")
                continue  # Continue with next state
        
        logging.info("Database build completed successfully")
        
    except Exception as e:
        logging.error(f"Error building database: {str(e)}")
        raise

async def main():
    """Parse command line arguments and run the build process."""
    parser = argparse.ArgumentParser(description="Build Medicare Supplement database with optimized combinatorial approach")
    parser.add_argument("-d", "--db-path", default="medicare.db", help="Path to SQLite database")
    parser.add_argument("--states", nargs="+", help="List of states to process (e.g., TX CA)")
    parser.add_argument("--naics", nargs="+", help="List of NAIC codes to process")
    parser.add_argument("-m", "--months", type=int, default=6, help="Number of months to process")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without database changes")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("-b", "--batch-size", type=int, default=5, help="Number of carriers to batch in API calls")
    
    args = parser.parse_args()
    logger = setup_logging(quiet=args.quiet)
    
    try:
        logging.info("Starting optimized database build process")
        await build_optimized_database(
            db_path=args.db_path,
            states=args.states,
            naics=args.naics,
            months=args.months,
            dry_run=args.dry_run,
            batch_size=args.batch_size
        )
        logging.info("Database build process completed")
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())