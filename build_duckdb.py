#!/usr/bin/env python3
import argparse
import asyncio
import logging
import json
import sys
import itertools
import random
import os
import uuid
import hashlib
from typing import List, Dict, Set, Tuple, Any, Optional
from datetime import datetime, timedelta
import duckdb
from async_csg import AsyncCSGRequest as csg
from zips import zipHolder
from config import Config
from copy import copy
from functools import reduce
from filter_utils import filter_quote
import httpx
import resource

# Increase system file limits if possible
try:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(4096, hard), hard))
    logging.info(f"Set file descriptor limit to {min(4096, hard)}")
except Exception as e:
    logging.warning(f"Unable to increase file descriptor limit: {e}")

# Global connection pool limits
MAX_CONNECTIONS = 100
GLOBAL_SEMAPHORE = None

def setup_logging(quiet: bool) -> None:
    """Set up logging to file and console."""
    log_filename = f'build_duckdb_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

def load_carrier_selections() -> List[str]:
    """Load carrier selections from config file."""
    try:
        with open('carrier_selections.json', 'r') as f:
            data = json.load(f)
            return [naic for naic, selected in data.items() if selected]
    except FileNotFoundError:
        logging.error("carrier_selections.json not found. Please run select_carriers.py first.")
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


class DuckDBMedicareBuilder:
    def __init__(self, db_path: str, max_rate_limit: int = 50):
        """Initialize the DuckDB Medicare rate database builder."""
        self.db_path = db_path
        self.max_rate_limit = max_rate_limit
        
        # Connect to DuckDB
        self.conn = duckdb.connect(db_path)
        
        # Initialize API client
        self.cr = csg(Config.API_KEY)
        
        # Initialize zipHolder
        self.zip_holder = zipHolder("static/uszips.csv")
        
        # Create necessary tables and indexes
        self._create_tables()
        
        # Create shared HTTP client for connection reuse
        self.limits = httpx.Limits(max_keepalive_connections=50, max_connections=MAX_CONNECTIONS)
        
        # Initialize semaphores for concurrency control
        global GLOBAL_SEMAPHORE
        GLOBAL_SEMAPHORE = asyncio.Semaphore(MAX_CONNECTIONS)
        self.region_semaphore = asyncio.Semaphore(20)  # Limit concurrent regions
        self.demographic_semaphore = asyncio.Semaphore(30)  # Limit concurrent demographic combinations
    
    def _create_tables(self):
        """Create optimized tables and indexes for rate lookups using region-based approach."""
        # Create regions table to track unique carrier-specific regions
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_regions (
                region_id TEXT,
                naic TEXT,
                state TEXT,
                mapping_type TEXT,
                region_data TEXT,  -- JSON array of ZIP codes or counties in the region
                PRIMARY KEY (region_id)
            )
        """)
        
        # Create region mapping table to map ZIP codes to regions
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS region_mapping (
                zip_code TEXT,
                county TEXT,
                region_id TEXT,
                naic TEXT,
                mapping_type TEXT,
                PRIMARY KEY (zip_code, county, naic)
            )
        """)
        
        # Add a table to store region metadata with hashes for deduplication
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS region_metadata (
                region_id TEXT,
                naic TEXT,
                state TEXT,
                mapping_type TEXT,
                region_hash TEXT,  -- Hash of the region's locations for deduplication
                PRIMARY KEY (region_id)
            )
        """)
        
        # Create rate_store table at the region level
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_store (
                region_id TEXT,
                gender TEXT,
                tobacco INTEGER,
                age INTEGER,
                naic TEXT,
                plan TEXT,
                rate FLOAT,
                discount_rate FLOAT,
                effective_date TEXT,
                state TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (region_id, gender, tobacco, age, naic, plan, effective_date, state)
            )
        """)
        
        # Create indexes for optimized lookups
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_region_lookup_zip ON region_mapping (zip_code, naic)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_region_lookup_county ON region_mapping (county, naic)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rate_lookup ON rate_store (region_id, gender, tobacco, age)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_naic ON rate_store (naic)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_effective_date ON rate_store (effective_date)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_region_hash ON region_metadata (naic, state, region_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_mapping_type ON region_mapping (mapping_type)")
        
        # Create carrier_info table for metadata
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS carrier_info (
                naic TEXT PRIMARY KEY,
                company_name TEXT,
                selected INTEGER DEFAULT 1
            )
        """)
        
        # Create metadata table to track processed combinations
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_data (
                state TEXT,
                naic TEXT,
                effective_date TEXT,
                processed_at TIMESTAMP,
                success BOOLEAN,
                PRIMARY KEY (state, naic, effective_date)
            )
        """)
        
        # Check if api_effective_date column exists in processed_data and add it if not
        try:
            result = self.conn.execute("SELECT api_effective_date FROM processed_data LIMIT 1")
        except Exception:
            logging.info("Adding api_effective_date column to processed_data table")
            self.conn.execute("ALTER TABLE processed_data ADD COLUMN api_effective_date TEXT")
            
        # Check if the region_mapping table has the mapping_type column
        try:
            self.conn.execute("SELECT mapping_type FROM region_mapping LIMIT 1")
        except Exception:
            logging.info("Adding mapping_type column to region_mapping table")
            self.conn.execute("ALTER TABLE region_mapping ADD COLUMN mapping_type TEXT")
            
        # Check if the region_mapping table has the county column
        try:
            self.conn.execute("SELECT county FROM region_mapping LIMIT 1")
        except Exception:
            logging.info("Adding county column to region_mapping table")
            self.conn.execute("ALTER TABLE region_mapping ADD COLUMN county TEXT")
            # Update existing rows to avoid null values
            self.conn.execute("UPDATE region_mapping SET county = '' WHERE county IS NULL")
            
        # Clean up orphaned processed entries
        self.cleanup_orphaned_entries()
    
    def cleanup_orphaned_entries(self):
        """Remove entries in the processed_data table that don't have actual rate data."""
        try:
            # Find processed entries that have no corresponding rate data
            orphaned = self.conn.execute("""
                SELECT p.state, p.naic, p.effective_date
                FROM processed_data p
                WHERE p.success = true
                AND NOT EXISTS (
                    SELECT 1 FROM rate_store r 
                    WHERE r.naic = p.naic AND r.state = p.state AND r.effective_date = p.effective_date
                )
            """).fetchall()
            
            if orphaned:
                logging.info(f"Found {len(orphaned)} orphaned processed entries with no rate data")
                
                # Delete the orphaned entries
                self.conn.execute("BEGIN TRANSACTION")
                for entry in orphaned:
                    self.conn.execute("""
                        DELETE FROM processed_data
                        WHERE state = ? AND naic = ? AND effective_date = ?
                    """, entry)
                self.conn.execute("COMMIT")
                    
                logging.info(f"Cleaned up {len(orphaned)} orphaned processed entries")
                
            # Also check for orphaned region records that have no rate data
            orphaned_regions = self.conn.execute("""
                SELECT rr.region_id
                FROM rate_regions rr
                WHERE NOT EXISTS (
                    SELECT 1 FROM rate_store rs
                    WHERE rs.region_id = rr.region_id
                )
            """).fetchall()
            
            if orphaned_regions:
                logging.info(f"Found {len(orphaned_regions)} orphaned region entries with no rate data")
                
                # We won't automatically delete them, just log for manual review
                with open(f'orphaned_regions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', 'w') as f:
                    for region in orphaned_regions:
                        f.write(f"{region[0]}\n")
                
        except Exception as e:
            logging.error(f"Error cleaning up orphaned entries: {str(e)}")
            
    def check_database_integrity(self):
        """Verify database integrity by checking relationships between tables."""
        try:
            logging.info("Checking database integrity...")
            
            # Check 1: All region_ids in rate_store exist in rate_regions
            missing_regions = self.conn.execute("""
                SELECT DISTINCT rs.region_id
                FROM rate_store rs
                LEFT JOIN rate_regions rr ON rs.region_id = rr.region_id
                WHERE rr.region_id IS NULL
            """).fetchall()
            
            if missing_regions:
                logging.error(f"Found {len(missing_regions)} region IDs in rate_store that don't exist in rate_regions")
            
            # Check 2: All region_ids in region_mapping exist in rate_regions
            missing_mapped_regions = self.conn.execute("""
                SELECT DISTINCT rm.region_id
                FROM region_mapping rm
                LEFT JOIN rate_regions rr ON rm.region_id = rr.region_id
                WHERE rr.region_id IS NULL
            """).fetchall()
            
            if missing_mapped_regions:
                logging.error(f"Found {len(missing_mapped_regions)} region IDs in region_mapping that don't exist in rate_regions")
            
            # Check 3: All NAICs in rate_store exist in carrier_info
            missing_carriers = self.conn.execute("""
                SELECT DISTINCT rs.naic
                FROM rate_store rs
                LEFT JOIN carrier_info ci ON rs.naic = ci.naic
                WHERE ci.naic IS NULL
            """).fetchall()
            
            if missing_carriers:
                logging.error(f"Found {len(missing_carriers)} NAICs in rate_store that don't exist in carrier_info")
                
            # Check 4: Count records in each table
            rate_count = self.conn.execute("SELECT COUNT(*) FROM rate_store").fetchone()[0]
            region_count = self.conn.execute("SELECT COUNT(*) FROM rate_regions").fetchone()[0]
            mapping_count = self.conn.execute("SELECT COUNT(*) FROM region_mapping").fetchone()[0]
            carrier_count = self.conn.execute("SELECT COUNT(*) FROM carrier_info").fetchone()[0]
            processed_count = self.conn.execute("SELECT COUNT(*) FROM processed_data").fetchone()[0]
            
            logging.info(f"Database contains: {rate_count} rates, {region_count} regions, " +
                        f"{mapping_count} ZIP mappings, {carrier_count} carriers, {processed_count} processed combinations")
            
            if rate_count == 0 or region_count == 0 or mapping_count == 0:
                logging.error("One or more tables are empty - database may be corrupted")
                
            # All checks passed
            if not (missing_regions or missing_mapped_regions or missing_carriers):
                logging.info("Database integrity check passed")
                return True
            else:
                logging.error("Database integrity check failed - see previous errors")
                return False
                
        except Exception as e:
            logging.error(f"Error checking database integrity: {str(e)}")
            return False
    
    async def get_available_carriers(self, state: str, effective_date: str) -> Set[str]:
        """Get all available carriers in a state with a single API call."""
        try:
            # Get a sample ZIP code for the state
            state_zips = self.zip_holder.lookup_zips_by_state(state)
            if not state_zips:
                logging.error(f"No ZIP codes found for state {state}")
                return set()
                
            sample_zip = state_zips[0]
            
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
            response = await self.cr.fetch_quote(**params)
            
            # Extract all unique NAIC codes from the response
            available_naics = set()
            for quote in response:
                naic = quote.get('company_base', {}).get('naic')
                company_name = quote.get('company_base', {}).get('name')
                if naic:
                    available_naics.add(naic)
                    # Store carrier info
                    self.conn.execute(
                        "INSERT OR REPLACE INTO carrier_info (naic, company_name, selected) VALUES (?, ?, 1)",
                        (naic, company_name)
                    )
            
            logging.info(f"Found {len(available_naics)} available carriers in {state}: {available_naics}")
            return available_naics
            
        except Exception as e:
            logging.error(f"Error getting available carriers for state {state}: {str(e)}")
            return set()
    
    async def get_rate_regions(self, state: str, naic: str) -> Tuple[List[Set[str]], str]:
        """
        Get the geographic regions for a carrier in a state.
        Returns a list of sets (each set contains locations in a region) and the mapping type (zip5 or county).
        """
        try:
            lookup_list, mapping_type = await self.cr.calc_naic_map_combined2(state, naic)
            if len(lookup_list) == 0:
                logging.warning(f"No regions found for {naic} in {state}")
                return [], ""
            
            logging.info(f"Found {len(lookup_list)} regions for {naic} in {state} (type: {mapping_type})")
            return lookup_list, mapping_type
            
        except Exception as e:
            logging.error(f"Error getting regions for {naic} in {state}: {str(e)}")
            return [], ""
    
    def get_representative_zip(self, state: str, region: Set[str], mapping_type: str) -> Tuple[str, Optional[str]]:
        """
        Get a representative ZIP code for a region.
        Returns tuple of (zip_code, county) where county is None for zip-based regions
        """
        if mapping_type == "zip5":
            # If region is ZIP-based, just pick the first ZIP
            return next(iter(region)), None
        else:
            # If region is county-based, get a ZIP code from each county
            # but also return the county name to use in API requests
            county = next(iter(region))
            zips = self.zip_holder.lookup_zip_by_county(state, county)
            if zips:
                return zips[0], county
            
            # Fallback
            logging.warning(f"No ZIP found for county {county} in {state}")
            return "", county
    
    def get_all_zips_for_region(self, state: str, region: Set[str], mapping_type: str) -> List[str]:
        """Get all ZIP codes for a region."""
        if mapping_type == "zip5":
            # If region is ZIP-based, return all ZIPs in the region
            return list(region)
        else:
            # If region is county-based, get all ZIPs for each county
            all_zips = []
            for county in region:
                county_zips = self.zip_holder.lookup_zip_by_county(state, county)
                all_zips.extend(county_zips)
            return all_zips
    
    def process_quote(self, quote: Dict, region_id: str, state: str) -> List[Dict]:
        """Process a quote response into rate records."""
        filtered = filter_quote(quote)
        if not filtered:
            return []
        
        gender = filtered['gender']
        tobacco = filtered['tobacco']
        age = filtered['age']
        plan = filtered['plan']
        base_rate = filtered['rate']
        naic = filtered.get('company_base', {}).get('naic', quote.get('company_base', {}).get('naic'))
        
        # Use the effective date from the API response
        # Use the effective_date from the API response, which may differ from the requested date
        effective_date = quote.get('effective_date')
        if not effective_date:
            logging.warning(f"API did not return effective_date in response. Using requested date as fallback.")
            effective_date = filtered.get('effective_date')
            
        # Debug log the API-returned effective date
        logging.info(f"API returned effective_date: {effective_date} for NAIC {naic}, plan {plan}")
        
        # Calculate rates for different ages using age_increases
        rate_mults = [1.0] + [x + 1 for x in filtered.get('age_increases', [])]
        try:
            discount_mult = (1 - filtered.get('discounts', [{}])[0].get('value', 0))
        except:
            discount_mult = 1
            
        ages = [age + i for i in range(len(rate_mults))]
        
        results = []
        for i, current_age in enumerate(ages):
            rate_value = round(base_rate * reduce(lambda x, y: x * y, rate_mults[:i + 1]), 2)
            discount_value = round(discount_mult * rate_value, 2)
            
            results.append({
                'region_id': region_id,
                'gender': gender,
                'tobacco': int(tobacco),
                'age': current_age,
                'naic': naic,
                'plan': plan,
                'rate': rate_value,
                'discount_rate': discount_value,
                'effective_date': effective_date,
                'state': state
            })
            
        return results
    
    async def fetch_rates(self, state: str, naic: str, region_id: str, representative_zip: str, 
                          representative_county: Optional[str], effective_date: str) -> List[Dict]:
        """Fetch rates for all demographics for a specific region."""
        try:
            # Generate all demographic combinations
            tobacco_options = [0, 1]
            gender_options = ["M", "F"]
            
            # Select plan options based on state
            if state == 'MA':
                plan_options = ['MA_CORE', 'MA_SUPP1']
            elif state == 'MN':
                plan_options = ['MN_BASIC', 'MN_EXTB']
            elif state == 'WI':
                plan_options = ['WIR_A50%']
            else:
                plan_options = ['N', 'G', 'F']
            
            # Generate combinations for key ages (65, 70, 75, etc.), then use rate multipliers for ages in between
            base_ages = [65, 70, 75, 80, 85, 90, 95]
            
            results = []
            
            # Process one plan at a time to reduce connection load
            for plan in plan_options:
                plan_tasks = []
                
                # Create tasks for parallel API calls with controlled concurrency
                for gender, tobacco, base_age in itertools.product(gender_options, tobacco_options, base_ages):
                    async def fetch_and_process(gender, tobacco, plan, age):
                        async with self.demographic_semaphore:
                            async with GLOBAL_SEMAPHORE:
                                params = {
                                    "zip5": representative_zip,
                                    "naic": naic,
                                    "gender": gender,
                                    "tobacco": tobacco,
                                    "age": age,
                                    "plan": plan,
                                    "effective_date": effective_date
                                }
                                
                                # If this is a county-based region, include the county in the parameters
                                if representative_county:
                                    params["county"] = representative_county
                                
                                try:
                                    # Fetch quote from API
                                    response = await self.cr.fetch_quote(**params)
                                    
                                    # Process response
                                    combo_results = []
                                    for quote in response:
                                        if quote.get('company_base', {}).get('naic') == naic:
                                            processed_rates = self.process_quote(quote, region_id, state)
                                            combo_results.extend(processed_rates)
                                    
                                    return combo_results
                                except Exception as e:
                                    logging.error(f"Error fetching rates for {naic}/{gender}/{tobacco}/{plan}/{age}: {str(e)}")
                                    return []
                
                    task = asyncio.create_task(fetch_and_process(gender, tobacco, plan, base_age))
                    plan_tasks.append(task)
                
                # Wait for all tasks for this plan to complete
                for completed_task in asyncio.as_completed(plan_tasks):
                    try:
                        combo_results = await completed_task
                        results.extend(combo_results)
                    except Exception as e:
                        logging.error(f"Task completion error: {str(e)}")
            
            return results
            
        except Exception as e:
            logging.error(f"Error fetching rates for {naic} in {state} at {representative_zip}: {str(e)}")
            return []
    
    def save_rates(self, rates: List[Dict]):
        """Save rates to the database in batches, avoiding duplicates based on the primary key."""
        if not rates:
            logging.info("No rates to save.")
            return
        
        # Extract values for batch insert
        rows = [
            (r['region_id'], r['gender'], r['tobacco'], r['age'], r['naic'], 
             r['plan'], r['rate'], r['discount_rate'], r['effective_date'], r['state'])
            for r in rates
        ]
        
        batch_size = 500  # Using reasonable batch size for better performance
        inserted_count = 0
        skipped_count = 0
        error_count = 0
        correction_log_file = f'rate_corrections_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        # Create a temporary table to handle bulk inserts and duplicate checking
        try:
            # Create a temp table with the same structure as rate_store
            self.conn.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_rates (
                    region_id TEXT,
                    gender TEXT,
                    tobacco INTEGER,
                    age INTEGER,
                    naic TEXT,
                    plan TEXT,
                    rate FLOAT,
                    discount_rate FLOAT,
                    effective_date TEXT,
                    state TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Process batches
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                batch_inserted = 0
                batch_skipped = 0
                
                try:
                    # Begin transaction for this batch
                    self.conn.execute("BEGIN TRANSACTION")
                    
                    # Clear temporary table
                    self.conn.execute("DELETE FROM temp_rates")
                    
                    # Insert batch into temporary table
                    self.conn.executemany("""
                        INSERT INTO temp_rates 
                        (region_id, gender, tobacco, age, naic, plan, rate, discount_rate, effective_date, state)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    
                    # Find existing records that would conflict
                    existing_records = self.conn.execute("""
                        SELECT r.region_id, r.gender, r.tobacco, r.age, r.naic, r.plan, 
                               r.rate, r.discount_rate, r.effective_date, r.state, t.rate, t.discount_rate
                        FROM rate_store r
                        JOIN temp_rates t ON 
                            r.region_id = t.region_id AND
                            r.gender = t.gender AND
                            r.tobacco = t.tobacco AND
                            r.age = t.age AND
                            r.naic = t.naic AND
                            r.plan = t.plan AND
                            r.effective_date = t.effective_date AND
                            r.state = t.state
                    """).fetchall()
                    
                    # Log potential corrections
                    for record in existing_records:
                        old_rate, old_discount = record[6], record[7]
                        new_rate, new_discount = record[10], record[11]
                        
                        if old_rate != new_rate or old_discount != new_discount:
                            # Log rate correction
                            with open(correction_log_file, 'a') as f:
                                f.write(
                                    f"{datetime.now()}: Potential rate correction for NAIC {record[4]}, "
                                    f"Plan {record[5]}, Effective Date {record[8]}, State {record[9]}: "
                                    f"Stored Rate ${old_rate:.2f} -> New Rate ${new_rate:.2f}, "
                                    f"Stored Discount ${old_discount:.2f} -> New Discount ${new_discount:.2f}\n"
                                )
                        
                        batch_skipped += 1
                    
                    # Insert records that don't exist in rate_store using a NOT EXISTS subquery
                    inserted = self.conn.execute("""
                        INSERT INTO rate_store 
                        (region_id, gender, tobacco, age, naic, plan, rate, discount_rate, effective_date, state)
                        SELECT t.region_id, t.gender, t.tobacco, t.age, t.naic, t.plan, 
                               t.rate, t.discount_rate, t.effective_date, t.state
                        FROM temp_rates t
                        WHERE NOT EXISTS (
                            SELECT 1 FROM rate_store r
                            WHERE r.region_id = t.region_id AND
                                  r.gender = t.gender AND
                                  r.tobacco = t.tobacco AND
                                  r.age = t.age AND
                                  r.naic = t.naic AND
                                  r.plan = t.plan AND
                                  r.effective_date = t.effective_date AND
                                  r.state = t.state
                        )
                    """)
                    
                    # Get number of rows inserted
                    batch_inserted = self.conn.execute("""
                        SELECT COUNT(*) FROM temp_rates t 
                        WHERE NOT EXISTS (
                            SELECT 1 FROM rate_store r
                            WHERE r.region_id = t.region_id AND
                                  r.gender = t.gender AND
                                  r.tobacco = t.tobacco AND
                                  r.age = t.age AND
                                  r.naic = t.naic AND
                                  r.plan = t.plan AND
                                  r.effective_date = t.effective_date AND
                                  r.state = t.state
                        )
                    """).fetchone()[0]
                    inserted_count += batch_inserted
                    skipped_count += batch_skipped
                    
                    # Commit transaction
                    self.conn.execute("COMMIT")
                    
                    logging.info(
                        f"Batch {i//batch_size + 1}/{(len(rows) + batch_size - 1) // batch_size}: "
                        f"Inserted {batch_inserted}, skipped {batch_skipped} records"
                    )
                    
                except Exception as e:
                    self.conn.execute("ROLLBACK")
                    error_count += 1
                    logging.error(f"Error in batch {i//batch_size + 1}: {str(e)}")
                    
                    # If too many errors, abort
                    if error_count > 5:
                        logging.error("Too many errors, aborting rate insertion")
                        break
            
            # Clean up temp table
            self.conn.execute("DROP TABLE IF EXISTS temp_rates")
            
        except Exception as e:
            logging.error(f"Fatal error in save_rates: {str(e)}")
            try:
                self.conn.execute("ROLLBACK")
                self.conn.execute("DROP TABLE IF EXISTS temp_rates")
            except:
                pass
            return
        
        # Verify data was saved by checking counts
        verification_count = self.conn.execute(
            """SELECT COUNT(*) FROM rate_store 
               WHERE naic = ? AND effective_date = ?""", 
            (rates[0]['naic'], rates[0]['effective_date'])
        ).fetchone()[0]
        
        if verification_count == 0:
            logging.error(f"No rates were saved for {rates[0]['naic']} on {rates[0]['effective_date']}")
        else:
            logging.info(
                f"Completed processing {len(rates)} potential rate records: "
                f"{inserted_count} inserted, {skipped_count} skipped, {verification_count} total in database"
            )
            
        if skipped_count > 0 and os.path.exists(correction_log_file):
            logging.info(f"Check {correction_log_file} for potential rate corrections.")
    
    def mark_processed(self, state: str, naic: str, effective_date: str, success: bool, api_effective_date: str = None):
        """Mark a carrier-state-date combination as processed."""
        try:
            # Check if api_effective_date column exists
            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO processed_data 
                    (state, naic, effective_date, api_effective_date, processed_at, success)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (state, naic, effective_date, api_effective_date or effective_date, datetime.now(), success))
            except Exception as col_error:
                if "api_effective_date" in str(col_error):
                    # Insert without the api_effective_date column
                    self.conn.execute("""
                        INSERT OR REPLACE INTO processed_data 
                        (state, naic, effective_date, processed_at, success)
                        VALUES (?, ?, ?, ?, ?)
                    """, (state, naic, effective_date, datetime.now(), success))
                else:
                    raise col_error
        except Exception as e:
            logging.error(f"Error marking processed: {str(e)}")
    
    def is_already_processed(self, state: str, naic: str, effective_date: str) -> bool:
        """
        Check if a carrier-state-date combination has already been processed successfully.
        This needs to look at the api_effective_date to see what dates the API actually returned.
        """
        # First check if it's marked as processed
        result = self.conn.execute("""
            SELECT api_effective_date FROM processed_data 
            WHERE state = ? AND naic = ? AND effective_date = ? AND success = true
        """, (state, naic, effective_date)).fetchone()
        
        if not result:
            # Not processed at all
            return False
            
        # Check if we have a record of the API's effective date from a previous run
        if result[0]:
            try:
                # If we've already processed this date and got api effective dates,
                # check if we have rate data for those dates
                api_dates = result[0].split(',')
                
                # For each API date, verify we have rate data
                for api_date in api_dates:
                    # Verify that we actually have rate data stored
                    rate_count = self.conn.execute("""
                        SELECT COUNT(*) FROM rate_store r 
                        JOIN region_mapping m ON r.region_id = m.region_id AND r.naic = m.naic
                        WHERE r.naic = ? AND r.state = ? AND r.effective_date = ?
                        LIMIT 1
                    """, (naic, state, api_date.strip())).fetchone()[0]
                    
                    if rate_count == 0:
                        # We have a record saying we processed it, but no actual rate data
                        # This suggests database corruption or incomplete processing
                        logging.warning(f"Record says we processed {state}/{naic}/{effective_date} " + 
                                       f"with API date {api_date}, but no rate data found. Will reprocess.")
                        return False
                
                # If we have rate data for all API effective dates, it's processed
                return True
            except Exception as e:
                logging.error(f"Error checking API dates: {str(e)}")
                return False
        else:
            # We don't have API effective dates stored, fall back to checking for any rate data
            rate_count = self.conn.execute("""
                SELECT COUNT(*) FROM rate_store r 
                JOIN region_mapping m ON r.region_id = m.region_id AND r.naic = m.naic
                WHERE r.naic = ? AND r.state = ?
                LIMIT 1
            """, (naic, state)).fetchone()[0]
            
            # Consider it processed only if we have rate data
            return rate_count > 0
    
    def get_region_hash(self, locations: Set[str]) -> str:
        """Generate a consistent hash ID for a set of locations (ZIP codes or counties)"""
        # Sort the locations for consistent hashing
        sorted_locations = sorted(locations)
        data_str = json.dumps(sorted_locations, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()[:12]
    
    def get_already_processed_regions(self, naic: str, state: str, effective_date: str) -> Set[str]:
        """Get regions that already have rates stored for the given parameters"""
        try:
            result = self.conn.execute("""
                SELECT DISTINCT region_id FROM rate_store
                WHERE naic = ? AND state = ? AND effective_date = ?
            """, (naic, state, effective_date)).fetchall()
            
            return {row[0] for row in result}
        except Exception as e:
            logging.error(f"Error checking processed regions: {str(e)}")
            return set()
    
    def save_region_data(self, state: str, naic: str, regions: List[Set[str]], mapping_type: str) -> List[str]:
        """
        Save region data to database and return list of region IDs.
        This handles both creating the region entries and mapping locations (ZIP codes or counties) to regions.
        Uses region hashes to deduplicate regions with identical location sets.
        """
        region_ids = []
        processed_region_hashes = set()  # Track regions we've already processed
        
        try:
            # Begin transaction for all operations
            self.conn.execute("BEGIN TRANSACTION")
            
            # Process each region
            for region_set in regions:
                # Convert to a sorted list for consistent hashing
                region_data_list = sorted(region_set)
                region_data_json = json.dumps(region_data_list)
                
                # Generate a hash for this region's locations
                region_hash = self.get_region_hash(region_set)
                
                # Skip if we've already processed an identical region in this batch
                if region_hash in processed_region_hashes:
                    logging.info(f"Skipping duplicate region set during batch processing (hash: {region_hash})")
                    continue
                
                # Check if this exact region (by hash) already exists for this carrier/state
                existing_region = self.conn.execute("""
                    SELECT region_id FROM region_metadata 
                    WHERE naic = ? AND state = ? AND region_hash = ?
                """, (naic, state, region_hash)).fetchone()
                
                if existing_region:
                    # Reuse the existing region_id
                    region_id = existing_region[0]
                    logging.info(f"Reusing existing region {region_id} for {naic} in {state} (hash: {region_hash})")
                else:
                    # Generate a new UUID for this region
                    region_id = str(uuid.uuid4())
                    
                    # Store the region metadata with its hash
                    self.conn.execute("""
                        INSERT INTO region_metadata
                        (region_id, naic, state, mapping_type, region_hash)
                        VALUES (?, ?, ?, ?, ?)
                    """, (region_id, naic, state, mapping_type, region_hash))
                    
                    # Store the region data
                    self.conn.execute("""
                        INSERT INTO rate_regions 
                        (region_id, naic, state, mapping_type, region_data)
                        VALUES (?, ?, ?, ?, ?)
                    """, (region_id, naic, state, mapping_type, region_data_json))
                    
                    if mapping_type == "zip5":
                        # For ZIP code-based regions, map all ZIP codes in this region to the region_id
                        zip_codes = list(region_set)
                        
                        # Create ZIP code to region mapping
                        zip_mappings = [(zip_code, "", region_id, naic, mapping_type) for zip_code in zip_codes]
                        
                        # Insert in batches
                        batch_size = 500
                        for i in range(0, len(zip_mappings), batch_size):
                            batch = zip_mappings[i:i+batch_size]
                            self.conn.executemany("""
                                INSERT OR REPLACE INTO region_mapping
                                (zip_code, county, region_id, naic, mapping_type)
                                VALUES (?, ?, ?, ?, ?)
                            """, batch)
                    else:
                        # For county-based regions, map both the counties and their ZIP codes to the region_id
                        counties = list(region_set)
                        all_mappings = []
                        
                        for county in counties:
                            # Map county directly using empty zip_code for county-only lookups
                            all_mappings.append(("", county, region_id, naic, mapping_type))
                            
                            # Also add mappings for each ZIP code in each county
                            county_zips = self.zip_holder.lookup_zip_by_county(state, county)
                            for zip_code in county_zips:
                                all_mappings.append((zip_code, county, region_id, naic, mapping_type))
                        
                        # Insert all mappings in batches
                        batch_size = 500
                        for i in range(0, len(all_mappings), batch_size):
                            batch = all_mappings[i:i+batch_size]
                            self.conn.executemany("""
                                INSERT OR REPLACE INTO region_mapping
                                (zip_code, county, region_id, naic, mapping_type)
                                VALUES (?, ?, ?, ?, ?)
                            """, batch)
                    
                    logging.info(f"Created new region {region_id} for {naic} in {state} with {len(region_set)} locations (hash: {region_hash})")
                
                # Add to return list and mark as processed
                region_ids.append(region_id)
                processed_region_hashes.add(region_hash)
            
            # Verify we have the expected number of region IDs
            unique_region_count = len(processed_region_hashes)
            if len(region_ids) != unique_region_count:
                logging.warning(f"Expected {unique_region_count} unique regions, but got {len(region_ids)} region IDs")
            
            # Commit all changes
            self.conn.execute("COMMIT")
            logging.info(f"Saved {len(region_ids)} regions for {naic} in {state} ({len(processed_region_hashes)} unique)")
            return region_ids
            
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logging.error(f"Error saving region data: {str(e)}")
            return []
    
    async def process_carrier_state(self, state: str, naic: str, effective_date: str, dry_run: bool = False) -> bool:
        """Process a specific carrier-state combination for an effective date."""
        try:
            # Check if already processed
            if self.is_already_processed(state, naic, effective_date):
                logging.info(f"Skipping already processed: {state}/{naic}/{effective_date}")
                return True
                
            if dry_run:
                logging.info(f"Dry run: would process {state}/{naic}/{effective_date}")
                return True
                
            logging.info(f"Processing {state}/{naic}/{effective_date}")
            
            # Get geographic regions for this carrier-state
            regions, mapping_type = await self.get_rate_regions(state, naic)
            if not regions:
                logging.warning(f"No regions found for {naic} in {state}")
                self.mark_processed(state, naic, effective_date, False)
                return False
            
            # Save region data to database and get region IDs (with deduplication)
            region_ids = self.save_region_data(state, naic, regions, mapping_type)
            if not region_ids:
                logging.error(f"Failed to save region data for {naic} in {state}")
                self.mark_processed(state, naic, effective_date, False)
                return False
            
            # OPTIMIZATION: Filter region IDs to only process one representative per unique hash
            # This prevents redundant API calls for regions that are geographically identical
            unique_region_ids = []
            processed_hashes = set()
            
            # Get hash for each region ID
            for region_id in region_ids:
                region_hash = self.conn.execute("""
                    SELECT region_hash FROM region_metadata
                    WHERE region_id = ?
                """, (region_id,)).fetchone()
                
                if region_hash and region_hash[0]:
                    region_hash = region_hash[0]
                    if region_hash not in processed_hashes:
                        unique_region_ids.append(region_id)
                        processed_hashes.add(region_hash)
            
            logging.info(f"Optimized: processing {len(unique_region_ids)} unique regions instead of {len(region_ids)} total regions (saved {len(region_ids) - len(unique_region_ids)} redundant API calls)")
            
            # Get regions that already have rates for this date
            processed_regions = self.get_already_processed_regions(naic, state, effective_date)
            regions_to_process = [r_id for r_id in unique_region_ids if r_id not in processed_regions]
            
            logging.info(f"Processing {len(regions_to_process)}/{len(unique_region_ids)} unique regions for {naic} in {state}")
            if len(regions_to_process) == 0:
                logging.info(f"All regions already have rates for {naic} in {state} on {effective_date}")
                self.mark_processed(state, naic, effective_date, True)
                return True
            
            # Process each region in batches with controlled concurrency
            all_rates = []
            api_effective_dates = set()  # Track unique API effective dates
            batch_size = 5  # Process 5 regions at a time
            
            for i in range(0, len(regions_to_process), batch_size):
                batch_region_ids = regions_to_process[i:i+batch_size]
                current_tasks = []
                
                for region_index, region_id in enumerate(batch_region_ids):
                    # Get a representative ZIP for this region
                    region_data_json = self.conn.execute("""
                        SELECT region_data FROM rate_regions WHERE region_id = ?
                    """, (region_id,)).fetchone()[0]
                    
                    region_data = json.loads(region_data_json)
                    region = set(region_data)
                    
                    rep_zip, rep_county = self.get_representative_zip(state, region, mapping_type)
                    if not rep_zip:
                        logging.warning(f"Could not find representative ZIP for region {region_id}")
                        continue
                    
                    # Create task for fetching rates for this region
                    async with self.region_semaphore:
                        task = asyncio.create_task(self.fetch_rates(
                            state, naic, region_id, rep_zip, rep_county, effective_date
                        ))
                        current_tasks.append((task, region_id, region_index))
                
                # Process fetched rates as they complete
                for task_data in current_tasks:
                    task, region_id, region_index = task_data
                    try:
                        # Wait for this region's rates
                        region_rates = await task
                        
                        if not region_rates:
                            logging.warning(f"No rates returned for {naic} in {state} for region {region_id}")
                            continue
                        
                        # Track API effective dates
                        if region_rates:
                            for rate in region_rates:
                                if 'effective_date' in rate:
                                    api_effective_dates.add(rate['effective_date'])
                        
                        logging.info(f"Processing rates for region {region_index+1}/{len(regions_to_process)} with {len(region_rates)} rate records")
                        all_rates.extend(region_rates)
                        
                        # Save rates in batches to avoid memory issues
                        if len(all_rates) >= 50000:
                            self.save_rates(all_rates)
                            all_rates = []
                    
                    except Exception as region_error:
                        logging.error(f"Error processing region {region_index+1} for {naic} in {state}: {str(region_error)}")
                        continue  # Continue with other regions
                
                # Save after each batch to free memory
                if all_rates:
                    self.save_rates(all_rates)
                    all_rates = []
            
            # Save any remaining rates
            if all_rates:
                self.save_rates(all_rates)
            
            # Verify rates were actually saved
            if len(api_effective_dates) > 0:
                placeholders = ','.join(['?'] * len(api_effective_dates))
                rate_count = self.conn.execute(f"""
                    SELECT COUNT(*) FROM rate_store 
                    WHERE naic = ? AND state = ? AND effective_date IN ({placeholders})
                """, (naic, state, *api_effective_dates)).fetchone()[0]
            else:
                # Fallback to checking requested date if no API dates found
                rate_count = self.conn.execute("""
                    SELECT COUNT(*) FROM rate_store 
                    WHERE naic = ? AND state = ?
                """, (naic, state)).fetchone()[0]
            
            if rate_count == 0:
                logging.error(f"No rates were saved for {naic} in {state} on {effective_date}")
                self.mark_processed(state, naic, effective_date, False)
                return False
            
            # Mark as processed, including all API effective dates we found
            api_dates_str = ','.join(sorted(api_effective_dates)) if api_effective_dates else None
            self.mark_processed(state, naic, effective_date, True, api_dates_str)
            logging.info(f"Successfully processed {state}/{naic}/{effective_date} with {rate_count} rates (API dates: {api_dates_str})")
            return True
            
        except Exception as e:
            logging.error(f"Error processing {state}/{naic}/{effective_date}: {str(e)}")
            self.mark_processed(state, naic, effective_date, False)
            return False
    
    async def build_database(self, states: List[str], selected_naics: List[str], effective_dates: List[str], dry_run: bool = False):
        """
        Build the database for specified states, carriers, and dates.
        
        Uses the natural effective dates from the API responses.
        """
        # Initialize API client
        await self.cr.async_init()
        await self.cr.fetch_token()
        
        if len(effective_dates) == 0:
            logging.error("No effective dates provided")
            return
            
        # Process all months fully
        # Process states in smaller groups to control total number of concurrent operations
        state_batch_size = 2  # Process 2 states at a time
        
        for i in range(0, len(states), state_batch_size):
            state_batch = states[i:i+state_batch_size]
            state_tasks = []
            
            # Create a task for each state in the current batch
            for state in state_batch:
                task = asyncio.create_task(self._process_state(state, selected_naics, effective_dates, dry_run))
                state_tasks.append((task, state))
            
            # Wait for all states in this batch to complete
            for task_data in state_tasks:
                task, state = task_data
                try:
                    await task
                except Exception as e:
                    logging.error(f"Error processing state {state}: {str(e)}")
    
        logging.info("Database build completed")
    
    async def _process_state(self, state: str, selected_naics: List[str], effective_dates: List[str], dry_run: bool = False):
        """Process all carriers for a single state."""
        try:
            logging.info(f"Processing state: {state}")
            
            # Get available carriers for this state
            available_carriers = await self.get_available_carriers(state, effective_dates[0])
            if not available_carriers:
                logging.warning(f"No carriers available for {state}")
                return
            
            # Filter carriers based on selected NAICs
            carriers_to_process = [n for n in selected_naics if n in available_carriers]
            logging.info(f"Processing {len(carriers_to_process)}/{len(available_carriers)} carriers in {state}")
            
            # Process carriers in batches to prevent too many open files
            carrier_batch_size = 3  # Process 3 carriers at a time
            
            for i in range(0, len(carriers_to_process), carrier_batch_size):
                carrier_batch = carriers_to_process[i:i+carrier_batch_size]
                
                # Create a semaphore to limit concurrency within this carrier batch
                batch_semaphore = asyncio.Semaphore(len(carrier_batch) * len(effective_dates))
                
                async def process_with_semaphore(state, naic, effective_date):
                    async with batch_semaphore:
                        return await self.process_carrier_state(state, naic, effective_date, dry_run)
                
                # Create tasks for this batch of carriers
                batch_tasks = []
                for naic in carrier_batch:
                    for effective_date in effective_dates:
                        task = asyncio.create_task(
                            process_with_semaphore(state, naic, effective_date)
                        )
                        batch_tasks.append((task, naic, effective_date))
                
                # Wait for all tasks in this batch to complete
                for task_data in batch_tasks:
                    task, naic, effective_date = task_data
                    try:
                        success = await task
                        if not success:
                            logging.warning(f"Failed to process {state}/{naic}/{effective_date}")
                    except Exception as task_error:
                        logging.error(f"Error in task for {state}/{naic}/{effective_date}: {str(task_error)}")
                
                # Allow some time between batches for resources to be released
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error processing state {state}: {str(e)}")
    
    def get_rate_by_zip(self, zip_code: str, gender: str, tobacco: int, age: int, effective_date: str) -> List[Dict]:
        """
        Get rates for all carriers for specific demographic at a ZIP code.
        This is a convenience method for external use.
        """
        try:
            result = self.conn.execute("""
                SELECT r.naic, c.company_name, r.plan, r.rate, r.discount_rate
                FROM rate_store r
                JOIN region_mapping m ON r.region_id = m.region_id AND r.naic = m.naic
                LEFT JOIN carrier_info c ON r.naic = c.naic
                WHERE m.zip_code = ? 
                  AND r.gender = ?
                  AND r.tobacco = ?
                  AND r.age = ?
                  AND r.effective_date = ?
                ORDER BY r.naic, r.plan
            """, [zip_code, gender, tobacco, age, effective_date]).fetchall()
            
            return [
                {
                    "naic": row[0],
                    "company_name": row[1] or "Unknown",
                    "plan": row[2],
                    "rate": row[3],
                    "discount_rate": row[4]
                }
                for row in result
            ]
            
        except Exception as e:
            logging.error(f"Error getting rates for {zip_code}: {str(e)}")
            return []
    
    def get_rate_by_county(self, state: str, county: str, gender: str, tobacco: int, age: int, effective_date: str) -> List[Dict]:
        """
        Get rates for all carriers for specific demographic at a county.
        This is a convenience method for external use.
        """
        try:
            result = self.conn.execute("""
                SELECT r.naic, c.company_name, r.plan, r.rate, r.discount_rate
                FROM rate_store r
                JOIN region_mapping m ON r.region_id = m.region_id AND r.naic = m.naic
                LEFT JOIN carrier_info c ON r.naic = c.naic
                WHERE m.county = ? 
                  AND r.state = ?
                  AND r.gender = ?
                  AND r.tobacco = ?
                  AND r.age = ?
                  AND r.effective_date = ?
                ORDER BY r.naic, r.plan
            """, [county, state, gender, tobacco, age, effective_date]).fetchall()
            
            return [
                {
                    "naic": row[0],
                    "company_name": row[1] or "Unknown",
                    "plan": row[2],
                    "rate": row[3],
                    "discount_rate": row[4]
                }
                for row in result
            ]
            
        except Exception as e:
            logging.error(f"Error getting rates for county {county} in {state}: {str(e)}")
            return []
    
    def optimize_database(self):
        """Run optimizations on the database."""
        logging.info("Running database optimizations...")
        
        # Vacuum the database
        self.conn.execute("VACUUM")
        
        # Run analyze to update statistics
        self.conn.execute("ANALYZE")
        
        logging.info("Database optimization complete")
    
    def close(self):
        """Close the database connection and clean up resources."""
        if hasattr(self, 'conn') and self.conn:
            try:
                logging.info("Running database optimizations...")
                # Vacuum the database
                self.conn.execute("VACUUM")
                
                # Run analyze to update statistics
                self.conn.execute("ANALYZE")
                
                logging.info("Database optimization complete")
                
                # Close DuckDB connection
                self.conn.close()
                logging.info("Database connection closed")
            except Exception as e:
                logging.error(f"Error during database close: {str(e)}")
                
        # Close any HTTP clients
        if hasattr(self, 'cr') and self.cr:
            try:
                if hasattr(self.cr, 'http_client') and self.cr.http_client:
                    self.cr.http_client.close()
                    logging.info("HTTP client connections closed")
            except Exception as e:
                logging.error(f"Error closing HTTP client: {str(e)}")


async def main():
    parser = argparse.ArgumentParser(description="Build a Medicare Supplement Rate database with DuckDB")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="DuckDB database file path")
    parser.add_argument("-m", "--months", type=int, default=6, help="Number of months to process")
    parser.add_argument("--states", nargs="+", help="List of states to process (e.g., TX CA)")
    parser.add_argument("--naics", nargs="+", help="List of NAIC codes to process")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")
    parser.add_argument("--max-connections", type=int, default=100, help="Maximum number of concurrent connections")
    parser.add_argument("--lookup", action="store_true", help="Lookup mode - query rates for a specific ZIP")
    parser.add_argument("--zip", type=str, help="ZIP code to lookup rates for")
    parser.add_argument("--age", type=int, default=65, help="Age to lookup rates for")
    parser.add_argument("--gender", type=str, choices=["M", "F"], default="M", help="Gender to lookup rates for")
    parser.add_argument("--tobacco", type=int, choices=[0, 1], default=0, help="Tobacco status to lookup rates for")
    parser.add_argument("--force-reprocess", action="store_true", help="Force reprocessing even if marked as already processed")
    parser.add_argument("--check-integrity", action="store_true", help="Run database integrity check only")
    parser.add_argument("--repair-database", action="store_true", help="Attempt to repair database issues by cleaning orphaned entries")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    if args.dry_run:
        logging.info("DRY RUN MODE - No changes will be made to the database")
    
    if args.force_reprocess:
        logging.info("FORCE REPROCESS MODE - Will reprocess data even if already marked as processed")
    
    # Set max connections
    global MAX_CONNECTIONS
    MAX_CONNECTIONS = args.max_connections
    
    try:
        # Initialize the database builder
        builder = DuckDBMedicareBuilder(args.db)
        
        # Run database integrity check if requested
        if args.check_integrity:
            logging.info("Running database integrity check...")
            builder.check_database_integrity()
            builder.close()
            return
            
        # Clean up orphaned entries if repair is requested
        if args.repair_database:
            logging.info("Repairing database by cleaning orphaned entries...")
            builder.cleanup_orphaned_entries()
            builder.check_database_integrity()
            builder.close()
            return
            
        # Always run a basic integrity check on startup
        builder.check_database_integrity()
        
        # If force reprocess is enabled, override the is_already_processed method
        if args.force_reprocess:
            builder.is_already_processed = lambda state, naic, effective_date: False
        
        # Lookup mode - query rates for a specific ZIP
        if args.lookup:
            if not args.zip:
                logging.error("ZIP code is required for lookup mode")
                return
                
            effective_dates = generate_effective_dates(args.months)
            effective_date = effective_dates[0]  # Use first month by default
            
            rates = builder.get_rate_by_zip(
                args.zip, args.gender, args.tobacco, args.age, effective_date
            )
            
            print(f"\nRates for ZIP {args.zip}, {args.gender}, age {args.age}, tobacco {args.tobacco}, {effective_date}:")
            print("=" * 80)
            print(f"{'NAIC':<8} {'Company':<30} {'Plan':<5} {'Rate':>10} {'Discount':>10}")
            print("-" * 80)
            
            for rate in rates:
                print(f"{rate['naic']:<8} {rate['company_name'][:30]:<30} {rate['plan']:<5} "
                      f"{rate['rate']:>10.2f} {rate['discount_rate']:>10.2f}")
            
            print("=" * 80)
            print(f"Total: {len(rates)} rates found")
            
            # Close connections and return
            builder.close()
            return
        
        # Standard operation - build database
        
        # Load carriers
        selected_naics = args.naics if args.naics else load_carrier_selections()
        logging.info(f"Selected carriers: {len(selected_naics)}")
        
        # Get states to process
        states_to_process = args.states if args.states else get_all_states()
        logging.info(f"States to process: {len(states_to_process)}")
        
        # Generate effective dates
        effective_dates = generate_effective_dates(args.months)
        logging.info(f"Effective dates: {effective_dates}")
        
        # Build the database
        await builder.build_database(
            states_to_process, 
            selected_naics, 
            effective_dates, 
            args.dry_run
        )
        
        # Run final database integrity check
        builder.check_database_integrity()
        
        # Close the database
        builder.close()
        
        logging.info("Database build completed")
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())