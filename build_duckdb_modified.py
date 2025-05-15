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

def load_carrier_selections_from_db(conn) -> List[str]:
    """Load carrier selections from database."""
    try:
        # Ensure the carrier_info table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS carrier_info (
                naic TEXT PRIMARY KEY,
                company_name TEXT,
                selected INTEGER DEFAULT 1,
                discount_category TEXT
            )
        """)
        
        # Get all selected carriers
        result = conn.execute("""
            SELECT naic FROM carrier_info WHERE selected = 1
        """).fetchall()
        
        selected_naics = [row[0] for row in result]
        
        # If no carriers are selected in the database, fall back to JSON file for backward compatibility
        if not selected_naics:
            logging.warning("No carriers selected in database, falling back to carrier_selections.json")
            return load_carrier_selections_from_json()
            
        return selected_naics
    except Exception as e:
        logging.error(f"Error loading carrier selections from database: {str(e)}")
        # Fall back to JSON file
        return load_carrier_selections_from_json()

def load_carrier_selections_from_json() -> List[str]:
    """Load carrier selections from JSON file (legacy method)."""
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
        
        # Create carrier_info table for metadata with selected flag
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS carrier_info (
                naic TEXT PRIMARY KEY,
                company_name TEXT,
                selected INTEGER DEFAULT 1,
                discount_category TEXT
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
            
        # Check if discount_category exists in carrier_info
        try:
            self.conn.execute("SELECT discount_category FROM carrier_info LIMIT 1")
        except Exception:
            logging.info("Adding discount_category column to carrier_info table")
            self.conn.execute("ALTER TABLE carrier_info ADD COLUMN discount_category TEXT")
            
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
                    # Store carrier info, preserving selected status if it exists
                    self.conn.execute("""
                        INSERT INTO carrier_info (naic, company_name, selected) 
                        VALUES (?, ?, 1)
                        ON CONFLICT (naic) DO UPDATE SET
                            company_name = EXCLUDED.company_name
                    """, (naic, company_name))
            
            logging.info(f"Found {len(available_naics)} available carriers in {state}: {available_naics}")
            return available_naics
            
        except Exception as e:
            logging.error(f"Error getting available carriers for state {state}: {str(e)}")
            return set()
    
    # The rest of the class methods remain the same...
    # For brevity, I've omitted them but would include them in a real implementation
    
    # ... [Rest of the DuckDBMedicareBuilder class implementation] ...


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
    parser.add_argument("--sync-carrier-data", action="store_true", help="Synchronize carrier_info table with carrier_selections.json file")
    
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
        
        # Synchronize carrier data if requested
        if args.sync_carrier_data:
            logging.info("Synchronizing carrier data between JSON and database...")
            # Load selections from JSON
            selections = {}
            if os.path.exists('carrier_selections.json'):
                with open('carrier_selections.json', 'r') as f:
                    selections = json.load(f)
            
            # Get all existing carriers in the database
            existing_carriers = builder.conn.execute("""
                SELECT naic FROM carrier_info
            """).fetchall()
            existing_naics = {row[0] for row in existing_carriers}
            
            # Start transaction
            builder.conn.execute("BEGIN TRANSACTION")
            
            try:
                # Update all existing carriers to inactive by default
                builder.conn.execute("""
                    UPDATE carrier_info SET selected = 0
                """)
                logging.info(f"Set all {len(existing_naics)} existing carriers to inactive")
                
                # Update or insert carriers from JSON
                for naic, selected in selections.items():
                    try:
                        builder.conn.execute("""
                            INSERT INTO carrier_info (naic, selected) VALUES (?, ?)
                            ON CONFLICT (naic) DO UPDATE SET selected = ?
                        """, (naic, 1 if selected else 0, 1 if selected else 0))
                    except Exception as e:
                        logging.error(f"Error updating carrier {naic}: {str(e)}")
                
                # Commit transaction
                builder.conn.execute("COMMIT")
                
                # Log summary
                active_count = builder.conn.execute("""
                    SELECT COUNT(*) FROM carrier_info WHERE selected = 1
                """).fetchone()[0]
                
                total_count = builder.conn.execute("""
                    SELECT COUNT(*) FROM carrier_info
                """).fetchone()[0]
                
                logging.info(f"Carrier sync complete: {active_count} active carriers, {total_count - active_count} inactive carriers out of {total_count} total carriers")
                
            except Exception as e:
                builder.conn.execute("ROLLBACK")
                logging.error(f"Error during carrier sync: {str(e)}")
            
            builder.close()
            return
        
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
        
        # Load carriers from database
        if args.naics:
            # Split comma-separated NAICs if provided
            selected_naics = [naic.strip() for naic in ','.join(args.naics).split(',')]
        else:
            # Load carriers from database
            selected_naics = load_carrier_selections_from_db(builder.conn)
            
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