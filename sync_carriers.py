#!/usr/bin/env python3
from datetime import datetime
import argparse
import duckdb
import json
import logging
import os
import sys
import subprocess
from typing import Dict, Optional

def setup_logging(quiet: bool = False) -> None:
    """Set up logging to file and console."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f'sync_carriers_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

def load_selections_from_json() -> Dict[str, bool]:
    """Load carrier selections from JSON file."""
    if not os.path.exists('carrier_selections.json'):
        logging.error("carrier_selections.json not found. Please run select_carriers.py first.")
        sys.exit(1)
        
    try:
        with open('carrier_selections.json', 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        logging.error("Invalid JSON in carrier_selections.json.")
        sys.exit(1)

def update_from_api(db_path: str, dry_run: bool = False, quiet: bool = False) -> bool:
    """Update carrier information from CSG API."""
    try:
        # Build command to run update_supp_carriers.py
        cmd = [sys.executable, "update_supp_carriers.py", "-d", db_path]
        if dry_run:
            cmd.append("--dry-run")
        if quiet:
            cmd.append("-q")
        
        logging.info("Updating carrier list from CSG API...")
        result = subprocess.run(cmd, check=True)
        logging.info("Carrier list update complete")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error updating carrier list from API: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error running update_supp_carriers.py: {e}")
        return False

def sync_carriers(db_path: str, dry_run: bool = False, quiet: bool = False):
    """Synchronize carrier_info table with carrier_selections.json."""
    # First update carriers from API
    api_success = update_from_api(db_path, dry_run, quiet)
    if not api_success:
        logging.warning("Proceeding with sync despite API update failure")
    
    # Load selections from JSON
    selections = load_selections_from_json()
    logging.info(f"Loaded {len(selections)} carriers from carrier_selections.json")
    
    # Count selected carriers in JSON
    selected_in_json = sum(1 for selected in selections.values() if selected)
    logging.info(f"JSON contains {selected_in_json} selected carriers")
    
    if dry_run:
        logging.info("DRY RUN MODE - No changes will be made to the database")
    
    try:
        # Connect to database
        conn = duckdb.connect(db_path)
        
        # Ensure carrier_info table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS carrier_info (
                naic TEXT PRIMARY KEY,
                company_name TEXT,
                selected INTEGER DEFAULT 1,
                discount_category TEXT
            )
        """)
        
        # Get all existing carriers in the database
        existing_carriers = conn.execute("""
            SELECT naic FROM carrier_info
        """).fetchall()
        existing_naics = {row[0] for row in existing_carriers}
        logging.info(f"Found {len(existing_naics)} existing carriers in database")
        
        if not dry_run:
            # Start transaction
            conn.execute("BEGIN TRANSACTION")
            
            try:
                # Update all existing carriers to inactive by default
                conn.execute("""
                    UPDATE carrier_info SET selected = 0
                """)
                logging.info(f"Set all {len(existing_naics)} existing carriers to inactive")
                
                # Update or insert carriers from JSON
                for naic, selected in selections.items():
                    try:
                        conn.execute("""
                            INSERT INTO carrier_info (naic, selected) VALUES (?, ?)
                            ON CONFLICT (naic) DO UPDATE SET selected = ?
                        """, (naic, 1 if selected else 0, 1 if selected else 0))
                    except Exception as e:
                        logging.error(f"Error updating carrier {naic}: {str(e)}")
                
                # Commit transaction
                conn.execute("COMMIT")
                
                # Log summary
                active_count = conn.execute("""
                    SELECT COUNT(*) FROM carrier_info WHERE selected = 1
                """).fetchone()[0]
                
                total_count = conn.execute("""
                    SELECT COUNT(*) FROM carrier_info
                """).fetchone()[0]
                
                logging.info(f"Carrier sync complete: {active_count} active carriers, {total_count - active_count} inactive carriers out of {total_count} total carriers")
                
                # Verify that active count matches selected count in JSON
                if active_count != selected_in_json:
                    logging.warning(f"Mismatch between selected carriers in JSON ({selected_in_json}) and active carriers in database ({active_count})")
                
            except Exception as e:
                conn.execute("ROLLBACK")
                logging.error(f"Error during carrier sync: {str(e)}")
                conn.close()
                sys.exit(1)
        else:
            # Report what would happen in dry run mode
            to_activate = [naic for naic, selected in selections.items() if selected]
            to_deactivate = existing_naics - set(to_activate)
            
            logging.info(f"Would activate {len(to_activate)} carriers")
            logging.info(f"Would deactivate {len(to_deactivate)} carriers")
            
        # Close connection
        conn.close()
        
    except Exception as e:
        logging.error(f"Error accessing database: {str(e)}")
        sys.exit(1)

def main():
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description="Synchronize carrier_info table with carrier_selections.json")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="Path to DuckDB database file")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without making them")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("--skip-api", action="store_true", help="Skip updating carriers from CSG API")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} not found.")
        sys.exit(1)
    
    # If not skipping API update, ensure update_supp_carriers.py exists
    if not args.skip_api and not os.path.exists("update_supp_carriers.py"):
        logging.error(f"update_supp_carriers.py not found. Run with --skip-api to bypass.")
        sys.exit(1)
    
    # Sync carriers - will update from API unless --skip-api is specified
    if args.skip_api:
        logging.info("Skipping API update as requested")
        sync_carriers(args.db, args.dry_run, args.quiet)
    else:
        sync_carriers(args.db, args.dry_run, args.quiet)
    
    if not args.quiet:
        print("\nCarrier synchronization complete!")
        if args.dry_run:
            print("Note: This was a dry run, no changes were made.")

if __name__ == "__main__":
    main()