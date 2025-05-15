#!/usr/bin/env python3
import argparse
import duckdb
import json
import logging
import os
import sys
import requests
from datetime import datetime
from typing import Dict, List, Set

def setup_logging(quiet: bool = False) -> None:
    """Set up logging to file and console."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f'update_carriers_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

def fetch_all_carriers() -> List[Dict]:
    """Fetch all carrier information from CSG API."""
    url = "https://csgapi.appspot.com/v1/med_supp/open/companies.json"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        carriers = response.json()
        logging.info(f"Successfully fetched {len(carriers)} carriers from CSG API")
        return carriers
    except requests.RequestException as e:
        logging.error(f"Error fetching carriers from CSG API: {e}")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error("Invalid JSON response from CSG API")
        sys.exit(1)

def ensure_carrier_table(conn):
    """Ensure carrier_info table exists with all required columns."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS carrier_info (
            naic TEXT PRIMARY KEY,
            company_name TEXT,
            selected INTEGER DEFAULT 0,
            discount_category TEXT,
            name_full TEXT,
            name_short TEXT,
            last_updated TIMESTAMP
        )
    """)
    
    # Check if we need to add the new columns
    try:
        conn.execute("SELECT name_full, name_short, last_updated FROM carrier_info LIMIT 1")
    except Exception:
        logging.info("Adding new columns to carrier_info table")
        try:
            conn.execute("ALTER TABLE carrier_info ADD COLUMN name_full TEXT")
            conn.execute("ALTER TABLE carrier_info ADD COLUMN name_short TEXT")
            conn.execute("ALTER TABLE carrier_info ADD COLUMN last_updated TIMESTAMP")
        except Exception as e:
            logging.error(f"Error adding columns: {e}")

def update_carriers(db_path: str, dry_run: bool = False):
    """Update carrier information in database from CSG API."""
    # Fetch carriers from API
    carriers = fetch_all_carriers()
    
    if dry_run:
        logging.info(f"DRY RUN: Would update {len(carriers)} carriers in database")
        for carrier in carriers[:5]:
            logging.info(f"Sample carrier: {carrier['naic']} - {carrier['name_full']}")
        return
    
    try:
        # Connect to database
        conn = duckdb.connect(db_path)
        
        # Ensure table structure
        ensure_carrier_table(conn)
        
        # Get existing carriers
        existing_naics = set()
        try:
            result = conn.execute("SELECT naic FROM carrier_info").fetchall()
            existing_naics = {row[0] for row in result}
            logging.info(f"Found {len(existing_naics)} existing carriers in database")
        except Exception as e:
            logging.error(f"Error fetching existing carriers: {e}")
        
        # Track changes
        added = set()
        updated = set()
        
        # Start transaction
        conn.execute("BEGIN TRANSACTION")
        
        try:
            # Process each carrier from the API
            current_time = datetime.now()
            for carrier in carriers:
                naic = carrier['naic']
                name_full = carrier['name_full']
                name_short = carrier['name']
                
                # For existing carriers, preserve the selected status
                if naic in existing_naics:
                    conn.execute("""
                        UPDATE carrier_info 
                        SET company_name = ?, 
                            name_full = ?, 
                            name_short = ?,
                            last_updated = ?
                        WHERE naic = ?
                    """, (name_short, name_full, name_short, current_time, naic))
                    updated.add(naic)
                else:
                    # For new carriers, default to not selected (0)
                    conn.execute("""
                        INSERT INTO carrier_info 
                        (naic, company_name, name_full, name_short, selected, last_updated) 
                        VALUES (?, ?, ?, ?, 0, ?)
                    """, (naic, name_short, name_full, name_short, current_time))
                    added.add(naic)
            
            # Mark carriers no longer in the API (but keep them in the database)
            api_naics = {carrier['naic'] for carrier in carriers}
            removed_naics = existing_naics - api_naics
            
            if removed_naics:
                logging.warning(f"Found {len(removed_naics)} carriers in database that are no longer in the API")
                # We don't delete them, just log them
                for naic in removed_naics:
                    logging.warning(f"Carrier NAIC {naic} is in database but not in API")
            
            # Commit transaction
            conn.execute("COMMIT")
            
            logging.info(f"Carrier update complete: {len(added)} carriers added, {len(updated)} carriers updated")
            
        except Exception as e:
            conn.execute("ROLLBACK")
            logging.error(f"Error during carrier update: {e}")
            conn.close()
            sys.exit(1)
        
        # Close connection
        conn.close()
        
    except Exception as e:
        logging.error(f"Error accessing database: {e}")
        sys.exit(1)
        
    return {
        'added': added,
        'updated': updated
    }

def main():
    parser = argparse.ArgumentParser(description="Update carrier information from CSG API")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="Path to DuckDB database file")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without making them")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} not found.")
        sys.exit(1)
    
    update_carriers(args.db, args.dry_run)
    
    if not args.quiet:
        print("\nCarrier update complete!")
        if args.dry_run:
            print("Note: This was a dry run, no changes were made.")

if __name__ == "__main__":
    main()