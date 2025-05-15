#!/usr/bin/env python3
import json
import duckdb
import argparse
import sys
import os
import logging
from typing import Dict, List, Any

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def load_carrier_selections() -> Dict[str, bool]:
    """Load carrier selections from JSON file."""
    try:
        with open('carrier_selections.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("carrier_selections.json not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error("Invalid JSON in carrier_selections.json.")
        sys.exit(1)

def update_carrier_status(db_path: str, dry_run: bool = False):
    """Update carrier status in the database based on carrier_selections.json."""
    # Load carrier selections
    selections = load_carrier_selections()
    
    if dry_run:
        logging.info(f"DRY RUN MODE - Would update {len(selections)} carriers")
    
    try:
        # Connect to database
        conn = duckdb.connect(db_path)
        
        # Get existing carriers from database
        existing_carriers = conn.execute(
            "SELECT naic FROM carrier_info"
        ).fetchall()
        existing_naics = {row[0] for row in existing_carriers}
        
        # Log info
        logging.info(f"Found {len(existing_naics)} carriers in database")
        
        # Track changes
        to_update = []
        to_insert = []
        
        # Prepare statements
        for naic, selected in selections.items():
            if naic in existing_naics:
                to_update.append((1 if selected else 0, naic))
            else:
                # New carrier to add - fetch company name via API if possible
                # For now, just use placeholder
                to_insert.append((naic, f"Carrier {naic}", 1 if selected else 0))
        
        # Log changes
        logging.info(f"Will update {len(to_update)} existing carriers")
        logging.info(f"Will insert {len(to_insert)} new carriers")
        
        if not dry_run:
            # Begin transaction
            conn.execute("BEGIN TRANSACTION")
            
            # Update existing carriers
            if to_update:
                conn.executemany(
                    "UPDATE carrier_info SET selected = ? WHERE naic = ?",
                    to_update
                )
            
            # Insert new carriers
            if to_insert:
                conn.executemany(
                    "INSERT INTO carrier_info (naic, company_name, selected) VALUES (?, ?, ?)",
                    to_insert
                )
            
            # Commit changes
            conn.execute("COMMIT")
            logging.info("Database updated successfully")
            
            # Verify changes
            active_count = conn.execute(
                "SELECT COUNT(*) FROM carrier_info WHERE selected = 1"
            ).fetchone()[0]
            inactive_count = conn.execute(
                "SELECT COUNT(*) FROM carrier_info WHERE selected = 0"
            ).fetchone()[0]
            logging.info(f"Active carriers: {active_count}, Inactive carriers: {inactive_count}")
        
        # Close connection
        conn.close()
        
    except Exception as e:
        logging.error(f"Error updating database: {str(e)}")
        if 'conn' in locals():
            try:
                conn.execute("ROLLBACK")
                conn.close()
            except:
                pass
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Update carrier status in the database based on carrier_selections.json")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="Path to DuckDB database file")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without making them")
    
    args = parser.parse_args()
    setup_logging()
    
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} not found.")
        sys.exit(1)
    
    update_carrier_status(args.db, args.dry_run)

if __name__ == "__main__":
    main()