#!/usr/bin/env python3
import argparse
import duckdb
import logging
import requests
import sys
from typing import Dict, Optional

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def fetch_carrier_info(naic: str) -> Optional[Dict]:
    """Try to fetch carrier information from CSG API."""
    try:
        url = "https://csgapi.appspot.com/v1/med_supp/open/companies.json"
        response = requests.get(url)
        
        if response.status_code == 200:
            carriers = response.json()
            for carrier in carriers:
                if carrier.get('naic') == naic:
                    return carrier
        
        return None
    except Exception as e:
        logging.error(f"Error fetching carrier info: {e}")
        return None

def add_carrier(db_path: str, naic: str, company_name: Optional[str] = None, 
                selected: bool = True, discount_category: Optional[str] = None,
                dry_run: bool = False):
    """Add or update a carrier in the database."""
    # Try to get carrier info from API if no company name provided
    if not company_name:
        carrier_info = fetch_carrier_info(naic)
        if carrier_info:
            company_name = carrier_info.get('name_full', f"Carrier {naic}")
            logging.info(f"Found carrier info: {company_name}")
        else:
            company_name = f"Carrier {naic}"
            logging.warning(f"Couldn't find carrier info for NAIC {naic}, using default name")
    
    if dry_run:
        logging.info(f"DRY RUN: Would add/update carrier {naic} ({company_name}) with selected={selected}")
        return
    
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
        
        # Add or update carrier
        conn.execute("""
            INSERT INTO carrier_info (naic, company_name, selected, discount_category)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (naic) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                selected = EXCLUDED.selected,
                discount_category = EXCLUDED.discount_category
        """, [naic, company_name, 1 if selected else 0, discount_category])
        
        # Also update carrier_selections.json file for backward compatibility
        try:
            import json
            import os
            
            selections = {}
            if os.path.exists('carrier_selections.json'):
                with open('carrier_selections.json', 'r') as f:
                    selections = json.load(f)
            
            selections[naic] = selected
            
            with open('carrier_selections.json', 'w') as f:
                json.dump(selections, f, indent=2)
                
            logging.info(f"Updated carrier_selections.json with NAIC {naic}")
        except Exception as e:
            logging.error(f"Error updating carrier_selections.json: {e}")
        
        # Verify the carrier was added
        result = conn.execute(
            "SELECT naic, company_name, selected, discount_category FROM carrier_info WHERE naic = ?",
            [naic]
        ).fetchone()
        
        if result:
            logging.info(f"Successfully added/updated carrier: {result}")
        else:
            logging.error(f"Failed to add carrier {naic}")
        
        # Close connection
        conn.close()
        
    except Exception as e:
        logging.error(f"Error adding carrier to database: {e}")
        sys.exit(1)

def list_carriers(db_path: str):
    """List all carriers in the database."""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Get all carriers
        carriers = conn.execute("""
            SELECT naic, company_name, selected, discount_category
            FROM carrier_info
            ORDER BY selected DESC, company_name
        """).fetchall()
        
        # Print carrier information
        print("\nCarriers in database:")
        print("=" * 80)
        print(f"{'NAIC':<8} {'Selected':<10} {'Discount Category':<20} {'Company Name'}")
        print("-" * 80)
        
        for carrier in carriers:
            naic, name, selected, discount = carrier
            print(f"{naic:<8} {'✓' if selected else '×':<10} {(discount or ''):<20} {name}")
        
        print("-" * 80)
        print(f"Total: {len(carriers)} carriers, {sum(1 for c in carriers if c[2])} selected")
        
        conn.close()
        
    except Exception as e:
        logging.error(f"Error listing carriers: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Add or update a carrier in the Medicare Supplement database")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="Path to DuckDB database file")
    parser.add_argument("-n", "--naic", type=str, help="NAIC code of the carrier to add/update")
    parser.add_argument("-c", "--company", type=str, help="Company name (optional, will try to fetch if not provided)")
    parser.add_argument("-s", "--selected", action="store_true", default=True, help="Mark carrier as selected")
    parser.add_argument("-u", "--unselected", action="store_true", help="Mark carrier as not selected")
    parser.add_argument("--discount", type=str, help="Discount category")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without making them")
    parser.add_argument("--list", action="store_true", help="List all carriers in the database")
    
    args = parser.parse_args()
    setup_logging()
    
    # Check if database exists
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} not found.")
        sys.exit(1)
    
    # List carriers if requested
    if args.list:
        list_carriers(args.db)
        return
    
    # Check if NAIC is provided
    if not args.naic:
        logging.error("NAIC code is required. Use --naic to specify the carrier.")
        sys.exit(1)
    
    # Set selected status (--unselected overrides --selected)
    selected = not args.unselected if args.unselected else args.selected
    
    # Add carrier
    add_carrier(
        args.db, 
        args.naic,
        args.company,
        selected,
        args.discount,
        args.dry_run
    )

if __name__ == "__main__":
    import os
    main()