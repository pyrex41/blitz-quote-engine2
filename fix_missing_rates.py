#!/usr/bin/env python3
import argparse
import logging
import sys
import os
from datetime import datetime
import duckdb
import asyncio
from async_csg import AsyncCSGRequest as csg
from config import Config
from filter_utils import filter_quote

def setup_logging():
    """Set up logging to file and console."""
    log_filename = f'fix_missing_rates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

def check_schema(conn):
    """Check if the rate_store table schema allows multiple effective dates."""
    try:
        result = conn.execute("""
            PRAGMA table_info(rate_store)
        """).fetchall()
        
        primary_key_columns = conn.execute("""
            SELECT 
              s.name as table_name, 
              p.name as column_name 
            FROM 
              sqlite_master s 
            JOIN 
              pragma_table_info(s.name) p 
            WHERE 
              s.name = 'rate_store' 
              AND p.pk > 0
            ORDER BY 
              p.pk
        """).fetchall()
        
        print("Table Schema:")
        for row in result:
            print(f"{row[1]} ({row[2]})")
        
        print("\nPrimary Key Columns:")
        for row in primary_key_columns:
            print(row[1])
            
        return "effective_date" in [row[1] for row in primary_key_columns]
        
    except Exception as e:
        logging.error(f"Error checking schema: {e}")
        return False

async def fetch_rates_for_comparison(zip_code, naic, effective_date):
    """Fetch rates from the API for comparison."""
    print(f"\nFetching rates for {zip_code}, {naic}, {effective_date}...")
    
    # Initialize API client
    cr = csg(Config.API_KEY)
    await cr.async_init()
    await cr.fetch_token()
    
    # Sample demographic combinations
    params = {
        "zip5": zip_code,
        "age": 65,
        "gender": "M",
        "tobacco": 0,
        "naic": naic,
        "plan": "G",
        "effective_date": effective_date
    }
    
    response = await cr.fetch_quote(**params)
    
    # Filter to matching quotes
    quotes = [q for q in response if q.get('company_base', {}).get('naic') == naic]
    if not quotes:
        print(f"No quotes found for {naic} at {effective_date}")
        return None
    
    # Process quote
    filtered = filter_quote(quotes[0])
    if not filtered:
        print(f"Could not filter quote for {naic} at {effective_date}")
        return None
    
    print(f"Found rate: ${filtered.get('rate')} for {effective_date}")
    return filtered

def list_existing_rates(conn, zip_code, naic):
    """List existing rates for the specified carrier and ZIP code."""
    try:
        result = conn.execute("""
            SELECT 
                r.naic, 
                c.company_name, 
                r.effective_date, 
                r.gender, 
                r.tobacco, 
                r.age, 
                r.plan, 
                r.rate, 
                r.discount_rate,
                r.region_id
            FROM rate_store r
            JOIN region_mapping m ON r.region_id = m.region_id AND r.naic = m.naic
            LEFT JOIN carrier_info c ON r.naic = c.naic
            WHERE m.zip_code = ? AND r.naic = ?
            ORDER BY r.effective_date, r.age, r.gender, r.tobacco, r.plan
        """, (zip_code, naic)).fetchall()
        
        print(f"\nExisting rates for {naic} at ZIP {zip_code}:")
        print(f"{'Date':<15} {'Age':<5} {'Gender':<8} {'Tobacco':<8} {'Plan':<5} {'Rate':<10} {'Discount':<10}")
        print("-" * 70)
        
        total_rates = 0
        region_ids = set()
        for row in result:
            region_ids.add(row[9])
            print(f"{row[2][:10]:<15} {row[5]:<5} {row[3]:<8} {row[4]:<8} {row[6]:<5} ${row[7]:<9.2f} ${row[8]:<9.2f}")
            total_rates += 1
        
        print(f"\nTotal: {total_rates} rate records in {len(region_ids)} regions")
        return region_ids, result
        
    except Exception as e:
        logging.error(f"Error listing rates: {e}")
        return set(), []

async def add_missing_rates(conn, zip_code, naic, from_date, to_date, api_check=True):
    """Add missing rates for specified carrier and ZIP code."""
    # Check if we need to verify rates with API
    rate_adjustment = 1.0  # Default no adjustment
    
    if api_check:
        # Fetch both sets of rates from API for comparison
        from_rate_data = await fetch_rates_for_comparison(zip_code, naic, from_date)
        to_rate_data = await fetch_rates_for_comparison(zip_code, naic, to_date)
        
        if from_rate_data and to_rate_data:
            # Calculate adjustment factor
            from_rate = from_rate_data.get('rate', 0)
            to_rate = to_rate_data.get('rate', 0)
            
            if from_rate > 0 and to_rate > 0:
                rate_adjustment = from_rate / to_rate
                print(f"\nRate adjustment factor: {rate_adjustment:.4f} ({from_rate:.2f} / {to_rate:.2f})")
    
    # Find existing regions and rates
    region_ids, existing_rates = list_existing_rates(conn, zip_code, naic)
    
    if not region_ids:
        print(f"No region_ids found for {naic} at ZIP {zip_code}")
        return False
    
    # Get records for the to_date
    to_date_records = [r for r in existing_rates if r[2].startswith(to_date)]
    
    if not to_date_records:
        print(f"No records found for {to_date}")
        return False
    
    # Group by region_id
    records_by_region = {}
    for record in to_date_records:
        region_id = record[9]
        if region_id not in records_by_region:
            records_by_region[region_id] = []
        records_by_region[region_id].append(record)
    
    # Insert new records with from_date
    print(f"\nAdding {from_date} rates based on {to_date} rates...")
    
    total_added = 0
    conn.execute("BEGIN TRANSACTION")
    
    try:
        for region_id, records in records_by_region.items():
            for record in records:
                # Adjust rate and discount_rate
                new_rate = record[7] * rate_adjustment
                new_discount_rate = record[8] * rate_adjustment
                
                # Insert new record with from_date
                conn.execute("""
                    INSERT OR IGNORE INTO rate_store
                    (region_id, gender, tobacco, age, naic, plan, rate, discount_rate, effective_date, state)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    region_id, record[3], record[4], record[5], 
                    record[0], record[6], new_rate, new_discount_rate, 
                    from_date, record[0][:2]  # Using first 2 chars of NAIC as state (just a placeholder)
                ))
                total_added += 1
        
        # Mark as processed in processed_data
        conn.execute("""
            INSERT OR IGNORE INTO processed_data 
            (state, naic, effective_date, api_effective_date, processed_at, success)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('MO', naic, from_date, from_date, datetime.now(), True))
        
        conn.execute("COMMIT")
        print(f"Successfully added {total_added} records for {from_date}")
        return True
        
    except Exception as e:
        conn.execute("ROLLBACK")
        logging.error(f"Error adding rates: {e}")
        return False

async def main():
    parser = argparse.ArgumentParser(description="Fix missing rates by adding records for a specific effective date")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="DuckDB database file path")
    parser.add_argument("--zip", type=str, default="64105", help="ZIP code to use for fixing rates")
    parser.add_argument("--naic", type=str, default="82538", help="NAIC code to fix")
    parser.add_argument("--from-date", type=str, default="2025-05-01", help="Date to add (missing date)")
    parser.add_argument("--to-date", type=str, default="2025-06-01", help="Reference date (existing date)")
    parser.add_argument("--no-api-check", action="store_true", help="Skip API check and use same rates")
    
    args = parser.parse_args()
    setup_logging()
    
    # Validate database exists
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} does not exist.")
        sys.exit(1)
    
    print(f"Using database: {args.db}")
    conn = duckdb.connect(args.db)
    
    try:
        # Check schema
        has_effective_date_pk = check_schema(conn)
        if not has_effective_date_pk:
            print("\nWARNING: effective_date may not be part of the primary key!")
        
        # Add missing rates
        success = await add_missing_rates(
            conn, 
            args.zip, 
            args.naic, 
            args.from_date, 
            args.to_date,
            not args.no_api_check
        )
        
        # List updated rates
        if success:
            list_existing_rates(conn, args.zip, args.naic)
            
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(main()) 