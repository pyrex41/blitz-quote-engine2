#!/usr/bin/env python3
import argparse
import csv
import duckdb
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Set, Tuple

def setup_logging(quiet: bool = False) -> None:
    """Set up logging to file and console."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f'get_carriers_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

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

def get_carrier_name_map(conn) -> Dict[str, str]:
    """Get a mapping of NAIC to carrier name."""
    try:
        result = conn.execute("""
            SELECT naic, 
                   COALESCE(name_full, company_name, 'Unknown') as name
            FROM carrier_info
        """).fetchall()
        
        return {row[0]: row[1] for row in result}
    except Exception as e:
        logging.error(f"Error getting carrier names: {e}")
        return {}

def get_carriers_by_state(db_path: str, states: List[str] = None) -> Dict[str, Dict[str, Set[str]]]:
    """
    Get carriers supported in each state.
    Returns a dictionary of states to carrier info.
    """
    if states is None:
        states = get_all_states()
    
    result = {}
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Get carrier name mapping
        carrier_names = get_carrier_name_map(conn)
        
        # Check if there's data in the rate_store table
        has_data = conn.execute("SELECT COUNT(*) FROM rate_store LIMIT 1").fetchone()[0] > 0
        
        if not has_data:
            logging.warning("No rate data found in database. Results may be incomplete.")
        
        # Process each state
        for state in states:
            try:
                # Get carriers with rate data for this state
                carriers_with_rates = conn.execute("""
                    SELECT DISTINCT naic
                    FROM rate_store
                    WHERE state = ?
                """, [state]).fetchall()
                
                state_carriers = {row[0] for row in carriers_with_rates}
                
                # Get carriers with regions but no rates
                carriers_with_regions = conn.execute("""
                    SELECT DISTINCT rr.naic
                    FROM rate_regions rr
                    LEFT JOIN rate_store rs ON rr.naic = rs.naic AND rr.state = rs.state
                    WHERE rr.state = ? AND rs.naic IS NULL
                """, [state]).fetchall()
                
                state_carriers_no_rates = {row[0] for row in carriers_with_regions}
                
                # Store results
                result[state] = {
                    'with_rates': state_carriers,
                    'without_rates': state_carriers_no_rates
                }
                
                logging.info(f"State {state}: Found {len(state_carriers)} carriers with rates, " +
                             f"{len(state_carriers_no_rates)} with regions but no rates")
                
            except Exception as e:
                logging.error(f"Error processing state {state}: {e}")
                result[state] = {'with_rates': set(), 'without_rates': set()}
        
        conn.close()
        
        return result, carrier_names
        
    except Exception as e:
        logging.error(f"Error accessing database: {e}")
        sys.exit(1)

def write_csv(results: Dict[str, Dict[str, Set[str]]], carrier_names: Dict[str, str], 
              output_path: str, selected_only: bool = False):
    """Write results to CSV file."""
    try:
        # Get list of all carriers across all states
        all_carriers = set()
        for state_data in results.values():
            all_carriers.update(state_data['with_rates'])
            if not selected_only:
                all_carriers.update(state_data['without_rates'])
        
        # Sort carriers by name
        sorted_carriers = sorted(all_carriers, key=lambda naic: carrier_names.get(naic, f"Unknown {naic}"))
        
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write first header row with carrier names
            header_names = ['State']
            for naic in sorted_carriers:
                name = carrier_names.get(naic, f"Unknown")
                header_names.append(name)
            writer.writerow(header_names)
            
            # Write second header row with NAIC codes
            header_naics = ['']
            for naic in sorted_carriers:
                header_naics.append(naic)
            writer.writerow(header_naics)
            
            # Write data for each state
            for state in sorted(results.keys()):
                row = [state]
                
                for naic in sorted_carriers:
                    if naic in results[state]['with_rates']:
                        row.append('1')  # Has rates
                    elif not selected_only and naic in results[state]['without_rates']:
                        row.append('0')  # Has regions but no rates
                    else:
                        row.append('')   # Not supported
                
                writer.writerow(row)
                
        logging.info(f"CSV file written to {output_path}")
        
    except Exception as e:
        logging.error(f"Error writing CSV file: {e}")

def main():
    parser = argparse.ArgumentParser(description="Get supported carriers by state and output to CSV")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="Path to DuckDB database file")
    parser.add_argument("-o", "--output", type=str, default=f"carrier_by_state_{datetime.now().strftime('%Y%m%d')}.csv", 
                      help="Output CSV file path")
    parser.add_argument("--states", nargs="+", help="List of states to process (e.g., TX CA)")
    parser.add_argument("--selected-only", action="store_true", help="Only include selected carriers")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} not found.")
        sys.exit(1)
    
    # Get carriers by state
    results, carrier_names = get_carriers_by_state(args.db, args.states)
    
    # If selected-only, filter the carrier names
    if args.selected_only:
        try:
            conn = duckdb.connect(args.db, read_only=True)
            selected_naics = conn.execute("SELECT naic FROM carrier_info WHERE selected = 1").fetchall()
            selected_naics = {row[0] for row in selected_naics}
            
            # Filter carrier_names to include only selected carriers
            carrier_names = {naic: name for naic, name in carrier_names.items() if naic in selected_naics}
            
            # Filter results to include only selected carriers
            for state in results:
                results[state]['with_rates'] = {naic for naic in results[state]['with_rates'] if naic in selected_naics}
                results[state]['without_rates'] = {naic for naic in results[state]['without_rates'] if naic in selected_naics}
            
            conn.close()
        except Exception as e:
            logging.error(f"Error filtering selected carriers: {e}")
    
    # Write results to CSV
    write_csv(results, carrier_names, args.output, args.selected_only)
    
    if not args.quiet:
        print(f"\nCarrier data written to {args.output}")
        print(f"Format: 1 = Carrier has rates, 0 = Carrier has regions but no rates, blank = No support")

if __name__ == "__main__":
    main()