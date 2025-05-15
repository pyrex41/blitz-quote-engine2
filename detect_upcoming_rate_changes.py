#!/usr/bin/env python3
import argparse
import csv
import duckdb
import logging
import os
import random
import sys
import json
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple

def setup_logging(quiet: bool) -> None:
    """Set up logging to file and console."""
    log_filename = f'rate_changes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

def calculate_date_range() -> Tuple[str, str]:
    """Calculate current date and 6 months ahead date."""
    today = datetime.now()
    current_date = today.strftime("%Y-%m-%d")
    
    # Calculate date 6 months ahead
    future_date = today + timedelta(days=180)
    future_date_str = future_date.strftime("%Y-%m-%d")
    
    return current_date, future_date_str

def get_all_states(conn) -> List[str]:
    """Get all states from the database."""
    result = conn.execute("""
        SELECT DISTINCT state 
        FROM rate_regions
        ORDER BY state
    """).fetchall()
    return [row[0] for row in result]

def get_all_carriers(conn) -> List[Tuple[str, str]]:
    """Get all carriers from the database."""
    result = conn.execute("""
        SELECT naic, company_name
        FROM carrier_info
        ORDER BY company_name
    """).fetchall()
    return [(row[0], row[1]) for row in result]

def get_regions_for_carrier_state(conn, state: str, carrier_naic: str) -> List[str]:
    """Get all regions for a specific carrier in a state."""
    result = conn.execute("""
        SELECT DISTINCT region_id
        FROM rate_regions
        WHERE state = ? AND naic = ?
        ORDER BY region_id
    """, (state, carrier_naic)).fetchall()
    
    logging.info(f"Found {len(result)} regions for carrier {carrier_naic} in state {state}")
    
    return [row[0] for row in result]

def sample_zips_from_region(conn, region: str) -> str:
    """Sample a single ZIP code from a region."""
    # Get all ZIP codes for this region
    result = conn.execute("""
        SELECT zip_code
        FROM region_mapping
        WHERE region_id = ?
        ORDER BY zip_code
        LIMIT 1
    """, (region,)).fetchall()
    
    if not result:
        logging.warning(f"No ZIP codes found for region {region}")
        return None
    
    return result[0][0]

def get_current_and_future_rates(conn, region_id: str, carrier_naic: str, current_date: str, future_date: str) -> Tuple[Dict, Dict]:
    """Get current and future rates for a region and carrier."""
    # Standard parameters
    age = 65
    gender = "M"
    tobacco = 0
    
    # Get state info for this region
    state_info = conn.execute("""
        SELECT state FROM rate_regions WHERE region_id = ? LIMIT 1
    """, (region_id,)).fetchone()
    
    if not state_info:
        return None, None
    
    state = state_info[0]
    
    # Determine plan based on state
    plan = "G"
    if state == 'MA':
        plan = 'MA_CORE'
    elif state == 'MN':
        plan = 'MN_BASIC'
    elif state == 'WI':
        plan = 'WIR_A50%'
    
    # Get current rate
    current_rate_query = """
        WITH latest_effective AS (
            SELECT MAX(effective_date) as max_date
            FROM rate_store
            WHERE region_id = ?
                AND naic = ?
                AND plan = ?
                AND gender = ?
                AND tobacco = ?
                AND age = ?
                AND effective_date <= ?
        )
        SELECT rate, discount_rate, effective_date
        FROM rate_store
        JOIN latest_effective ON effective_date = max_date
        WHERE region_id = ?
            AND naic = ?
            AND plan = ?
            AND gender = ?
            AND tobacco = ?
            AND age = ?
        LIMIT 1
    """
    
    current_rate_result = conn.execute(
        current_rate_query,
        (region_id, carrier_naic, plan, gender, tobacco, age, current_date, 
         region_id, carrier_naic, plan, gender, tobacco, age)
    ).fetchone()
    
    # Get future rate (if any)
    future_rate_query = """
        WITH future_effective AS (
            SELECT MIN(effective_date) as min_date
            FROM rate_store
            WHERE region_id = ?
                AND naic = ?
                AND plan = ?
                AND gender = ?
                AND tobacco = ?
                AND age = ?
                AND effective_date > ? 
                AND effective_date <= ?
        )
        SELECT rate, discount_rate, effective_date
        FROM rate_store
        JOIN future_effective ON effective_date = min_date
        WHERE region_id = ?
            AND naic = ?
            AND plan = ?
            AND gender = ?
            AND tobacco = ?
            AND age = ?
        LIMIT 1
    """
    
    future_rate_result = conn.execute(
        future_rate_query,
        (region_id, carrier_naic, plan, gender, tobacco, age, current_date, future_date,
         region_id, carrier_naic, plan, gender, tobacco, age)
    ).fetchone()
    
    current_rate = None
    if current_rate_result:
        current_rate = {
            'rate': current_rate_result[0],
            'discount_rate': current_rate_result[1],
            'effective_date': current_rate_result[2]
        }
    
    future_rate = None
    if future_rate_result:
        future_rate = {
            'rate': future_rate_result[0],
            'discount_rate': future_rate_result[1],
            'effective_date': future_rate_result[2]
        }
    
    return current_rate, future_rate

def create_report(results: List[Dict], output_file: str) -> None:
    """Create a CSV report of rate changes."""
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['State', 'Carrier Name', 'NAIC', 'Region', 
                      'ZIP Code', 'Current Rate', 'Current Effective Date', 
                      'Future Rate', 'Future Effective Date', 
                      'Change Amount', 'Change Percent']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    
    # Calculate statistics for logging
    states = set(result['State'] for result in results)
    carriers = set((result['NAIC'], result['Carrier Name']) for result in results)
    regions = set(result['Region'] for result in results)
    
    logging.info(f"Report created: {output_file}")
    logging.info(f"Report includes {len(results)} rate changes across {len(regions)} unique regions")
    logging.info(f"Changes found in {len(states)} states for {len(carriers)} carriers")

def create_markdown_report(results: List[Dict], output_file: str) -> None:
    """Create a compact markdown report of rate changes organized by date, carrier, and state."""
    # Group data by effective date
    date_groups = {}
    for result in results:
        future_date = datetime.strptime(result['Future Effective Date'].split('T')[0], '%Y-%m-%d')
        date_key = future_date.strftime('%B %Y')  # e.g., "June 2025"
        month_year = future_date.strftime('%m/%Y')
        
        if date_key not in date_groups:
            date_groups[date_key] = []
        
        date_groups[date_key].append(result)
    
    # Sort dates chronologically
    sorted_dates = sorted(date_groups.keys(), key=lambda x: datetime.strptime(x, '%B %Y'))
    
    # Get pricing parameters used (they're consistent across runs)
    sample_result = results[0] if results else None
    age = 65  # From get_current_and_future_rates()
    gender = "M"  # From get_current_and_future_rates()
    tobacco = 0  # From get_current_and_future_rates()
    
    # Open file for writing
    with open(output_file, 'w') as md_file:
        md_file.write("# Medicare Rate Changes Report\n\n")
        
        # Add pricing parameters info
        md_file.write("**Pricing Parameters Used:** ")
        md_file.write(f"Age: {age}, Gender: {gender}, Tobacco Status: {'Yes' if tobacco else 'No'}\n\n")
        
        # Add style for PDF formatting with landscape orientation
        md_file.write("<style>\n")
        md_file.write("@page { size: landscape; margin: 0.5in; }\n")  # Set landscape orientation
        md_file.write("body { margin: 0.5in; }\n")
        md_file.write("table { font-size: 12px; border-collapse: collapse; width: 100%; margin-bottom: 20px; }\n")
        md_file.write("th, td { border: 1px solid #ddd; padding: 4px; }\n")
        md_file.write("tr:nth-child(even) { background-color: #f2f2f2; }\n")
        md_file.write("th { background-color: #4CAF50; color: white; text-align: left; }\n")
        md_file.write(".carrier-row { background-color: #e6f2ff; font-weight: bold; }\n")
        md_file.write(".state-row { background-color: #f2f2f2; font-style: italic; }\n")
        md_file.write(".state-spacer { height: 15px; background-color: white; border: none; }\n")
        md_file.write("@media print { body { font-size: 11pt; } h2 { page-break-before: always; } }\n")
        md_file.write("</style>\n\n")
        
        # Process each date group
        for date_key in sorted_dates:
            md_file.write(f"## Rate Changes Effective {date_key}\n\n")
            
            # Group by carrier within this date
            carrier_groups = {}
            for result in date_groups[date_key]:
                carrier_key = f"{result['Carrier Name']} ({result['NAIC']})"
                
                if carrier_key not in carrier_groups:
                    carrier_groups[carrier_key] = []
                
                carrier_groups[carrier_key].append(result)
            
            # Sort carriers alphabetically
            sorted_carriers = sorted(carrier_groups.keys())
            
            # Create a table for this date
            md_file.write("| Carrier | State | Zip | Current Rate | Future Rate | Change | % |\n")
            md_file.write("|---------|-------|-----|--------------|-------------|--------|---|\n")
            
            # Process carriers within this date
            for carrier_key in sorted_carriers:
                # Group by state
                state_groups = {}
                for result in carrier_groups[carrier_key]:
                    state_key = result['State']
                    
                    if state_key not in state_groups:
                        state_groups[state_key] = []
                    
                    state_groups[state_key].append(result)
                
                # Sort states alphabetically
                sorted_states = sorted(state_groups.keys())
                
                # Calculate carrier average for this date
                carrier_change_percents = [float(result['Change Percent'].strip('%')) for result in carrier_groups[carrier_key]]
                carrier_avg_change = sum(carrier_change_percents) / len(carrier_change_percents)
                
                # Add carrier row
                md_file.write(f"| **{carrier_key}** | | | | | | **{carrier_avg_change:.2f}%** |\n")
                
                # Process states within this carrier
                for i, state_key in enumerate(sorted_states):
                    # Add spacing between states (except for the first state after carrier)
                    if i > 0:
                        md_file.write(f"| | | | | | | |\n")
                    
                    # Calculate state average
                    state_change_percents = [float(result['Change Percent'].strip('%')) for result in state_groups[state_key]]
                    state_avg_change = sum(state_change_percents) / len(state_change_percents)
                    
                    # Add state row
                    md_file.write(f"| | **{state_key}** | | | | | **{state_avg_change:.2f}%** |\n")
                    
                    # Sort results by ZIP code
                    sorted_results = sorted(state_groups[state_key], key=lambda x: x['ZIP Code'])
                    
                    # Add rows for each ZIP in state
                    for result in sorted_results:
                        md_file.write(f"| | | {result['ZIP Code']} | {result['Current Rate']} | {result['Future Rate']} | {result['Change Amount']} | {result['Change Percent']} |\n")
                
                # Add spacing after each carrier
                if len(sorted_states) > 0:
                    md_file.write(f"| | | | | | | |\n")
            
            # Calculate date average
            date_change_percents = [float(result['Change Percent'].strip('%')) for result in date_groups[date_key]]
            date_avg_change = sum(date_change_percents) / len(date_change_percents)
            md_file.write(f"\n**Average Rate Change for {date_key}: {date_avg_change:.2f}%**\n\n")
        
        # Add summary section
        md_file.write("## Summary\n\n")
        
        # Calculate overall statistics
        all_change_percents = [float(result['Change Percent'].strip('%')) for result in results]
        overall_avg = sum(all_change_percents) / len(all_change_percents)
        
        md_file.write(f"**Overall Average Rate Change: {overall_avg:.2f}%**\n")
        md_file.write(f"**Total Regions with Changes: {len(set(result['Region'] for result in results))}**\n")
        md_file.write(f"**Total States: {len(set(result['State'] for result in results))}**\n")
        md_file.write(f"**Total Carriers: {len(set(result['Carrier Name'] for result in results))}**\n")
        
        # Add script for auto PDF conversion
        md_file.write("\n<script>\n")
        md_file.write("window.onload = function() {\n")
        md_file.write("  if (window.location.protocol !== 'file:') {\n")
        md_file.write("    window.print();\n")
        md_file.write("  }\n")
        md_file.write("};\n")
        md_file.write("</script>\n")
    
    logging.info(f"Markdown report created: {output_file}")
    
    # Generate PDF filename from markdown filename
    pdf_output = output_file.replace('.md', '.pdf')
    
    try:
        # Try to find pandoc for PDF conversion
        pandoc_check = os.system("which pandoc > /dev/null 2>&1")
        wkhtmltopdf_check = os.system("which wkhtmltopdf > /dev/null 2>&1")
        
        if pandoc_check == 0:
            logging.info(f"Converting markdown to PDF using pandoc...")
            os.system(f"pandoc {output_file} -o {pdf_output} -V geometry:landscape")
            logging.info(f"PDF report created: {pdf_output}")
        elif wkhtmltopdf_check == 0:
            logging.info(f"Converting markdown to PDF using wkhtmltopdf...")
            os.system(f"wkhtmltopdf --orientation Landscape {output_file} {pdf_output}")
            logging.info(f"PDF report created: {pdf_output}")
        else:
            logging.info(f"PDF conversion tools not found. Opening markdown file in browser for printing.")
            if sys.platform == 'darwin':  # macOS
                os.system(f"open {output_file}")
            elif sys.platform == 'win32':  # Windows
                os.system(f"start {output_file}")
            else:  # Linux
                os.system(f"xdg-open {output_file}")
    except Exception as e:
        logging.error(f"Error converting to PDF: {e}")
        logging.info("Please open the markdown file in a browser and use print to save as PDF.")

def create_state_markdown_report(results: List[Dict], output_file: str) -> None:
    """Create a compact markdown report of rate changes organized by date, state, and carrier."""
    # Group data by effective date
    date_groups = {}
    for result in results:
        future_date = datetime.strptime(result['Future Effective Date'].split('T')[0], '%Y-%m-%d')
        date_key = future_date.strftime('%B %Y')  # e.g., "June 2025"
        month_year = future_date.strftime('%m/%Y')
        
        if date_key not in date_groups:
            date_groups[date_key] = []
        
        date_groups[date_key].append(result)
    
    # Sort dates chronologically
    sorted_dates = sorted(date_groups.keys(), key=lambda x: datetime.strptime(x, '%B %Y'))
    
    # Get pricing parameters used (they're consistent across runs)
    sample_result = results[0] if results else None
    age = 65  # From get_current_and_future_rates()
    gender = "M"  # From get_current_and_future_rates()
    tobacco = 0  # From get_current_and_future_rates()
    
    # Open file for writing
    with open(output_file, 'w') as md_file:
        md_file.write("# Medicare Rate Changes Report (State-Centric View)\n\n")
        
        # Add pricing parameters info
        md_file.write("**Pricing Parameters Used:** ")
        md_file.write(f"Age: {age}, Gender: {gender}, Tobacco Status: {'Yes' if tobacco else 'No'}\n\n")
        
        # Add style for PDF formatting with landscape orientation
        md_file.write("<style>\n")
        md_file.write("@page { size: landscape; margin: 0.5in; }\n")  # Set landscape orientation
        md_file.write("body { margin: 0.5in; }\n")
        md_file.write("table { font-size: 12px; border-collapse: collapse; width: 100%; margin-bottom: 20px; }\n")
        md_file.write("th, td { border: 1px solid #ddd; padding: 4px; }\n")
        md_file.write("tr:nth-child(even) { background-color: #f2f2f2; }\n")
        md_file.write("th { background-color: #4CAF50; color: white; text-align: left; }\n")
        md_file.write(".state-row { background-color: #e6f2ff; font-weight: bold; }\n")
        md_file.write(".carrier-row { background-color: #f2f2f2; font-style: italic; }\n")
        md_file.write(".spacer { height: 15px; background-color: white; border: none; }\n")
        md_file.write("@media print { body { font-size: 11pt; } h2 { page-break-before: always; } }\n")
        md_file.write("</style>\n\n")
        
        # Process each date group
        for date_key in sorted_dates:
            md_file.write(f"## Rate Changes Effective {date_key}\n\n")
            
            # Group by state within this date
            state_groups = {}
            for result in date_groups[date_key]:
                state_key = result['State']
                
                if state_key not in state_groups:
                    state_groups[state_key] = []
                
                state_groups[state_key].append(result)
            
            # Sort states alphabetically
            sorted_states = sorted(state_groups.keys())
            
            # Create a table for this date
            md_file.write("| State | Carrier | Zip | Current Rate | Future Rate | Change | % |\n")
            md_file.write("|-------|---------|-----|--------------|-------------|--------|---|\n")
            
            # Process states within this date
            for state_key in sorted_states:
                # Group by carrier
                carrier_groups = {}
                for result in state_groups[state_key]:
                    carrier_key = f"{result['Carrier Name']} ({result['NAIC']})"
                    
                    if carrier_key not in carrier_groups:
                        carrier_groups[carrier_key] = []
                    
                    carrier_groups[carrier_key].append(result)
                
                # Sort carriers alphabetically
                sorted_carriers = sorted(carrier_groups.keys())
                
                # Calculate state average for this date
                state_change_percents = [float(result['Change Percent'].strip('%')) for result in state_groups[state_key]]
                state_avg_change = sum(state_change_percents) / len(state_change_percents)
                
                # Add state row
                md_file.write(f"| **{state_key}** | | | | | | **{state_avg_change:.2f}%** |\n")
                
                # Process carriers within this state
                for i, carrier_key in enumerate(sorted_carriers):
                    # Add spacing between carriers (except for the first carrier after state)
                    if i > 0:
                        md_file.write(f"| | | | | | | |\n")
                    
                    # Calculate carrier average for this state
                    carrier_change_percents = [float(result['Change Percent'].strip('%')) for result in carrier_groups[carrier_key]]
                    carrier_avg_change = sum(carrier_change_percents) / len(carrier_change_percents)
                    
                    # Add carrier row
                    md_file.write(f"| | **{carrier_key}** | | | | | **{carrier_avg_change:.2f}%** |\n")
                    
                    # Sort results by ZIP code
                    sorted_results = sorted(carrier_groups[carrier_key], key=lambda x: x['ZIP Code'])
                    
                    # Add rows for each ZIP for this carrier
                    for result in sorted_results:
                        md_file.write(f"| | | {result['ZIP Code']} | {result['Current Rate']} | {result['Future Rate']} | {result['Change Amount']} | {result['Change Percent']} |\n")
                
                # Add spacing after each state
                if len(sorted_carriers) > 0:
                    md_file.write(f"| | | | | | | |\n")
            
            # Calculate date average
            date_change_percents = [float(result['Change Percent'].strip('%')) for result in date_groups[date_key]]
            date_avg_change = sum(date_change_percents) / len(date_change_percents)
            md_file.write(f"\n**Average Rate Change for {date_key}: {date_avg_change:.2f}%**\n\n")
        
        # Add summary section
        md_file.write("## Summary\n\n")
        
        # Calculate overall statistics
        all_change_percents = [float(result['Change Percent'].strip('%')) for result in results]
        overall_avg = sum(all_change_percents) / len(all_change_percents)
        
        md_file.write(f"**Overall Average Rate Change: {overall_avg:.2f}%**\n")
        md_file.write(f"**Total Regions with Changes: {len(set(result['Region'] for result in results))}**\n")
        md_file.write(f"**Total States: {len(set(result['State'] for result in results))}**\n")
        md_file.write(f"**Total Carriers: {len(set(result['Carrier Name'] for result in results))}**\n")
        
        # Add script for auto PDF conversion
        md_file.write("\n<script>\n")
        md_file.write("window.onload = function() {\n")
        md_file.write("  if (window.location.protocol !== 'file:') {\n")
        md_file.write("    window.print();\n")
        md_file.write("  }\n")
        md_file.write("};\n")
        md_file.write("</script>\n")
    
    logging.info(f"State-centric markdown report created: {output_file}")
    
    # Generate PDF filename from markdown filename
    pdf_output = output_file.replace('.md', '.pdf')
    
    try:
        # Try to find pandoc for PDF conversion
        pandoc_check = os.system("which pandoc > /dev/null 2>&1")
        wkhtmltopdf_check = os.system("which wkhtmltopdf > /dev/null 2>&1")
        
        if pandoc_check == 0:
            logging.info(f"Converting markdown to PDF using pandoc...")
            os.system(f"pandoc {output_file} -o {pdf_output} -V geometry:landscape")
            logging.info(f"PDF report created: {pdf_output}")
        elif wkhtmltopdf_check == 0:
            logging.info(f"Converting markdown to PDF using wkhtmltopdf...")
            os.system(f"wkhtmltopdf --orientation Landscape {output_file} {pdf_output}")
            logging.info(f"PDF report created: {pdf_output}")
        else:
            logging.info(f"PDF conversion tools not found. Opening markdown file in browser for printing.")
            if sys.platform == 'darwin':  # macOS
                os.system(f"open {output_file}")
            elif sys.platform == 'win32':  # Windows
                os.system(f"start {output_file}")
            else:  # Linux
                os.system(f"xdg-open {output_file}")
    except Exception as e:
        logging.error(f"Error converting to PDF: {e}")
        logging.info("Please open the markdown file in a browser and use print to save as PDF.")

def main():
    parser = argparse.ArgumentParser(description="Detect upcoming Medicare rate changes")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="DuckDB database file path")
    parser.add_argument("-o", "--output", type=str, default=f"rate_changes_{datetime.now().strftime('%Y%m%d')}.csv", 
                        help="Output CSV file name")
    parser.add_argument("-s", "--states", nargs="+", help="List of states to analyze (e.g., TX CA)")
    parser.add_argument("-n", "--naics", nargs="+", help="List of NAIC codes to analyze")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("-m", "--markdown", type=str, help="Generate markdown report file name")
    parser.add_argument("--groupby", type=str, choices=["carrier", "state"], default="carrier",
                      help="Group the markdown report by carrier (default) or by state")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    # Validate database exists
    if not os.path.exists(args.db):
        logging.error(f"Database file {args.db} does not exist.")
        sys.exit(1)
    
    # Calculate date range
    current_date, future_date = calculate_date_range()
    logging.info(f"Checking for rate changes between {current_date} and {future_date}")
    
    # Connect to database
    conn = duckdb.connect(args.db)
    
    try:
        # Get states to analyze
        states_to_analyze = args.states
        if not states_to_analyze:
            states_to_analyze = get_all_states(conn)
            logging.info(f"Analyzing all {len(states_to_analyze)} states")
        
        # Get carriers to analyze
        carriers_to_analyze = []
        if args.naics:
            for naic in args.naics:
                carrier_name = conn.execute(
                    "SELECT company_name FROM carrier_info WHERE naic = ? LIMIT 1", 
                    (naic,)
                ).fetchone()
                
                if carrier_name:
                    carriers_to_analyze.append((naic, carrier_name[0]))
                else:
                    logging.warning(f"Carrier with NAIC {naic} not found in database")
        else:
            carriers_to_analyze = get_all_carriers(conn)
            logging.info(f"Analyzing all {len(carriers_to_analyze)} carriers")
        
        # Store results
        results = []
        
        # Track carriers with rate changes
        carriers_with_changes = set()
        
        # Process each state and carrier
        for state in states_to_analyze:
            logging.info(f"Processing state: {state}")
            
            for carrier_naic, carrier_name in carriers_to_analyze:
                # Get regions for this carrier and state
                regions = get_regions_for_carrier_state(conn, state, carrier_naic)
                carrier_has_change = False
                
                for region in regions:
                    # Get sample ZIP code for this region
                    zip_code = sample_zips_from_region(conn, region)
                    if not zip_code:
                        continue
                    
                    # Get current and future rates
                    current_rate, future_rate = get_current_and_future_rates(
                        conn, region, carrier_naic, current_date, future_date
                    )
                    
                    # Skip if no rates found or no change detected
                    if not current_rate or not future_rate:
                        continue
                    
                    # Skip if rates are the same
                    if current_rate['rate'] == future_rate['rate']:
                        continue
                    
                    # Calculate change
                    change_amount = future_rate['rate'] - current_rate['rate']
                    change_percent = (change_amount / current_rate['rate']) * 100 if current_rate['rate'] > 0 else 0
                    
                    # Add to results - one row per region
                    results.append({
                        'State': state,
                        'Carrier Name': carrier_name,
                        'NAIC': carrier_naic,
                        'Region': region,
                        'ZIP Code': zip_code,
                        'Current Rate': f"${current_rate['rate']:.2f}",
                        'Current Effective Date': current_rate['effective_date'],
                        'Future Rate': f"${future_rate['rate']:.2f}",
                        'Future Effective Date': future_rate['effective_date'],
                        'Change Amount': f"${change_amount:.2f}",
                        'Change Percent': f"{change_percent:.2f}%"
                    })
                    
                    carrier_has_change = True
                
                if carrier_has_change:
                    carriers_with_changes.add((carrier_naic, carrier_name))
        
        # Create reports
        if results:
            unique_regions = set(result['Region'] for result in results)
            logging.info(f"Found {len(results)} rate changes across {len(unique_regions)} unique regions")
            logging.info(f"Changes found for {len(carriers_with_changes)} carriers")
            
            # Always create CSV report
            create_report(results, args.output)
            
            # Create markdown report if requested
            if args.markdown:
                if args.groupby == "state":
                    create_state_markdown_report(results, args.markdown)
                else:
                    create_markdown_report(results, args.markdown)
        else:
            logging.info("No rate changes found in the specified date range")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main() 