#!/usr/bin/env python3
import asyncio
import duckdb
import uuid
import json
import logging
import sys
import os
from async_csg import AsyncCSGRequest as csg
from config import Config
from filter_utils import filter_quote
from collections import defaultdict
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Database path
DB_PATH = "medicare.duckdb"

# Missouri ZIP codes from the CSV file
MO_ZIP_CODES = [
    "64149",  # Region: 1284d6ce-4db0-4720-ad66-ebd3a7580f7f
    "64040",  # Region: 1ba7a98b-6494-444d-80cd-0f963f2c00cb
    "64463",  # Region: 20187681-efb6-4f48-bf78-30f0c7f5bd02
    "64473",  # Region: 23b3e123-cec4-400d-8b44-e070dce441bd
    "65483",  # Region: 37f509db-9b5f-469b-a0b8-1c42a69d31ae
    "63442",  # Region: 43829cfe-e4d1-4071-ae25-97353149cc5e
    "63943",  # Region: 50b00ed8-ccbf-4ac3-92e0-d74aa2a4af4f
    "64482",  # Region: 532f7fb1-c9c6-4a57-8396-18ce2ee85294
    "64040",  # Region: 66da9eba-1f40-4166-b72c-e51833ff24ab
    "65655",  # Region: 6e904a20-ecea-4e9d-9a19-af7661ba19a7
    "64080",  # Region: 7e21aeaf-ce15-4f76-8e2b-a2d092001fc7
    "65634",  # Region: 8e4d312b-b19a-4b3f-b38a-95f990e4c68d
    "65804",  # Region: 9a7fe616-c069-4533-9c34-c4da31d97135
    "64667",  # Region: a997082d-631d-4afe-85a9-76575b0cbc25
    "63137",  # Region: d928c118-1be6-4586-a63a-940df10bdc7c
    "63040",  # Region: dc752c97-ae4f-4838-a57b-16596623255c
    "65040",  # Region: e3d6cbf9-ca8f-4c9d-959b-241b1ce9736a
    "64141",  # Region: e7f0583e-1609-4ab0-b992-db5b31382ee0
    "64670"   # Region: efb93b4f-1808-4919-ad01-6fd84254c692
]

# Base parameter set
base_params = {
    "age": 65,
    "gender": "M",
    "tobacco": 0,
    "naic": "82538",  # Allstate Health Solutions
    "plan": "G",
    "effective_date": "2025-06-01"
}

def get_hash_id(data):
    """Generate a consistent hash ID for a data structure"""
    data_str = json.dumps(data, sort_keys=True)
    return hashlib.md5(data_str.encode()).hexdigest()[:12]

async def fetch_quote_with_location_data(cr, zip_code):
    """Fetch quote and extract location/region data for a zip code."""
    params = base_params.copy()
    params["zip5"] = zip_code
    
    # Fetch quote
    print(f"Fetching quote for ZIP: {zip_code}")
    response = await cr.fetch_quote(**params)
    
    # Filter to quotes matching the requested NAIC
    quotes = [q for q in response if q.get('company_base', {}).get('naic') == params['naic']]
    
    if not quotes:
        print(f"No quotes found for ZIP: {zip_code}")
        return None
    
    quote = quotes[0]  # Use the first matching quote
    
    # Extract rate information
    monthly_rate = None
    annual_rate = None
    
    try:
        # Check various rate formats
        if isinstance(quote.get('rate'), (int, float)):
            monthly_rate = quote['rate']
        elif isinstance(quote.get('rate'), dict):
            rate_dict = quote['rate']
            monthly_rate = rate_dict.get('month')
            annual_rate = rate_dict.get('annual')
    except Exception as e:
        print(f"Error extracting rate from quote: {e}")
    
    # Extract the location_base information which contains the full list of ZIPs or counties
    location_data = None
    
    # Look for location_base in various places
    if 'location_base' in quote:
        location_data = quote['location_base']
    elif 'territory' in quote and 'location_base' in quote['territory']:
        location_data = quote['territory']['location_base']
    elif 'region' in quote and 'location_base' in quote['region']:
        location_data = quote['region']['location_base']
    
    # Extract the ZIP codes or counties list
    zip_list = []
    county_list = []
    
    if location_data:
        if 'zip5' in location_data:
            zip_list = location_data['zip5']
        elif 'zip3' in location_data:
            zip_list = location_data['zip3']
        
        if 'county' in location_data:
            county_list = location_data['county']
    
    # Save the raw response for analysis
    output_file = f"response_{zip_code}.json"
    with open(output_file, 'w') as f:
        json.dump(quote, f, indent=2)
    
    # Return the information for analysis
    return {
        'zip_code': zip_code,
        'monthly_rate': monthly_rate,
        'annual_rate': annual_rate,
        'zip_list': zip_list,
        'county_list': county_list,
        'location_data': location_data,
        'raw_response': quote,
        'effective_date': quote.get('effective_date')
    }

async def analyze_api_region_groupings():
    print("\n=== API Region Grouping Analysis Tool ===\n")
    
    # Initialize API client
    cr = csg(Config.API_KEY)
    await cr.async_init()
    await cr.fetch_token()
    
    # Create tasks for all ZIP code fetches to run concurrently
    tasks = []
    for zip_code in MO_ZIP_CODES:
        task = asyncio.create_task(fetch_quote_with_location_data(cr, zip_code))
        tasks.append(task)
    
    # Wait for all tasks to complete
    results = []
    for task in tasks:
        result = await task
        if result:
            results.append(result)
    
    print(f"\nCompleted fetching quotes for {len(results)} ZIP codes")
    
    # Now analyze the responses to find unique location groups
    location_groups = defaultdict(list)
    rate_groups = defaultdict(list)
    
    for result in results:
        zip_code = result['zip_code']
        monthly_rate = result.get('monthly_rate')
        
        # Add to rate group if rate is available
        if monthly_rate is not None:
            rate_groups[monthly_rate].append(zip_code)
        
        # Look for location data to group
        zip_list = result.get('zip_list', [])
        county_list = result.get('county_list', [])
        
        # Create a signature for the location group
        if zip_list:
            # Sort the zip list for consistent hashing
            sorted_zips = sorted(zip_list)
            group_hash = get_hash_id(sorted_zips)
            location_groups[group_hash].append({
                'zip_code': zip_code,
                'location_type': 'zip',
                'location_count': len(zip_list),
                'monthly_rate': monthly_rate,
                'sample_locations': sorted_zips[:5]  # First 5 for display
            })
        elif county_list:
            # Sort the county list for consistent hashing
            sorted_counties = sorted(county_list)
            group_hash = get_hash_id(sorted_counties)
            location_groups[group_hash].append({
                'zip_code': zip_code,
                'location_type': 'county',
                'location_count': len(county_list),
                'monthly_rate': monthly_rate,
                'sample_locations': sorted_counties[:5]  # First 5 for display
            })
    
    # Print rate groupings
    print("\n=== ZIP CODES GROUPED BY MONTHLY RATE ===\n")
    for rate, zips in rate_groups.items():
        print(f"Rate ${rate/100:.2f} ({len(zips)} ZIP codes): {', '.join(zips)}")
    
    # Print location groupings
    print("\n=== UNIQUE LOCATION GROUPS FROM API RESPONSES ===\n")
    print(f"Found {len(location_groups)} unique location groups")
    
    for group_hash, items in location_groups.items():
        sample_item = items[0]
        print(f"\nGroup {group_hash}: {len(items)} ZIP codes, {sample_item['location_count']} locations")
        print(f"  Type: {sample_item['location_type']}")
        print(f"  Sample Locations: {sample_item['sample_locations']}")
        print(f"  ZIP codes in this group: {[item['zip_code'] for item in items]}")
        
        # Check if rates are the same within this group
        rates = set(item['monthly_rate'] for item in items if item['monthly_rate'] is not None)
        if rates:
            print(f"  Rates in this group: {[rate/100 for rate in rates]}")
    
    # Compare with database for these ZIP codes
    await check_database_regions(MO_ZIP_CODES, base_params['naic'])
    
    print("\n" + "="*50)

async def check_database_regions(zip_codes, naic):
    """Check database for region information about these ZIP codes."""
    try:
        print("\n=== CHECKING DATABASE FOR REGION MAPPINGS ===\n")
        conn = duckdb.connect(DB_PATH)
        
        # Get the region mappings for our ZIP codes
        placeholders = ','.join(['?'] * len(zip_codes))
        unique_zips = list(set(zip_codes))  # Remove any duplicates
        
        query = f"""
            SELECT m.zip_code, m.region_id, r.state, r.mapping_type, r.region_data
            FROM region_mapping m
            JOIN rate_regions r ON m.region_id = r.region_id
            WHERE m.zip_code IN ({placeholders})
              AND m.naic = ?
        """
        
        results = conn.execute(query, unique_zips + [naic]).fetchall()
        
        # Group by region ID
        regions_by_id = defaultdict(list)
        region_data_map = {}
        
        for zip_code, region_id, state, mapping_type, region_data in results:
            regions_by_id[region_id].append(zip_code)
            if region_id not in region_data_map:
                region_data_map[region_id] = {
                    'state': state,
                    'mapping_type': mapping_type,
                    'region_data': region_data
                }
        
        print(f"Found {len(regions_by_id)} regions in database for these ZIP codes:")
        for region_id, zips in regions_by_id.items():
            print(f"Region {region_id}: {zips}")
            
            # Parse region data to get full list of ZIPs/counties
            try:
                region_data_json = region_data_map[region_id]['region_data']
                region_data_obj = json.loads(region_data_json) if region_data_json else []
                print(f"  Full region contains {len(region_data_obj)} locations")
                if len(region_data_obj) <= 10:
                    print(f"  Full location data: {region_data_obj}")
                else:
                    print(f"  Sample locations: {region_data_obj[:5]}...")
            except Exception as e:
                print(f"  Error parsing region data: {e}")
        
        # Get rates for each region
        if regions_by_id:
            region_ids = list(regions_by_id.keys())
            placeholders = ','.join(['?'] * len(region_ids))
            
            rate_query = f"""
                SELECT DISTINCT region_id, rate
                FROM rate_store
                WHERE region_id IN ({placeholders})
                  AND naic = ?
                  AND gender = 'M'
                  AND tobacco = 0
                  AND age = 65
                  AND plan = 'G'
            """
            
            rate_results = conn.execute(rate_query, region_ids + [naic]).fetchall()
            
            # Group regions by rate
            regions_by_rate = defaultdict(list)
            for region_id, rate in rate_results:
                regions_by_rate[rate].append(region_id)
            
            print("\nRegions grouped by rate:")
            for rate, regions in regions_by_rate.items():
                print(f"Rate ${rate:.2f}: {len(regions)} regions")
                for region_id in regions:
                    print(f"  {region_id}: {regions_by_id[region_id]}")
        
        # Close connection
            conn.close()
            
        except Exception as e:
        print(f"Error checking database for region mappings: {e}")

if __name__ == "__main__":
    # Use asyncio.run to run the entire program asynchronously
    asyncio.run(analyze_api_region_groupings()) 