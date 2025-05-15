from fastapi import APIRouter, Depends, HTTPException, Query, Security
from sqlalchemy import or_, text
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from app.database import get_db, get_duckdb_conn
from app.models import GroupMapping, CompanyNames, CarrierSelection, DuckDBRateStore, DuckDBCarrierInfo, DuckDBRegionMapping, DuckDBRegionMetadata
import json
from zips import zipHolder
import os
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN
import dotenv
from async_csg import AsyncCSGRequest
from config import Config
import asyncio
from filter_utils import filter_quote_fields
from datetime import datetime, timedelta
from filter_utils import Quote, QuoteInt, QuoteResponse, use_int, QuoteComparison
import time
from statistics import mean, median
from normalize_county import normalize_county_name
from thefuzz import process
from pprint import pprint
dotenv.load_dotenv()

router = APIRouter()

# Initialize CSG client for fallback
csg_client = AsyncCSGRequest(Config.API_KEY)
zip_helper = zipHolder("static/uszips.csv")



VALID_STATE_CODES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC'
}

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# Check if we're running on Replit
IS_REPLIT = os.getenv('REPLIT', 'False').lower() == 'true'

API_KEYS = set(['yVujgWOYsLOJxGaicK69TPYVKgwMmqgb'])

def get_state_specific_plan(state: str, default_plan: Optional[str] = None) -> Optional[str]:
    """Get state-specific plan override if applicable"""
    if default_plan and default_plan.upper() in ['G', 'F']:
        state_plan_mapping = {
            'MN': 'MN_EXTB',
            'WI': 'WI_HDED', 
            'MA': 'MA_SUPP1'
        }
    else:
        state_plan_mapping = {
            'MN': 'MN_BASIC',
            'WI': 'WI_BASE',
            'MA': 'MA_CORE'
        }
    return state_plan_mapping.get(state, default_plan)

def get_effective_date() -> str:
    """Get the effective date for quotes (first of next month)"""
    return (datetime.now() + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')

def validate_inputs(zip_code: str, state: str, county: Optional[str], gender: Optional[str]) -> tuple:
    """Validate input parameters and return processed values"""
    if not zip_code or not state:
        raise HTTPException(status_code=400, detail="State and zip_code must be provided")

    if len(state) != 2 or state.upper() not in VALID_STATE_CODES:
        raise HTTPException(status_code=400, detail="Invalid state code")
    
    if not zip_code.isdigit() or len(zip_code) != 5:
        raise HTTPException(status_code=400, detail="Invalid ZIP code format")

    # Validate zip code and get county
    valid_counties = zip_helper.lookup_county(zip_code)
    if not valid_counties or valid_counties == ['None']:
        raise HTTPException(status_code=400, detail="Invalid ZIP code")

    # Process county with fuzzy matching
    processed_county = None
    if county:
        # First try exact match with normalized name
        normalized_county = normalize_county_name(county)
        if normalized_county in valid_counties:
            processed_county = normalized_county
        else:
            # Try fuzzy matching if exact match fails
            best_match = process.extractOne(normalized_county, valid_counties)
            if best_match and best_match[1] >= 80:  # Minimum similarity score of 80%
                processed_county = best_match[0]
            else:
                processed_county = valid_counties[0]  # Fallback to first valid county
    else:
        processed_county = valid_counties[0]

    # Process gender
    processed_gender = None
    if gender:
        gender_upper = gender.upper()
        if gender_upper not in ['M', 'F', 'MALE', 'FEMALE']:
            raise HTTPException(status_code=400, detail="Gender must be 'M', 'F', 'male', or 'female'")
        processed_gender = 'M' if gender_upper in ['M', 'MALE'] else 'F'

    return zip_code, state.upper(), processed_county, processed_gender

def calculate_rate_with_increases(base_rate: float, base_age: int, target_age: int, 
                                age_increases: List[float]) -> float:
    """Calculate rate for a specific age including age-based increases"""
    age_diff = target_age - base_age
    if age_diff < 0:
        return 0.0
        
    rate_multiplier = 1.0
    for i in range(age_diff):
        if i < len(age_increases):
            rate_multiplier *= (1 + age_increases[i])
            
    return base_rate * rate_multiplier

def process_filtered_quote(quote_data: Dict[str, Any], requested_ages: List[int]) -> List[Quote]:
    """Process a raw quote into a list of Quote objects"""
    quotes_list = []
    base_rate = quote_data.get('rate', 0)
    base_age = quote_data.get('age', 65)
    age_increases = quote_data.get('age_increases', [])
    discount_category = quote_data.get('discount_category', None)
    for age in (requested_ages or [base_age]):
        rate = calculate_rate_with_increases(base_rate, base_age, age, age_increases)
        if rate <= 0:
            continue
            
        # Calculate discount if available
        try:
            discount_mult = (1 - quote_data['discounts'][0].get('value'))
        except:
            discount_mult = 1
        discount_rate = round(rate * discount_mult, 2)
        
        quotes_list.append(Quote(
            age=age,
            gender=quote_data['gender'],
            plan=quote_data['plan'],
            tobacco=quote_data['tobacco'],
            rate=rate,
            discount_rate=discount_rate,
            discount_category=discount_category
        ))
        
    return quotes_list

def get_api_key(api_key_header: str = Security(api_key_header)):
    """Validate API key"""
    if not api_key_header:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="API key is missing"
        )
    
    if api_key_header not in API_KEYS:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Could not validate API key"
        )
    
    return api_key_header


async def fetch_quotes_from_csg(db: Session, zip_code: str, county: str, state: str, 
                              age: List[int], tobacco: Optional[bool] = None, 
                              gender: Optional[str] = None, plans: List[str] = None,
                              naic: Optional[List[str]] = None,
                              effective_date: Optional[str] = None,
                              all_carriers: bool = False) -> List[QuoteResponse]:
    """Fetch quotes directly from CSG API"""
    try:
        # Validate required parameters
        if not age:
            age = range(65, 100)

        if tobacco is None:
            tobaccoOptions = [True, False]
        else: 
            tobaccoOptions = [tobacco]

        if not gender:
            genderOptions = ['M', 'F']
        else:
            genderOptions = [gender]

        # Ensure token is initialized
        if not hasattr(csg_client, 'token') or not csg_client.token:
            await csg_client.async_init()
            await csg_client.fetch_token()
            
        # Double check token after initialization
        if not csg_client.token:
            raise HTTPException(status_code=500, detail="Failed to initialize CSG client token")
        
        effective_date_processed = effective_date or get_effective_date()

        # Validate effective date format and value
        if effective_date_processed:
            try:
                # Parse the effective date string
                effective_date_obj = datetime.strptime(effective_date_processed, '%Y-%m-%d').date()
                
                # Check if effective date is not before today
                today = datetime.now().date()
                if effective_date_obj < today:
                    raise HTTPException(
                        status_code=400, 
                        detail="Effective date cannot be before today"
                    )
                                    
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid effective date format. Must be YYYY-MM-DD"
                )
            
        queries = []


        base_naic_list = get_naic_list(db, state)
        print(f"base_naic_list: {base_naic_list}")

        for tobacco in tobaccoOptions:
            for gender in genderOptions:
                for a in age:
                    for plan in plans:
                        query_data = {
                            'zip5': zip_code,
                            'county': county,
                            'age': a,
                            'tobacco': int(tobacco),
                            'gender': gender,
                            'plan': get_state_specific_plan(state, plan),
                            'effective_date': effective_date_processed,
                        }
                        if all_carriers:
                            pass
                        elif naic:
                            query_data['naic'] = naic
                        else:
                            query_data['naic'] = base_naic_list
                        queries.append(query_data)

        raw_tasks = [csg_client.fetch_quote(**query) for query in queries]
        raw_quotes = await asyncio.gather(*raw_tasks)
        raw_quotes_flattened = [item for sublist in raw_quotes for item in sublist]
        filtered_quotes = filter_quote_fields((raw_quotes_flattened, None))
        results = []
        
        for quote in filtered_quotes:
                
            # Skip if NAIC doesn't match the filter
            if naic and quote.get('naic') not in naic:
                continue
                
            quotes_list = process_filtered_quote(quote, age)
            if quotes_list:
                qr = QuoteResponse(
                    naic=quote.get('naic'),
                    group=-1,  # Default group for direct CSG queries
                    company_name=quote.get('name'),
                    quotes=list(map(use_int, quotes_list))
                )
                if qr.naic == '60380':
                    qr.company_name = 'AFLAC'
                results.append(qr)
                
        return results
        
    except Exception as e:
        error_msg = f"Error fetching quotes from CSG: {str(e)}"
        if hasattr(e, '__traceback__'):
            import traceback
            tb = ''.join(traceback.format_tb(e.__traceback__))
            error_msg = f"{error_msg}\nTraceback:\n{tb}"
        raise HTTPException(status_code=500, detail=error_msg)

async def fetch_quotes_from_db(db: Session, state: str, zip_code: str, county: str,
                             age: Optional[List[int]], tobacco: Optional[bool],
                             gender: Optional[str], plan: Optional[str],
                             naic: Optional[List[str]] = None,
                             effective_date: Optional[str] = None) -> List[QuoteResponse]:
    """Fetch quotes from the DuckDB database with updated effective date logic"""
    print(f"Fetching quotes from DuckDB: state={state}, zip={zip_code}, plan={plan}, effective_date={effective_date}")
    
    # Get DuckDB connection
    conn = get_duckdb_conn()
    
    # If DuckDB connection failed, return empty results
    if conn is None:
        print("No DuckDB connection available. Falling back to CSG API.")
        return []
    
    # Default effective date if not provided
    effective_date_processed = effective_date or get_effective_date()
    print(f"Processed effective date: {effective_date_processed}")
    
    # Initialize results list
    results = []
    
    # Convert tobacco to integer for DuckDB query
    tobacco_int = 1 if tobacco else 0
    
    # Get age value for query
    query_age = age[0] if age and len(age) > 0 else None
    if not query_age:
        return []
    
    try:
        # Step 1: Find region_id for the zip code
        region_query = """
        SELECT rm.region_id, rm.naic, rm.zip_code, meta.state
        FROM region_mapping rm
        JOIN region_metadata meta ON rm.region_id = meta.region_id
        WHERE rm.zip_code = ? AND meta.state = ?
        """
        
        if naic:
            region_query += " AND rm.naic IN ("
            region_query += ", ".join(["?" for _ in naic])
            region_query += ")"
            region_params = [zip_code, state] + naic
        else:
            region_params = [zip_code, state]
        
        region_results = conn.execute(region_query, region_params).fetchall()
        
        if not region_results:
            print(f"No region found for zip={zip_code}, state={state}")
            return []
        
        print(f"Found {len(region_results)} regions")
        
        # Step 2: Get carrier info for display names
        carrier_info = {}
        carriers_query = "SELECT naic, company_name, selected, discount_category FROM carrier_info"
        carrier_results = conn.execute(carriers_query).fetchall()
        for carrier in carrier_results:
            carrier_info[carrier[0]] = {
                'company_name': carrier[1],
                'selected': carrier[2],
                'discount_category': carrier[3]
            }
        
        # Step 3: For each region, fetch quotes
        for region in region_results:
            region_id = region[0]
            region_naic = region[1]
            
            # Skip if naic filter is provided and this region's naic doesn't match
            if naic and region_naic not in naic:
                continue
            
            # Use the most recent effective date query
            rate_query = DuckDBRateStore.get_most_recent_effective_date_query()
            
            # Parameters for the rate query
            rate_params = [
                region_id,
                gender,
                tobacco_int,
                query_age,
                region_naic,
                plan,
                state,
                # Additional parameters for the subquery
                region_id,
                gender,
                tobacco_int,
                query_age,
                region_naic,
                plan,
                state,
                effective_date_processed
            ]
            
            print(f"Executing query with params: {rate_params}")
            rate_results = conn.execute(rate_query, rate_params).fetchall()
            
            if not rate_results:
                print(f"No rates found for region={region_id}, naic={region_naic}")
                continue
            
            print(f"Found {len(rate_results)} rates for region={region_id}, naic={region_naic}")
            
            # Process each rate result
            for rate_row in rate_results:
                # Map DuckDB result to columns based on rate_store schema
                # [region_id, gender, tobacco, age, naic, plan, rate, discount_rate, effective_date, state, created_at]
                rate_data = {
                    'region_id': rate_row[0],
                    'gender': rate_row[1],
                    'tobacco': rate_row[2],
                    'age': rate_row[3],
                    'naic': rate_row[4],
                    'plan': rate_row[5],
                    'rate': rate_row[6],
                    'discount_rate': rate_row[7],
                    'effective_date': rate_row[8],
                    'state': rate_row[9]
                }
                
                # Create a Quote object
                quote = Quote(
                    age=rate_data['age'],
                    gender=rate_data['gender'],
                    plan=rate_data['plan'],
                    tobacco=bool(rate_data['tobacco']),
                    rate=rate_data['rate'],
                    discount_rate=rate_data['discount_rate'] or rate_data['rate'],
                    discount_category=carrier_info.get(rate_data['naic'], {}).get('discount_category')
                )
                
                # Check if we already have a QuoteResponse for this naic
                existing_response = next((r for r in results if r.naic == rate_data['naic']), None)
                
                if existing_response:
                    # Add quote to existing response
                    existing_response.quotes.append(use_int(quote))
                else:
                    # Create new QuoteResponse
                    company_name = carrier_info.get(rate_data['naic'], {}).get('company_name', 'Unknown')
                    
                    # Special case for AFLAC
                    if rate_data['naic'] == '60380':
                        company_name = 'AFLAC'
                        
                    # Create and add new QuoteResponse
                    quote_response = QuoteResponse(
                        naic=rate_data['naic'],
                        group=-1,  # Default value since we don't have group concept in new schema
                        company_name=company_name,
                        quotes=[use_int(quote)]
                    )
                    results.append(quote_response)
    except Exception as e:
        print(f"Error retrieving quotes from DuckDB: {str(e)}")
        return []
    
    # Sort results by naic
    sorted_results = sorted(results, key=lambda x: x.naic or '')
    return sorted_results


@router.get("/quotes/", response_model=List[QuoteResponse], dependencies=[Depends(get_api_key)])
async def get_quotes(
    zip_code: str,
    state: str,
    age: int,
    tobacco: bool,
    gender: str,
    plans: List[str] = Query(...),
    county: Optional[str] = None,
    naic: Optional[List[str]] = Query(None),
    effective_date: Optional[str] = None,
    carriers: Optional[str] = Query("supported", regex="^(all|supported)$"),
    db: Session = Depends(get_db),
):
    """Get quotes from database with CSG fallback"""
    # Validate and process inputs
    zip_code, state, county, gender = validate_inputs(zip_code, state, county, gender)

    all_carriers = carriers == "all"

    default_effective_date = get_effective_date()
    effective_date_processed = effective_date or default_effective_date
    print(f"effective_date_processed: {effective_date_processed}")

    try:
        if all_carriers:    
            return await fetch_quotes_from_csg(db, zip_code, county, state, [age], tobacco, gender, plans, [], effective_date_processed, all_carriers=True)
        else:
            # Try database first
            results = []
            print(f"Fetching quotes from database for {len(plans)} plans")
            plans_to_fetch = []
            naics_to_fetch = {}
            for plan in plans:
                print(f"Fetching quotes for plan {plan}")
                db_results = await fetch_quotes_from_db(
                    db, state, zip_code, county, [age], tobacco, gender, plan, naic, effective_date_processed
                )
                if db_results:
                    print(f"Found {len(db_results)} quotes for plan {plan}")
                    results.extend(db_results)
                    naicFilt = naic if naic else get_naic_list(db, state)
                    for n in naicFilt:
                        if n not in [q.naic for q in results]:
                            d = naics_to_fetch.get(plan, [])
                            d.append(n)
                            naics_to_fetch[plan] = d
                else:
                    print(f"No quotes found for plan {plan} in database")
                    plans_to_fetch.append(plan)

            # Fetch missing quotes from CSG
            tasks = []
            if plans_to_fetch:
                print(f"Fetching quotes from CSG for {len(plans_to_fetch)} plans")
                task = fetch_quotes_from_csg(
                    db, zip_code, county, state, [age], tobacco, gender, plans_to_fetch, naic, effective_date_processed, all_carriers=all_carriers
                )
                tasks.append(task)
            if naics_to_fetch:
                print(f"Fetching quotes from CSG for {len(naics_to_fetch)} plans with missing NAICs")
                for plan, naics in naics_to_fetch.items():
                    print(f"Fetching quotes for plan {plan} with NAICs: {naics}")
                    task = fetch_quotes_from_csg(
                        db, zip_code, county, state, [age], tobacco, gender, [plan], naics, effective_date_processed, all_carriers=True
                    )
                    tasks.append(task)

            # Gather all CSG results and flatten properly
            if tasks:
                print(f"Gathering {len(tasks)} CSG tasks")
                csg_results = await asyncio.gather(*tasks)
                print(f"Received {len(csg_results)} CSG result lists")
                pprint(csg_results)
                for result_list in csg_results:
                    if result_list:  # Check if the result list is not empty
                        print(f"Adding {len(result_list)} quotes from CSG result list")
                        results.extend(result_list)
                    else:
                        print("Empty CSG result list, skipping")

            sorted_results = sorted(results, key=lambda x: x.naic or '')
            print(f"Sorted results: {sorted_results}")
            return sorted_results

    except Exception as e:
        # Log the error and fall back to CSG
        print(f"Database query failed: {str(e)}")
        return await fetch_quotes_from_csg(
            db, zip_code, county, state, [age], tobacco, gender, plans, naic, effective_date_processed, all_carriers=all_carriers
        )
    
def get_naic_list(db: Session, state: str) -> List[str]:
    """Get list of selected NAICs for a state from DuckDB"""
    conn = get_duckdb_conn()
    
    # If DuckDB connection failed, fallback to SQLite
    if conn is None:
        print("No DuckDB connection available for get_naic_list. Falling back to SQLite.")
        try:
            res = db.query(GroupMapping.naic).distinct()\
                .join(CarrierSelection, GroupMapping.naic == CarrierSelection.naic)\
                .filter(GroupMapping.state == state)\
                .filter(CarrierSelection.selected == 1)\
                .all()
            return [r[0] for r in res]
        except Exception as e:
            print(f"Error retrieving NAIC list from SQLite: {str(e)}")
            return []

    try:
        query = """
        SELECT DISTINCT rm.naic
        FROM region_mapping rm
        JOIN region_metadata meta ON rm.region_id = meta.region_id
        JOIN carrier_info ci ON rm.naic = ci.naic
        WHERE meta.state = ? AND ci.selected = 1
        """
        
        results = conn.execute(query, [state]).fetchall()
        return [r[0] for r in results]
    except Exception as e:
        print(f"Error retrieving NAIC list from DuckDB: {str(e)}")
        # Fallback to SQLite
        try:
            res = db.query(GroupMapping.naic).distinct()\
                .join(CarrierSelection, GroupMapping.naic == CarrierSelection.naic)\
                .filter(GroupMapping.state == state)\
                .filter(CarrierSelection.selected == 1)\
                .all()
            return [r[0] for r in res]
        except Exception as e2:
            print(f"Error retrieving NAIC list from SQLite fallback: {str(e2)}")
            return []


@router.get("/quotes/csg", response_model=List[QuoteResponse], dependencies=[Depends(get_api_key)])
async def get_quotes_from_csg(
    zip_code: str,
    state: str,
    age: int,
    tobacco: bool,
    gender: str,
    plans: List[str] = Query(...),
    county: Optional[str] = None,
    naic: Optional[List[str]] = Query(None),
    effective_date: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Get quotes directly from CSG API for testing"""
    # Validate and process inputs
    zip_code, state, county, gender = validate_inputs(zip_code, state, county, gender)
    
    # Ensure token is initialized
    if not hasattr(csg_client, 'token') or not csg_client.token:
        await csg_client.async_init()
        await csg_client.fetch_token()
        
    # Double check token after initialization
    if not csg_client.token:
        raise HTTPException(status_code=500, detail="Failed to initialize CSG client token")
    
    # Fetch quotes from CSG (pass age as a single-item list for compatibility)
    return await fetch_quotes_from_csg(
        db, zip_code, county, state, [age], tobacco, gender, plans, naic, effective_date, all_carriers=False
    )


class QuoteRequest(BaseModel):
    zip_code: str
    state: str
    age: int
    tobacco: bool
    gender: str
    plans: List[str]
    county: Optional[str] = None
    naic: Optional[List[str]] = None
    effective_date: Optional[str] = None
    carriers: Optional[str] = Query("supported", regex="^(all|supported)$")
    
    
@router.post("/quotes/", response_model=List[QuoteResponse], dependencies=[Depends(get_api_key)])
async def post_quotes(
    request: QuoteRequest,
    db: Session = Depends(get_db),
):
    """Get quotes from database with CSG fallback (POST version)"""
    return await get_quotes(
        zip_code=request.zip_code,
        state=request.state,
        age=request.age,
        tobacco=request.tobacco,
        gender=request.gender,
        plans=request.plans,
        county=request.county,
        naic=request.naic,
        effective_date=request.effective_date,
        carriers=request.carriers,
        db=db
    )
