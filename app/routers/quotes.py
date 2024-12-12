from fastapi import APIRouter, Depends, HTTPException, Query, Security
from sqlalchemy import or_, text
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from app.database import get_db
from app.models import GroupMapping, CompanyName
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
            discount_rate=discount_rate
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


async def fetch_quotes_from_csg(zip_code: str, county: str, state: str, 
                              age: List[int], tobacco: Optional[bool] = None, 
                              gender: Optional[str] = None, plans: List[str] = None,
                              naic: Optional[List[str]] = None,
                              effective_date: Optional[str] = None) -> List[QuoteResponse]:
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
                        if naic:
                            query_data['naic'] = naic
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
                results.append(QuoteResponse(
                    naic=quote.get('naic'),
                    group=-1,  # Default group for direct CSG queries
                    company_name=quote.get('name'),
                    quotes=list(map(use_int, quotes_list))
                ))
                
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
    """Fetch quotes from the database"""
    print(f"effective_date: {effective_date}")
    query = db.query(GroupMapping, CompanyName.name).outerjoin(
        CompanyName, GroupMapping.naic == CompanyName.naic
    ).filter(
        GroupMapping.state == state,
        or_(GroupMapping.location == zip_code, GroupMapping.location == county)
    )
    
    if naic:
        query = query.filter(GroupMapping.naic.in_(naic))
        
    group_mappings = query.all()

    if not group_mappings:
        return []

    results = []
    for mapping, company_name in group_mappings:
        store_key = f"{state}:{mapping.naic}:{mapping.naic_group}"
        
        # Build pattern for the inner JSON keys
        inner_key_parts = [
            f"{age[0]}" if age else "%",              # age
            f"{gender}" if gender else "%",            # gender
            f"{plan}" if plan else "%",               # plan
            f"{str(tobacco)}" if tobacco is not None else "%"  # tobacco
        ]
        inner_key_pattern = ":".join(inner_key_parts)
        print(f"Looking up store_key: {store_key}, inner pattern: {inner_key_pattern}")
        
        sql_query = text("""
            WITH json_data AS (
                SELECT value as json_blob
                FROM rate_store 
                WHERE key = :store_key
                AND effective_date = :effective_date
            ),
            matched_objects AS (
                SELECT value as obj
                FROM json_data, json_each(json_blob)
                WHERE key LIKE :inner_key_pattern
            )
            SELECT json_group_array(obj) as result
            FROM matched_objects;
        """)

        result = db.execute(sql_query, {
            'store_key': store_key,
            'inner_key_pattern': inner_key_pattern,
            'effective_date': effective_date or get_effective_date()
        }).scalar()

        if result:
            try:
                # Parse the outer JSON array
                quotes_array = json.loads(result)
                # Parse each quote object
                quotes = []
                for quote_data in quotes_array:
                    if isinstance(quote_data, str):
                        # If the quote is still a string, parse it again
                        quote_data = json.loads(quote_data)
                    quotes.append(Quote(**quote_data))
                    
                if quotes:
                    results.append(QuoteResponse(
                        naic=mapping.naic,
                        group=mapping.naic_group,
                        company_name=company_name or "Unknown",
                        quotes=list(map(use_int, quotes))
                    ))
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                print(f"Raw result: {result}")
            except Exception as e:
                print(f"Error processing quotes: {e}")
                print(f"Raw result: {result}")

    return results


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
    db: Session = Depends(get_db),
):
    """Get quotes from database with CSG fallback"""
    # Validate and process inputs
    zip_code, state, county, gender = validate_inputs(zip_code, state, county, gender)

    default_effective_date = get_effective_date()
    effective_date_processed = effective_date or default_effective_date
    print(f"effective_date_processed: {effective_date_processed}")

    try:
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
                if naic:  # Only check missing NAICs if we have a NAIC filter
                    for n in naic:
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
                zip_code, county, state, [age], tobacco, gender, plans_to_fetch, naic, effective_date_processed
            )
            tasks.append(task)
        if naics_to_fetch:
            for plan, naics in naics_to_fetch.items():
                task = fetch_quotes_from_csg(
                    zip_code, county, state, [age], tobacco, gender, [plan], naics, effective_date_processed
                )
                tasks.append(task)

        # Gather all CSG results and flatten properly
        if tasks:
            csg_results = await asyncio.gather(*tasks)
            for result_list in csg_results:
                if result_list:  # Check if the result list is not empty
                    results.extend(result_list)

        sorted_results = sorted(results, key=lambda x: x.naic or '')
        print(f"Sorted results: {sorted_results}")
        return sorted_results

    except Exception as e:
        # Log the error and fall back to CSG
        print(f"Database query failed: {str(e)}")
        return await fetch_quotes_from_csg(
            zip_code, county, state, [age], tobacco, gender, plans, naic, effective_date_processed
        )

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
        zip_code, county, state, [age], tobacco, gender, plans, naic, effective_date
    )

@router.get("/quotes/compare", response_model=QuoteComparison, dependencies=[Depends(get_api_key)])
async def compare_quotes(
    zip_code: str,
    state: str,
    age: int,
    tobacco: bool,
    gender: str,
    plan: str,
    county: Optional[str] = None,
    naic: Optional[List[str]] = Query(None),
    effective_date: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Compare quotes from database and CSG API"""
    # Validate and process inputs
    zip_code, state, county, gender = validate_inputs(zip_code, state, county, gender)
    
    # Fetch quotes from both sources (pass age as a single-item list for compatibility)
    db_quotes = await fetch_quotes_from_db(
        db, state, zip_code, county, [age], tobacco, gender, plan, naic, effective_date
    )
    
    csg_quotes = await fetch_quotes_from_csg(
        zip_code, county, state, [age], tobacco, gender, [plan], naic, effective_date
    )
    
    # Compare quotes and identify differences
    differences = []
    has_differences = False
    
    # Create lookup dictionaries for easier comparison
    db_lookup = {(q.naic, quote.age, quote.plan): quote.rate 
                 for q in db_quotes 
                 for quote in q.quotes}
    csg_lookup = {(q.naic, quote.age, quote.plan): quote.rate 
                  for q in csg_quotes 
                  for quote in q.quotes}
    
    # Check for quotes in DB that differ or don't exist in CSG
    for naic_age_plan, db_rate in db_lookup.items():
        naic, age, plan = naic_age_plan
        csg_rate = csg_lookup.get(naic_age_plan)
        if csg_rate is None:
            differences.append(f"Quote exists in DB but not in CSG: NAIC={naic}, Age={age}, Plan={plan}")
            has_differences = True
        elif abs(db_rate - csg_rate) > 0.01:  # Allow for small floating point differences
            differences.append(
                f"Rate difference for NAIC={naic}, Age={age}, Plan={plan}: "
                f"DB=${db_rate:.2f} vs CSG=${csg_rate:.2f}"
            )
            has_differences = True
    
    # Check for quotes in CSG that don't exist in DB
    for naic_age_plan in csg_lookup:
        if naic_age_plan not in db_lookup:
            naic, age, plan = naic_age_plan
            differences.append(f"Quote exists in CSG but not in DB: NAIC={naic}, Age={age}, Plan={plan}")
            has_differences = True
    
    return QuoteComparison(
        has_differences=has_differences,
        db_quotes=db_quotes,
        csg_quotes=csg_quotes,
        differences=differences if has_differences else None
    )

class TimingResult(BaseModel):
    approach: str
    times: List[float]
    average_time: float
    total_quotes: int

class TimingResponse(BaseModel):
    single_query_results: TimingResult
    chunked_query_results: TimingResult
    chunk_size: int
    iterations: int

@router.get("/quotes/time", response_model=TimingResponse, dependencies=[Depends(get_api_key)])
async def benchmark_quote_approaches(
    iterations: int = 10,
    chunk_size: int = 3,
):
    """Benchmark different approaches to fetching quotes for multiple NAICs"""
    # Validate inputs
    zip_code = "90210"
    state = "CA"
    county = "Los Angeles"
    age = 65
    tobacco = False
    gender = "M"
    plan = "G"
    
    naics = [
        "20699",
        "79413",
        "71412",
        "82538",
        "60380",
        "60984"
    ]

    # Approach 1: Single query with all NAICs
    single_query_times = []
    single_query_total_quotes = 0
    
    for _ in range(iterations):
        start_time = time.time()
        results = await fetch_quotes_from_csg(
            zip_code, county, state, [age], tobacco, gender, [plan], naics
        )
        single_query_times.append(time.time() - start_time)
        single_query_total_quotes += sum(len(r.quotes) for r in results)

    # Approach 2: Multiple queries with chunked NAICs
    chunked_query_times = []
    chunked_query_total_quotes = 0
    
    for _ in range(iterations):
        start_time = time.time()
        all_results = []
        
        # Split NAICs into chunks
        for i in range(0, len(naics), chunk_size):
            naic_chunk = naics[i:i + chunk_size]
            chunk_results = await fetch_quotes_from_csg(
                zip_code, county, state, [age], tobacco, gender, [plan], naic_chunk
            )
            all_results.extend(chunk_results)
            
        chunked_query_times.append(time.time() - start_time)
        chunked_query_total_quotes += sum(len(r.quotes) for r in all_results)

    return TimingResponse(
        single_query_results=TimingResult(
            approach="single_query",
            times=single_query_times,
            average_time=mean(single_query_times),
            min_time=min(single_query_times),
            max_time=max(single_query_times),
            median_time=median(single_query_times),
            total_quotes=single_query_total_quotes // iterations
        ),
        chunked_query_results=TimingResult(
            approach="chunked_queries",
            times=chunked_query_times,
            average_time=mean(chunked_query_times),
            min_time=min(chunked_query_times),
            max_time=max(chunked_query_times),
            median_time=median(chunked_query_times),
            total_quotes=chunked_query_total_quotes // iterations
        ),
        chunk_size=chunk_size,
        iterations=iterations
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
        db=db
    )
