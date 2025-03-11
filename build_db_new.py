# build_db_new.py
import json
from typing import List, Dict, Any
from zips import zipHolder
from async_csg import AsyncCSGRequest as csg
from aiolimiter import AsyncLimiter
from filter_utils import filter_quote
from config import Config
from functools import reduce
import asyncio
import csv
import logging
import itertools
import random
from copy import copy
import operator
from datetime import datetime, timedelta
from db_operations_log import DBOperationsLogger
from pprint import pprint
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='medicare_supplement_rate_db.log',
    filemode='a'
)

logger = logging.getLogger(__name__)

class MedicareSupplementRateDB:
    def __init__(self, db_path: str, log_operations: bool = True, log_file: str = None):
        self.conn = libsql.connect(db_path)
        self.cr = csg(Config.API_KEY)
        if log_operations:
            log_filename = log_file if log_file else f"db_operations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            self.db_logger = DBOperationsLogger(log_filename)
        else:
            self.db_logger = None
        self._create_tables()
        self.zip_holder = zipHolder("static/uszips.csv")
        self.limiter = AsyncLimiter(max_rate=20, time_period=1)
        self.default_parameters = {
            "age": 65,
            "gender": "M",
            "plan": "G",
            "tobacco": 0,
        }

    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rate_store (
                key TEXT,
                effective_date TEXT,
                value TEXT,
                PRIMARY KEY (key, effective_date)
            )
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_rate_store_date 
            ON rate_store(effective_date)
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_mapping (
                naic TEXT,
                state TEXT,
                location TEXT,
                naic_group INTEGER,
                PRIMARY KEY (naic, state, location)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_type (
                naic TEXT,
                state TEXT,
                group_zip INTEGER,
                PRIMARY KEY (naic, state)
            )
        ''')
        self.conn.commit()

    def get_selected_carriers(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                SELECT naic, company_name 
                FROM carrier_selection 
                WHERE selected = 1
            ''')
            return [{'naic': row[0], 'name': row[1]} for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error reading carrier_selection table: {str(e)}")
            return []
    
    async def get_available_naics(self, state: str, effective_date: str):
        params = copy(self.default_parameters)
        params["effective_date"] = effective_date
        zip_random = random.choice(self.zip_holder.lookup_zips_by_state(state))
        params["zip5"] = zip_random
        response = await self.cr.fetch_quote(**params)
        out = set([q.get('company_base',{}).get("naic") for q in response])
        out.discard(None)
        return out
    
    def get_existing_naics(self, state: str):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT DISTINCT naic 
            FROM group_mapping 
            WHERE state = ?
        ''', (state,))
        return set(row[0] for row in cursor.fetchall())
    
    def remove_naic(self, naic: str, state: str, include_rates: bool = False):
        self._execute_and_log(
            'DELETE FROM group_mapping WHERE naic = ? AND state = ?',
            (naic, state)
        )
        if include_rates:
            self._remove_rates(state, naic)

    def _remove_rates(self, key: str):
        self._execute_and_log(
            'DELETE FROM rate_store WHERE key LIKE ?',
            (key,)
        )

    async def set_state_map_naic(self, naic: str, state: str):
        lookup_list, mapping_type = await self.cr.calc_naic_map_combined2(state, naic)
        if len(lookup_list) == 0:
            return False
        
        logging.info(f"lookup_list: {lookup_list}")
        logging.info(f"mapping_type: {mapping_type}")
        
        # Prepare data for bulk insert
        group_mapping_data = []
        saved_groups = []
        for i, group in enumerate(lookup_list, 1):
            group_mapping_data.extend((naic, state, x, i) for x in group)
            saved_groups.append(f"{state}:{naic}:{i}")

        print(f"saved_groups: {saved_groups}")

        # Bulk insert group mappings
        cursor = self.conn.cursor()
        if len(group_mapping_data) > 0:
            cursor.executemany('''
                INSERT OR REPLACE INTO group_mapping (naic, state, location, naic_group)
                VALUES (?, ?, ?, ?)
            ''', group_mapping_data)

            # Insert group type
            cursor.execute('''
                INSERT OR REPLACE INTO group_type (naic, state, group_zip)
                VALUES (?, ?, ?)
            ''', (naic, state, int(mapping_type == "zip5")))

        self.conn.commit()
        return True

    def get_rate_tasks(self, state: str, naic: str, effective_date: str):
        # get group_type for a given state, naic
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT group_zip FROM group_type WHERE state = ? AND naic = ?
        ''', (state, naic))
        group_type = 'zip5' if bool(cursor.fetchone()[0]) else 'county'

        # get all unique naic_group from group_mapping for a given state, naic
        cursor.execute('''
            SELECT DISTINCT naic_group FROM group_mapping WHERE state = ? AND naic = ?
        ''', (state, naic))
        naic_groups = [x[0] for x in cursor.fetchall()]

        # get 10 locations from group_mapping for each naic,state, naic_group
        all_tasks = []
        for naic_group in naic_groups:
            cursor.execute('''
                SELECT location FROM group_mapping WHERE state = ? AND naic = ? AND naic_group = ? LIMIT 10
            ''', (state, naic, naic_group))
            label = f"{state}:{naic}:{naic_group}"
            location_list = [x[0] for x in cursor.fetchall()]
            tasks, _ = self.build_naic_requests(label, location_list, naic, group_type, effective_date)
            all_tasks.extend(tasks)
        return all_tasks

    def build_naic_requests(self, label, location_list, naic: str, mapping_type: str, effective_date: str):
        arg_holder = []
        tasks = []
        main_location = location_list[0]
        tobacco_options = [0, 1]
        age_options = [65, 70, 75, 80, 85, 90, 95]
        gender_options = ["M", "F"]

        state = label.split(":")[0]
        if state == 'MA':
            plan_options = ['MA_CORE', 'MA_SUPP1']
        elif state == 'MN':
            plan_options = ['MN_BASIC', 'MN_EXTB']
        elif state == 'WI':
            plan_options = ['WIR_A50%']
        else:
            plan_options = ['N', 'G', 'F']
        

        additional_keys = ["tobacco", "age", "gender", "plan"]
        additional_values = [tobacco_options, age_options, gender_options, plan_options]
        #naic = label.split(":")[1]

        #print(main_location)   
        args = {
            "select": 0,
            "naic": naic,
            "label": label,
            "effective_date": effective_date,
        }
        if mapping_type == 'zip5':
            args['zip5'] = main_location
            args['zip5_fallback'] = location_list[1:]
        else:
            try:
                all_zips0 = list(map(lambda x: self.zip_holder.lookup_zip_by_county(state, x), location_list))
                singe_county_zips = []
                for zips in all_zips0:
                    for z in zips:
                        counties = self.zip_holder.lookup_county2(z)
                        if len(counties) == 1:
                            singe_county_zips.append(z)
                random_zip = random.choice(singe_county_zips)
                singe_county_zips.remove(random_zip) 
                args['zip5'] = random_zip
                args['zip5_fallback'] = singe_county_zips[:10]

                # hardcoded because lots of overlap in counties / zips
                if label == 'VA:67369:2':
                    args['zip5'] = '22209'
            except Exception as e:  
                logging.error(f"Error processing {location_list}: {e}")
                return [], []

        if state in ['NY', 'MA']:
            args.pop('naic')

        combinations = [
            dict(zip(additional_keys, values))
            for values in itertools.product(*additional_values)
        ]

        self._remove_rates(label)

        for (i, combination) in enumerate(combinations):
            args = copy(args)
            cargs = copy(args)
            cargs.update(combination)
            arg_holder.append(cargs)
            tasks.append(self.fetch_and_process_and_save(cargs, retry=10))

        return tasks, arg_holder    
    
    async def fetch_and_process(self, cargs, retry):
        results, label = await self.fetch_helper(cargs, retry)
        fr = [winnow_quotes(process_quote(q, label)) for q in results]
        return fr, label
    
    async def fetch_and_process_and_save(self, cargs, retry):
        fr, label = await self.fetch_and_process(cargs, retry)
        for ls in fr:
            dic = dic_build(ls)
            #pprint(dic)
            self._save_results(dic, cargs['effective_date'])
        return fr, label

    async def fetch_helper(self, args, retry=3, fallback_index=0, max_empty_attempts=5):
        original_zip5 = args['zip5']
        zip5_fallback = args.pop('zip5_fallback')
        label = args.pop('label')
        
        empty_results_count = 0  # Track number of empty results

        while fallback_index < len(zip5_fallback):
            current_retry = retry
            while current_retry > 0:
                try:    
                    async with self.limiter:
                        results = await self.cr.load_response_inner(args)
                        if results:  # If we got any results
                            return results, label
                        else:
                            empty_results_count += 1
                            logging.warning(f"No results for {args['zip5']}")
                            if empty_results_count >= max_empty_attempts:
                                logging.warning(f"Giving up after {max_empty_attempts} empty results for {label}")
                                return [], label
                            break  # Break inner loop to try next ZIP code
                except Exception as e:
                    logging.error(f"An error occurred for request: {args}")
                    logging.error(f"Error details: {e}")
                    if current_retry > 1:
                        logging.info(f"Retrying request: {args} (Retry attempt: {11 - current_retry})")
                        await asyncio.sleep(0.2)
                        current_retry -= 1
                    else:
                        break
            
            fallback_index += 1
            if fallback_index < len(zip5_fallback):
                args['zip5'] = zip5_fallback[fallback_index]
            
        # If all fallbacks have been exhausted, restore original values and log a warning
        args['zip5'] = original_zip5
        logging.warning(f"All retry attempts and fallback locations exhausted for args: {args}")
        return [], label
    
    def _save_results(self, dic, effective_date):
        for k, v in dic.items():
            self._set_rate(k, v, effective_date)

    def _set_rate(self, key: str, value: Dict[str, Any], effective_date: str):
        # Use INSERT OR REPLACE with json_set to append to array
        self._execute_and_log(
            '''INSERT INTO rate_store (key, effective_date, value) 
               VALUES (?, ?, json(?))
               ON CONFLICT(key, effective_date) 
               DO UPDATE SET value = json_patch(
                   CASE 
                       WHEN value IS NULL THEN '{}' 
                       ELSE value 
                   END,
                   json(?)
               )''',
            (key, effective_date, json.dumps(value), json.dumps(value))
        )
    
    def _get_group_id(self, naic: str, state: str, location: str) -> int:
        cursor = self.conn.cursor()
        result = cursor.execute('''
            SELECT naic_group FROM group_mapping
            WHERE naic = ? AND state = ? AND location = ?
        ''', (naic, state, location)).fetchone()
        return result[0] if result else None

    def _get_rate(self, key: str, effective_date: str) -> Any:
        logging.info(f"Getting key: {key} for effective date: {effective_date}")
        cursor = self.conn.cursor()
        result = cursor.execute(
            'SELECT value FROM rate_store WHERE key = ? AND effective_date = ?', 
            (key, effective_date)
        ).fetchone()
        
        if result:
            # Parse the JSON string directly from the value column
            return json.loads(result[0])
        return None

    def _execute_and_log(self, query: str, params: Any = None, many: bool = False):
        cursor = self.conn.cursor()
        if many:
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params)
        
        if self.db_logger:
            self.db_logger.log_operation(
                'executemany' if many else 'execute',
                query,
                params
            )
        self.conn.commit()

    async def fetch_current_rates(self, state, zip_code, county, effective_date, naic_list = None):
        csg = self.cr
        query_data = {
            'zip5': zip_code,
            'county': county,
            'age': 65,
            'gender': 'M',
            'tobacco': 0,
            'effective_date': effective_date,
            'plan': 'G',  # Assuming Plan G, modify as needed
            'select': 0
        }
        if state == 'MN':
            query_data['plan'] = 'MN_BASIC'
        elif state == 'WI':
            query_data['plan'] = 'WI_BASE'
        elif state == 'MA':
            query_data['plan'] = 'MA_CORE'

        if naic_list is not None:
            query_data['naic'] = naic_list
    
        return await csg.fetch_quote(**query_data)
    
    async def get_naic_data(self, state, zip5, county, available_naics):
        existing_naics = self.get_existing_naics(state)
        selected_naics = [x["naic"] for x in self.get_selected_carriers()]
        naic_list = available_naics.intersection(selected_naics)

        nais_to_remove = existing_naics - available_naics
        for naic in nais_to_remove:
            pass #self.remove_naic(naic, state, include_rates=True)

        missing_naics = set()

        labels = {}
        for naic in naic_list:
            group_id = self._get_group_id(naic, state, zip5)
            if group_id is None:
                group_id = self._get_group_id(naic, state, county)
            if group_id is not None:
                labels[naic] = f"{state}:{naic}:{group_id}"
            else:
                logging.warning(f"No group id found for naic: {naic} in state: {state} and zip: {zip5}")
                missing_naics.add(naic)
        inv_labels = {v: k for k, v in labels.items()}
        naic_short_list = list(set(naic_list) - missing_naics)
        return labels, inv_labels, naic_list, missing_naics

    async def check_rate_changes(self, state, zip5=None, effective_date: str = None, available_naics: set = None):
        logging.info(f"Checking rate changes for state: {state}")
        
        # Get a random zip code for the state
        state_zips = [k for k, v in self.zip_holder.zip_states.items() if v == state]
        if not state_zips:
            logging.warning(f"No zip codes found for state: {state}")
            return
        
        random_zip = zip5 if zip5 else random.choice(state_zips)
        logging.info(f"Using zip: {random_zip}")
        matching_county = self.zip_holder.lookup_county(random_zip)[0]
        logging.info(f"matching_county: {matching_county}")
        
        # Get the carriers for this state
        if available_naics is None:
            available_naics = await self.get_available_naics(state, effective_date)

        labels, inv_labels, naic_list, missing_naics = await self.get_naic_data(state, random_zip, matching_county, available_naics)
        naic_short_list = list(set(naic_list) - missing_naics)

        current_rates = await self.fetch_current_rates(state, random_zip, matching_county, effective_date)
        current_rates = [q for q in current_rates if q.get('company_base', {}).get('naic') in naic_short_list]
        logging.info(f"number of rates: {len(current_rates)}")
        logging.info(f"zip / county: {random_zip} / {matching_county}")

        processed_quotes = [quote for q in current_rates for quote in process_quote(q, labels[q['company_base']['naic']])]

    
        if state == 'MN':
            compare_plan = 'MN_BASIC'
        elif state == 'WI':
            compare_plan = 'WI_BASE'
        elif state == 'MA':
            compare_plan = 'MA_CORE'
        else:
            compare_plan = 'G'

        rdic = {}
        for q in processed_quotes:
            dic = rdic.get(q['label'], {})
            if q['label']:
                q_key = f"{q['age']}:{q['gender']}:{q['plan']}:{q['tobacco']}"
                dic[q_key] = q
                rdic[q['label']] = dic

        copy_empty_rate_tasks = []
        for naic in naic_short_list:
            copy_empty_rate_tasks.append(self.copy_latest_rates(state, naic, effective_date))
        await asyncio.gather(*copy_empty_rate_tasks)

        stored_rates = {k: self._get_rate(k, effective_date) for k in rdic.keys()}
        sr = {}
        for k, dic in stored_rates.items():
            if dic is None:  # Handle case where no stored rates exist
                sr[k] = None
                continue
                
            test_case = dic.get("65:M:G:False")
            sr[k] = test_case


        s_dic = {}
        for k, v in rdic.items():
            stored_rate = rdic.get(k, {}).get(f"65:M:G:False")
            fetched_rate = sr.get(k)
            if stored_rate is None or fetched_rate is None:
                s_dic[inv_labels[k]] = True
            else:
                s_dic[inv_labels[k]] = fetched_rate['rate'] != stored_rate['rate']

        for naic in list(missing_naics):
            s_dic[naic] = True
 
        return rdic, sr, s_dic
    

    async def get_rates_for_date(self, state: str, naic: str, effective_date: str) -> Dict:
        """Get all rates for a given state/naic combination on a specific date"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT key, value 
            FROM rate_store 
            WHERE key LIKE ? AND effective_date = ?
        ''', (f"{state}:{naic}:%", effective_date))
        
        results = {}
        for key, value in cursor.fetchall():
            if value:  # Check if value exists and is not None
                results[key] = json.loads(value)
        return results

    async def copy_rates(self, state: str, naic: str, source_date: str, target_date: str) -> bool:
        """Copy rates from source_date to target_date for a given state/naic combination"""
        # Get rates from source date
        source_rates = await self.get_rates_for_date(state, naic, source_date)
        if not source_rates:
            logging.warning(f"No rates found to copy from {source_date} for {state} {naic}")
            return False

        # Copy rates to target date
        cursor = self.conn.cursor()
        for key, value in source_rates.items():
            self._execute_and_log(
                '''INSERT OR REPLACE INTO rate_store (key, effective_date, value)
                   VALUES (?, ?, ?)''',
                (key, target_date, json.dumps(value))
            )
        
        logging.info(f"Copied {len(source_rates)} rates from {source_date} to {target_date} for {state} {naic}")
        return True
    
    async def copy_latest_rates(self, state: str, naic: str, target_date: str, force: bool = False):
        # Check if target date data exists and is valid JSON
        if not force:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) 
                FROM rate_store 
                WHERE key LIKE ? 
                AND effective_date = ?
                AND json_valid(value)
            ''', (f"{state}:{naic}:%", target_date))
            
            if cursor.fetchone()[0] > 0:
                logging.info(f"Rates already exist for {state} {naic} on {target_date}")
                return True

        latest_rates = await self.get_most_recent_rates(state, naic)
        for key, value in latest_rates.items():
            self._execute_and_log(
                '''INSERT OR REPLACE INTO rate_store (key, effective_date, value)
                   VALUES (?, ?, ?)''',
                (key, target_date, json.dumps(value['rate_data']))
            )
            logging.info(f"Copied {key} rates from {value['effective_date']} to {target_date}")
        return True
            
    
    async def get_most_recent_rates(self, state: str, naic: str) -> Dict:
        """Get the most recent rates for each group_id for a given state/naic combination"""
        cursor = self.conn.cursor()
        cursor.execute('''
            WITH RankedRates AS (
                SELECT 
                    key,
                    value,
                    effective_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY replace(replace(key, ?, ''), ':', '') 
                        ORDER BY effective_date DESC
                    ) as rn
                FROM rate_store
                WHERE key LIKE ?
                AND json_valid(value)
            )
            SELECT key, value, effective_date
            FROM RankedRates 
            WHERE rn = 1
        ''', (f"{state}:{naic}:", f"{state}:{naic}:%"))
        
        results = {}
        for key, value, effective_date in cursor.fetchall():
            if value:  # Check if value exists and is not None
                results[key] = {
                    'rate_data': json.loads(value),
                    'effective_date': effective_date
                }
        return results

    def get_discount_category(self, naic: str) -> str:
        """Get discount category for a single NAIC."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT discount_category 
            FROM carrier_selection 
            WHERE naic = ?
        ''', (naic,))
        result = cursor.fetchone()
        return result[0] if result else None

    def get_discount_categories(self, naics: List[str]) -> Dict[str, str]:
        """Get discount categories for multiple NAICs.
        
        Args:
            naics: List of NAIC strings
            
        Returns:
            dict: Dictionary mapping NAICs to their discount categories
        """
        cursor = self.conn.cursor()
        # Using placeholders for the IN clause
        placeholders = ','.join('?' * len(naics))
        cursor.execute(f'''
            SELECT naic, discount_category 
            FROM carrier_selection 
            WHERE naic IN ({placeholders})
        ''', naics)
        return dict(cursor.fetchall())

def process_quote(q0, label):
    logging.info(f"Processing quote: {label}")
    quote = filter_quote(q0)
    if quote is None:
        return []
    gender = quote['gender']
    tobacco = quote['tobacco']
    age = quote['age']
    plan = quote['plan']
    rate = quote['rate']
    rate_mults = [1.0] + [x + 1 for x in quote['age_increases']]
    try:
        discount_mult = (1 - quote['discounts'][0].get('value'))
    except:
        discount_mult = 1
    ages = [age + i for i in range(len(rate_mults))]
    arr = []
    for i, age in enumerate(ages):  
        rate_value = round(rate * reduce(lambda x, y: x * y, rate_mults[:i + 1]), 2)
        discount_value = round(discount_mult * rate_value, 2)
        arr.append({
            'age': age,
            'gender': gender,
            'plan': plan,
            'tobacco': tobacco,
            'rate': rate_value,
            'discount_rate': discount_value,
            'label': label
        })
    return arr

def winnow_quotes(quotes):
    unique_quotes = {}
    for quote in quotes:
        key = (quote['age'], quote['gender'], quote['plan'], quote['tobacco'])
        if key in unique_quotes:
            if quote['rate'] > unique_quotes[key]['rate']:
                unique_quotes[key] = quote
        else:
            unique_quotes[key] = quote
    return list(unique_quotes.values())

def dic_build(flat_list):
    dic = {}
    for q in flat_list:
        label = q['label']
        arr = dic.get(label, [])
        arr.append(q)
        dic[label] = arr

    dic_out = {}
    for label, arr in dic.items():
        d = {}
        for q in arr:
            q_key = f"{q['age']}:{q['gender']}:{q['plan']}:{q['tobacco']}"
            d[q_key] = q
        dic_out[label] = d
    return dic_out
