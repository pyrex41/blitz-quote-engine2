import httpx
from httpx import ReadTimeout

from toolz.functoolz import pipe
from datetime import datetime, timedelta
import configparser
from copy import copy
import asyncio
from babel.numbers import format_currency

from config import Config

from aiocache import cached

import time
from pprint import pprint
import csv
import logging
import random


def process_st(state_counties):
  out = set()
  for x in state_counties:
    xx = x.replace('ST ', 'ST. ').replace('SAINT ', 'ST. ').replace('SAINTE', 'STE.')
    out.add(xx)
  return out

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from zips import zipHolder

# try to get current token

TIMEOUT = 60.0



lookup_dic = {}


def rate_limited(interval):

  def decorator(function):
    last_called = [0.0]

    def wrapper(*args, **kwargs):
      elapsed = time.time() - last_called[0]
      if elapsed >= interval:
        last_called[0] = time.time()
        fetch_sheet_and_export_to_csv()
      return function(*args, **kwargs)

    return wrapper

  return decorator


#fetch_sheet_and_export_to_csv()


def csv_to_dict(filename):
  with open(filename, 'r') as file:
    reader = csv.DictReader(file)
    result = {}
    for row in reader:
      # Convert 'Category' and 'ID' to integers
      row["Category"] = map_cat(row["Category"])  #int(row["Category"])
      # Check for null string key and filter it out
      if "" in row:
        del row[""]
      # Replace blank strings with None
      for key, value in row.items():
        if value == '':
          row[key] = None
      result[row["ID"]] = row
  return result


def map_cat(a_or_b: str):
  if a_or_b.lower() == "a":
    return 0
  elif a_or_b.lower() == "b":
    return 1
  else:
    return 2


class AsyncCSGRequest:

  def __init__(self, api_key):
    self.uri = 'https://csgapi.appspot.com/v1/'
    self.token_uri = "https://medicare-school-quote-tool.herokuapp.com/api/csg_token"
    self.api_key = api_key
    self.token = None  # Will be set asynchronously in an init method
    self.request_count = 0

  async def async_init(self):
    try:
      await self.set_token(await self.parse_token('token.txt'))
    except Exception as e:
      print(f"Could not parse token file: {e}")
      await self.set_token()

  async def parse_token(self, file_name):
    # Assuming the token file contains a section [token-config] with a token entry
    parser = configparser.ConfigParser()
    with open(file_name, 'r') as file:
      parser.read_file(file)
    return parser.get('token-config', 'token')

  async def set_token(self, token=None):
    self.token = token if token else await self.fetch_token()
    # Token is set, no need to write to a file unless it's a new token
    if not token:
      # Write the token to 'token.txt' asynchronously
      with open('token.txt', 'w') as f:
        f.write(f"[token-config]\ntoken={self.token}")

  async def fetch_token(self):
    async with httpx.AsyncClient() as client:
      resp = await client.get(self.token_uri)
    if resp.status_code == 200:
      token = resp.json().get("csg_token")
      logging.info(f"Fetched_token is {token}")
      await self.set_token(token)
      return token
    else:
      return await self.fetch_token_fallback()

  async def fetch_token_fallback(self):
    ep = 'auth.json'
    values = {'api_key': self.api_key}
    async with httpx.AsyncClient() as client:
      resp = await client.post(self.uri + ep, json=values)
      resp.raise_for_status(
      )  # Will raise an exception for 4XX and 5XX status codes
      token = resp.json()['token']
      logging.warn(f"Reset token via csg: {token}")
      return token

  def GET_headers(self):
    return {'Content-Type': 'application/json', 'x-api-token': self.token}

  async def reset_token(self):
    print('Resetting token asynchronously')
    await self.set_token(token=None)

  async def get(self, uri, params, full_response=False):
    async with httpx.AsyncClient(timeout=10.0) as client:
      resp = await client.get(uri, params=params, headers=self.GET_headers())
      if resp.status_code == 403:
        await self.reset_token()
        resp = await client.get(uri, params=params, headers=self.GET_headers())
      resp.raise_for_status(
      )  # Will raise an exception for 4XX and 5XX status codes
      self.request_count += 1
      return resp.json() if not full_response else resp

  async def get(self, uri, params, retry=3):
    for _ in range(retry):  # Retry up to 3 times
      try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT) as client:  # Increase timeout
          resp = await client.get(uri,
                                  params=params,
                                  headers=self.GET_headers())
          if resp.status_code == 403:
            await self.reset_token()
            resp = await client.get(uri,
                                    params=params,
                                    headers=self.GET_headers())
          resp.raise_for_status(
          )  # Will raise an exception for 4XX and 5XX status codes
          self.request_count += 1
          return resp.json()
      except ReadTimeout:
        print("Request timed out. Retrying...")
    raise Exception(f"Request failed after {retry} attempts")

  async def _fetch_pdp(self, zip5):
    ep = 'medicare_advantage/quotes.json'
    payload = {
        'zip5': zip5,
        'plan': 'pdp',
    }
    resp = await self.get(self.uri + ep, params=payload)
    return resp

  async def fetch_pdp(self, zip5, *years):
    resp = await self._fetch_pdp(zip5)
    try:
      return self.format_pdp(resp, *years)
    except Exception as ee:
      emsg = {
          'Plan Name': "ERROR",
          'Plan Type': str(ee),
          'State': "CA",
          'rate': format_currency(0, 'USD', locale='en_US'),
          'year': list(years)[0]
      }
      return [emsg]

  def format_pdp(self, pdp_results, *_years):
    out = []
    years = list(_years)
    if len(years) == 0:
      years.append(datetime.today().year)
    for pdpr in pdp_results:
      dt_format = "%Y-%m-%dT%H:%M:%SZ"
      st_dt = pdpr['effective_date']
      dt = datetime.strptime(st_dt, dt_format)
      info = {
          'Plan Name': pdpr['plan_name'],
          'Plan Type': pdpr['plan_type'],
          'State': pdpr['state'],
          'rate': format_currency(pdpr['month_rate'] / 100,
                                  'USD',
                                  locale='en_US'),
          'year': dt.year
      }
      out.append(info)
    fout = filter(lambda x: x['year'] in years, out)
    return list(fout)

  async def fetch_quote(self, **kwargs):
    acceptable_args = [
        'zip5', 'county', 'age', 'gender', 'tobacco', 'plan', 'select',
        'effective_date', 'apply_discounts', 'apply_fees', 'offset', 'naic'
    ]
    payload = {}

    if 'retry' in kwargs:
      retry = kwargs.pop('retry')
    else:
      retry = 3

    for arg_name, val in kwargs.items():
      lowarg = arg_name.lower()
      if lowarg in acceptable_args:
        payload[lowarg] = val
    payload['apply_discounts'] = int(payload.get('apply_discounts', 0))

    ep = 'med_supp/quotes.json'
    resp = await self.get(self.uri + ep, params=payload, retry=retry)
    return resp

  async def fetch_advantage(self, **kwargs):
    acceptable_args = [
        'zip5', 'state', 'county', 'plan', 'offset', 'effective_date', 'sort',
        'order'
    ]
    payload = {}

    for arg_name, val in kwargs.items():
      lowarg = arg_name.lower()
      if lowarg in acceptable_args:
        payload[lowarg] = val

    if 'zip5' not in kwargs:
      raise ValueError("The 'zip5' argument is required.")

    ep = 'medicare_advantage/quotes.json'
    resp = await self.get(self.uri + ep, params=payload)
    return resp

  @rate_limited(3600)
  def format_rates(self, quotes, household):
    dic = {}
    for i, q in enumerate(quotes):
      rate = int(q['rate']['month'])
      naic = q['company_base']['naic']
      company_name = q['company_base']['name']
      plan = q['plan']

      if q['select']:
        k = company_name + ' // Select'
      else:
        k = company_name
      qq = q['rating_class']
      if qq:

        kk = k + ' // ' + q['rating_class']
      else:
        kk = k

      # workaround for those carriers in CSG that have multiple entries to handle discounts
      # may need something better if there's other reasons for multipe naic codes -- would require a rewrite
      arr = dic.get(naic, [])
      cat = 2
      disp = kk

      name_dict = csv_to_dict('cat.csv')

      ddic = name_dict.get(naic)
      if ddic:
        sub = False
        i = 1
        while i < 10:
          s = str(i)
          if ddic.get(s):
            sval = ddic[s]
            if sval.lower() in kk.lower():
              naic = f"{naic}00{s}"
              disp = f"{ddic.get('Name')} // {ddic.get(s, '').capitalize()}"
              cat = 1
              sub = True
              break
          i += 1
        if not sub:
          cat = ddic.get("Category", 2)
          disp = ddic.get("Name", kk)

      arr.append({
          "fullname": kk,
          "rate": rate,
          "naic": naic,
          "category": cat,
          "display": disp
      })
      dic[naic] = arr

    # continued workaround for carriers in CSG that don't handle household correctly
    d = []
    for a in dic.values():
      if len(a) == 1:  # this is the way it should work but CSG is pretty lame
        if bool(household):
          d = d + a
        else:
          # handling an edge case for Allstate where it returns a single "Rooommate" but doesn't put household in the fields
          a_filt = list(
              filter(lambda x: has_household(x) == bool(household), a))
          if len(a_filt) < len(a):
            d = d + a_filt
          else:
            d = d + a
      else:
        # what about the case(s) where len(2) but they actually aren't putting household in the fields? Trying to handle that here
        a_filt = list(filter(lambda x: has_household(x) == bool(household), a))
        if len(a_filt) < len(a):
          a_add = a_filt
        else:
          a_add = a

        a_add = sorted(a_add, key=lambda x: "//" in x["fullname"])
        if len(a_add) > 1:
          for i in range(1, len(a_add)):
            a_add[i]["category"] = 1  # category 1 for anything after the first

        d = d + a_add

    slist = sorted(d, key=lambda x: x["rate"])
    out_list = []
    for dic in slist:
      out_list.append({
          'company': dic["fullname"],
          'rate': dic["rate"] /
          100,  #format_currency(dic["rate"]/100, 'USD', locale='en_US'),
          'naic': dic["naic"],
          'plan': plan,
          'category': dic["category"],
          'display': dic["display"],
          'type': f'Supplemental',
      })
    return out_list

  def filter_quote(self,
                   quote_resp,
                   household=False,
                   custom_naic=None,
                   select=False):

    try:
      fresp = list(filter(lambda x: x['select'] == False,
                          quote_resp)) if not select else quote_resp
    except Exception as e:
      logging.error(f"Error in filter_quote: {str(e)}")
      raise

    if custom_naic:
      return pipe(
          list(
              filter(lambda x: int(x['company_base']['naic']) in custom_naic,
                     fresp)), self.format_rates)
    else:
      return self.format_rates(fresp, household=household)

  def format_results(self, results):
    logging.info(results)
    plan_dict = {}
    for r in results:
      for ol in r:
        plan = ol['plan']
        arr = plan_dict.get(plan, [])
        arr.append({
            'company': ol['company'],
            'rate': ol['rate'],
            'naic': ol['naic'],
            'category': ol['category'],
            'display': ol['display']
        })
        plan_dict[plan] = arr
    return plan_dict

  async def load_response_inner(self, query_data, delay=None):
    if delay:
      await asyncio.sleep(delay)
      print("Sleeiping ", delay)
    resp = await self.fetch_quote(**query_data, retry=4)
    return resp

  async def load_response_all_inner(self, query_data, delay=None):
    results = {}
    plans_ = query_data.pop('plan')
    tasks = []
    p_actual = []
    for p in ['A', 'B', 'C', 'D', 'F', 'G', 'HDF', 'HDG', 'K', 'L', 'M', 'N']:
      if p in plans_:
        p_actual.append(p)
    for i, p in enumerate(p_actual):
      qu = copy(query_data)
      qu['plan'] = p
      tasks.append(self.load_response_inner(qu, delay))

    for task in asyncio.as_completed(tasks):
      result = await task
      results.update(result)

    return results  # self.format_results(results)

  @cached(ttl=36_000)
  async def load_response_all(self, query_data, delay=None):
    return await self.load_response_all_inner(query_data, delay=delay)

  @cached(ttl=36_000)
  async def load_response(self, query_data, delay=None):
    return await self.load_response_inner(query_data, delay=delay)

  async def get_companies(self):
    uri = self.uri + "medicare_advantage/open/companies.json"
    resp = await self.get(uri, {})
    return resp

  async def calc_counties(self, state):
    zips = zipHolder('static/uszips.csv')
    state_zips = [k for (k, v) in zips.zip_states.items() if v == state]
    state_zip_county = []

    for z in state_zips:
      counties = zips.lookup_county(z) 

      try:
        for c in counties:
          if c != 'None':
            state_zip_county.append((z, c))
          else:
            logging.warn(f"County for {z} is {c}")
      except Exception as ee:
        logging.warn(ee)

    lookup_dict = {
        '20699': {},  # Ace / Chubb
        '72052': {},  # Aetna
        '79413': {},  # UHC
    }

    for tup in state_zip_county:
      z, county = tup
      dt = (datetime.now() +
            timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')
      for k in lookup_dict.keys():
        if z in lookup_dict[k].keys():
          continue
        else:
          logging.info(f"submitting query for zip: {z} - naic: {k}")
          r = await self.fetch_quote(zip5=int(z),
                                     county=county,
                                     age=65,
                                     gender="M",
                                     tobacco=0,
                                     effective_date=dt,
                                     naic=k,
                                     plan="N")
          zbase = r[0]['location_base']['zip5']
          logging.info(f"{len(zbase)} zips for {k}")
          zip_zero = next((z for z in zbase if z in state_zips), None)
          for i in zbase:
            lookup_dict[k][i] = zip_zero

    stats = {'total_zips': len(state_zips)}
    for k, v in lookup_dict.items():
      stats[k] = len(v)
      stats[f"{k}-unique"] = len(set(v))
    out = {}
    for kk, vdic in lookup_dict.items():
      outmap = {v: i for i, v in enumerate(list(set(vdic.values())))}
      d = {k: outmap[v] for k, v in vdic.items()}
      out[kk] = d

    return out, stats

  async def calc_counties2(self, state):
    zips = zipHolder('static/uszips.csv')
    state_zips = [k for (k, v) in zips.zip_states.items() if v == state]
    state_zip_county = []

    for z in state_zips:
      counties = zips.lookup_county(z)

      try:
        for c in counties:
          if c != 'None':
            state_zip_county.append((z, c))
          else:
            logging.warn(f"County for {z} is {c}")
      except Exception as ee:
        logging.warn(ee)
        
    #from lookup_index import lookup_dic
    lookup_list = { k[0]: [] for k in lookup_dic.get(state, [])}
    flat_list = { k[0]: [] for k in lookup_dic.get(state, [])}

    for tup in state_zip_county:
      z, county = tup
      dt = (datetime.now() +
            timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')
      for k in lookup_list:
        if z not in flat_list[k]:
          try:
            rr = await self.fetch_quote(zip5=int(z),
                                        county=county,
                                        age=65,
                                        gender="M",
                                        tobacco=0,
                                        effective_date=dt,
                                        naic=k,
                                        plan="N")
            #naic_filter = lambda x: x['company_base']['naic'] == k
            #rr = list(filter(naic_filter, rrr))
            if len(rr) > 0:
              x = rr[0]
              zbase = x['location_base']['zip5']
              kk = x['company_base']['naic']
              logging.info(f"{len(zbase)} zips for {kk}")
              if kk not in flat_list:
                flat_list[kk] = []
                lookup_list[kk] = []
              lookup_list[kk].append(zbase)
              flat_list[kk] = list(set(flat_list[kk] + zbase))
              logging.info(f"{len(flat_list[kk])} zips for {kk} -- {z}")
            else:
              logging.warn(f"No results for {z} - {county}")
          except Exception as ee:
            logging.warn(f"No results for {z} - {county} -- {ee}")
    for k in lookup_list:
      logging.info(f"{k} has {len(lookup_list[k])} zip regions")

    return lookup_list
    
  async def calc_naic_map_zip(self, state, naic, first_result=None):
    if state == 'WY' and naic == '82538': # workaround for weirdness
      logging.warn(f"{naic} skipped by workaround")
      return []
    zips = zipHolder('static/uszips.csv')
    state_zips = [k for (k, v) in zips.zip_states.items() if v == state]

    lookup_list = []
    processed_zips = set()

    zero_count = 0
    random.shuffle(state_zips)

    params = {
      "zip5": state_zips[0],
      "age": 65,
      "gender": "M",
      "tobacco": 0,
      "effective_date": (datetime.now() + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d'),
      "naic": naic,
    }
    if state not in ['MN', 'WI', 'MA', 'NY']:
      params["plan"] = "G"

    first_result = await self.fetch_quote(**params) if first_result is None else first_result 
    if len(first_result) == 0:
      logging.warn(f"No results for {naic} in {state}. The plan may not be offered in this state.")
      return []
    else:
      zbase = set(first_result[0]['location_base']['zip5'])
      logging.info(f"{len(zbase)} zips for {naic}")
      lookup_list.append(zbase)
      processed_zips.update(zbase)

    for z in state_zips:
      if z not in processed_zips:
        try:
          params['zip5'] = z
          rr = await self.fetch_quote(**params)
          if len(rr) > 0:
            x = rr[0]
            zbase = set(x['location_base']['zip5'])
            logging.info(f"{len(zbase)} zips for {naic}")
            
            # Check if zbase is already in lookup_list
            existing_index = next((i for i, existing_zbase in enumerate(lookup_list) if existing_zbase == zbase), None)
            if existing_index is not None:
                # Replace existing zbase
                zbase.add(z)
                lookup_list[existing_index] = zbase
            else:
                # Append new zbase
                zbase.add(z)
                lookup_list.append(zbase)

            processed_zips.update(zbase)

            if len(zbase) == 0:
              zero_count += 1
              if zero_count > 30:
                logging.warn(f"{naic} has {zero_count} zero regions -- exiting")
                return []
          else:
            logging.warn(f"No results for {z}")
        except Exception as ee:
          logging.warn(f"No results for {z} -- {ee}")

    logging.info(f"{naic} has {len(lookup_list)} zip regions")

    return lookup_list
  
  async def calc_humana_workaround(self, state_counties, sc_dict, processed_counties, params, zips, list_of_groups):
    group_extra = set()
    p_state_counties = process_st(state_counties)

    city_items = set()
    for county in p_state_counties:
      if county not in processed_counties:
        try:
          county_zip = next(z for z in sc_dict.get(county, []))
          logging.info(f"county_zip: {county_zip}")
          params['zip5'] = county_zip

          if county_zip is None:
            logging.warn(f"No 1:1 zip code found for county: {county}")
            zip_to_use = zips.lookup_zip(county)
            params['zip5'] = zip_to_use
            params['county'] = county
          elif 'county' in params:
            params.pop('county')
          
          rr = await self.fetch_quote(**params)
          
          if len(rr) > 0:
            x = rr[0]
            county_base_raw = set(x['location_base']['county'])
            county_base_raw = process_st(county_base_raw)

            for x in list(county_base_raw):
              if x.endswith(' CITY'): 
                city_items.add(x[:-5])
              else:
                x_in = [x in group for group in list_of_groups]
                if not any(x_in):
                  group_extra.add(x)
                processed_counties.add(x)
          else:
            logging.warn(f"No results for {county_zip} - {county}")
        except Exception as ee:
          logging.warn(f"Error processing {county}: {ee}")
    return list_of_groups + [group_extra]
  
  
  async def calc_naic_map_county(self, state, naic, first_result=None):
    if state == 'WY' and naic == '82538':  # workaround for weirdness
        logging.warn(f"{naic} skipped by workaround")
        return []

    zips = zipHolder('static/uszips.csv')
    state_zips = [k for (k, v) in zips.zip_states.items() if v == state]
    state_counties = set()

    single_county_zips = set()
    sc_dict = {}

    for z in state_zips:
        counties = zips.lookup_county2(z)
        state_counties.update(counties)
        if len(counties) == 1:
            single_county_zips.add(z)
            zz = sc_dict.get(counties[0], [])
            zz.append(z)
            sc_dict[counties[0]] = zz

    


    # Shuffle the single_county_zips list
    single_county_zips = list(single_county_zips)
    random.shuffle(single_county_zips)

    state_counties.discard('None')
    state_counties.discard(None)

    # Log counties that aren't keys in sc_dict
    counties_not_in_sc_dict = state_counties - set(sc_dict.keys())
    for county in counties_not_in_sc_dict:
        logging.warn(f"County not found in sc_dict: {county}; no 1:1 zip code found")

    processed_counties = set()
    processed_zips = set()

    zero_count = 0

    lookup_list = []

    params = {
      "zip5": single_county_zips[0],
      "age": 65,
      "gender": "M",
      "tobacco": 0,
      "effective_date": (datetime.now() + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d'),
      "naic": naic,
    }
    if state not in ['MN', 'WI', 'MA', 'NY']:
      params["plan"] = "G"

    first_result = await self.fetch_quote(**params) if first_result is None else first_result 
    
    city_items = set()
    if len(first_result) == 0:
      logging.warn(f"No results for {naic} in {state}. The plan may not be offered in this state.")
      return []
    elif len(first_result[0]['location_base']['county']) == 0:
      logging.warn(f"This state/naic does not support county mapping: {state}/{naic}")
      return []
    else:
      county_base_raw = set(first_result[0]['location_base']['county'])
      logging.info(f"{len(county_base_raw)} counties for {naic}")
      
      for x in list(county_base_raw):
        if x.endswith(' CITY'): # VA workaround
          city_items.add(x[:-5])
        if state == 'FL':
          if x == 'SAINT JOHNS': # FL workaround
            city_items.add(x)
            county_base_raw.add('ST. JOHNS')

      county_base = county_base_raw - city_items
      

      
      county_base = process_st(county_base_raw)

      lookup_list.append(county_base)
      processed_counties.update(county_base)
      logging.info(f"{len(processed_counties)} counties processed for {naic}")


    
      # workaround for HUMANA LA
      if state == 'LA' and naic in ['73288', '60984', '60052']:
        group1 = set([
          'JEFFERSON',
          'ORLEANS',
          'PLAQUEMINES',
          'ST. BERNARD',
          'ST. CHARLES',
          'ST. TAMMANY',
          'WASHINGTON',
        ])
        return await self.calc_humana_workaround(state_counties, sc_dict, processed_counties, params, zips, [group1])
      
      # workaround for HUMANA AL
      if state in ['AL', 'MD', 'AK'] and naic in ['73288', '60984', '60052', '88595', '60219']:
        return await self.calc_humana_workaround(state_counties, sc_dict, processed_counties, params, zips, [])
      
      if state == 'TX' and naic in ['73288', '60984', '60052', '88595', '60219']:
        group1 = set([
            'AUSTIN', 'BAILEY', 'BRAZORIA', 'CHAMBERS', 'COLORADO', 'FORT BEND', 'GALVESTON', 'HARDIN', 
            'HARRIS', 'JEFFERSON', 'LIBERTY', 'MATAGORDA', 'MONTGOMERY', 'ORANGE', 'SAN JACINTO', 'WALKER', 
            'WALLER', 'WASHINGTON', 'WHARTON'
        ])
        group2 = set([
            'ANDREWS', 'ARANSAS', 'BEE', 'BORDEN', 'BROOKS', 'CALHOUN', 'CAMP', 'CLAY', 'COLLIN', 'COMAL', 
            'COOKE', 'CRANE', 'DALLAS', 'DELTA', 'DENTON', 'DEWITT', 'DUVAL', 'ECTOR', 'ELLIS', 'FANNIN', 
            'FRANKLIN', 'GLASSCOCK', 'GRAYSON', 'GUADALUPE', 'HOPKINS', 'HOWARD', 'HUNT', 'JACKSON', 
            'JEFF DAVIS', 'JIM HOGG', 'JIM WELLS', 'KARNES', 'KAUFMAN', 'KENEDY', 'KLEBERG', 'LAMAR', 
            'LAVACA', 'LOVING', 'MARTIN', 'MIDLAND', 'MONTAGUE', 'NAVARRO', 'NUECES', 'PECOS', 'RAINS', 
            'RED RIVER', 'REEVES', 'REFUGIO', 'ROCKWALL', 'SAN PATRICIO', 'TITUS', 'UPTON', 'VAN ZANDT', 
            'VICTORIA', 'WARD', 'WILSON', 'WINKLER', 'WOOD'
        ])
        return await self.calc_humana_workaround(state_counties, sc_dict, processed_counties, params, zips, [group1, group2])

      if state == 'IL' and naic in ['73288', '60984', '60052', '88595', '60219']:
        group1 = set([
          'COOK', 'DEKALB', 'DUPAGE', 'GRUNDY', 'KANE', 'KENDALL', 'LASALLE', 'LAKE', 
          'LIVINGSTON', 'MCHENRY', 'WILL'
        ])
        group2 = set([
          'BOND', 'CALHOUN', 'CHAMPAIGN', 'CLINTON', 'FORD', 'GREENE', 'IROQUOIS', 
          'JERSEY', 'KANKAKEE', 'MACOUPIN', 'MADISON', 'MONROE', 'MONTGOMERY', 
          'PERRY', 'RANDOLPH', 'ST. CLAIR', 'WASHINGTON'
        ])
        return await self.calc_humana_workaround(state_counties, sc_dict, processed_counties, params, zips, [group1, group2])
      
      # workaround for HUMANA MO
      if state == 'MO' and naic in ['73288', '60984', '60052', '88595', '60219']:
        group1 = set([
            'ADAIR', 'BATES', 'CLAY', 'COLE', 'JACKSON', 'JEFFERSON', 'LINCOLN',
            'MARIES', 'OSAGE', 'PLATTE', 'RANDOLPH', 'RAY', 'ST. CHARLES',
            'ST. LOUIS', 'SCHUYLER',
        ])
        group2 = set([
            'ANDREW', 'AUDRAIN', 'BARTON', 'BOONE', 'BUCHANAN', 'CALDWELL',
            'CAMDEN', 'CASS', 'CLARK', 'CLINTON', 'DAVIESS', 'GENTRY',
            'HICKORY', 'JASPER', 'KNOX', 'LAFAYETTE', 'MACON', 'MARION',
            'MILLER', 'MONITEAU', 'NEWTON', 'PHELPS', 'PIKE', 'PULASKI',
            'PUTNAM', 'RALLS', 'ST. CLAIR', 'ST. FRANCOIS', 'STE. GENEVIEVE',
            'SCOTLAND', 'SULLIVAN', 'TANEY', 'WARREN', 'WASHINGTON',
        ])
        return await self.calc_humana_workaround(state_counties, sc_dict, processed_counties, params, zips, [group1, group2])
      # workaround for HUMANA FL
      if state == 'FL' and naic in ['73288', '60984', '60052', '88595', '60219']:
        group1 = set([
          'BROWARD',
          'MIAMI-DADE',
          'PALM BEACH',
        ])
        group2 = set([
          'BAKER',
          'BAY',
          'BREVARD',
          'CHARLOTTE',
          'CLAY',
          'COLLIER',
          'DUVAL',
          'HERNANDO',
          'HILLSBOROUGH',
          'INDIAN RIVER',
          'LAKE',
          'LEE',
          'MANATEE',
          'MARTIN',
          'NASSAU',
          'OKALOOSA',
          'ORANGE',
          'OSCEOLA',
          'PASCO',
          'PINELLAS',
          'ST. JOHNS',
          'ST. LUCIE',
          'SARASOTA',
          'SEMINOLE',
          'VOLUSIA'
        ])
        return await self.calc_humana_workaround(state_counties, sc_dict, processed_counties, params, zips, [group1, group2])
      
      if state == 'MI' and naic in ['73288', '60984', '60052', '88595', '60219']:
        return []
      if state == 'MI' and naic in ['60984']:
        group1 = set([
          'GRATIOT',
          'MACOMB',
          'OAKLAND',
          'WAYNE',
        ])
        group2 = set([
          'ALPENA',
          'ARENAC',
          'BAY',
          'BRANCH',
          'CALHOUN',
          'CLARE',
          'CRAWFORD',
          'GENESEE',
          'GLADWIN',
          'INGHAM',
          'ISABELLA',
          'JACKSON',
          'LAPEER',
          'LIVINGSTON',
          'LUCE',
          'MANISTEE',
          'MONROE',
          'MONTCALM',
          'MONTMORENCY',
          'ROSCOMMON',
          'SAGINAW',
          'SANILAC',
          'SHIAWASSEE',
          'ST. CLAIR',
          'TUSCOLA',
          'WASHTENAW'
        ])
        return await self.calc_humana_workaround(state_counties, sc_dict, processed_counties, params, zips, [group1, group2])
      
      for county in state_counties:
        if county not in processed_counties and county not in city_items:
            logging.info(f"Processing county: {county}")
            try:
                # Find a zip code for this county
                county_zip = next(z for z in sc_dict.get(county, []))
                logging.info(f"county_zip: {county_zip}")
                params['zip5'] = county_zip

                if county_zip is None:
                  logging.warn(f"No 1:1 zip code found for county: {county}")
                  zip_to_use = zips.lookup_zip(county)
                  params['zip5'] = zip_to_use
                  params['county'] = county
                elif 'county' in params:
                  params.pop('county')
                
                rr = await self.fetch_quote(**params)
                
                if len(rr) > 0:
                    x = rr[0]
                    county_base_raw = set(x['location_base']['county'])
                    if state == 'LA':
                      county_base_raw = process_st(county_base_raw)

                    cbr_i = list(county_base_raw)
                    for x in cbr_i:
                      if x.endswith(' CITY'):
                          city_items.add(x[:-5])
                      if state == 'FL':
                        if x == 'SAINT JOHNS':
                          city_items.add(x)
                          county_base_raw.add('ST. JOHNS')
                        if x == 'SAINT LUCIE':
                          city_items.add(x)
                          county_base_raw.add('ST. LUCIE')

                          
                    county_base = county_base_raw - city_items

                    logging.info(f"{len(county_base)} counties for {naic}")
                    
                    # Check if county_base is already in lookup_list
                    existing_index = next((i for i, s in enumerate(lookup_list) if s == county_base), None)
                    if existing_index is not None:
                        # Replace the existing set with the new one
                        county_base.add(county)
                        lookup_list[existing_index] = county_base
                    else:
                        # If not found, append the new set
                        county_base.add(county)
                        lookup_list.append(county_base)
                    
                    processed_counties.update(county_base)
                    logging.info(f"{len(processed_counties)} counties processed for {naic} -- {county}")
                    
                    if len(county_base) == 0:
                        zero_count += 1
                        if zero_count > 5:
                            logging.warn(f"{naic} has {zero_count} zero regions -- exiting")
                            return []
                else:
                    logging.warn(f"No results for {county_zip} - {county}")
            except Exception as ee:
                logging.warn(f"Error processing {county}: {ee}")

    logging.info(f"{naic} has {len(lookup_list)} county regions")

    # Check for missing counties
    missing_counties = set(state_counties) - processed_counties
    if missing_counties:
        logging.warn(f"Missing counties for NAIC {naic} in state {state}: {', '.join(sorted(missing_counties))}")
    
    # Check coverage percentage
    coverage_percentage = (len(processed_counties) / len(state_counties)) * 100
    logging.info(f"Coverage for NAIC {naic} in state {state}: {coverage_percentage:.2f}% ({len(processed_counties)}/{len(state_counties)} counties)")

    if coverage_percentage < 95:
        logging.warn(f"Low coverage ({coverage_percentage:.2f}%) for NAIC {naic} in state {state}")

    return lookup_list
  
  async def calc_naic_map_combined2(self, state, naic, effective_date = None):
     zips = zipHolder('static/uszips.csv')
     state_zips = [k for (k, v) in zips.zip_states.items() if v == state]
     random.shuffle(state_zips)

     if effective_date is None:
        effective_date = (datetime.now() + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')

     params = {
      "zip5": state_zips[0],
      "age": 65,
      "gender": "M",
      "tobacco": 0,
      "effective_date": effective_date,
      "naic": naic,
     }

     try:
        first_result = await self.fetch_quote(**params)
        if len(first_result) == 0:
          logging.warn(f"No results for {naic} in {state}. The plan may not be offered in this state.")
          out = ([], None)
        elif len(first_result[0]['location_base']['zip5']) > 0:
          res = await self.calc_naic_map_zip(state, naic, first_result)
          out = (res, 'zip5') 
        elif len(first_result[0]['location_base']['county']) > 0:
          res = await self.calc_naic_map_county(state, naic, first_result)
          out = (res, 'county')
        else:
          logging.warn(f"This state/naic does not support combined mapping: {state}/{naic}")
          out = ([], None)
     except Exception as ee:
        logging.warn(f"EXA -- Error in initial query for {state}, {naic}: {ee}")
        out = ([], None)
     lookup_list0, mapping_type = out  
     lookup_list = sorted(lookup_list0, key = len, reverse = True)
     return lookup_list, mapping_type

  async def calc_naic_map_combined(self, state, naic):
    zips = zipHolder('static/uszips.csv')
    state_zips = [k for (k, v) in zips.zip_states.items() if v == state]
    state_counties = set()

    for z in state_zips:
        counties = zips.lookup_county(z)
        state_counties.update(counties)

    state_counties.discard('None')
    state_counties = list(state_counties)

    lookup_list = []
    processed_items = set()
    mapping_type = None

    zero_count = 0
    max_zero_count = 10
    max_error_count = 10
    error_count = 0
    consecutive_empty_results = 0
    max_consecutive_empty_results = 5

    dt = (datetime.now() + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')

    # Shuffle zips and counties to avoid always starting with the same one
    random.shuffle(state_zips)
    random.shuffle(state_counties)


    # Try to determine mapping type
    for initial_item in state_zips + state_counties:
        try:
            if initial_item in state_zips:
                initial_county = random.choice(zips.lookup_county(initial_item))
                quote_args = {
                    "zip5": initial_item,
                    "county": initial_county,
                    "age": 65,
                    "gender": "M",
                    "tobacco": 0,
                    "effective_date": dt,
                    "naic": naic
                }
                if state not in ['MN', 'WI', 'MA', 'NY']:
                    quote_args["plan"] = "G"

                pprint(quote_args)
                rr = await self.fetch_quote(**quote_args)
            else:  # It's a county
                initial_zip = next(z for z in state_zips if initial_item in zips.lookup_county(z))
                quote_args = {
                    "zip5": initial_zip,
                    "county": initial_item,
                    "age": 65,
                    "gender": "M",
                    "tobacco": 0,
                    "effective_date": dt,
                    "naic": naic
                }
                if state not in ['MN', 'WI', 'MA', 'NY']:
                    quote_args["plan"] = "G"
                rr = await self.fetch_quote(**quote_args)
            
            if len(rr) > 0:
                consecutive_empty_results = 0  # Reset counter on successful result
                x = rr[0]
                if 'zip5' in x['location_base'] and x['location_base']['zip5']:
                    mapping_type = 'zip5'
                    items_to_process = state_zips
                elif 'county' in x['location_base'] and x['location_base']['county']:
                    mapping_type = 'county'
                    items_to_process = state_counties
                else:
                    continue  # Try next item if we can't determine mapping type

                base_items = x['location_base'][mapping_type]
                logging.info(f"{len(base_items)} {mapping_type}s for {naic}")
                lookup_list.append(base_items)
                processed_items.update(base_items)
                break  # We've successfully determined the mapping type, exit the loop
            else:
                consecutive_empty_results += 1
                if consecutive_empty_results >= max_consecutive_empty_results:
                    logging.warn(f"No results found for {naic} in {state} after {consecutive_empty_results} attempts. Aborting.")
                    return [], None
            
        except Exception as ee:
            logging.warn(f"EX -- Error in initial query for {state}, {initial_item}: {ee}")
            error_count += 1
            if error_count > max_error_count:
                logging.error(f"Max error count reached for {state}, {naic}. Aborting.")
                return [], None

    if not mapping_type:
        logging.error(f"Unable to determine mapping type for {naic} in {state}")
        return [], None

  
    city_items = []
    for item in items_to_process:
        if item not in processed_items and item not in city_items:
            print(f"Processing item: {item}")
            try:
                if mapping_type == 'zip5':
                    county = random.choice(zips.lookup_county(item))
                    if state in ['MN', 'WI', 'MA', 'NY']:
                        rr = await self.fetch_quote(zip5=item,
                                                    county=county,
                                                    age=65,
                                                    gender="M",
                                                    tobacco=0,
                                                    effective_date=dt,
                                                    naic=naic)
                    else:
                        rr = await self.fetch_quote(zip5=item,
                                                    county=county,
                                                    age=65,
                                                    gender="M",
                                                    tobacco=0,
                                                    effective_date=dt,
                                                    naic=naic,
                                                    plan="G")
                else:  # county
                    #if naic == '25178' and categorize_county(item) == 'GENERAL_GROUP' and results['GENERAL_GROUP']:
                        #continue
                    
                    county_zip = next(z for z in state_zips if item in zips.lookup_county(z))
                    if state in ['MN', 'WI', 'MA', 'NY']:
                        rr = await self.fetch_quote(zip5=county_zip,
                                                    county=item,
                                                    age=65,
                                                    gender="M",
                                                    tobacco=0,
                                                    effective_date=dt,
                                                    naic=naic)
                    else:
                        rr = await self.fetch_quote(zip5=county_zip,
                                                    county=item,
                                                    age=65,
                                                    gender="M",
                                                    tobacco=0,
                                                    effective_date=dt,
                                                    naic=naic,
                                                    plan="G")

                if len(rr) > 0:
                    consecutive_empty_results = 0  # Reset counter on successful result
                    x = rr[0]
                    base_items = x['location_base'][mapping_type]

                    if base_items:
                        if mapping_type == 'county':
                          print("base_items", base_items)
                          for x in base_items:
                              if x.endswith(' CITY'):
                                  city_name = x[:-5]  # Remove ' CITY' from the end
                                  city_items.append(city_name)
                        lookup_list.append(base_items)
                        processed_items.update(base_items)
                        logging.info(f"{len(processed_items)} {mapping_type}s processed for {naic} -- {item}")
                    else:
                        zero_count += 1
                        if zero_count > max_zero_count:
                            logging.warn(f"{naic} has {zero_count} zero regions -- exiting")
                            break
                else:
                    consecutive_empty_results += 1
                    if consecutive_empty_results >= max_consecutive_empty_results:
                        logging.warn(f"No results found for {naic} in {state} after {consecutive_empty_results} consecutive attempts. Aborting.")
                        break
                    logging.warn(f"No results for {item}")

            except Exception as ee:
                logging.warn(f"Error processing {item}: {ee}")
                error_count += 1
                if error_count > max_error_count:
                    logging.error(f"Max error count reached for {state}, {naic}. Aborting.")
                    break



    if not lookup_list:
        logging.warn(f"No data found for {naic} in {state}. The plan may not be offered in this state.")
        return [], None
    # Remove duplicate sublists from lookup_list
    unique_lookup_list = []
    seen = set()
    for sublist in lookup_list:
        # Convert the sublist to a tuple so it can be hashed
        sublist_tuple = tuple(sorted(sublist))
        if sublist_tuple not in seen:
            seen.add(sublist_tuple)
            unique_lookup_list.append(sublist)

    lookup_list = unique_lookup_list
    logging.info(f"{naic} has {len(lookup_list)} {mapping_type} regions")
    return lookup_list, mapping_type

def has_household2(xx):
  x = xx["name"]
  rating_class = xx["rating_class"]
  if rating_class:
    kk = x + ' // ' + rating_class
  else:
    kk = x
  nm = kk.lower()
  # Load name_dict from cat.csv
  name_dict = csv_to_dict('cat.csv')

  nm_list = set(
      [x['Household'].lower() for x in name_dict.values() if x['Household']])
  for x in nm_list:
    if x in nm:
      return True, kk
  return False, kk


def has_household(x):
  kk = x["fullname"]
  nm = kk.lower()
  # Load name_dict from cat.csv
  name_dict = csv_to_dict('cat.csv')

  nm_list = set(
      [x['Household'].lower() for x in name_dict.values() if x['Household']])
  for x in nm_list:
    if x in nm:
      return True
  return False


# Example usage
async def main():
  csg = AsyncCSGRequest(Config.API_KEY)
  await csg.async_init()
  # Example of making a request
  query_data = {
      'zip5': '23060',
      'gender': 'M',
      'age': 65,
      'county': 'HENRICO',
      'tobacco': 0,
      'effective_date': '2024-10-01',
      'plan': 'N',
      'naic': '25178'
  }
  response = await csg.load_response_all(query_data, delay=.2)
  return response


""" 
# Run the async main function           
r = lambda : asyncio.run(main())
import time
import statistics

from tqdm import tqdm

run_times = []
for _ in tqdm(range(20)):
    start_time = time.time()
    r()
    run_times.append(time.time() - start_time)

print("--- Min: %s seconds ---" % min(run_times))
print("--- Median: %s seconds ---" % statistics.median(run_times))
print("--- Max: %s seconds ---" % max(run_times))   
print("--- Mean: %s seconds ---" % statistics.mean(run_times))
"""
