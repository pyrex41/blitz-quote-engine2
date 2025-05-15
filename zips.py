# zips.py
import json

class zipHolder():

    def __init__(self, file_name=None):
        # Ignore file_name parameter, always use zip_data.json
        self.load_zips("/Users/reuben/blitz-quote-engine2/static/zip_data.json")

    def __call__(self, zip5, show_state=False):
        county = self.lookup_county(zip5)
        if show_state:
            state = self.lookup_state(zip5)
            return county, state
        return county

    def lookup_county(self, zip5):
        return self.zip_counties.get(str(zip5).zfill(5), ['None'])
    
    def lookup_county2(self, zip5):
        return self.zip_counties.get(str(zip5).zfill(5), None)

    def lookup_state(self, zip5):
        return self.zip_states.get(str(zip5).zfill(5), ['None'])

    def lookup_state2(self, zip5):
        return self.zip_states.get(str(zip5).zfill(5), 'None')

    def lookup_zip_by_county(self, state, county):
        return self.zip_by_county.get(f"{state.upper()}", {}).get(county.upper(), [])
    
    def lookup_zips_by_state(self, state):
        return self.zip_by_states.get(state, [])

    def load_zips(self, file_name):
        # Load JSON data
        with open(file_name, 'r') as f:
            zip_data = json.load(f)
        
        zip_c = {}
        zip_s = {}
        
        # Process each zip code entry
        for zip_code, data in zip_data.items():
            # Extract counties (already in list format in the JSON)
            counties = [county.upper() for county in data.get('counties', [])]
            zip_c[zip_code] = counties
            
            # Extract state
            state = data.get('state', 'None')
            zip_s[zip_code] = state
        
        # Build lookup by county
        zip_by_county = {}
        for zip_code, counties in zip_c.items():
            for county in counties:
                state = zip_s.get(zip_code)
                dic = zip_by_county.get(state, {})
                ls = dic.get(county, [])
                ls.append(zip_code)
                dic[county] = ls
                zip_by_county[state] = dic
        
        # Build lookup by state
        zip_by_states = {}
        for zip_code, state in zip_s.items():
            ls = zip_by_states.get(state, [])
            ls.append(zip_code)
            zip_by_states[state] = ls
        
        # Store the lookup dictionaries
        self.zip_counties = zip_c
        self.zip_states = zip_s
        self.zip_by_county = zip_by_county
        self.zip_by_states = zip_by_states