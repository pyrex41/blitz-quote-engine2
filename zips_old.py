# zips.py
from csv import DictReader

class zipHolder():

    def __init__(self, file_name):
        self.load_zips(file_name)

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
        return self.zip_by_county.get(f"{state.upper()}",{}).get(county.upper(), [])
    
    def lookup_zips_by_state(self, state):
        return self.zip_by_states.get(state, [])

    def load_zips(self, file_name):
        zip_c = {}
        zip_s = {}
        with open(file_name, mode='r') as cf:
            cr = DictReader(cf)
            first_row = True
            for row in cr:
                if first_row:
                    first_row = False
                else:
                    zip_c[(row['zip'])] = [
                        i.upper() for i in row['county_names_all'].split('|')
                    ]
                    zip_s[(row['zip'])] = row['state_id']
        zip_by_county = {}
        for zip, clist in zip_c.items():
            for c in clist:
                state = zip_s.get(zip)
                dic = zip_by_county.get(state, {})
                ls = dic.get(c, [])
                ls.append(zip)
                dic[c] = ls
                zip_by_county[state] = dic
        self.zip_counties = zip_c
        self.zip_states = zip_s
        self.zip_by_county = zip_by_county
        zip_by_states = {}
        for zip, state in zip_s.items():
            ls = zip_by_states.get(state, [])
            ls.append(zip)
            zip_by_states[state] = ls
        self.zip_by_states = zip_by_states
