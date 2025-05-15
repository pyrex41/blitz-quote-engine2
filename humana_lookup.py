#!/usr/bin/env python3
import asyncio
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from async_csg import AsyncCSGRequest
from config import Config
from zips import zipHolder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"humana_lookup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

# Humana NAICs from the API
HUMANA_NAICS = [
    "84603",  # HUMANA INSURANCE OF PUERTO RICO, INC
    "95642",  # Humana Health Benefit Plan of Louisiana, Inc.
    "60219",  # Humana Insurance Company
    "95158",  # Humana Insurance Company
    "88595",  # Humana Insurance Company
    "73288",  # Humana Insurance Company
    "69671",  # Humana Insurance Company
    "60052",  # Humana Insurance Company
    "60984",  # Humana Insurance Company
    "70580",  # Humana Insurance Company
    "12634",  # Humana Insurance Company of New York
]

# States to check (50 states + DC)
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC"
]

# Puerto Rico for the PR specific NAIC
TERRITORIES = ["PR"]

ALL_STATES = US_STATES + TERRITORIES

class HumanaLookup:
    def __init__(self):
        self.csg = AsyncCSGRequest(Config.API_KEY)
        self.zips = zipHolder('static/uszips.csv')
        self.results = defaultdict(list)
        self.rating_classes = defaultdict(dict)
    
    async def init(self):
        await self.csg.async_init()
        await self.csg.fetch_token()
    
    async def find_state_zip(self, state: str) -> str:
        """Find a representative zip code for the given state."""
        state_zips = [k for k, v in self.zips.zip_states.items() if v == state]
        if state_zips:
            return state_zips[0]
        else:
            # Fallback zip codes for territories not in the zip database
            fallbacks = {
                "PR": "00901",  # San Juan, PR
            }
            return fallbacks.get(state)
    
    async def check_naic_in_state(self, state: str, naic: str) -> Tuple[bool, List[str]]:
        """Check if a NAIC is active in a state and get its rating classes."""
        zip_code = await self.find_state_zip(state)
        if not zip_code:
            logging.warning(f"No zip code found for state {state}")
            return False, []
        
        # Calculate effective date (first of next month)
        effective_date = (datetime.now() + timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')
        
        try:
            # Default plan is G, but some states have different plans
            plan = "G"
            if state == "MA":
                plan = "MA_CORE"
            elif state == "MN":
                plan = "MN_BASIC"
            elif state == "WI":
                plan = "WI_BASIC"
            
            # Make the API call
            response = await self.csg.fetch_quote(
                zip5=zip_code,
                age=65,
                gender="M",
                tobacco=0,
                effective_date=effective_date,
                naic=naic,
                plan=plan
            )
            
            if response and len(response) > 0:
                # Extract rating classes from the response
                rating_classes = set()
                for quote in response:
                    rating_class = quote.get('rating_class', '')
                    if rating_class:
                        rating_classes.add(rating_class)
                
                logging.info(f"NAIC {naic} is active in {state} with rating classes: {list(rating_classes)}")
                return True, list(rating_classes)
            else:
                logging.info(f"NAIC {naic} is NOT active in {state}")
                return False, []
                
        except Exception as e:
            logging.error(f"Error checking NAIC {naic} in state {state}: {str(e)}")
            return False, []
    
    async def check_all_states(self):
        """Check all Humana NAICs across all states."""
        tasks = []
        for state in ALL_STATES:
            for naic in HUMANA_NAICS:
                tasks.append(self.process_state_naic(state, naic))
        
        # Process in batches to avoid overwhelming the API
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            await asyncio.gather(*batch)
            
            # Add a small delay between batches
            if i + batch_size < len(tasks):
                await asyncio.sleep(1)
    
    async def process_state_naic(self, state: str, naic: str):
        """Process a single state-NAIC combination."""
        active, rating_classes = await self.check_naic_in_state(state, naic)
        if active:
            self.results[state].append({
                "naic": naic,
                "rating_classes": rating_classes
            })
            # Also store in the rating_classes dict
            self.rating_classes[naic][state] = rating_classes
    
    def save_results(self):
        """Save the results to JSON files."""
        # Save state -> NAICs mapping
        with open('humana_by_state.json', 'w') as f:
            json.dump(self.results, f, indent=2)
        
        # Save NAIC -> states mapping
        naic_to_states = {}
        for naic in HUMANA_NAICS:
            states_active = []
            for state, naics in self.results.items():
                if any(n["naic"] == naic for n in naics):
                    states_active.append(state)
            naic_to_states[naic] = states_active
        
        with open('humana_naics.json', 'w') as f:
            json.dump(naic_to_states, f, indent=2)
        
        # Save rating classes by NAIC
        with open('humana_rating_classes.json', 'w') as f:
            json.dump(self.rating_classes, f, indent=2)
        
        # Generate a summary report
        with open('humana_summary.md', 'w') as f:
            f.write("# Humana Medicare Supplement Insurance Summary\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## NAICs by State\n\n")
            for state in sorted(self.results.keys()):
                f.write(f"### {state}\n\n")
                for naic_data in self.results[state]:
                    naic = naic_data["naic"]
                    rating_classes = naic_data["rating_classes"]
                    f.write(f"- **{naic}**: {', '.join(rating_classes) if rating_classes else 'No rating classes'}\n")
                f.write("\n")
            
            f.write("## States by NAIC\n\n")
            for naic, states in naic_to_states.items():
                if states:
                    f.write(f"### NAIC: {naic}\n\n")
                    f.write(f"Active in {len(states)} states: {', '.join(sorted(states))}\n\n")
                    
                    # Add rating classes by state for this NAIC
                    if naic in self.rating_classes:
                        f.write("Rating classes by state:\n\n")
                        for state, classes in sorted(self.rating_classes[naic].items()):
                            f.write(f"- **{state}**: {', '.join(classes) if classes else 'No rating classes'}\n")
                    f.write("\n")

async def main():
    lookup = HumanaLookup()
    await lookup.init()
    
    logging.info("Starting Humana NAIC lookup across all states...")
    await lookup.check_all_states()
    
    lookup.save_results()
    logging.info("Lookup complete. Results saved to JSON and markdown files.")

if __name__ == "__main__":
    asyncio.run(main()) 