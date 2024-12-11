from build_db_new import MedicareSupplementRateDB
from build_db_new import process_quote

import asyncio
from pprint import pprint

import logging
import time

ar = asyncio.run

async def init(db):
    await db.cr.async_init()
    await db.cr.fetch_token()

state_list = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC"
        ]


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch quotes for random zip codes")
    parser.add_argument("-d", "--db", type=str, help="Database to use")
    args = parser.parse_args()
    db = MedicareSupplementRateDB(args.db)

    # Initialize database connection and token inside the async context
    await init(db)
    
    return db
    tasks = []
    for state in state_list:
        naics = db.get_existing_naics(state)
        for naic in naics:
            for td in ["2025-02-01", "2025-03-01", "2025-04-01"]:
                tasks.append(db.copy_rates(state, naic, "2025-01-01", td))
                                           
    logging.info(f"Tasks: {len(tasks)}. Running sequentially.")
    start_time = time.time()
    for t in tasks:
        await t
    end_time = time.time()
    logging.info(f"Time taken: {end_time - start_time} seconds")
    return db
    
if __name__ == "__main__":
    db = asyncio.run(main())
    p = {
        "age": 65,
        "gender": "M",
        "plan": "G",
        "tobacco": 0,
        "apply_discounts": 0,
        "zip5": "21520",
        "naic": "60052"
    }
    r = ar(db.cr.fetch_quote(**p))
    qq0 = []
    for q in r:
        qq0.extend(process_quote(q, "test"))
    qqq = [q for q in qq0 if q['age'] == 65]
    pprint(qqq)
    