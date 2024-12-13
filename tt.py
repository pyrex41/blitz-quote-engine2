from build_db_new import MedicareSupplementRateDB
from build_db_new import process_quote

import asyncio
from pprint import pprint

import logging
import time
import copy
ar = asyncio.run

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models import CarrierSelection

# Add global variable at the top level
selected_naics = []

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
    
    # Create SQLAlchemy engine and session
    engine = create_engine(f'sqlite:///{args.db}')
    Session = sessionmaker(bind=engine)
    db_session = Session()
    
    db = MedicareSupplementRateDB(args.db)

    # Initialize database connection and token inside the async context
    await init(db)
    
    # Get selected carriers using the correct method
    global selected_naics
    selected_carriers = db.get_selected_carriers()
    selected_naics = set([carrier['naic'] for carrier in selected_carriers])
    
    return db, db_session
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
    db, db_session = asyncio.run(main())
    p = {
        "age": 65,
        "gender": "M",
        "plan": "N",
        "tobacco": 0,
        "apply_discounts": 0,
        "zip5": "75201",
        "select": 0
    }

    # Create an async function to handle the concurrent quote fetching
    async def fetch_quotes():
        r = db.cr.fetch_quote(**p)
        p1 = copy.deepcopy(p)
        p1['zip5'] = "66210"
        r1 = db.cr.fetch_quote(**p1)
        p2 = copy.deepcopy(p)
        p2['zip5'] = "91010"
        r2 = db.cr.fetch_quote(**p2)

        # Now gather the quotes within the async context
        results = await asyncio.gather(r, r1, r2)
        return results

    # Run the async function
    rr = asyncio.run(fetch_quotes())
    rr_flat = [item for sublist in rr for item in sublist]

    dq = {}
    rcq = {}
    vq = {}
    for q in rr_flat:
        pq = process_quote(q, "test")
        pq65 = [pq for pq in pq if pq['age'] == 65]
        naic = q.get('company_base', {}).get('naic', None)
        name = q.get('company_base', {}).get('name', None)
        nn = (name, naic)
        if naic is not None and len(pq65) > 0 and nn not in dq:
            disc = q['discount_category']
            dq[nn] = disc if disc != '' else None
            rcq[nn] = q['rating_class']
            vq[nn] = q['view_type']
    for k,v in dq.items():
        print('--------------')
        print(k)
        print(f'---->')
        print(f'      {v}')
        print()
        print()
        
        # Add database update for non-None discount categories
        name, naic = k
        if v is not None:
            try:
                # Update or create carrier selection record
                carrier = db_session.query(CarrierSelection).filter_by(naic=naic).first()
                if carrier:
                    carrier.discount_category = v
                else:
                    new_carrier = CarrierSelection(
                        naic=naic,
                        company_name=name,
                        selected=1,  # Assuming we want to select carriers we're updating
                        discount_category=v
                    )
                    db_session.add(new_carrier)
                
                db_session.commit()
            except Exception as e:
                print(f"Error updating database for NAIC {naic}: {e}")
                db_session.rollback()

    db_session.close()
