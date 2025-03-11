#!/usr/bin/env python3
import argparse
import asyncio
import logging
import json
from datetime import datetime
from build_db_new import MedicareSupplementRateDB
from check_script import get_default_effective_date

def setup_logging(quiet: bool) -> None:
    log_filename = f'update_carrier_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

def get_effective_dates(effective_date: str = None, months: int = None) -> list:
    """Get list of effective dates to process."""
    if effective_date:
        return [effective_date]
    elif months:
        return [get_default_effective_date(i) for i in range(months)]
    else:
        return [get_default_effective_date()]

async def update_specific_carrier(db, state, naic, effective_date, dry_run=False):
    """
    Force an update for a specific carrier in a specific state.
    
    Args:
        db: Database instance
        state: State code (e.g., 'TX')
        naic: NAIC code for the carrier (e.g., '12345')
        effective_date: Effective date in YYYY-MM-DD format
        dry_run: If True, don't actually update the database
        
    Returns:
        dict: Results of the operation
    """
    logging.info(f"Updating carrier NAIC {naic} in state {state} for date {effective_date}")
    
    if dry_run:
        logging.info("DRY RUN - Would update the following:")
        logging.info(f"    - State: {state}")
        logging.info(f"    - NAIC: {naic}")
        logging.info(f"    - Effective Date: {effective_date}")
        return {"dry_run": True, "state": state, "naic": naic, "effective_date": effective_date}
    
    # Set the state map for this NAIC
    try:
        mapping_result = await db.set_state_map_naic(naic, state)
        logging.info(f"Mapping result: {mapping_result}")
        
        # Get and execute rate tasks
        rate_tasks = db.get_rate_tasks(state, naic, effective_date)
        logging.info(f"Generated {len(rate_tasks)} rate tasks")
        
        if rate_tasks:
            results = await asyncio.gather(*rate_tasks)
            logging.info(f"Completed {len(results)} rate tasks successfully")
            return {
                "success": True,
                "state": state,
                "naic": naic, 
                "effective_date": effective_date,
                "tasks_completed": len(results)
            }
        else:
            logging.warning(f"No rate tasks generated for {state}:{naic} on {effective_date}")
            return {
                "success": False,
                "state": state,
                "naic": naic,
                "effective_date": effective_date,
                "error": "No rate tasks generated"
            }
            
    except Exception as e:
        logging.error(f"Error updating carrier {naic} in {state}: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return {
            "success": False,
            "state": state,
            "naic": naic,
            "effective_date": effective_date,
            "error": str(e)
        }

async def main():
    parser = argparse.ArgumentParser(description="Force an update for a specific carrier in a specific state")
    parser.add_argument("-s", "--state", type=str, required=True, help="State code (e.g., TX)")
    parser.add_argument("-n", "--naic", type=str, required=True, help="NAIC code of the carrier")
    parser.add_argument("-e", "--effective-date", type=str, help="Effective date (YYYY-MM-DD)")
    parser.add_argument("-m", "--months", type=int, help="Number of months ahead to process")
    parser.add_argument("-d", "--db", type=str, required=True, help="Database file path")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")
    parser.add_argument("--out", type=str, help="Path to output file to save results")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    # Get list of dates to process
    effective_dates = get_effective_dates(args.effective_date, args.months)
    
    if not args.dry_run:
        db = MedicareSupplementRateDB(db_path=args.db)
        await db.cr.async_init()
        await db.cr.fetch_token()
        
        # Process each effective date
        all_results = []
        for effective_date in effective_dates:
            result = await update_specific_carrier(db, args.state, args.naic, effective_date, args.dry_run)
            all_results.append(result)
    else:
        all_results = [
            await update_specific_carrier(None, args.state, args.naic, date, args.dry_run)
            for date in effective_dates
        ]
    
    # Print results
    print(json.dumps(all_results, indent=2))
    
    # Save to file if requested
    if args.out:
        with open(args.out, 'w') as f:
            json.dump(all_results, f, indent=2)
    
    return all_results

if __name__ == "__main__":
    asyncio.run(main()) 