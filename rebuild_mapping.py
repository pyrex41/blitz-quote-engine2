#!/usr/bin/env python3
import argparse
import asyncio
import logging
import json
from datetime import datetime
from build_db_new import MedicareSupplementRateDB

def setup_logging(quiet: bool) -> None:
    log_filename = f'rebuild_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

async def rebuild_state_naic_mapping(db, state, naic, dry_run=False):
    """
    Rebuild the mapping for a specific carrier (NAIC) in a specific state.
    
    Args:
        db: MedicareSupplementRateDB instance
        state: State code (e.g., 'TX')
        naic: NAIC code of the carrier (e.g., '12345')
        dry_run: If True, don't actually update the database
        
    Returns:
        dict: Results of the mapping operation
    """
    logging.info(f"Rebuilding mapping for NAIC {naic} in state {state}")
    
    if dry_run:
        logging.info("DRY RUN - Would rebuild mapping for:")
        logging.info(f"    - State: {state}")
        logging.info(f"    - NAIC: {naic}")
        return {
            "dry_run": True,
            "state": state,
            "naic": naic
        }
    
    try:
        # First, delete existing mapping for this NAIC/state combination
        cursor = db.conn.cursor()
        
        # Log how many mappings exist before deletion
        cursor.execute(
            "SELECT COUNT(*) FROM group_mapping WHERE state = ? AND naic = ?", 
            (state, naic)
        )
        existing_count = cursor.fetchone()[0]
        logging.info(f"Found {existing_count} existing mappings for {state}:{naic}")
        
        if existing_count > 0:
            logging.info(f"Deleting existing mappings for {state}:{naic}")
            cursor.execute(
                "DELETE FROM group_mapping WHERE state = ? AND naic = ?", 
                (state, naic)
            )
            db.conn.commit()
        
        # Now rebuild the mapping
        result = await db.set_state_map_naic(naic, state)
        
        # Check how many mappings were created
        cursor.execute(
            "SELECT COUNT(*) FROM group_mapping WHERE state = ? AND naic = ?", 
            (state, naic)
        )
        new_count = cursor.fetchone()[0]
        
        return {
            "success": True,
            "state": state,
            "naic": naic,
            "previous_mappings": existing_count,
            "new_mappings": new_count,
            "mapping_result": result
        }
        
    except Exception as e:
        logging.error(f"Error rebuilding mapping for {naic} in {state}: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return {
            "success": False,
            "state": state,
            "naic": naic,
            "error": str(e)
        }

async def rebuild_all_for_state(db, state, dry_run=False):
    """Rebuild mappings for all carriers in a specific state."""
    logging.info(f"Rebuilding all carrier mappings for state {state}")
    
    # Get all NAICs for this state
    naics = db.get_existing_naics(state)
    logging.info(f"Found {len(naics)} carriers for state {state}")
    
    results = []
    for naic in naics:
        result = await rebuild_state_naic_mapping(db, state, naic, dry_run)
        results.append(result)
        
    return results

async def main():
    parser = argparse.ArgumentParser(description="Rebuild carrier-state ZIP code mappings")
    parser.add_argument("-s", "--state", type=str, help="State code (e.g., TX)")
    parser.add_argument("-n", "--naic", type=str, help="NAIC code of the carrier")
    parser.add_argument("-a", "--all", action="store_true", help="Rebuild all mappings for all states")
    parser.add_argument("--all-for-state", action="store_true", help="Rebuild all carrier mappings for specified state")
    parser.add_argument("-d", "--db", type=str, required=True, help="Database file path")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    parser.add_argument("--out", type=str, help="Path to output file to save results")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    if args.dry_run:
        logging.info("DRY RUN MODE - No changes will be made to the database")
    
    if not any([args.state, args.all]):
        logging.error("You must specify a state (-s), use --all-for-state with -s, or use --all")
        return
    
    if args.all_for_state and not args.state:
        logging.error("--all-for-state requires a state (-s)")
        return
        
    if args.naic and args.all_for_state:
        logging.error("Cannot specify both --naic and --all-for-state")
        return
        
    if args.naic and args.all:
        logging.error("Cannot specify both --naic and --all")
        return

    # Initialize database connection
    if not args.dry_run:
        db = MedicareSupplementRateDB(db_path=args.db)
        await db.cr.async_init()
        await db.cr.fetch_token()
    else:
        db = None  # Will not be used in dry run
    
    results = []
    
    if args.all:
        # Get all states from the database
        if not args.dry_run:
            cursor = db.conn.cursor()
            cursor.execute("SELECT DISTINCT state FROM group_mapping")
            states = [row[0] for row in cursor.fetchall()]
            
            for state in states:
                state_results = await rebuild_all_for_state(db, state, args.dry_run)
                results.extend(state_results)
        else:
            logging.info("DRY RUN - Would rebuild all mappings for all states")
            results.append({"dry_run": True, "message": "Would rebuild all mappings for all states"})
            
    elif args.all_for_state:
        # Rebuild all carrier mappings for this state
        results = await rebuild_all_for_state(db, args.state, args.dry_run)
        
    elif args.naic:
        # Rebuild mapping for specific carrier in specific state
        if not args.dry_run:
            result = await rebuild_state_naic_mapping(db, args.state, args.naic, args.dry_run)
            results.append(result)
        else:
            results.append({
                "dry_run": True, 
                "state": args.state, 
                "naic": args.naic,
                "message": f"Would rebuild mapping for {args.naic} in {args.state}"
            })
            
    else:
        # Get all carriers for this state and rebuild their mappings
        if not args.dry_run:
            naics = db.get_existing_naics(args.state)
            if not naics:
                logging.warning(f"No existing carriers found for state {args.state}")
                naics = []
                
            for naic in naics:
                result = await rebuild_state_naic_mapping(db, args.state, naic, args.dry_run)
                results.append(result)
        else:
            results.append({
                "dry_run": True, 
                "state": args.state,
                "message": f"Would rebuild all carrier mappings for state {args.state}"
            })
    
    # Print results
    print(json.dumps(results, indent=2))
    
    # Save to file if requested
    if args.out:
        with open(args.out, 'w') as f:
            json.dump(results, f, indent=2)
    
    logging.info("Mapping rebuild completed")
    return results

if __name__ == "__main__":
    asyncio.run(main()) 