#!/usr/bin/env python3
import argparse
import sqlite3
import json
import os
import sys
import pandas as pd
from typing import List, Dict, Any, Tuple, Set, Optional
from tabulate import tabulate
from datetime import datetime

class DatabaseComparator:
    """Class to compare two Medicare Supplement rate databases."""
    
    def __init__(self, db1_path: str, db2_path: str, verbose: bool = False):
        """Initialize with paths to the two databases to compare."""
        self.db1_path = db1_path
        self.db2_path = db2_path
        self.verbose = verbose
        
        # Validate database files exist
        if not os.path.exists(db1_path):
            raise FileNotFoundError(f"Database file not found: {db1_path}")
        if not os.path.exists(db2_path):
            raise FileNotFoundError(f"Database file not found: {db2_path}")
        
        # Connect to databases
        self.conn1 = sqlite3.connect(db1_path)
        self.conn2 = sqlite3.connect(db2_path)
        
        # Enable JSON functions
        self.conn1.enable_load_extension(True)
        self.conn2.enable_load_extension(True)
        
        # Set connections to return rows as dictionaries
        self.conn1.row_factory = sqlite3.Row
        self.conn2.row_factory = sqlite3.Row
        
        print(f"Connected to databases:\n  - {db1_path}\n  - {db2_path}")
    
    def close(self):
        """Close database connections."""
        self.conn1.close()
        self.conn2.close()
    
    def get_tables(self) -> Tuple[List[str], List[str], List[str]]:
        """Get tables from both databases and return common, db1-only, and db2-only tables."""
        cursor1 = self.conn1.cursor()
        cursor2 = self.conn2.cursor()
        
        tables1 = set(row[0] for row in cursor1.execute("SELECT name FROM sqlite_master WHERE type='table'"))
        tables2 = set(row[0] for row in cursor2.execute("SELECT name FROM sqlite_master WHERE type='table'"))
        
        common_tables = tables1.intersection(tables2)
        db1_only_tables = tables1 - tables2
        db2_only_tables = tables2 - tables1
        
        return list(common_tables), list(db1_only_tables), list(db2_only_tables)
    
    def compare_table_schemas(self) -> List[Dict[str, Any]]:
        """Compare schemas for all common tables."""
        common_tables, _, _ = self.get_tables()
        results = []
        
        for table in common_tables:
            cursor1 = self.conn1.cursor()
            cursor2 = self.conn2.cursor()
            
            # Get table schemas
            schema1 = [dict(row) for row in cursor1.execute(f"PRAGMA table_info({table})")]
            schema2 = [dict(row) for row in cursor2.execute(f"PRAGMA table_info({table})")]
            
            # Compare column names and types
            columns1 = {col['name']: col['type'] for col in schema1}
            columns2 = {col['name']: col['type'] for col in schema2}
            
            # Find differences
            diff_cols = []
            for col_name, col_type in columns1.items():
                if col_name not in columns2:
                    diff_cols.append(f"Column '{col_name}' exists in DB1 but not in DB2")
                elif columns2[col_name] != col_type:
                    diff_cols.append(f"Column '{col_name}' has type '{col_type}' in DB1 but '{columns2[col_name]}' in DB2")
            
            for col_name, col_type in columns2.items():
                if col_name not in columns1:
                    diff_cols.append(f"Column '{col_name}' exists in DB2 but not in DB1")
            
            if diff_cols:
                results.append({
                    "table": table,
                    "differences": diff_cols
                })
            
        return results
    
    def compare_table_counts(self) -> List[Dict[str, Any]]:
        """Compare row counts for all common tables."""
        common_tables, _, _ = self.get_tables()
        results = []
        
        for table in common_tables:
            cursor1 = self.conn1.cursor()
            cursor2 = self.conn2.cursor()
            
            count1 = cursor1.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            count2 = cursor2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            
            if count1 != count2:
                results.append({
                    "table": table,
                    "db1_count": count1,
                    "db2_count": count2,
                    "difference": count1 - count2
                })
        
        return results
    
    def get_available_naics(self) -> Tuple[List[str], List[str], List[str]]:
        """Get NAICs available in both databases and their differences."""
        cursor1 = self.conn1.cursor()
        cursor2 = self.conn2.cursor()
        
        try:
            naics1 = set(row[0] for row in cursor1.execute("SELECT DISTINCT naic FROM group_mapping"))
            naics2 = set(row[0] for row in cursor2.execute("SELECT DISTINCT naic FROM group_mapping"))
            
            common_naics = sorted(naics1.intersection(naics2))
            db1_only_naics = sorted(naics1 - naics2)
            db2_only_naics = sorted(naics2 - naics1)
            
            return common_naics, db1_only_naics, db2_only_naics
        except sqlite3.OperationalError:
            print("Error: Could not query group_mapping table. It may not exist in one or both databases.")
            return [], [], []
    
    def get_available_states(self) -> Tuple[List[str], List[str], List[str]]:
        """Get states available in both databases and their differences."""
        cursor1 = self.conn1.cursor()
        cursor2 = self.conn2.cursor()
        
        try:
            states1 = set(row[0] for row in cursor1.execute("SELECT DISTINCT state FROM group_mapping"))
            states2 = set(row[0] for row in cursor2.execute("SELECT DISTINCT state FROM group_mapping"))
            
            common_states = sorted(states1.intersection(states2))
            db1_only_states = sorted(states1 - states2)
            db2_only_states = sorted(states2 - states1)
            
            return common_states, db1_only_states, db2_only_states
        except sqlite3.OperationalError:
            print("Error: Could not query group_mapping table. It may not exist in one or both databases.")
            return [], [], []
    
    def get_available_effective_dates(self) -> Tuple[List[str], List[str], List[str]]:
        """Get effective dates available in both databases and their differences."""
        cursor1 = self.conn1.cursor()
        cursor2 = self.conn2.cursor()
        
        try:
            dates1 = set(row[0] for row in cursor1.execute("SELECT DISTINCT effective_date FROM rate_store"))
            dates2 = set(row[0] for row in cursor2.execute("SELECT DISTINCT effective_date FROM rate_store"))
            
            common_dates = sorted(dates1.intersection(dates2))
            db1_only_dates = sorted(dates1 - dates2)
            db2_only_dates = sorted(dates2 - dates1)
            
            return common_dates, db1_only_dates, db2_only_dates
        except sqlite3.OperationalError:
            print("Error: Could not query rate_store table. It may not exist in one or both databases.")
            return [], [], []
    
    def compare_mapping_for_carrier_state(self, naic: str, state: str) -> Dict[str, Any]:
        """Compare mapping data for a specific carrier and state."""
        cursor1 = self.conn1.cursor()
        cursor2 = self.conn2.cursor()
        
        try:
            # Get group type (zip vs county)
            group_type1 = cursor1.execute(
                "SELECT group_zip FROM group_type WHERE naic = ? AND state = ?",
                (naic, state)
            ).fetchone()
            
            group_type2 = cursor2.execute(
                "SELECT group_zip FROM group_type WHERE naic = ? AND state = ?",
                (naic, state)
            ).fetchone()
            
            if not group_type1 or not group_type2:
                return {
                    "naic": naic,
                    "state": state,
                    "error": "Mapping data missing in one or both databases"
                }
            
            group_type1 = bool(group_type1[0])
            group_type2 = bool(group_type2[0])
            
            if group_type1 != group_type2:
                return {
                    "naic": naic,
                    "state": state,
                    "difference": f"Group type mismatch: DB1={'ZIP' if group_type1 else 'County'}, DB2={'ZIP' if group_type2 else 'County'}"
                }
            
            # Compare mapping groups
            groups1 = cursor1.execute(
                "SELECT DISTINCT naic_group FROM group_mapping WHERE naic = ? AND state = ?",
                (naic, state)
            ).fetchall()
            
            groups2 = cursor2.execute(
                "SELECT DISTINCT naic_group FROM group_mapping WHERE naic = ? AND state = ?",
                (naic, state)
            ).fetchall()
            
            groups1 = set(row[0] for row in groups1)
            groups2 = set(row[0] for row in groups2)
            
            if groups1 != groups2:
                return {
                    "naic": naic,
                    "state": state,
                    "difference": f"Group mapping mismatch: DB1={sorted(groups1)}, DB2={sorted(groups2)}"
                }
            
            # For each group, compare location counts
            differences = []
            for group in sorted(groups1):
                count1 = cursor1.execute(
                    "SELECT COUNT(*) FROM group_mapping WHERE naic = ? AND state = ? AND naic_group = ?",
                    (naic, state, group)
                ).fetchone()[0]
                
                count2 = cursor2.execute(
                    "SELECT COUNT(*) FROM group_mapping WHERE naic = ? AND state = ? AND naic_group = ?",
                    (naic, state, group)
                ).fetchone()[0]
                
                if count1 != count2:
                    differences.append(f"Group {group}: Location count mismatch (DB1={count1}, DB2={count2})")
            
            if differences:
                return {
                    "naic": naic,
                    "state": state,
                    "differences": differences
                }
            
            return {
                "naic": naic,
                "state": state,
                "match": True
            }
        
        except sqlite3.OperationalError as e:
            return {
                "naic": naic,
                "state": state,
                "error": f"SQL error: {str(e)}"
            }
    
    def parse_rate_json(self, json_str: str) -> Dict[str, Any]:
        """Parse rate data JSON and return a standardized dictionary."""
        if not json_str:
            return {}
        
        try:
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError:
            return {}
    
    def compare_rates(self, naic: str, state: str, effective_date: str, dimensions: List[str] = None) -> List[Dict[str, Any]]:
        """
        Compare rates for a specific carrier, state, and effective date across dimensions.
        Dimensions can include: age, gender, tobacco, plan
        """
        if not dimensions:
            dimensions = ["65:M:G:0", "65:F:G:0", "65:M:G:1", "70:M:G:0", "75:M:G:0"]
        
        cursor1 = self.conn1.cursor()
        cursor2 = self.conn2.cursor()
        
        differences = []
        
        # Get all groups for this carrier/state
        try:
            groups = cursor1.execute(
                "SELECT DISTINCT naic_group FROM group_mapping WHERE naic = ? AND state = ?",
                (naic, state)
            ).fetchall()
            
            groups = [row[0] for row in groups]
            
            for group in groups:
                label = f"{state}:{naic}:{group}"
                
                # Compare rates for this group across dimensions
                for dimension in dimensions:
                    # Format query for the key in rate_store
                    rate_key = f"{label}"
                    query_params = (rate_key, effective_date)
                    
                    # Get rate data from both databases
                    rate1 = cursor1.execute(
                        "SELECT value FROM rate_store WHERE key = ? AND effective_date = ?",
                        query_params
                    ).fetchone()
                    
                    rate2 = cursor2.execute(
                        "SELECT value FROM rate_store WHERE key = ? AND effective_date = ?",
                        query_params
                    ).fetchone()
                    
                    # Handle cases where data is missing in one database
                    if not rate1 and not rate2:
                        continue  # No data in either database
                    elif not rate1:
                        differences.append({
                            "naic": naic,
                            "state": state,
                            "group": group,
                            "dimension": dimension,
                            "difference": "Rate exists in DB2 but not in DB1"
                        })
                        continue
                    elif not rate2:
                        differences.append({
                            "naic": naic,
                            "state": state,
                            "group": group,
                            "dimension": dimension,
                            "difference": "Rate exists in DB1 but not in DB2"
                        })
                        continue
                    
                    # Parse JSON rate data
                    rate_data1 = self.parse_rate_json(rate1[0])
                    rate_data2 = self.parse_rate_json(rate2[0])
                    
                    # Check if dimension exists in both rate data sets
                    if dimension not in rate_data1 and dimension not in rate_data2:
                        continue  # Dimension not in either database
                    elif dimension not in rate_data1:
                        differences.append({
                            "naic": naic,
                            "state": state,
                            "group": group,
                            "dimension": dimension,
                            "difference": f"Dimension exists in DB2 but not in DB1"
                        })
                        continue
                    elif dimension not in rate_data2:
                        differences.append({
                            "naic": naic,
                            "state": state,
                            "group": group,
                            "dimension": dimension,
                            "difference": f"Dimension exists in DB1 but not in DB2"
                        })
                        continue
                    
                    # Compare rate values
                    rate_value1 = rate_data1[dimension].get('rate', 0)
                    rate_value2 = rate_data2[dimension].get('rate', 0)
                    
                    if rate_value1 != rate_value2:
                        differences.append({
                            "naic": naic,
                            "state": state,
                            "group": group,
                            "dimension": dimension,
                            "rate_db1": rate_value1,
                            "rate_db2": rate_value2,
                            "difference": f"Rate mismatch: DB1=${rate_value1}, DB2=${rate_value2}"
                        })
            
            return differences
        
        except sqlite3.OperationalError as e:
            return [{
                "naic": naic,
                "state": state,
                "error": f"SQL error: {str(e)}"
            }]
    
    def run_comprehensive_comparison(self, state: str = None, naic: str = None,
                                     effective_date: str = None) -> Dict[str, Any]:
        """Run a comprehensive comparison across multiple dimensions."""
        results = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "db1_path": self.db1_path,
            "db2_path": self.db2_path,
            "tables": {},
            "schemas": {},
            "counts": {},
            "metadata": {},
            "mappings": {},
            "rates": {}
        }
        
        # Compare tables
        common_tables, db1_only, db2_only = self.get_tables()
        results["tables"] = {
            "common": common_tables,
            "db1_only": db1_only,
            "db2_only": db2_only
        }
        
        # Compare schemas
        schema_diffs = self.compare_table_schemas()
        results["schemas"] = {
            "differences": schema_diffs,
            "match": len(schema_diffs) == 0
        }
        
        # Compare row counts
        count_diffs = self.compare_table_counts()
        results["counts"] = {
            "differences": count_diffs,
            "match": len(count_diffs) == 0
        }
        
        # Compare metadata availability
        common_naics, db1_only_naics, db2_only_naics = self.get_available_naics()
        common_states, db1_only_states, db2_only_states = self.get_available_states()
        common_dates, db1_only_dates, db2_only_dates = self.get_available_effective_dates()
        
        results["metadata"] = {
            "naics": {
                "common": common_naics,
                "db1_only": db1_only_naics,
                "db2_only": db2_only_naics
            },
            "states": {
                "common": common_states,
                "db1_only": db1_only_states,
                "db2_only": db2_only_states
            },
            "effective_dates": {
                "common": common_dates,
                "db1_only": db1_only_dates,
                "db2_only": db2_only_dates
            }
        }
        
        # Filter based on provided parameters
        states_to_check = [state] if state else common_states
        naics_to_check = [naic] if naic else common_naics
        dates_to_check = [effective_date] if effective_date else common_dates
        
        # Short-circuit if no common data to compare
        if not states_to_check or not naics_to_check or not dates_to_check:
            print("Warning: No common data to compare based on specified parameters.")
            return results
        
        # Compare mappings
        print(f"Comparing mappings for {len(states_to_check)} states and {len(naics_to_check)} carriers...")
        mapping_diffs = []
        for s in states_to_check:
            for n in naics_to_check:
                result = self.compare_mapping_for_carrier_state(n, s)
                if not result.get("match", False):
                    mapping_diffs.append(result)
        
        results["mappings"] = {
            "differences": mapping_diffs,
            "match": len(mapping_diffs) == 0
        }
        
        # Compare rates
        print(f"Comparing rates for {len(states_to_check)} states, {len(naics_to_check)} carriers, and {len(dates_to_check)} dates...")
        rate_diffs = []
        dimensions = ["65:M:G:0", "65:F:G:0", "70:M:G:0", "75:M:G:0"]
        
        for s in states_to_check:
            for n in naics_to_check:
                for d in dates_to_check:
                    diffs = self.compare_rates(n, s, d, dimensions)
                    rate_diffs.extend(diffs)
        
        results["rates"] = {
            "differences": rate_diffs,
            "match": len(rate_diffs) == 0
        }
        
        return results
    
    def print_comparison_summary(self, results: Dict[str, Any]):
        """Print a human-readable summary of comparison results."""
        print("\n==== Database Comparison Summary ====")
        print(f"Timestamp: {results['timestamp']}")
        print(f"Database 1: {results['db1_path']}")
        print(f"Database 2: {results['db2_path']}")
        
        # Table differences
        print("\n== Tables ==")
        print(f"Common tables: {len(results['tables']['common'])}")
        if results['tables']['db1_only']:
            print(f"Tables only in DB1: {', '.join(results['tables']['db1_only'])}")
        if results['tables']['db2_only']:
            print(f"Tables only in DB2: {', '.join(results['tables']['db2_only'])}")
        
        # Schema differences
        print("\n== Schemas ==")
        if results['schemas']['match']:
            print("All common table schemas match")
        else:
            print(f"Schema differences found in {len(results['schemas']['differences'])} tables:")
            for diff in results['schemas']['differences']:
                print(f"  - Table {diff['table']}:")
                for d in diff['differences']:
                    print(f"    * {d}")
        
        # Count differences
        print("\n== Row Counts ==")
        if results['counts']['match']:
            print("All common table row counts match")
        else:
            print(f"Row count differences found in {len(results['counts']['differences'])} tables:")
            print(tabulate(results['counts']['differences'], headers="keys", tablefmt="pretty"))
        
        # Metadata differences
        print("\n== Metadata Availability ==")
        
        print("States:")
        if not results['metadata']['states']['db1_only'] and not results['metadata']['states']['db2_only']:
            print(f"  Both databases have the same states ({len(results['metadata']['states']['common'])})")
        else:
            if results['metadata']['states']['db1_only']:
                print(f"  States only in DB1: {', '.join(results['metadata']['states']['db1_only'])}")
            if results['metadata']['states']['db2_only']:
                print(f"  States only in DB2: {', '.join(results['metadata']['states']['db2_only'])}")
        
        print("NAICs:")
        if not results['metadata']['naics']['db1_only'] and not results['metadata']['naics']['db2_only']:
            print(f"  Both databases have the same carriers ({len(results['metadata']['naics']['common'])})")
        else:
            if results['metadata']['naics']['db1_only']:
                print(f"  Carriers only in DB1: {', '.join(results['metadata']['naics']['db1_only'])}")
            if results['metadata']['naics']['db2_only']:
                print(f"  Carriers only in DB2: {', '.join(results['metadata']['naics']['db2_only'])}")
        
        print("Effective Dates:")
        if not results['metadata']['effective_dates']['db1_only'] and not results['metadata']['effective_dates']['db2_only']:
            print(f"  Both databases have the same effective dates ({len(results['metadata']['effective_dates']['common'])})")
        else:
            if results['metadata']['effective_dates']['db1_only']:
                print(f"  Dates only in DB1: {', '.join(results['metadata']['effective_dates']['db1_only'])}")
            if results['metadata']['effective_dates']['db2_only']:
                print(f"  Dates only in DB2: {', '.join(results['metadata']['effective_dates']['db2_only'])}")
        
        # Mapping differences
        print("\n== Mapping Differences ==")
        if results['mappings']['match']:
            print("All mappings match")
        else:
            print(f"Mapping differences found for {len(results['mappings']['differences'])} carrier-state combinations:")
            for diff in results['mappings']['differences'][:10]:  # Show first 10
                print(f"  - NAIC {diff['naic']} in {diff['state']}: {diff.get('difference', diff.get('error', 'Unknown difference'))}")
            if len(results['mappings']['differences']) > 10:
                print(f"    ... and {len(results['mappings']['differences']) - 10} more")
        
        # Rate differences
        print("\n== Rate Differences ==")
        if results['rates']['match']:
            print("All rates match")
        else:
            print(f"Rate differences found for {len(results['rates']['differences'])} entries:")
            
            # Group differences by state/naic for better readability
            grouped_diffs = {}
            for diff in results['rates']['differences']:
                key = f"{diff['state']}:{diff['naic']}"
                if key not in grouped_diffs:
                    grouped_diffs[key] = []
                grouped_diffs[key].append(diff)
            
            # Print summary of differences by group
            print(f"Differences found in {len(grouped_diffs)} carrier-state combinations:")
            for i, (key, diffs) in enumerate(list(grouped_diffs.items())[:5]):  # Show first 5 groups
                state, naic = key.split(':')
                print(f"  - {state}/{naic}: {len(diffs)} differences")
                for j, diff in enumerate(diffs[:3]):  # Show first 3 diffs per group
                    if 'rate_db1' in diff and 'rate_db2' in diff:
                        print(f"    * Group {diff['group']}, {diff['dimension']}: DB1=${diff['rate_db1']}, DB2=${diff['rate_db2']}")
                    else:
                        print(f"    * Group {diff['group']}, {diff['dimension']}: {diff.get('difference', 'Unknown difference')}")
                if len(diffs) > 3:
                    print(f"    ... and {len(diffs) - 3} more differences")
            
            if len(grouped_diffs) > 5:
                print(f"  ... and {len(grouped_diffs) - 5} more carrier-state combinations with differences")
        
        # Overall summary
        print("\n== Overall Result ==")
        if (results['schemas']['match'] and results['counts']['match'] and 
            results['mappings']['match'] and results['rates']['match']):
            print("✅ Databases are IDENTICAL for the compared scope")
        else:
            print("❌ Databases have DIFFERENCES")

def main():
    parser = argparse.ArgumentParser(description="Compare two Medicare Supplement rate databases")
    parser.add_argument("db1", help="Path to first database file")
    parser.add_argument("db2", help="Path to second database file")
    parser.add_argument("--state", help="Limit comparison to a specific state (e.g., TX)")
    parser.add_argument("--naic", help="Limit comparison to a specific carrier NAIC")
    parser.add_argument("--date", help="Limit comparison to a specific effective date (YYYY-MM-DD)")
    parser.add_argument("--output", help="Path to save JSON output of comparison results")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    
    args = parser.parse_args()
    
    try:
        comparator = DatabaseComparator(args.db1, args.db2, args.verbose)
        
        print("Running comprehensive comparison. This may take a while...")
        results = comparator.run_comprehensive_comparison(args.state, args.naic, args.date)
        
        # Print human-readable summary
        comparator.print_comparison_summary(results)
        
        # Save detailed results if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nDetailed results saved to {args.output}")
        
        # Close database connections
        comparator.close()
        
        # Return exit code based on comparison result
        if (results['schemas']['match'] and results['counts']['match'] and 
            results['mappings']['match'] and results['rates']['match']):
            return 0  # Success - databases match
        else:
            return 1  # Failure - databases have differences
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return 2  # Error during comparison

if __name__ == "__main__":
    sys.exit(main())