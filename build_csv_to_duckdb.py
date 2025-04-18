#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import json
from datetime import datetime
import duckdb
import pandas as pd
import glob

def setup_logging(quiet: bool = False) -> None:
    """Set up logging to file and console."""
    log_filename = f'build_csv_to_duckdb_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler() if not quiet else logging.NullHandler()
        ]
    )

class CSVToDuckDBBuilder:
    def __init__(self, db_path: str, csv_dir: str):
        """Initialize the DuckDB Medicare rate database builder from CSV files."""
        self.db_path = db_path
        self.csv_dir = csv_dir
        
        # Connect to DuckDB
        self.conn = duckdb.connect(db_path)
        
        # Create necessary tables
        self._create_tables()

    def _create_tables(self):
        """Create tables based on the CSV structure."""
        logging.info("Creating database tables if they don't exist")
        
        # Create rates table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rates (
                Company TEXT,
                Company_Old TEXT,
                NAIC TEXT,
                Plan TEXT,
                State TEXT,
                Area INTEGER,
                Zip_Lookup_Code INTEGER,
                Gender TEXT,
                Tobacco TEXT,
                Couple_Fac TEXT,
                Eff_Date TIMESTAMP,
                Rate_Type TEXT,
                Age_For_Sorting INTEGER,
                Lowest_Rate TEXT,
                Highest_Rate TEXT,
                Age TEXT,
                Monthly_Rate DECIMAL(10,2),
                Quarterly_Rate DECIMAL(10,2),
                Semi_Annual_Rate DECIMAL(10,2),
                Annual_Rate DECIMAL(10,2),
                Policy_Fee TEXT,
                Household_Discount TEXT
            )
        """)
        
        # Create zip_codes table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS zip_codes (
                Zip_Lookup_Code INTEGER,
                State TEXT,
                County TEXT,
                City TEXT,
                ZIP3 TEXT,
                ZIP5 TEXT
            )
        """)
        
        # Create underwriting_conditions table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS underwriting_conditions (
                Company TEXT,
                Company_Old TEXT,
                State TEXT,
                Category TEXT,
                Condition TEXT,
                Criteria TEXT
            )
        """)
        
        # Create processed_data table (needed for API refresh)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_data (
                state TEXT,
                naic TEXT,
                effective_date TEXT,
                api_effective_date TEXT,
                processed_at TIMESTAMP,
                success BOOLEAN,
                PRIMARY KEY (state, naic, effective_date)
            )
        """)
        
        # Create optimized rate lookup views and tables
        self._create_optimized_views()
    
    def _create_optimized_views(self):
        """Create optimized views for rate lookups."""
        logging.info("Creating optimized views for rate lookups")
        
        # Create rate_regions table based on API schema
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_regions (
                region_id TEXT PRIMARY KEY,
                naic TEXT,
                state TEXT,
                mapping_type TEXT,
                region_data TEXT,
                zip_lookup_code INTEGER
            )
        """)
        
        # Create region_mapping table needed by API refresh
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS region_mapping (
                zip_code TEXT,
                region_id TEXT,
                naic TEXT,
                PRIMARY KEY (zip_code, naic)
            )
        """)
        
        # Create optimized rate_store table that will be used for lookups
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_store (
                region_id TEXT,
                gender TEXT,
                tobacco INTEGER,
                age INTEGER,
                naic TEXT,
                plan TEXT,
                rate DECIMAL(10,2),
                discount_rate DECIMAL(10,2),
                effective_date TIMESTAMP,
                state TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (region_id, gender, tobacco, age, naic, plan, effective_date, state)
            )
        """)
        
        # Create carrier_info table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS carrier_info (
                naic TEXT PRIMARY KEY,
                company_name TEXT,
                selected INTEGER DEFAULT 1
            )
        """)
        
        # Create indexes for optimized lookups
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rates_lookup ON rates (NAIC, State, Zip_Lookup_Code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rates_zip_lookup ON rates (Zip_Lookup_Code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_zip_codes_lookup ON zip_codes (Zip_Lookup_Code)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_zip_codes_zip5 ON zip_codes (ZIP5)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rate_store_lookup ON rate_store (region_id, gender, tobacco, age)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_rate_store_naic ON rate_store (naic)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_region_mapping_zip ON region_mapping (zip_code)")
    
    def import_rates_csv(self, file_path: str, batch_size: int = 10000):
        """Import rates from CSV file."""
        try:
            logging.info(f"Importing rates from {file_path}")
            
            # Read CSV in chunks to handle large files
            total_imported = 0
            for chunk in pd.read_csv(file_path, sep=';', chunksize=batch_size, dtype=str):
                # Clean column names by removing quotation marks and handling special characters
                chunk.columns = [col.strip('"') for col in chunk.columns]
                
                # Convert numeric columns
                for col in ['Monthly_Rate', 'Quarterly_Rate', 'Semi-Annual_Rate', 'Annual_Rate']:
                    if col in chunk.columns:
                        # Handle potential non-numeric values
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                
                for col in ['Area', 'Zip_Lookup_Code', 'Age_For_Sorting']:
                    if col in chunk.columns:
                        # Handle potential non-numeric values
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                
                # Convert date column
                if 'Eff_Date' in chunk.columns:
                    chunk['Eff_Date'] = pd.to_datetime(chunk['Eff_Date'], errors='coerce')
                
                # Insert the data using DuckDB's append method
                self.conn.register('chunk_df', chunk)
                
                # Insert into rates table
                self.conn.execute("""
                    INSERT INTO rates
                    SELECT * FROM chunk_df
                """)
                
                # Also populate carrier_info table
                self.conn.execute("""
                    INSERT OR REPLACE INTO carrier_info (naic, company_name, selected)
                    SELECT DISTINCT NAIC, Company, 1
                    FROM chunk_df
                """)
                
                total_imported += len(chunk)
                logging.info(f"Imported {total_imported} rate records so far")
            
            logging.info(f"Completed importing {total_imported} rate records from {file_path}")
            return total_imported
            
        except Exception as e:
            logging.error(f"Error importing rates from {file_path}: {str(e)}")
            return 0
    
    def import_zip_codes_csv(self, file_path: str, batch_size: int = 10000):
        """Import ZIP codes from CSV file."""
        try:
            logging.info(f"Importing ZIP codes from {file_path}")
            
            # Read CSV in chunks to handle large files
            total_imported = 0
            for chunk in pd.read_csv(file_path, sep=';', chunksize=batch_size, dtype=str):
                # Clean column names by removing quotation marks and handling special characters
                chunk.columns = [col.strip('"') for col in chunk.columns]
                
                # Rename columns to match our schema
                column_mapping = {
                    'ZIP LOOKUP CODE': 'Zip_Lookup_Code',
                    'STATE': 'State',
                    'COUNTY': 'County',
                    'CITY': 'City',
                    'ZIP-3': 'ZIP3',
                    'ZIP-5': 'ZIP5'
                }
                
                chunk = chunk.rename(columns=column_mapping)
                
                # Convert numeric columns
                if 'Zip_Lookup_Code' in chunk.columns:
                    chunk['Zip_Lookup_Code'] = pd.to_numeric(chunk['Zip_Lookup_Code'], errors='coerce')
                
                # Insert the data using DuckDB's append method
                self.conn.register('chunk_df', chunk)
                
                # Insert into zip_codes table
                self.conn.execute("""
                    INSERT INTO zip_codes
                    SELECT * FROM chunk_df
                """)
                
                total_imported += len(chunk)
                logging.info(f"Imported {total_imported} ZIP code records so far")
            
            logging.info(f"Completed importing {total_imported} ZIP code records from {file_path}")
            return total_imported
            
        except Exception as e:
            logging.error(f"Error importing ZIP codes from {file_path}: {str(e)}")
            return 0
    
    def import_underwriting_csv(self, file_path: str, batch_size: int = 10000):
        """Import underwriting conditions from CSV file."""
        try:
            logging.info(f"Importing underwriting conditions from {file_path}")
            
            # Read CSV in chunks to handle large files
            total_imported = 0
            for chunk in pd.read_csv(file_path, sep=';', chunksize=batch_size, dtype=str):
                # Clean column names by removing quotation marks and handling special characters
                chunk.columns = [col.strip('"') for col in chunk.columns]
                
                # Insert the data using DuckDB's append method
                self.conn.register('chunk_df', chunk)
                
                # Insert into underwriting_conditions table
                self.conn.execute("""
                    INSERT INTO underwriting_conditions
                    SELECT * FROM chunk_df
                """)
                
                total_imported += len(chunk)
                logging.info(f"Imported {total_imported} underwriting condition records so far")
            
            logging.info(f"Completed importing {total_imported} underwriting condition records from {file_path}")
            return total_imported
            
        except Exception as e:
            logging.error(f"Error importing underwriting conditions from {file_path}: {str(e)}")
            return 0
    
    def populate_optimized_tables(self):
        """Populate the optimized tables for rate lookups."""
        try:
            logging.info("Populating optimized rate lookup tables")
            
            # Step 1: Populate rate_regions with unique region IDs and appropriate mapping type
            self.conn.execute("""
                INSERT INTO rate_regions (region_id, naic, state, mapping_type, region_data, zip_lookup_code)
                SELECT 
                    CONCAT(NAIC, '_', State, '_', Zip_Lookup_Code) as region_id,
                    NAIC as naic,
                    State as state,
                    'zip5' as mapping_type,  -- Assuming ZIP-based region mapping
                    NULL as region_data,     -- Will be updated in next step
                    Zip_Lookup_Code as zip_lookup_code
                FROM 
                    (SELECT DISTINCT NAIC, State, Zip_Lookup_Code FROM rates)
                WHERE 
                    NOT EXISTS (
                        SELECT 1 FROM rate_regions 
                        WHERE region_id = CONCAT(NAIC, '_', State, '_', Zip_Lookup_Code)
                    )
            """)
            
            # Step 2: Update region_data with ZIP codes in JSON format
            # First get all regions
            regions = self.conn.execute("""
                SELECT region_id, naic, state, zip_lookup_code
                FROM rate_regions 
                WHERE region_data IS NULL
            """).fetchall()
            
            for region in regions:
                region_id, naic, state, zip_lookup_code = region
                
                # Get all ZIP codes for this region
                zip_codes = self.conn.execute("""
                    SELECT ZIP5 
                    FROM zip_codes 
                    WHERE Zip_Lookup_Code = ? AND State = ?
                """, [zip_lookup_code, state]).fetchall()
                
                # Format as list of strings
                zip_list = [z[0] for z in zip_codes if z[0]]
                
                # Convert to JSON string
                json_data = json.dumps(zip_list)
                
                # Update region_data
                self.conn.execute("""
                    UPDATE rate_regions 
                    SET region_data = ? 
                    WHERE region_id = ?
                """, [json_data, region_id])
            
            # Step 3: Populate region_mapping with ZIP to region lookups
            self.conn.execute("""
                INSERT INTO region_mapping (zip_code, region_id, naic)
                SELECT
                    z.ZIP5 as zip_code,
                    r.region_id,
                    r.naic
                FROM
                    rate_regions r
                JOIN
                    zip_codes z ON r.zip_lookup_code = z.Zip_Lookup_Code AND r.state = z.State
                WHERE
                    z.ZIP5 IS NOT NULL AND z.ZIP5 != ''
                    AND NOT EXISTS (
                        SELECT 1 FROM region_mapping
                        WHERE zip_code = z.ZIP5 AND naic = r.naic
                    )
            """)
            
            # Step 4: Populate rate_store with normalized rate data
            # Translating T/NT to tobacco integer
            self.conn.execute("""
                INSERT INTO rate_store 
                (region_id, gender, tobacco, age, naic, plan, rate, discount_rate, effective_date, state)
                SELECT 
                    CONCAT(r.NAIC, '_', r.State, '_', r.Zip_Lookup_Code) as region_id,
                    r.Gender as gender,
                    CASE WHEN r.Tobacco = 'Tobacco' THEN 1 ELSE 0 END as tobacco,
                    CAST(r.Age_For_Sorting as INTEGER) as age,
                    r.NAIC as naic,
                    r.Plan as plan,
                    r.Monthly_Rate as rate,
                    CASE 
                        WHEN r.Household_Discount IS NOT NULL AND r.Household_Discount != '' 
                        THEN r.Monthly_Rate * (1 - CAST(REPLACE(r.Household_Discount, '%', '') as DECIMAL) / 100)
                        ELSE r.Monthly_Rate
                    END as discount_rate,
                    r.Eff_Date as effective_date,
                    r.State as state
                FROM rates r
                LEFT JOIN rate_store rs ON 
                    CONCAT(r.NAIC, '_', r.State, '_', r.Zip_Lookup_Code) = rs.region_id AND
                    r.Gender = rs.gender AND
                    (CASE WHEN r.Tobacco = 'Tobacco' THEN 1 ELSE 0 END) = rs.tobacco AND
                    CAST(r.Age_For_Sorting as INTEGER) = rs.age AND
                    r.NAIC = rs.naic AND
                    r.Plan = rs.plan AND
                    r.Eff_Date = rs.effective_date AND
                    r.State = rs.state
                WHERE rs.region_id IS NULL
            """)
            
            # Step 5: Populate processed_data table with initial entries
            self.conn.execute("""
                INSERT INTO processed_data (state, naic, effective_date, api_effective_date, processed_at, success)
                SELECT DISTINCT
                    r.state, 
                    r.naic, 
                    CAST(r.effective_date AS TEXT) as effective_date,
                    CAST(r.effective_date AS TEXT) as api_effective_date,
                    CURRENT_TIMESTAMP as processed_at,
                    true as success
                FROM 
                    rate_store r
                LEFT JOIN 
                    processed_data p ON r.state = p.state AND r.naic = p.naic AND CAST(r.effective_date AS TEXT) = p.effective_date
                WHERE 
                    p.state IS NULL
            """)
            
            region_count = self.conn.execute("SELECT COUNT(*) FROM rate_regions").fetchone()[0]
            mapping_count = self.conn.execute("SELECT COUNT(*) FROM region_mapping").fetchone()[0]
            rate_store_count = self.conn.execute("SELECT COUNT(*) FROM rate_store").fetchone()[0]
            processed_count = self.conn.execute("SELECT COUNT(*) FROM processed_data").fetchone()[0]
            
            logging.info(f"Created {region_count} unique rate regions")
            logging.info(f"Created {mapping_count} ZIP code to region mappings")
            logging.info(f"Populated rate_store with {rate_store_count} rate records")
            logging.info(f"Populated processed_data with {processed_count} entries")
            
            return True
            
        except Exception as e:
            logging.error(f"Error populating optimized tables: {str(e)}")
            return False
    
    def get_rate_by_zip(self, zip_code: str, gender: str, tobacco: int, age: int, effective_date: str) -> list:
        """Get rates for all carriers for specific demographic at a ZIP code."""
        try:
            # Using region_mapping for lookup to match API approach
            result = self.conn.execute("""
                SELECT 
                    rs.naic, 
                    c.company_name, 
                    rs.plan, 
                    rs.rate, 
                    rs.discount_rate
                FROM rate_store rs
                JOIN region_mapping rm ON rs.region_id = rm.region_id AND rs.naic = rm.naic
                LEFT JOIN carrier_info c ON rs.naic = c.naic
                WHERE rm.zip_code = ?
                  AND rs.gender = ?
                  AND rs.tobacco = ?
                  AND rs.age = ?
                  AND rs.effective_date <= ?
                ORDER BY rs.naic, rs.plan
            """, [zip_code, gender, tobacco, age, effective_date]).fetchall()
            
            return [
                {
                    "naic": row[0],
                    "company_name": row[1] or "Unknown",
                    "plan": row[2],
                    "rate": row[3],
                    "discount_rate": row[4]
                }
                for row in result
            ]
            
        except Exception as e:
            logging.error(f"Error getting rates for {zip_code}: {str(e)}")
            return []
    
    def import_all_csv_files(self):
        """Import all CSV files from the specified directory."""
        try:
            # Import rates CSV files
            rates_files = glob.glob(os.path.join(self.csv_dir, "rates*.csv"))
            for file_path in rates_files:
                self.import_rates_csv(file_path)
            
            # Import ZIP codes CSV files
            zip_files = glob.glob(os.path.join(self.csv_dir, "zip_codes*.csv"))
            for file_path in zip_files:
                self.import_zip_codes_csv(file_path)
            
            # Import underwriting CSV files
            uw_files = glob.glob(os.path.join(self.csv_dir, "UW*.csv"))
            for file_path in uw_files:
                self.import_underwriting_csv(file_path)
            
            # Populate optimized tables
            self.populate_optimized_tables()
            
            return True
            
        except Exception as e:
            logging.error(f"Error importing CSV files: {str(e)}")
            return False
    
    def optimize_database(self):
        """Run optimizations on the database."""
        logging.info("Running database optimizations...")
        
        # Vacuum the database
        self.conn.execute("VACUUM")
        
        # Run analyze to update statistics
        self.conn.execute("ANALYZE")
        
        logging.info("Database optimization complete")
    
    def close(self):
        """Close the database connection."""
        if hasattr(self, 'conn') and self.conn:
            try:
                self.optimize_database()
                self.conn.close()
                logging.info("Database connection closed")
            except Exception as e:
                logging.error(f"Error during database close: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Build a Medicare Supplement Rate database from CSV files using DuckDB")
    parser.add_argument("-d", "--db", type=str, default="medicare.duckdb", help="DuckDB database file path")
    parser.add_argument("-c", "--csv_dir", type=str, default="Medicare Supplement Data Feed - SAMPLE", help="Directory containing CSV files")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress console output")
    parser.add_argument("--lookup", action="store_true", help="Lookup mode - query rates for a specific ZIP")
    parser.add_argument("--zip", type=str, help="ZIP code to lookup rates for")
    parser.add_argument("--age", type=int, default=65, help="Age to lookup rates for")
    parser.add_argument("--gender", type=str, choices=["M", "F"], default="M", help="Gender to lookup rates for")
    parser.add_argument("--tobacco", type=int, choices=[0, 1], default=0, help="Tobacco status to lookup rates for")
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"), help="Effective date to lookup rates for (YYYY-MM-DD)")
    
    args = parser.parse_args()
    setup_logging(args.quiet)
    
    try:
        # Initialize the database builder
        builder = CSVToDuckDBBuilder(args.db, args.csv_dir)
        
        # Lookup mode - query rates for a specific ZIP
        if args.lookup:
            if not args.zip:
                logging.error("ZIP code is required for lookup mode")
                return
                
            rates = builder.get_rate_by_zip(
                args.zip, args.gender, args.tobacco, args.age, args.date
            )
            
            print(f"\nRates for ZIP {args.zip}, {args.gender}, age {args.age}, tobacco {args.tobacco}, {args.date}:")
            print("=" * 80)
            print(f"{'NAIC':<8} {'Company':<30} {'Plan':<5} {'Rate':>10} {'Discount':>10}")
            print("-" * 80)
            
            for rate in rates:
                print(f"{rate['naic']:<8} {rate['company_name'][:30]:<30} {rate['plan']:<5} "
                      f"{rate['rate']:>10.2f} {rate['discount_rate']:>10.2f}")
            
            print("=" * 80)
            print(f"Total: {len(rates)} rates found")
        else:
            # Import all CSV files and build the database
            builder.import_all_csv_files()
            builder.optimize_database()
            logging.info("Database build completed successfully")
        
        # Close the database
        builder.close()
        
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 