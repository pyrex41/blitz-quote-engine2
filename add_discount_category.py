import sqlite3
import duckdb

# Connect to the SQLite database
sqlite_conn = sqlite3.connect('msr_target.db')
sqlite_cursor = sqlite_conn.cursor()

# Connect to the DuckDB database
duck_conn = duckdb.connect('medicare.duckdb')

# First, add the discount_category column to the carrier_info table if it doesn't exist
duck_conn.execute("""
ALTER TABLE carrier_info
ADD COLUMN IF NOT EXISTS discount_category VARCHAR;
""")

# Get the discount_category data from SQLite
sqlite_cursor.execute("""
SELECT naic, discount_category 
FROM carrier_selection
WHERE discount_category IS NOT NULL AND discount_category != '';
""")

discount_data = sqlite_cursor.fetchall()

# Update the DuckDB table with the discount_category data
for naic, discount_category in discount_data:
    duck_conn.execute("""
    UPDATE carrier_info
    SET discount_category = ?
    WHERE naic = ?;
    """, [discount_category, naic])

# Commit the changes
duck_conn.commit()

# Check the results
result = duck_conn.execute("SELECT naic, company_name, selected, discount_category FROM carrier_info WHERE discount_category IS NOT NULL LIMIT 10").fetchall()
print("Updated records:")
for row in result:
    print(row)

# Close connections
sqlite_conn.close()
duck_conn.close()

print("Discount category column added and populated successfully!") 