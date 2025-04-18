#!/bin/bash

# delete_state.sh - Script to delete all data for a state from medicare.duckdb
# Usage: ./delete_state.sh STATE_CODE
# Example: ./delete_state.sh TX

# Check if a state code was provided
if [ $# -ne 1 ]; then
    echo "Error: Missing state code argument"
    echo "Usage: $0 STATE_CODE"
    echo "Example: $0 TX"
    exit 1
fi

# Get the state code and convert to uppercase
STATE=$(echo $1 | tr '[:lower:]' '[:upper:]')

# Validate state code format (2 letters)
if ! [[ $STATE =~ ^[A-Z]{2}$ ]]; then
    echo "Error: Invalid state code format. Must be 2 letters (e.g., TX, CA)"
    exit 1
fi

# Database file path
DB_FILE="medicare.duckdb"

# Check if database file exists
if [ ! -f "$DB_FILE" ]; then
    echo "Error: Database file '$DB_FILE' not found"
    exit 1
fi

# Confirm with the user
echo "WARNING: This will delete ALL data for state '$STATE' from the database."
echo "Are you sure you want to continue? (y/n)"
read -r CONFIRM

if [[ ! $CONFIRM =~ ^[Yy]$ ]]; then
    echo "Operation cancelled."
    exit 0
fi

# Count records before deletion
echo "Counting records for state '$STATE'..."
REGION_COUNT=$(duckdb "$DB_FILE" "SELECT COUNT(*) FROM rate_regions WHERE state = '$STATE';" -csv -noheader)
RATE_COUNT=$(duckdb "$DB_FILE" "SELECT COUNT(*) FROM rate_store WHERE state = '$STATE';" -csv -noheader)

echo "Found $REGION_COUNT regions and $RATE_COUNT rates for state '$STATE'"

# Execute deletion with transaction
echo "Deleting data for state '$STATE'..."
duckdb "$DB_FILE" "
BEGIN TRANSACTION;

-- Delete from region_mapping
DELETE FROM region_mapping 
WHERE region_id IN (SELECT region_id FROM rate_regions WHERE state = '$STATE');

-- Delete from rate_store 
DELETE FROM rate_store 
WHERE region_id IN (SELECT region_id FROM rate_regions WHERE state = '$STATE') 
   OR state = '$STATE';

-- Delete from rate_regions
DELETE FROM rate_regions WHERE state = '$STATE';

-- Delete processed data
DELETE FROM processed_data WHERE state = '$STATE';

COMMIT;
"

# Verify deletion
REMAINING_REGIONS=$(duckdb "$DB_FILE" "SELECT COUNT(*) FROM rate_regions WHERE state = '$STATE';" -csv -noheader)
REMAINING_RATES=$(duckdb "$DB_FILE" "SELECT COUNT(*) FROM rate_store WHERE state = '$STATE';" -csv -noheader)

if [ "$REMAINING_REGIONS" -eq 0 ] && [ "$REMAINING_RATES" -eq 0 ]; then
    echo "Successfully deleted all data for state '$STATE'"
else
    echo "Error: Deletion may not have been complete."
    echo "Remaining regions: $REMAINING_REGIONS"
    echo "Remaining rates: $REMAINING_RATES"
    exit 1
fi

# Optimize database
echo "Optimizing database..."
duckdb "$DB_FILE" "VACUUM; ANALYZE;"

echo "Operation completed successfully."
echo "Deleted $REGION_COUNT regions and $RATE_COUNT rates for state '$STATE'"