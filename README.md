# Medicare Supplement Rate Update System

This system manages and updates Medicare Supplement rates across multiple states and carriers. It consists of several scripts that work together to check for rate changes and apply updates efficiently.

## Safety First

**Always backup your database before running updates:**
```bash
cp msr_target.db msr_target_copy.db
```

## Scripts Overview

### 1. check_script.py
Checks for rate changes across states and carriers.

**Key Features:**
- Can check all states or specific states
- Supports checking multiple months ahead
- Outputs results to JSON for use with map_file.py

**Common Usage:**
```bash
# Check all states for next 3 months
python check_script.py -a -m 3 -d msr_target.db -o check_results.json

# Check specific states with more thorough checking (3 ZIPs per state)
python check_script.py --multiple SC TX FL -m 3 -n 3 -d msr_target.db -o state_check.json
```

**Key Options:**
- `-a`: Process all states
- `--multiple STATE1 STATE2`: Process specific states
- `-m MONTHS`: Number of months to check ahead
- `-n NUM_ZIPS`: Number of ZIP codes to check per state
- `-o OUTPUT`: Output file for results
- `-d DB`: Database file to use
- `--no-sync`: Skip Turso sync

### 2. map_file.py
Performs bulk updates based on check_script.py results.

**Common Usage:**
```bash
# Update rates based on check results
python map_file.py -d msr_target.db -f check_results.json -m 3
```

**Key Options:**
- `-d DB`: Database file to use
- `-f FILE`: Input JSON file from check_script.py
- `-m MONTHS`: Number of months to process
- `--dry-run`: Show what would be updated without making changes

### 3. update_carrier.py
Targets specific carrier/state combinations for updates.

**Common Usage:**
```bash
# Update specific carrier for next 3 months
python update_carrier.py -s SC -n 60052 -d msr_target.db -m 3

# Update for specific date
python update_carrier.py -s SC -n 60052 -d msr_target.db -e 2025-04-01
```

**Key Options:**
- `-s STATE`: State code (required)
- `-n NAIC`: NAIC code of carrier (required)
- `-d DB`: Database file (required)
- `-m MONTHS`: Number of months ahead to process
- `-e DATE`: Specific effective date (YYYY-MM-DD)
- `--dry-run`: Show what would be updated without making changes
- `--out FILE`: Save results to JSON file

### 4. rebuild_mapping.py
Rebuilds the ZIP code to carrier mappings in the database.

**Key Features:**
- Can rebuild mappings for specific carriers, states, or all combinations
- Fixes issues with missing ZIP codes in rate maps
- Does not modify rate data, only rebuilds mappings

**Common Usage:**
```bash
# Rebuild mapping for a specific carrier in a state
python rebuild_mapping.py -s TX -n 60984 -d msr_target.db

# Rebuild all carrier mappings for a state
python rebuild_mapping.py -s TX --all-for-state -d msr_target.db

# Rebuild all mappings in the database
python rebuild_mapping.py -a -d msr_target.db
```

**Key Options:**
- `-s STATE`: State code
- `-n NAIC`: NAIC code of carrier
- `-a`: Rebuild all mappings for all states
- `--all-for-state`: Rebuild all carrier mappings for specified state
- `-d DB`: Database file (required)
- `--dry-run`: Show what would be updated without making changes
- `--out FILE`: Save results to JSON file

## Complete Workflow

1. **Initial Database Backup**
   ```bash
   cp msr_target.db msr_target_copy.db
   ```

2. **Check for Changes**
   ```bash
   # Broad check across all states
   python check_script.py -a -m 3 -d msr_target.db -o check_results.json
   ```

3. **Bulk Update**
   ```bash
   # Apply updates based on check results
   python map_file.py -d msr_target.db -f check_results.json -m 3
   ```

4. **Verification**
   ```bash
   # Run another check to verify updates
   python check_script.py -a -m 3 -d msr_target.db -o verification.json
   ```

5. **Targeted Updates** (if needed)
   ```bash
   # Update specific problematic carriers
   python update_carrier.py -s STATE -n NAIC -d msr_target.db -m 3
   ```

6. **Fix Mapping Issues** (if needed)
   ```bash
   # Rebuild mappings for specific problematic carrier
   python rebuild_mapping.py -s STATE -n NAIC -d msr_target.db
   ```

## Date Handling

All scripts handle dates consistently:
- Dates are always the 1st of the month
- `-m` flag processes multiple months (current month + months ahead)
- `-e` flag can specify a particular effective date
- Default behavior processes the next month if no date specified

## Common Patterns

1. **Thorough State Check:**
   ```bash
   python check_script.py --multiple SC TX FL -m 3 -n 3 -d msr_target.db -o detailed_check.json
   ```

2. **Dry Run Updates:**
   ```bash
   python map_file.py -d msr_target.db -f check_results.json --dry-run
   python update_carrier.py -s SC -n 60052 -d msr_target.db -m 3 --dry-run
   ```

3. **Multiple State/Carrier Updates:**
   ```bash
   # Check specific states
   python check_script.py --multiple SC TX FL -m 3 -o state_check.json
   
   # Update those states
   python map_file.py -d msr_target.db -f state_check.json -m 3
   
   # Target specific carriers if needed
   python update_carrier.py -s SC -n 60052 -d msr_target.db -m 3
   python update_carrier.py -s TX -n 71412 -d msr_target.db -m 3
   ```

4. **Fixing ZIP Code Mapping Issues:**
   ```bash
   # Fix mapping for a specific carrier that's missing ZIP codes
   python rebuild_mapping.py -s MI -n 60984 -d msr_target.db
   
   # Fix all mappings for a problematic state
   python rebuild_mapping.py -s MI --all-for-state -d msr_target.db
   ```

## Troubleshooting

1. **If updates fail:**
   - Check the log files (*.log) for error messages
   - Use `--dry-run` to verify what would be updated
   - Try updating specific carriers with update_carrier.py

2. **If changes aren't appearing:**
   - Verify the effective dates being processed
   - Check if the state/NAIC combination is correct
   - Run check_script.py again to verify changes

3. **Database issues:**
   - Restore from backup: `cp msr_target_copy.db msr_target.db`
   - Ensure Turso sync is working (unless using --no-sync)

4. **Missing ZIP codes in mappings:**
   - Use rebuild_mapping.py to rebuild the carrier-state mappings
   - Check the group_mapping table to verify ZIP codes are correctly mapped