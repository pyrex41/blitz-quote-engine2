#!/usr/bin/env python3

import requests
import json
from datetime import datetime
import sys
from pprint import pprint

# API endpoint and key
API_URL = "http://localhost:8001/quotes/"
API_KEY = "yVujgWOYsLOJxGaicK69TPYVKgwMmqgb"

# Test parameters
params = {
    "zip_code": "33180",
    "state": "FL",
    "age": 65,
    "tobacco": "false",
    "gender": "M",
    "plans": "G",
    "carriers": "supported"
}

# Effective dates to test - focusing on the dates that worked
effective_dates = [
    "2025-05-01",
    "2025-06-01",
    "2025-07-01"
]

# Headers
headers = {"X-API-Key": API_KEY}

# Store results for each date
results = {}

def get_quotes(effective_date):
    """Get quotes for a specific effective date"""
    params_with_date = params.copy()
    params_with_date["effective_date"] = effective_date
    
    try:
        response = requests.get(API_URL, params=params_with_date, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching quotes for {effective_date}: {e}")
        return []

# Get quotes for each effective date
for date in effective_dates:
    print(f"Fetching quotes for effective date: {date}")
    results[date] = get_quotes(date)
    print(f"Found {len(results[date])} quotes for {date}")

# Analyze results to find differences
differences = {}

# Find all unique NAICs
all_naics = set()
for date_results in results.values():
    for quote in date_results:
        all_naics.add(quote["naic"])

# Compare rates across dates for each NAIC
for naic in all_naics:
    naic_rates = {}
    for date in effective_dates:
        for quote in results[date]:
            if quote["naic"] == naic:
                if quote["quotes"] and len(quote["quotes"]) > 0:
                    # Get the first quote rate
                    naic_rates[date] = {
                        "rate": quote["quotes"][0]["rate"],
                        "discount_rate": quote["quotes"][0]["discount_rate"],
                        "company_name": quote["company_name"]
                    }
                break
    
    # Check if rates differ across dates
    rates_list = [(date, naic_rates.get(date, {}).get("rate")) for date in effective_dates if date in naic_rates]
    
    # Only check if we have at least two dates with rates
    if len(rates_list) >= 2:
        # Extract just the rates
        rates = [rate for _, rate in rates_list if rate is not None]
        
        # Check if the rates are different
        if len(set(rates)) > 1:
            differences[naic] = {
                "company_name": naic_rates[effective_dates[0]]["company_name"] if effective_dates[0] in naic_rates else "Unknown",
                "rates": {date: naic_rates.get(date, {}).get("rate") for date in effective_dates if date in naic_rates},
                "discount_rates": {date: naic_rates.get(date, {}).get("discount_rate") for date in effective_dates if date in naic_rates}
            }

# Print differences
if differences:
    print("\nNAICs with rate changes across effective dates:")
    for naic, data in differences.items():
        print(f"\nNAIC: {naic} - {data['company_name']}")
        print("Rates:")
        for date, rate in data["rates"].items():
            print(f"  {date}: ${rate/100:.2f}")
        print("Discount Rates:")
        for date, rate in data["discount_rates"].items():
            print(f"  {date}: ${rate/100:.2f}")
else:
    print("\nNo rate differences found across the effective dates.")

print("\nSummary:")
print(f"Total NAICs: {len(all_naics)}")
print(f"NAICs with rate changes: {len(differences)}")

# Save full results to a file
with open("effective_date_test_results.json", "w") as f:
    json.dump({
        "test_run": datetime.now().isoformat(),
        "all_results": results,
        "differences": differences
    }, f, indent=2) 