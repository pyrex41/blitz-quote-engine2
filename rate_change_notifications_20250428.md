# Medicare Rate Change Notifications

Generated on: 2025-04-28 18:19:05

Date range: 2025-04-28 → 2025-10-01

## Important Notes

* Rates are stored using the API's natural effective dates, not the requested dates
* A carrier might have multiple rates with different effective dates in the database
* Rate changes are detected by comparing rates returned by the API, not the dates

## Rate Changes Detected

### Cigna Ins Co (65269) - CO

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - UT

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - RI

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - WA

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### Cigna Hlth & Life Ins Co (67369) - OR

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### Cigna National Health Ins Co (61727) - OK

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - OK

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-01-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - SC

- Requested query date: 2025-10-01
- Status: Failed - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - TX

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-07-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - VA

- Requested query date: 2025-10-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes


## Summary

- Total carriers checked: 358
- Carriers with rate changes: 9
- Carriers successfully updated: 9

### Updates by State

**CO**:
- Cigna Ins Co (65269)

**UT**:
- UnitedHealthcare Ins Co (79413)

**RI**:
- UnitedHealthcare Ins Co (79413)

**WA**:
- UnitedHealthcare Ins Co (79413)

**OR**:
- Cigna Hlth & Life Ins Co (67369)

**OK**:
- Cigna National Health Ins Co (61727)
- UnitedHealthcare Ins Co (79413)

**TX**:
- UnitedHealthcare Ins Co (79413)

**VA**:
- UnitedHealthcare Ins Co (79413)


## Verification Query

To verify that rates with their natural effective dates are stored correctly, run this query:

```sql
SELECT c.company_name, r.naic, r.plan, r.rate, r.discount_rate, r.effective_date
FROM rate_store r
JOIN region_mapping m ON r.region_id = m.region_id AND r.naic = m.naic
LEFT JOIN carrier_info c ON r.naic = c.naic
WHERE m.zip_code = '64105' AND r.plan = 'G' AND r.gender = 'M'
AND r.tobacco = 0 AND r.age = 65 AND r.naic = '82538'
ORDER BY r.effective_date;
```

This should show entries with their natural effective dates as provided by the API.
