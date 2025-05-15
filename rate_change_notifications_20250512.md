# Medicare Rate Change Notifications

Generated on: 2025-05-12 20:52:59

Date range: 2025-05-12 → 2025-11-01

## Important Notes

* Rates are stored using the API's natural effective dates, not the requested dates
* A carrier might have multiple rates with different effective dates in the database
* Rate changes are detected by comparing rates returned by the API, not the dates

## Rate Changes Detected

### American Family Life Assur Co of Col (60380) - CA

- Requested query date: 2025-11-01
- API returned effective date(s): 2025-07-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### Allstate Health Solutions (82538) - UT

- Requested query date: 2025-11-01
- API returned effective date(s): 2025-05-09T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### Allstate Health Solutions (82538) - FL

- Requested query date: 2025-11-01
- API returned effective date(s): 2025-07-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - OK

- Requested query date: 2025-11-01
- API returned effective date(s): 2025-01-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - WI

- Requested query date: 2025-11-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### UnitedHealthcare Ins Co (79413) - SC

- Requested query date: 2025-11-01
- Status: Failed - Full refresh triggered due to rate changes

### American Family Life Assur Co of Col (60380) - MS

- Requested query date: 2025-11-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes

### American Family Life Assur Co of Col (60380) - NJ

- Requested query date: 2025-11-01
- API returned effective date(s): 2025-06-01T00:00:00
- Status: Successful - Full refresh triggered due to rate changes


## Summary

- Total carriers checked: 358
- Carriers with rate changes: 7
- Carriers successfully updated: 7

### Updates by State

**CA**:
- American Family Life Assur Co of Col (60380)

**UT**:
- Allstate Health Solutions (82538)

**FL**:
- Allstate Health Solutions (82538)

**OK**:
- UnitedHealthcare Ins Co (79413)

**WI**:
- UnitedHealthcare Ins Co (79413)

**MS**:
- American Family Life Assur Co of Col (60380)

**NJ**:
- American Family Life Assur Co of Col (60380)


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
