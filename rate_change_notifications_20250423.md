# Medicare Rate Change Notifications

Generated on: 2025-04-23 10:15:11

Date range: 2025-04-23 → 2025-10-01

## Important Notes

* Rates are stored using the API's natural effective dates, not the requested dates
* A carrier might have multiple rates with different effective dates in the database
* Rate changes are detected by comparing rates returned by the API, not the dates

## Rate Changes Detected


## Summary

- Total carriers checked: 358
- Carriers with rate changes: 0
- Carriers successfully updated: 0

### Updates by State


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
