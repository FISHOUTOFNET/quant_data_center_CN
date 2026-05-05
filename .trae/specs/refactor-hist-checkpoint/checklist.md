# Checklist

- [x] `_hist_date_range()` function correctly reads min/max dates from stored parquet files
- [x] `plan_akshare_hist_tasks()` sets `start_date` and `end_date` from actual stored data
- [x] `_prefilter_hist_tasks()` correctly skips tasks where `end_date >= calendar_date`
- [x] Pre-filtering happens before API calls, reducing unnecessary API requests
- [x] Checkpoint records accurately reflect actual data date ranges
- [x] Full mode with no existing data still uses config start date as fallback
- [x] Logging shows skipped task count and ratio for transparency
- [x] Existing tests pass after changes (hist-related tests pass; delist/spot test failures are pre-existing issues unrelated to this change)
