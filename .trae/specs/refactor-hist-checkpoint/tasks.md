# Tasks

- [x] Task 1: Add `_hist_date_range()` function to read actual date range from stored data
  - [x] SubTask 1.1: Create function that reads parquet file and returns (min_date, max_date)
  - [x] SubTask 1.2: Handle case when file doesn't exist (return None, None)
  - [x] SubTask 1.3: Handle case when file exists but is empty or has no date column

- [x] Task 2: Modify `plan_akshare_hist_tasks()` to use actual date range
  - [x] SubTask 2.1: Call `_hist_date_range()` for each code and adjust combination
  - [x] SubTask 2.2: Set `AkShareHistTask.start_date` and `end_date` from stored data
  - [x] SubTask 2.3: Keep original behavior for full mode when no existing data (use config start date)

- [x] Task 3: Add `_prefilter_hist_tasks()` function for pre-filtering by calendar date
  - [x] SubTask 3.1: Get latest calendar date from store
  - [x] SubTask 3.2: Skip tasks where `end_date >= latest_calendar_date` and output file exists
  - [x] SubTask 3.3: Log skipped task count and ratio

- [x] Task 4: Integrate prefilter into `update_akshare_hist()` main function
  - [x] SubTask 4.1: Call `_prefilter_hist_tasks()` before task execution loop
  - [x] SubTask 4.2: Remove redundant `should_skip_checkpoint()` call for hist tasks (or keep for other datasets)

- [x] Task 5: Update checkpoint record to use actual date range
  - [x] SubTask 5.1: Verify checkpoint_row uses task.start_date and task.end_date correctly
  - [x] SubTask 5.2: Ensure success_metadata and failed_metadata use correct dates

# Task Dependencies
- Task 2 depends on Task 1
- Task 3 depends on Task 2 (needs tasks with actual date range)
- Task 4 depends on Task 3
- Task 5 depends on Task 2
