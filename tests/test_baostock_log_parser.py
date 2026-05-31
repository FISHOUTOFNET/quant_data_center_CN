from __future__ import annotations

from datetime import datetime

from tests.performance.baostock_log_parser import analyze_lines


def test_log_parser_reports_overlaps_unmatched_api_and_session_errors() -> None:
    lines = [
        "2026-05-16 17:20:52.525 | INFO | src.pipeline.update_daily:update_daily:83 - Daily update started run_id=run-a pid=100 thread=MainThread dataset=all mode=full force=False resume=True candidate_start=1990-01-01 candidate_end=2026-05-15",
        "2026-05-16 17:20:53.008 | INFO | src.api.baostock_client:login:88 - Baostock login succeeded run_id=run-a pid=100 thread=MainThread",
        "2026-05-16 17:21:00.000 | INFO | src.api.baostock_client:_call_once:113 - Baostock API started run_id=run-a pid=100 thread=MainThread action=query_history_k_data_plus code=sh.600000",
        "2026-05-16 17:21:01.000 | INFO | src.api.baostock_client:_call_once:113 - Baostock API started run_id=run-a pid=100 thread=MainThread action=query_history_k_data_plus code=sh.600000",
        "2026-05-16 17:21:02.000 | ERROR | src.pipeline.update_daily:update_daily:376 - Daily bar API failed for sh.600000",
        "src.api.baostock_client.BaostockError: query_history_k_data_plus failed: 10001001 用户未登录",
        "2026-05-16 17:45:03.868 | INFO | src.pipeline.update_daily:update_daily:83 - Daily update started run_id=run-b pid=200 thread=MainThread dataset=all mode=full force=False resume=True candidate_start=1990-01-01 candidate_end=2026-05-15",
        "2026-05-16 17:45:04.313 | INFO | src.api.baostock_client:login:88 - Baostock login succeeded run_id=run-b pid=200 thread=MainThread",
        "2026-05-16 17:46:00.000 | INFO | src.api.baostock_client:logout:95 - Baostock logout completed run_id=run-b pid=200 thread=MainThread",
        "2026-05-16 17:46:01.000 | INFO | src.pipeline.update_daily:update_daily:407 - Daily update completed run_id=run-b pid=200 thread=MainThread records=0 success=0 failed=0 skipped=0",
    ]

    report = analyze_lines(lines)

    assert report["run_count"] == 2
    assert report["unfinished_runs"] == ["run-a"]
    assert report["overlapping_runs"] == [{"previous_run_id": "run-a", "next_run_id": "run-b"}]
    assert report["duplicate_open_api_starts"][0]["run_id"] == "run-a"
    assert report["unmatched_api_starts"][0]["code"] == "sh.600000"
    assert report["session_error_count"] == 1
    assert report["session_error_codes"] == ["sh.600000"]


def test_log_parser_summarizes_api_io_flush_refetch_and_silent_gap() -> None:
    lines = [
        "2026-05-20 18:19:12.652 | INFO | src.pipeline.update_daily:_update_daily_impl:117 - Daily update started run_id=daily-a pid=21064 thread=MainThread dataset=all mode=partial force=False resume=True candidate_start=1990-01-01 candidate_end=2026-05-20",
        "2026-05-20 18:19:13.000 | INFO | src.api.baostock_client:_call_once:206 - Baostock API started run_id=daily-a pid=21064 thread=MainThread action=query_adjust_factor code=sh.600000",
        "2026-05-20 18:19:13.250 | INFO | src.api.baostock_client:_call_once:214 - Baostock API completed run_id=daily-a pid=21064 thread=MainThread action=query_adjust_factor code=sh.600000",
        "2026-05-20 18:19:14.000 | INFO | src.storage.parquet_store:write_dataset:381 - Dataset Parquet stored run_id=daily-a pid=21064 thread=update-baostock-daily-background_0 dataset=baostock_cn_stock_daily_bar_qfq rows=2 path=C:\\data.parquet",
        "2026-05-20 18:19:15.000 | INFO | src.pipeline.services:flush:76 - Pipeline metadata flush completed run_id=daily-a pid=21064 thread=update-baostock-daily-background_0 elapsed=0.500s run_rows=200 status_rows=200 checkpoint_rows=200",
        "2026-05-20 18:19:16.000 | WARNING | src.pipeline.update_daily_worker:_log_full_refetch:425 - Daily lookback unadjusted_empty_lookback for baostock_cn_stock_daily_bar_qfq sh.600000 from 2026-05-06 to 2026-05-20; refetching from 1990-01-01",
    ]

    report = analyze_lines(lines, now=datetime(2026, 5, 20, 18, 22, 0), silent_gap_seconds=120)

    run = report["runs"]["daily-a"]
    assert run["dataset"] == "all"
    assert run["mode"] == "partial"
    assert run["api_call_count"] == 1
    assert run["api_total_seconds"] == 0.25
    assert run["api_max_seconds"] == 0.25
    assert run["parquet_write_count"] == 1
    assert run["metadata_flush_count"] == 1
    assert run["metadata_flush_total_seconds"] == 0.5
    assert run["full_refetch_count"] == 1
    assert report["silent_runs"] == [{"run_id": "daily-a", "seconds_since_last_log": 164.0}]
