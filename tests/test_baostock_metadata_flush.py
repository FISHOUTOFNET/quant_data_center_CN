from __future__ import annotations

from src.sources.baostock.update_daily_worker import _DailyUpdateBackgroundWorker


class _FailingMetadataBatch:
    def flush(self) -> None:
        raise RuntimeError("metadata store unavailable")


def test_flush_metadata_failure_record_has_cli_fields() -> None:
    worker = _DailyUpdateBackgroundWorker(
        store=None,
        config=None,
        mode="partial",
        start_date="2026-06-12",
        end_date="2026-06-12",
        metadata_batch=_FailingMetadataBatch(),
    )

    result = worker.flush_metadata()

    assert len(result.run_records) == 1
    record = result.run_records[0]
    assert record["dataset"] == "__metadata__"
    assert record["code"] == "*"
    assert record["status"] == "failed"
    assert record["row_count"] == 0
    assert "metadata store unavailable" in str(record["error_stack"])
