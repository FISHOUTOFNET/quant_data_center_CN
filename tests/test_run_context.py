from __future__ import annotations


def test_pipeline_run_context_restores_previous_value() -> None:
    from src.utils.run_context import current_pipeline_run_id, pipeline_log_identity, pipeline_run_context

    assert current_pipeline_run_id() is None

    with pipeline_run_context("run-outer"):
        assert current_pipeline_run_id() == "run-outer"
        identity = pipeline_log_identity()
        assert identity["run_id"] == "run-outer"
        assert isinstance(identity["pid"], int)
        assert isinstance(identity["thread"], str)

        with pipeline_run_context("run-inner"):
            assert current_pipeline_run_id() == "run-inner"

        assert current_pipeline_run_id() == "run-outer"

    assert current_pipeline_run_id() is None
