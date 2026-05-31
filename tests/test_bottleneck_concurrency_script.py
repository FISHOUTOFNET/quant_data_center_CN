from __future__ import annotations

from tests.performance import test_bottleneck_concurrency as concurrency_module


def test_concurrency_test_injects_background_worker_override(tmp_path, monkeypatch) -> None:
    config = concurrency_module.setup_test_environment(tmp_path)
    observed_workers = []

    def fake_update_daily(**kwargs):
        import src.pipeline.update_daily as update_daily_module

        resolved_config = update_daily_module.ConfigManager(kwargs["root"])
        observed_workers.append(resolved_config.get("pipeline.background_workers"))
        return []

    monkeypatch.setattr(concurrency_module, "update_daily", fake_update_daily)

    concurrency_module.run_concurrency_test(
        codes=["sh.600000"],
        end_date="2024-01-31",
        root=tmp_path,
        workers=7,
        config=config,
    )

    assert observed_workers == [7]
