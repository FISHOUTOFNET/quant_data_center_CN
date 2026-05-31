from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.tools.akshare_stock_history_audit import (
    ApiCandidate,
    ProbeResult,
    build_call_kwargs,
    discover_candidates,
    earliest_observed_time,
    fetch_html,
    main,
    run_probe_rounds,
    write_markdown_report,
)


def test_discover_candidates_includes_history_and_excludes_current() -> None:
    html = """
    <h2>历史行情数据-东财</h2>
    <p>接口: stock_zh_a_hist</p>
    <div class="highlight"><pre>ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20170301", end_date="20240501", adjust="")</pre></div>
    <h2>实时行情数据</h2>
    <p>接口: stock_zh_a_spot_em</p>
    <div class="highlight"><pre>ak.stock_zh_a_spot_em()</pre></div>
    """

    candidates = discover_candidates(html)

    assert [item.endpoint for item in candidates] == ["stock_zh_a_hist"]
    assert candidates[0].sample_kwargs["symbol"] == "000001"


def test_discover_candidates_reads_documented_parameter_defaults() -> None:
    html = """
    <h2>历史行情数据-东财</h2>
    <p>接口: stock_zh_a_hist</p>
    <p>输入参数</p>
    <p>symbol str symbol='603777'; 股票代码</p>
    <p>period str period='daily'; choice of {'daily', 'weekly', 'monthly'}</p>
    <p>start_date str start_date='20210301'; 开始查询的日期</p>
    <p>end_date str end_date='20210616'; 结束查询的日期</p>
    <h2>实时行情数据-东财</h2>
    <p>接口: stock_us_spot_em</p>
    <p>描述: 美股实时行情</p>
    """

    candidates = discover_candidates(html)

    assert [item.endpoint for item in candidates] == ["stock_zh_a_hist"]
    assert candidates[0].sample_kwargs == {
        "symbol": "603777",
        "period": "daily",
        "start_date": "20210301",
        "end_date": "20210616",
    }


def test_probe_runs_failed_candidates_in_separate_rounds() -> None:
    calls: list[str] = []
    candidates = [
        ApiCandidate(endpoint="stock_a", title="A", sample_call="", sample_kwargs={}, section_text="历史"),
        ApiCandidate(endpoint="stock_b", title="B", sample_call="", sample_kwargs={}, section_text="历史"),
    ]

    def caller(candidate: ApiCandidate, kwargs: dict[str, object]) -> pd.DataFrame:
        prior_endpoint_calls = len([c for c in calls if c.startswith(candidate.endpoint)])
        calls.append(f"{candidate.endpoint}:{prior_endpoint_calls + 1}")
        if candidate.endpoint == "stock_a":
            return pd.DataFrame({"date": ["2020-01-01"]})
        if prior_endpoint_calls < 2:
            raise RuntimeError("temporary")
        return pd.DataFrame({"date": ["2019-01-01"]})

    results = run_probe_rounds(candidates, caller=caller, rounds=3, today=date(2024, 5, 1))

    assert [item.status for item in results] == ["accessible", "accessible"]
    assert [item.rounds_attempted for item in results] == [1, 3]
    assert calls == ["stock_a:1", "stock_b:1", "stock_b:2", "stock_b:3"]


def test_build_call_kwargs_expands_date_windows_but_preserves_single_report_date() -> None:
    window = ApiCandidate(
        endpoint="stock_window",
        title="历史",
        sample_call="",
        sample_kwargs={"start_date": "20200101", "end_date": "20210101"},
        section_text="历史",
    )
    report = ApiCandidate(
        endpoint="stock_report",
        title="财务",
        sample_call="",
        sample_kwargs={"date": "20240331"},
        section_text="财务",
    )

    assert build_call_kwargs(window, date(2024, 5, 1)) == {"start_date": "19900101", "end_date": "20240501"}
    assert build_call_kwargs(report, date(2024, 5, 1)) == {"date": "20240331"}


def test_build_call_kwargs_uses_year_format_for_year_windows() -> None:
    candidate = ApiCandidate(
        endpoint="stock_year",
        title="历史",
        sample_call="",
        sample_kwargs={"start_year": "2000", "end_year": "2019"},
        section_text="历史",
    )

    assert build_call_kwargs(candidate, date(2024, 5, 1)) == {"start_year": "1990", "end_year": "2019"}


def test_earliest_observed_time_reads_columns_and_index() -> None:
    frame = pd.DataFrame({"trade_date": ["2020-01-02", "1999-12-31"], "value": [1, 2]})
    assert earliest_observed_time(frame) == "1999-12-31"

    indexed = pd.DataFrame({"value": [1]}, index=pd.to_datetime(["1998-01-05"]))
    assert earliest_observed_time(indexed) == "1998-01-05"


def test_earliest_observed_time_ignores_time_only_values() -> None:
    frame = pd.DataFrame({"time": ["10:04:01", "15:00:00"], "value": [1, 2]})

    assert earliest_observed_time(frame) is None


def test_write_markdown_report_overwrites_latest_state(tmp_path: Path) -> None:
    output = tmp_path / "report.md"
    candidate = ApiCandidate("stock_a", "A", "ak.stock_a()", {}, "历史")
    first = [ProbeResult(candidate, "inaccessible", None, 0, 3, {}, "old")]
    second = [ProbeResult(candidate, "accessible", "2020-01-01", 1, 1, {}, "")]

    write_markdown_report(output, first, generated_at=datetime(2024, 1, 1, 1, 0), akshare_version="fake")
    write_markdown_report(output, second, generated_at=datetime(2024, 1, 2, 1, 0), akshare_version="fake")

    text = output.read_text(encoding="utf-8")
    assert "2024-01-02T01:00:00" in text
    assert "accessible" in text
    assert "old" not in text


def test_main_writes_report_from_injected_html_and_caller(tmp_path: Path) -> None:
    output = tmp_path / "report.md"
    html = """
    <h2>历史</h2>
    <p>接口: stock_a</p>
    <pre>ak.stock_a(start_date="20200101", end_date="20240101")</pre>
    """

    exit_code = main(
        [
            "--output",
            str(output),
            "--rounds",
            "1",
            "--workers",
            "2",
        ],
        fetch_html=lambda _: html,
        caller=lambda candidate, kwargs: pd.DataFrame({"date": ["2020-01-01"]}),
        now=lambda: datetime(2024, 1, 2, 3, 4),
    )

    assert exit_code == 0
    assert "stock_a" in output.read_text(encoding="utf-8")


def test_fetch_html_uses_browser_user_agent(monkeypatch) -> None:
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self) -> bytes:
            return b"ok"

    def fake_urlopen(request, timeout):
        captured["user_agent"] = request.headers["User-agent"]
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("src.tools.akshare_stock_history_audit.urlopen", fake_urlopen)

    assert fetch_html("https://example.test", timeout_seconds=12) == "ok"
    assert "Mozilla" in captured["user_agent"]
    assert captured["timeout"] == 12
