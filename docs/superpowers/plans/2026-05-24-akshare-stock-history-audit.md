# AkShare Stock History API Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone AkShare stock historical API audit helper that discovers candidates, probes them in three separated rounds, reports earliest observed dates, and regenerates Markdown documentation.

**Architecture:** Put reusable logic in `src/tools/akshare_stock_history_audit.py` and keep the runnable entry point in `scripts/audit_akshare_stock_history_apis.py`. The helper fetches the AkShare stock docs page, classifies historical endpoints, parses sample calls, probes installed AkShare functions, and writes `docs/akshare_stock_history_api_accessibility.md`.

**Tech Stack:** Python 3.10+, pandas, urllib/request HTML fetch, inspect/ast parsing, pytest.

---

## File Structure

- Create `src/tools/__init__.py`: package marker for helper modules.
- Create `src/tools/akshare_stock_history_audit.py`: discovery, probing, retry orchestration, earliest-date extraction, Markdown rendering, and CLI-compatible `main`.
- Create `scripts/audit_akshare_stock_history_apis.py`: thin direct script entry point.
- Create `tests/test_akshare_stock_history_audit.py`: unit tests for parser, retry rounds, earliest date extraction, and report rendering.
- Create `docs/akshare_stock_history_api_accessibility.md`: generated report target after running the helper.

### Task 1: Parser and Classification

**Files:**
- Create: `tests/test_akshare_stock_history_audit.py`
- Create: `src/tools/__init__.py`
- Create: `src/tools/akshare_stock_history_audit.py`

- [ ] **Step 1: Write failing parser test**

```python
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
```

- [ ] **Step 2: Run parser test to verify it fails**

Run: `pytest tests/test_akshare_stock_history_audit.py::test_discover_candidates_includes_history_and_excludes_current -q`
Expected: FAIL because `src.tools.akshare_stock_history_audit` does not exist.

- [ ] **Step 3: Implement minimal parser**

Implement `ApiCandidate`, `discover_candidates`, `_extract_sample_kwargs`, and keyword classifier in `src/tools/akshare_stock_history_audit.py`.

- [ ] **Step 4: Run parser test to verify it passes**

Run: `pytest tests/test_akshare_stock_history_audit.py::test_discover_candidates_includes_history_and_excludes_current -q`
Expected: PASS.

### Task 2: Round-Based Probe Orchestration

**Files:**
- Modify: `tests/test_akshare_stock_history_audit.py`
- Modify: `src/tools/akshare_stock_history_audit.py`

- [ ] **Step 1: Write failing retry test**

```python
def test_probe_runs_failed_candidates_in_separate_rounds() -> None:
    calls: list[str] = []
    candidates = [
        ApiCandidate(endpoint="stock_a", title="A", sample_call="", sample_kwargs={}, section_text="历史"),
        ApiCandidate(endpoint="stock_b", title="B", sample_call="", sample_kwargs={}, section_text="历史"),
    ]

    def caller(candidate: ApiCandidate, kwargs: dict[str, object]) -> pd.DataFrame:
        calls.append(f"{candidate.endpoint}:{len([c for c in calls if c.startswith(candidate.endpoint)]) + 1}")
        if candidate.endpoint == "stock_a":
            return pd.DataFrame({"date": ["2020-01-01"]})
        if len([c for c in calls if c.startswith("stock_b")]) < 3:
            raise RuntimeError("temporary")
        return pd.DataFrame({"date": ["2019-01-01"]})

    results = run_probe_rounds(candidates, caller=caller, rounds=3, today=date(2024, 5, 1))
    assert [item.status for item in results] == ["accessible", "accessible"]
    assert [item.rounds_attempted for item in results] == [1, 3]
    assert calls == ["stock_a:1", "stock_b:1", "stock_b:2", "stock_b:3"]
```

- [ ] **Step 2: Run retry test to verify it fails**

Run: `pytest tests/test_akshare_stock_history_audit.py::test_probe_runs_failed_candidates_in_separate_rounds -q`
Expected: FAIL because `run_probe_rounds` does not exist.

- [ ] **Step 3: Implement retry orchestration**

Implement `ProbeResult`, `build_call_kwargs`, `_normalize_date_window_kwargs`, and `run_probe_rounds` so only failed endpoints are retried in later rounds.

- [ ] **Step 4: Run retry test to verify it passes**

Run: `pytest tests/test_akshare_stock_history_audit.py::test_probe_runs_failed_candidates_in_separate_rounds -q`
Expected: PASS.

### Task 3: Earliest Date Extraction and Report Rendering

**Files:**
- Modify: `tests/test_akshare_stock_history_audit.py`
- Modify: `src/tools/akshare_stock_history_audit.py`

- [ ] **Step 1: Write failing earliest/report tests**

```python
def test_earliest_observed_time_reads_columns_and_index() -> None:
    frame = pd.DataFrame({"trade_date": ["2020-01-02", "1999-12-31"], "value": [1, 2]})
    assert earliest_observed_time(frame) == "1999-12-31"

    indexed = pd.DataFrame({"value": [1]}, index=pd.to_datetime(["1998-01-05"]))
    assert earliest_observed_time(indexed) == "1998-01-05"

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_akshare_stock_history_audit.py::test_earliest_observed_time_reads_columns_and_index tests/test_akshare_stock_history_audit.py::test_write_markdown_report_overwrites_latest_state -q`
Expected: FAIL because functions are missing.

- [ ] **Step 3: Implement earliest extraction and Markdown rendering**

Implement `earliest_observed_time`, `render_markdown_report`, and `write_markdown_report`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_akshare_stock_history_audit.py -q`
Expected: PASS.

### Task 4: CLI Entry Point and Generated Report

**Files:**
- Create: `scripts/audit_akshare_stock_history_apis.py`
- Modify: `src/tools/akshare_stock_history_audit.py`
- Create/Update: `docs/akshare_stock_history_api_accessibility.md`

- [ ] **Step 1: Write failing CLI smoke test**

```python
def test_main_writes_report_from_injected_html_and_caller(tmp_path: Path) -> None:
    output = tmp_path / "report.md"
    html = '<h2>历史</h2><p>接口: stock_a</p><pre>ak.stock_a(start_date="20200101", end_date="20240101")</pre>'
    exit_code = main(
        [
            "--output",
            str(output),
            "--rounds",
            "1",
        ],
        fetch_html=lambda _: html,
        caller=lambda candidate, kwargs: pd.DataFrame({"date": ["2020-01-01"]}),
        now=lambda: datetime(2024, 1, 2, 3, 4),
    )
    assert exit_code == 0
    assert "stock_a" in output.read_text(encoding="utf-8")
```

- [ ] **Step 2: Run smoke test to verify it fails**

Run: `pytest tests/test_akshare_stock_history_audit.py::test_main_writes_report_from_injected_html_and_caller -q`
Expected: FAIL because `main` does not support dependency injection.

- [ ] **Step 3: Implement CLI and script**

Add argparse options for `--source-url`, `--output`, `--rounds`, `--timeout-seconds`, and `--sleep-between-rounds`. Add default AkShare caller with timeout protection.

- [ ] **Step 4: Run smoke and full helper tests**

Run: `pytest tests/test_akshare_stock_history_audit.py -q`
Expected: PASS.

- [ ] **Step 5: Run the helper once to generate/update the report**

Run: `python scripts/audit_akshare_stock_history_apis.py --rounds 1 --timeout-seconds 30`
Expected: Creates or updates `docs/akshare_stock_history_api_accessibility.md`. If network or endpoint failures occur, they are recorded in the report.

### Task 5: Quality Verification

**Files:**
- All changed files.

- [ ] **Step 1: Run focused tests**

Run: `pytest tests/test_akshare_stock_history_audit.py -q`
Expected: PASS.

- [ ] **Step 2: Run lint on touched Python files**

Run: `ruff check src/tools/akshare_stock_history_audit.py scripts/audit_akshare_stock_history_apis.py tests/test_akshare_stock_history_audit.py`
Expected: PASS.

- [ ] **Step 3: Review git diff**

Run: `git diff -- docs/superpowers/specs/2026-05-24-akshare-stock-history-audit-design.md docs/superpowers/plans/2026-05-24-akshare-stock-history-audit.md src/tools/akshare_stock_history_audit.py scripts/audit_akshare_stock_history_apis.py tests/test_akshare_stock_history_audit.py docs/akshare_stock_history_api_accessibility.md`
Expected: Diff contains only the audit helper, tests, spec, plan, and generated report.

## Self-Review

The plan covers discovery, classification, three separated retry rounds, earliest observed time extraction, Markdown regeneration, script entry point, and focused verification. No placeholders are present; type names and file paths are consistent with the spec.
