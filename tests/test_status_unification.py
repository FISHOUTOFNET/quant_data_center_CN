"""P0-6: Unified pipeline status classification tests.

Verifies that ``src.pipeline.step_health`` is the single authority for
classifying pipeline statuses (success / success_degraded / skipped /
failure / fatal-terminal) and that the orchestrator's ``_final_exit_code``
honors the unified classification when computing the multi-target final
exit code.

Sections:
    6.1 - Single authority classification (is_*_status helpers + sets)
    6.2 - Fatal terminal status handling in evaluate_step_health
    6.3 - Multi-target regression via _final_exit_code (orchestrator)
"""

from __future__ import annotations

import pytest

from src.pipeline.step_health import (
    DEGRADED_SUCCESS_STATUSES,
    FAILURE_STATUSES,
    FATAL_TERMINAL_STATUSES,
    SKIPPED_STATUSES,
    SUCCESS_STATUSES,
    baostock_market_session_policy,
    evaluate_step_health,
    is_degraded_success_status,
    is_failure_status,
    is_fatal_terminal_status,
    is_skipped_status,
    is_success_status,
)
from src.tools.run_update_daily import DailyStep, _final_exit_code


# ---------------------------------------------------------------------------
# Record / step-state builders
# ---------------------------------------------------------------------------


def _success(
    dataset: str = "baostock_cn_stock_daily_bar_unadjusted",
    code: str = "sh.600000",
) -> dict[str, object]:
    return {"dataset": dataset, "code": code, "status": "success", "row_count": 1, "error_stack": ""}


def _failed(
    dataset: str = "baostock_cn_stock_daily_bar_unadjusted",
    code: str = "sh.600000",
    status: str = "failed",
) -> dict[str, object]:
    return {"dataset": dataset, "code": code, "status": status, "row_count": 0, "error_stack": "boom"}


def _step_row(status: str, exit_code: int | None = None) -> dict[str, object]:
    """Build a step_state row like the orchestrator records in the state file."""

    row: dict[str, object] = {"status": status}
    if exit_code is not None:
        row["exit_code"] = exit_code
    return row


def _daily_step(step_id: str) -> DailyStep:
    """Build a minimal DailyStep for _final_exit_code tests."""

    return DailyStep(step_id, step_id, ("cmd",))


# ===========================================================================
# 6.1 Single authority classification tests
# ===========================================================================


# --- 6.1.1: is_success_status ---

def test_is_success_status_true_for_success() -> None:
    assert is_success_status("success") is True


@pytest.mark.parametrize(
    "status",
    [
        "success_degraded",
        "failed",
        "partial",
        "cancelled",
        "stalled",
        "timed_out",
        "skipped",
        "skipped_checkpoint",
        "failed_resource_locked",
        "failed_timeout_cleanup",
        "abandoned",
        "",
        None,
    ],
)
def test_is_success_status_false_for_other_statuses(status: object) -> None:
    assert is_success_status(status) is False


# --- 6.1.2: is_failure_status for all known failure statuses ---

@pytest.mark.parametrize(
    "status",
    [
        "failed",
        "partial",
        "cancelled",
        "stalled",
        "timed_out",
        "failed_resource_locked",
        "failed_timeout_cleanup",
        "abandoned",
    ],
)
def test_is_failure_status_true_for_known_failure_statuses(status: str) -> None:
    assert is_failure_status(status) is True


# --- 6.1.3: is_failure_status prefix match for failed_* ---

def test_is_failure_status_true_for_failed_prefix() -> None:
    assert is_failure_status("failed_anything") is True


# --- 6.1.4: is_failure_status False for non-failures ---

@pytest.mark.parametrize("status", ["success", "skipped", ""])
def test_is_failure_status_false_for_non_failures(status: str) -> None:
    assert is_failure_status(status) is False


# --- 6.1.5: is_skipped_status ---

@pytest.mark.parametrize(
    "status",
    ["skipped", "skipped_checkpoint", "skipped_timeout"],
)
def test_is_skipped_status_true(status: str) -> None:
    assert is_skipped_status(status) is True


@pytest.mark.parametrize("status", ["success", "failed", "partial", ""])
def test_is_skipped_status_false_for_non_skipped(status: str) -> None:
    assert is_skipped_status(status) is False


# --- 6.1.6: is_fatal_terminal_status ---

@pytest.mark.parametrize(
    "status",
    ["partial", "cancelled", "stalled", "timed_out"],
)
def test_is_fatal_terminal_status_true(status: str) -> None:
    assert is_fatal_terminal_status(status) is True


# --- 6.1.7: failed is NOT fatal-terminal (it's fatal but not terminal) ---

def test_is_fatal_terminal_status_false_for_failed() -> None:
    assert is_fatal_terminal_status("failed") is False


@pytest.mark.parametrize(
    "status",
    ["failed", "failed_resource_locked", "failed_timeout_cleanup", "abandoned", "success", "skipped"],
)
def test_is_fatal_terminal_status_false_for_non_terminal(status: str) -> None:
    assert is_fatal_terminal_status(status) is False


# --- is_degraded_success_status (part of the single authority) ---

def test_is_degraded_success_status_true_for_success_degraded() -> None:
    assert is_degraded_success_status("success_degraded") is True


@pytest.mark.parametrize("status", ["success", "failed", "partial", "skipped", ""])
def test_is_degraded_success_status_false_for_other_statuses(status: str) -> None:
    assert is_degraded_success_status(status) is False


# --- The authority sets must match the helper semantics ---

def test_success_statuses_set_only_contains_success() -> None:
    assert SUCCESS_STATUSES == frozenset({"success"})


def test_degraded_success_statuses_set_only_contains_success_degraded() -> None:
    assert DEGRADED_SUCCESS_STATUSES == frozenset({"success_degraded"})


def test_skipped_statuses_set_contains_skipped_and_checkpoint() -> None:
    assert SKIPPED_STATUSES == frozenset({"skipped", "skipped_checkpoint"})


def test_failure_statuses_set_contains_all_specified_failures() -> None:
    expected = frozenset(
        {
            "failed",
            "partial",
            "cancelled",
            "stalled",
            "timed_out",
            "failed_resource_locked",
            "failed_timeout_cleanup",
            "abandoned",
        }
    )
    assert FAILURE_STATUSES == expected


def test_fatal_terminal_statuses_set_contains_only_terminal_four() -> None:
    assert FATAL_TERMINAL_STATUSES == frozenset({"partial", "cancelled", "stalled", "timed_out"})


def test_fatal_terminal_is_subset_of_failure() -> None:
    # Every fatal-terminal status must also be a failure status so the
    # orchestrator's is_failure_status() check covers them uniformly.
    assert FATAL_TERMINAL_STATUSES.issubset(FAILURE_STATUSES)


# ===========================================================================
# 6.2 Fatal terminal status in evaluate_step_health
# ===========================================================================


@pytest.mark.parametrize(
    "status",
    ["partial", "cancelled", "stalled", "timed_out"],
)
def test_evaluate_single_fatal_terminal_record_yields_failed_not_degraded(status: str) -> None:
    """6.2.1-6.2.4: A single fatal-terminal record in a tolerant policy → failed.

    The tolerant policy (baostock_market_session_policy) would normally allow
    a small number of ordinary ``failed`` records to degrade to
    ``success_degraded``, but a fatal-terminal status (partial / cancelled /
    stalled / timed_out) must always escalate to ``failed``.
    """

    records = [_failed(status=status)]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert summary.status != "success_degraded"
    assert status in summary.fatal_statuses
    assert "fatal status" in summary.reason


def test_evaluate_few_failed_records_below_threshold_yield_success_degraded() -> None:
    """6.2.5: A small number of ordinary 'failed' records → success_degraded."""

    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "success_degraded"
    assert summary.failed_records == 1
    assert summary.fatal_statuses == ()
    assert "within policy tolerance" in summary.reason


def test_evaluate_failed_records_above_threshold_yield_failed() -> None:
    """6.2.6: A 'failed' record count above the tolerant threshold → failed."""

    records = [_success(code=f"sh.60000{i}") for i in range(100)]
    for i in range(51):
        records.append(_failed(code=f"sh.6999{i:02d}"))
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert summary.failed_records == 51
    assert "exceeds max_failed_records=50" in summary.reason


def test_evaluate_fatal_terminal_escalates_even_with_many_successes() -> None:
    """A single partial among many successes still fails (not degraded)."""

    records = [_success(code=f"sh.60000{i}") for i in range(500)]
    records.append(_failed(code="sh.699999", status="partial"))
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert "partial" in summary.fatal_statuses


def test_evaluate_failed_prefix_not_treated_as_fatal_terminal() -> None:
    """A failed_* prefix status (e.g. failed_resource_locked) is fatal via
    policy.fatal_statuses, NOT via is_fatal_terminal_status. Verify the
    distinction: it produces 'failed' but is_fatal_terminal_status is False.
    """

    status = "failed_resource_locked"
    assert is_fatal_terminal_status(status) is False
    records = [_success(code="sh.600000"), _failed(code="sh.699999", status=status)]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert status in summary.fatal_statuses


# ===========================================================================
# 6.3 Multi-target regression (orchestrator final exit code)
# ===========================================================================


def test_final_exit_code_partial_in_any_step_forces_nonzero() -> None:
    """6.3.1: security_master=success, daily_bar=partial, valuation=success → fails."""

    steps = [
        _daily_step("security_master"),
        _daily_step("daily_bar"),
        _daily_step("valuation"),
    ]
    step_state = {
        "security_master": _step_row("success", 0),
        "daily_bar": _step_row("partial", 1),
        "valuation": _step_row("success", 0),
    }
    assert _final_exit_code(steps, step_state, None) != 0


def test_final_exit_code_one_success_and_one_cancelled_forces_nonzero() -> None:
    """6.3.2: one success + one cancelled → fails."""

    steps = [_daily_step("a"), _daily_step("b")]
    step_state = {
        "a": _step_row("success", 0),
        "b": _step_row("cancelled", 1),
    }
    assert _final_exit_code(steps, step_state, None) != 0


def test_final_exit_code_one_success_and_one_stalled_forces_nonzero() -> None:
    """6.3.3: one success + one stalled → fails."""

    steps = [_daily_step("a"), _daily_step("b")]
    step_state = {
        "a": _step_row("success", 0),
        "b": _step_row("stalled", 126),
    }
    assert _final_exit_code(steps, step_state, None) != 0


def test_final_exit_code_one_success_and_one_timed_out_forces_nonzero() -> None:
    """6.3.4: one success + one timed_out → fails."""

    steps = [_daily_step("a"), _daily_step("b")]
    step_state = {
        "a": _step_row("success", 0),
        "b": _step_row("timed_out", 124),
    }
    assert _final_exit_code(steps, step_state, None) != 0


def test_final_exit_code_all_success_yields_zero() -> None:
    """6.3.5: all success → exit 0."""

    steps = [_daily_step("a"), _daily_step("b"), _daily_step("c")]
    step_state = {
        "a": _step_row("success", 0),
        "b": _step_row("success", 0),
        "c": _step_row("success", 0),
    }
    assert _final_exit_code(steps, step_state, None) == 0


def test_final_exit_code_success_degraded_step_yields_zero() -> None:
    """6.3.6: tolerant source step with few failed records → success_degraded, exit 0.

    Builds the success_degraded verdict via evaluate_step_health first (to
    prove the tolerant threshold absorbed the failures), then feeds the
    resulting status into _final_exit_code to confirm the orchestrator does
    not treat success_degraded as a failure.
    """

    records = [_success(code=f"sh.60000{i}") for i in range(200)]
    records.append(_failed(code="sh.699999"))
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "success_degraded"
    # success_degraded must NOT be classified as a failure by the authority
    assert is_failure_status(summary.status) is False

    steps = [_daily_step("source"), _daily_step("derived")]
    step_state = {
        "source": _step_row(summary.status, 0),
        "derived": _step_row("success", 0),
    }
    assert _final_exit_code(steps, step_state, None) == 0


def test_final_exit_code_fatal_terminal_not_downgraded_by_tolerant_threshold() -> None:
    """6.3.7: fatal terminal status NOT downgraded by tolerant threshold → fails.

    A tolerant policy would degrade a small number of ordinary ``failed``
    records to ``success_degraded``, but a single ``partial`` (fatal-terminal)
    record must escalate to ``failed``. The orchestrator's _final_exit_code
    must then force a non-zero exit.
    """

    # Step 1: evaluate_step_health must NOT downgrade partial to success_degraded
    records = [_success(code="sh.600000"), _failed(code="sh.600001", status="partial")]
    summary = evaluate_step_health(records, baostock_market_session_policy())
    assert summary.status == "failed"
    assert summary.status != "success_degraded"

    # Step 2: _final_exit_code must force a non-zero exit for the failed step
    steps = [_daily_step("source"), _daily_step("derived")]
    step_state = {
        "source": _step_row(summary.status, 1),
        "derived": _step_row("success", 0),
    }
    assert _final_exit_code(steps, step_state, None) != 0


@pytest.mark.parametrize(
    "status,exit_code",
    [
        ("partial", 1),
        ("cancelled", 1),
        ("stalled", 126),
        ("timed_out", 124),
        ("failed", 1),
        ("failed_resource_locked", 125),
        ("failed_timeout_cleanup", 125),
        ("abandoned", 1),
        ("failed_anything", 1),
    ],
)
def test_final_exit_code_any_failure_status_forces_nonzero(status: str, exit_code: int) -> None:
    """Regression: every failure status from the authority forces non-zero exit."""

    steps = [_daily_step("a"), _daily_step("b")]
    step_state = {
        "a": _step_row("success", 0),
        "b": _step_row(status, exit_code),
    }
    assert is_failure_status(status) is True
    assert _final_exit_code(steps, step_state, None) != 0


def test_final_exit_code_returns_recorded_exit_code_for_failure() -> None:
    """When a step failed with a specific exit code, that code is propagated."""

    steps = [_daily_step("a")]
    step_state = {"a": _step_row("failed", 42)}
    assert _final_exit_code(steps, step_state, None) == 42


def test_final_exit_code_defaults_to_one_when_exit_code_missing() -> None:
    """A failed step without an exit_code field yields exit 1."""

    steps = [_daily_step("a")]
    step_state = {"a": _step_row("partial")}  # no exit_code key
    assert _final_exit_code(steps, step_state, None) == 1


def test_final_exit_code_failed_exit_code_argument_takes_precedence() -> None:
    """If failed_exit_code is provided (non-None), it short-circuits."""

    steps = [_daily_step("a")]
    step_state = {"a": _step_row("success", 0)}
    assert _final_exit_code(steps, step_state, 7) == 7


def test_final_exit_code_blocked_step_forces_nonzero() -> None:
    """A 'blocked' step (not in FAILURE_STATUSES) still forces exit 1."""

    steps = [_daily_step("a"), _daily_step("b")]
    step_state = {
        "a": _step_row("success", 0),
        "b": _step_row("blocked", 1),
    }
    assert is_failure_status("blocked") is False
    assert _final_exit_code(steps, step_state, None) == 1


def test_final_exit_code_skipped_and_success_degraded_do_not_fail() -> None:
    """skipped / skipped_checkpoint / success_degraded are neutral or success."""

    steps = [_daily_step("a"), _daily_step("b"), _daily_step("c")]
    step_state = {
        "a": _step_row("success", 0),
        "b": _step_row("skipped"),
        "c": _step_row("success_degraded", 0),
    }
    assert _final_exit_code(steps, step_state, None) == 0
