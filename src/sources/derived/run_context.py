"""Unified build run context for derived builds.

The :class:`BuildRunContext` is the single source of truth for a derived build
run's identity. The ``run_id`` is the join key across:

* the build journal (``<run_id>.json``),
* the progress state file (``<run_id>.state.json``),
* the process lock owner,
* the run log, and
* the result status returned to the CLI.

Construction order (enforced by callers):

1. :class:`BuildPlanner` produces a :class:`DerivedBuildPlan`.
2. ``find_resumable_journal`` looks for a resumable journal.
3. The final ``run_id`` is decided (resume → old journal run id; else new id).
4. :class:`BuildRunContext` is created with that run id.
5. :class:`ProgressReporter` is created using ``context.progress_path``.

This avoids the previous bug where a fresh progress path was created with a
new run id and then the local run id was swapped for the journal's old run id,
leaving the progress file orphaned.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from threading import Event

from src.sources.derived.journal import BuildJournal
from src.sources.derived.plan import DerivedBuildPlan


@dataclass(frozen=True)
class BuildRunContext:
    """Immutable identity for one derived build run.

    Attributes:
        run_id: Stable run identifier shared by journal, progress, lock, log.
        target: Derived target name (``daily_bar``, ``valuation``, ...).
        dataset_id: Underlying dataset id (``cn_stock_daily_bar``, ...).
        journal_path: Path to ``<run_id>.json`` under ``derived-runs/``.
        progress_path: Path to ``<run_id>.state.json`` under ``derived-runs/``.
        run_log_path: Optional run log path (may be ``None`` in unit tests).
        cancel_event: Cooperative cancellation event shared with workers.
    """

    run_id: str
    target: str
    dataset_id: str
    journal_path: Path
    progress_path: Path
    run_log_path: Path | None = None
    cancel_event: Event = field(default_factory=Event)


def build_run_context_paths(
    *,
    metadata_dir: Path,
    run_id: str,
) -> tuple[Path, Path]:
    """Return ``(journal_path, progress_path)`` for a given run id.

    Both files live under ``metadata_dir/derived-runs/`` so that the journal
    and progress state share the same sibling relationship used by
    ``latest_progress_state_path`` and ``find_resumable_journal``.
    """

    runs_dir = metadata_dir / "derived-runs"
    return (
        runs_dir / f"{run_id}.json",
        runs_dir / f"{run_id}.state.json",
    )


def make_build_run_context(
    *,
    plan: DerivedBuildPlan,
    journal: BuildJournal,
    metadata_dir: Path,
    run_log_path: Path | None = None,
    cancel_event: Event | None = None,
    progress_path_override: Path | None = None,
) -> BuildRunContext:
    """Build a :class:`BuildRunContext` from a resumable or fresh journal.

    The journal's ``run_id`` is authoritative: it is the same id whether the
    journal was just created or resumed from a prior run. The progress path is
    derived from that run id (not from a directory scan).

    When ``progress_path_override`` is provided (e.g. by the daily orchestrator
    via ``QDC_DERIVED_PROGRESS_PATH``), the progress state file is pinned to
    that exact path regardless of the journal run id. This is the single
    authoritative contract between the orchestrator's stall detector and the
    derived build subprocess: the orchestrator decides the path before spawn,
    passes it via env, and reads only that path — never a directory scan.

    The journal path is always derived from the journal run id so resume
    semantics are preserved: a resumed run keeps its old journal file while
    writing progress to the orchestrator-pinned path.
    """

    journal_path, default_progress_path = build_run_context_paths(metadata_dir=metadata_dir, run_id=journal.run_id)
    progress_path = progress_path_override or default_progress_path
    return BuildRunContext(
        run_id=journal.run_id,
        target=journal.target,
        dataset_id=journal.dataset_id,
        journal_path=journal_path,
        progress_path=progress_path,
        run_log_path=run_log_path,
        cancel_event=cancel_event or Event(),
    )
