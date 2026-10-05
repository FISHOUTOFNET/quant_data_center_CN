"""Minimal shared filesystem safety primitives for raw-path authorization.

Provides a single low-level mechanism for detecting link-like filesystem nodes
(symlinks, Windows junctions, other reparse points) WITHOUT following them,
plus a thin policy wrapper used by :func:`paths.resolve_runtime_paths` raw-root
authorization.

Design notes
------------
* ``os.lstat`` (POSIX) / ``os.stat(follow_symlinks=False)`` (Windows) reads the
  node itself, never the target.
* POSIX: ``stat.S_ISLNK(st.st_mode)`` identifies symlinks.
* Windows: ``st.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT``
  identifies symlinks, junctions, mount points, and other reparse points.
  ``st_reparse_tag`` is read for diagnostics only; the deletion boundary
  conservatively rejects ALL reparse points, not just known tags.
* Python 3.10/3.11 compatibility: does NOT depend on ``Path.is_junction()``
  or ``os.path.isjunction()`` (Python 3.12+ only).
* No third-party dependencies, no inheritance hierarchy.
"""

from __future__ import annotations

import os
import stat
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal

__all__ = [
    "FilesystemNodeInspection",
    "FilesystemNodeKind",
    "LinkLikeError",
    "describe_link_like",
    "inspect_filesystem_node",
    "is_link_like_or_reparse",
    "validate_managed_root_candidate",
]

SourceName = Literal["explicit", "environment", "settings", "default"]

# Windows reparse tags (for diagnostics only — the boundary rejects all tags).
_IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003  # junctions
_IO_REPARSE_TAG_SYMLINK = 0xA000000C  # Windows symlinks


class FilesystemNodeKind(Enum):
    """Classification of a filesystem node based on a single ``lstat`` result."""

    REGULAR_FILE = "regular_file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    JUNCTION = "junction"
    OTHER_REPARSE_POINT = "other_reparse_point"
    OTHER = "other"


@dataclass(frozen=True)
class FilesystemNodeInspection:
    """Immutable result of inspecting a filesystem node via ``os.lstat``.

    Carries the classification (``kind``) plus the raw attributes needed for
    diagnostics. Does NOT cache the ``stat_result`` — callers needing size/mtime
    must call ``Path.stat()`` separately (that is a different safety decision
    stage and may legitimately require its own system call).

    ``is_link_like`` is the policy predicate shared by managed-root authorization,
    cleanup discovery, and pre-delete verification: any symlink, junction, or
    other reparse point.
    """

    path: Path
    kind: FilesystemNodeKind
    mode: int
    file_attributes: int | None = None
    reparse_tag: int | None = None

    @property
    def is_link_like(self) -> bool:
        """True for symlinks, junctions, and other reparse points."""

        return self.kind in (
            FilesystemNodeKind.SYMLINK,
            FilesystemNodeKind.JUNCTION,
            FilesystemNodeKind.OTHER_REPARSE_POINT,
        )


def inspect_filesystem_node(path: Path) -> FilesystemNodeInspection:
    """Inspect ``path`` via a single ``os.lstat`` and classify the node.

    Does NOT follow symlinks, does NOT call ``.resolve()``, does NOT catch
    exceptions, does NOT make policy decisions. ``FileNotFoundError``,
    ``PermissionError``, ``OSError``, and ``ValueError`` all propagate to the
    caller so callers can implement fail-closed or skip-and-log policies.

    Args:
        path: The filesystem path to inspect (raw, unresolved).

    Returns:
        A :class:`FilesystemNodeInspection` describing the node.

    Raises:
        FileNotFoundError: The path does not exist.
        PermissionError: Insufficient permissions to stat the path.
        OSError: Other OS-level stat failure.
        ValueError: Embedded null bytes or other path encoding issues.
    """

    st = os.lstat(path)
    return _classify_lstat_result(Path(path), st)


def _classify_lstat_result(path: Path, st: os.stat_result) -> FilesystemNodeInspection:
    """Classify a raw ``lstat`` result into a :class:`FilesystemNodeInspection`."""

    mode = st.st_mode
    file_attributes = getattr(st, "st_file_attributes", None)
    reparse_tag = getattr(st, "st_reparse_tag", None)

    # POSIX symlinks and Windows symlinks (created via ``mklink`` without /J)
    # both set S_ISLNK. Check this first so real symlinks are never misclassified.
    if stat.S_ISLNK(mode):
        kind = FilesystemNodeKind.SYMLINK
    elif (
        sys.platform == "win32"
        and file_attributes is not None
        and (file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT)
    ):
        # Windows reparse point that is NOT a POSIX symlink (S_ISLNK was false).
        # Junctions have tag MOUNT_POINT; Windows-native symlinks that lack
        # S_ISLNK have tag SYMLINK; everything else is an unknown reparse tag.
        if reparse_tag == _IO_REPARSE_TAG_MOUNT_POINT:
            kind = FilesystemNodeKind.JUNCTION
        elif reparse_tag == _IO_REPARSE_TAG_SYMLINK:
            kind = FilesystemNodeKind.SYMLINK
        else:
            kind = FilesystemNodeKind.OTHER_REPARSE_POINT
    elif stat.S_ISREG(mode):
        kind = FilesystemNodeKind.REGULAR_FILE
    elif stat.S_ISDIR(mode):
        kind = FilesystemNodeKind.DIRECTORY
    else:
        kind = FilesystemNodeKind.OTHER

    return FilesystemNodeInspection(
        path=path,
        kind=kind,
        mode=mode,
        file_attributes=file_attributes,
        reparse_tag=reparse_tag,
    )


class LinkLikeError(RuntimeError):
    """Raised when a managed-root candidate is a link-like/reparse node.

    The marker binds to a directory identity; a symlink/junction target can
    be swapped after authorization, so the deletion boundary must never
    depend on link resolution. Fail closed.
    """


def is_link_like_or_reparse(path: Path) -> bool:
    """Return ``True`` if ``path`` is a symlink, junction, or other reparse point.

    Strict wrapper around :func:`inspect_filesystem_node`. Does NOT swallow
    ``FileNotFoundError``, ``OSError``, ``PermissionError``, or ``ValueError``
    — callers that need fail-closed or skip-and-log behavior must catch those
    explicitly or call :func:`inspect_filesystem_node` directly.
    """

    return inspect_filesystem_node(path).is_link_like


def describe_link_like(path: Path) -> str:
    """Return a short diagnostic string for a link-like node (for logs).

    Returns ``"not-link-like"`` if the node is not a reparse point. Returns
    ``"uninspectable(...)"`` if ``lstat`` fails — callers should treat that
    as a reason to fail closed (managed root) or skip (cleanup traversal).
    """

    try:
        inspection = inspect_filesystem_node(path)
    except (OSError, ValueError) as exc:
        return f"uninspectable({exc.__class__.__name__})"
    return _describe_inspection_kind(inspection)


def _describe_inspection_kind(inspection: FilesystemNodeInspection) -> str:
    """Map an inspection's ``kind`` to a short diagnostic label."""

    if inspection.kind == FilesystemNodeKind.SYMLINK:
        return "symlink"
    if inspection.kind == FilesystemNodeKind.JUNCTION:
        return "junction"
    if inspection.kind == FilesystemNodeKind.OTHER_REPARSE_POINT:
        tag = inspection.reparse_tag
        if tag is not None:
            return f"reparse-tag={tag:#x}"
        return "reparse-point"
    return "not-link-like"


def validate_managed_root_candidate(
    raw_path: str | Path,
    *,
    source: SourceName = "explicit",
) -> Path:
    """Authorize a raw managed-root candidate and return its resolved form.

    This is the single raw-root authorization entry used by
    :func:`paths.resolve_runtime_paths` for all four sources (explicit,
    environment, settings, default). It enforces the P0 contract: the RAW
    (unresolved) candidate is checked for symlink/junction/reparse identity
    BEFORE any ``.resolve()`` call, so a symlink managed root cannot be
    washed away by resolution.

    Steps:
    1. ``expanduser`` (no resolve).
    2. Single ``inspect_filesystem_node`` (one ``os.lstat``) — if the node
       exists and is a symlink/junction/reparse point -> fail closed with
       :class:`LinkLikeError`. If the node cannot be inspected for reasons
       other than non-existence (``PermissionError`` / ``OSError`` /
       ``ValueError``) -> fail closed. A non-existent path
       (``FileNotFoundError``) has no link-like node to reject; the
       repo/home/root checks in :func:`paths._reject_unsafe_log_root` guard
       the resolved path.
    3. Only after validation passes, ``.resolve()`` and return. Resolve
       failures (``OSError`` / ``RuntimeError`` / ``ValueError``, including
       symlink loops) are also fail-closed.

    The repo/home/filesystem-root checks are NOT performed here — they live
    in :func:`paths._reject_unsafe_log_root` and run at marker creation /
    cleanup time. This function only guards raw identity (link-like) so
    that resolution does not destroy the evidence.

    Args:
        raw_path: The unresolved candidate (from CLI flag, env var,
            settings, or default helper).
        source: Origin of the candidate, included in error messages for
            diagnostics.
    """

    candidate = Path(raw_path).expanduser()
    try:
        inspection = inspect_filesystem_node(candidate)
    except FileNotFoundError:
        # Path doesn't exist yet — no link-like node to reject. Intermediate
        # symlinks are resolved by ``.resolve()``; the repo/home/root checks
        # in ``_reject_unsafe_log_root`` guard the resolved path.
        pass
    except (OSError, ValueError) as exc:
        raise LinkLikeError(
            f"Refusing to authorize managed root from {source!r}: cannot inspect "
            f"raw path {candidate!r}: {exc.__class__.__name__}: {exc}"
        ) from exc
    else:
        if inspection.is_link_like:
            kind_label = _describe_inspection_kind(inspection)
            raise LinkLikeError(
                f"Refusing to authorize {kind_label} managed root from {source!r}: "
                f"{candidate!r}. The marker binds to a directory identity; a link "
                f"target can be swapped after authorization."
            )
    # Only after validation passes, resolve. Symlink loops raise RuntimeError;
    # permission issues raise OSError; encoding issues raise ValueError. All
    # must fail closed — a managed root that cannot be resolved is not safe.
    try:
        return candidate.resolve()
    except (OSError, RuntimeError, ValueError) as exc:
        raise LinkLikeError(
            f"Refusing to authorize managed root from {source!r}: cannot resolve "
            f"raw path {candidate!r}: {exc.__class__.__name__}: {exc}"
        ) from exc
