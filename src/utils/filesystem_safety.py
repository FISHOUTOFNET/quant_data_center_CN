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
* No third-party dependencies, no enums, no inheritance hierarchy.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path
from typing import Literal

__all__ = [
    "LinkLikeError",
    "describe_link_like",
    "is_link_like_or_reparse",
    "validate_managed_root_candidate",
]

SourceName = Literal["explicit", "environment", "settings", "default"]

# Windows reparse tags (for diagnostics only — the boundary rejects all tags).
_IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003  # junctions
_IO_REPARSE_TAG_SYMLINK = 0xA000000C  # Windows symlinks


class LinkLikeError(RuntimeError):
    """Raised when a managed-root candidate is a link-like/reparse node.

    The marker binds to a directory identity; a symlink/junction target can
    be swapped after authorization, so the deletion boundary must never
    depend on link resolution. Fail closed.
    """


def is_link_like_or_reparse(path: Path) -> bool:
    """Return ``True`` if ``path`` is a symlink, junction, or other reparse point.

    Inspects the node itself (via ``os.lstat``) and never follows the link.
    On Python 3.12+ this agrees with ``Path.is_symlink()`` /
    ``Path.is_junction()``; on Python 3.10/3.11 it provides the same
    capability without requiring those APIs.

    If the node cannot be inspected (``OSError`` / ``ValueError``), returns
    ``False``. Callers that need fail-closed behavior must use
    :func:`validate_managed_root_candidate` (which raises on inspect failure
    for managed roots) or implement their own skip-and-log policy (cleanup
    traversal).
    """

    try:
        st = os.lstat(path)
    except (OSError, ValueError):
        return False
    if stat.S_ISLNK(st.st_mode):
        return True
    if sys.platform == "win32":
        # FILE_ATTRIBUTE_REPARSE_POINT covers symlinks, junctions, mount
        # points, and other reparse tags. Conservatively reject all of them
        # rather than maintaining a known-tag allowlist.
        attrs = getattr(st, "st_file_attributes", 0) or 0
        if attrs & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            return True
    return False


def describe_link_like(path: Path) -> str:
    """Return a short diagnostic string for a link-like node (for logs).

    Returns ``"not-link-like"`` if the node is not a reparse point. Returns
    ``"uninspectable(...)"`` if ``lstat`` fails — callers should treat that
    as a reason to fail closed (managed root) or skip (cleanup traversal).
    """

    try:
        st = os.lstat(path)
    except OSError as exc:
        return f"uninspectable({exc.__class__.__name__})"
    if stat.S_ISLNK(st.st_mode):
        return "symlink"
    if sys.platform == "win32":
        attrs = getattr(st, "st_file_attributes", 0) or 0
        if attrs & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            tag = getattr(st, "st_reparse_tag", None)
            if tag == _IO_REPARSE_TAG_MOUNT_POINT:
                return "junction"
            if tag == _IO_REPARSE_TAG_SYMLINK:
                return "windows-symlink"
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
    2. ``os.lstat`` the raw node — if the node exists and is a
       symlink/junction/reparse point → fail closed with
       :class:`LinkLikeError`. If the node cannot be inspected for reasons
       other than non-existence (e.g. ``PermissionError``) → fail closed.
       A non-existent path has no link-like node to reject; the
       repo/home/root checks in :func:`paths._reject_unsafe_log_root` will
       guard the resolved path.
    3. Only after validation passes, ``.resolve()`` and return.

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
        os.lstat(candidate)
    except FileNotFoundError:
        # Path doesn't exist yet — no link-like node to reject. Intermediate
        # symlinks are resolved by ``.resolve()``; the repo/home/root checks
        # in ``_reject_unsafe_log_root`` guard the resolved path.
        return candidate.resolve()
    except OSError as exc:
        raise LinkLikeError(
            f"Refusing to authorize managed root from {source!r}: cannot inspect "
            f"raw path {candidate!r}: {exc.__class__.__name__}: {exc}"
        ) from exc
    if is_link_like_or_reparse(candidate):
        kind = describe_link_like(candidate)
        raise LinkLikeError(
            f"Refusing to authorize {kind} managed root from {source!r}: "
            f"{candidate!r}. The marker binds to a directory identity; a link "
            f"target can be swapped after authorization."
        )
    return candidate.resolve()
