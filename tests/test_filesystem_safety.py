"""Unit tests for the shared filesystem safety primitives.

Covers :mod:`src.utils.filesystem_safety` — the low-level mechanism that
detects link-like nodes (symlinks, Windows junctions, other reparse points)
without following them, plus the managed-root candidate authorization policy.

Cross-platform notes
--------------------
* POSIX symlink tests use REAL symlinks (created by ``Path.symlink_to``).
* Windows junction/reparse-point tests mock ``os.lstat`` and ``sys.platform``
  so they run on any OS. Real Windows junction coverage lives in
  ``tests/test_windows_junction_integration.py``.
* ``PermissionError`` / ``FileNotFoundError`` / generic ``OSError`` paths are
  mocked so they are deterministic regardless of the test interpreter.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from src.utils import filesystem_safety
from src.utils.filesystem_safety import (
    FilesystemNodeInspection,
    FilesystemNodeKind,
)

# ---------------------------------------------------------------------------
# Helpers for mocking os.lstat
# ---------------------------------------------------------------------------


class _FakeStatResult:
    """Minimal stand-in for ``os.stat_result`` used by the safety helpers.

    Only the attributes read by ``inspect_filesystem_node`` are populated:
    ``st_mode`` and, on Windows, ``st_file_attributes`` and ``st_reparse_tag``.
    """

    def __init__(
        self,
        *,
        st_mode: int = 0o040755,  # directory by default
        st_file_attributes: int = 0,
        st_reparse_tag: int = 0,
    ) -> None:
        self.st_mode = st_mode
        self.st_file_attributes = st_file_attributes
        self.st_reparse_tag = st_reparse_tag


def _mock_lstat(monkeypatch: pytest.MonkeyPatch, path: Path, result: os.stat_result) -> None:
    """Patch ``os.lstat`` so that ``path`` returns ``result``; others fall back."""

    real_lstat = os.lstat

    def fake_lstat(target: object) -> os.stat_result:
        target_path = Path(str(target))
        if target_path == path:
            return result
        return real_lstat(target)

    monkeypatch.setattr(filesystem_safety.os, "lstat", fake_lstat)


# ---------------------------------------------------------------------------
# inspect_filesystem_node — real filesystem nodes
# ---------------------------------------------------------------------------


class TestInspectFilesystemNodeRealNodes:
    """``inspect_filesystem_node`` classifies real nodes correctly."""

    def test_regular_file(self, tmp_path: Path) -> None:
        target = tmp_path / "file.txt"
        target.write_text("hi", encoding="utf-8")
        inspection = filesystem_safety.inspect_filesystem_node(target)
        assert inspection.kind is FilesystemNodeKind.REGULAR_FILE
        assert inspection.is_link_like is False
        assert inspection.path == target

    def test_regular_directory(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.DIRECTORY
        assert inspection.is_link_like is False

    def test_symlink_classified_as_symlink(self, tmp_path: Path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        inspection = filesystem_safety.inspect_filesystem_node(link)
        assert inspection.kind is FilesystemNodeKind.SYMLINK
        assert inspection.is_link_like is True

    def test_symlink_to_file_classified_as_symlink(self, tmp_path: Path) -> None:
        target = tmp_path / "target.txt"
        target.write_text("hi", encoding="utf-8")
        link = tmp_path / "link.txt"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        inspection = filesystem_safety.inspect_filesystem_node(link)
        assert inspection.kind is FilesystemNodeKind.SYMLINK
        assert inspection.is_link_like is True

    def test_missing_path_raises_file_not_found(self, tmp_path: Path) -> None:
        missing = tmp_path / "does-not-exist"
        with pytest.raises(FileNotFoundError):
            filesystem_safety.inspect_filesystem_node(missing)


# ---------------------------------------------------------------------------
# inspect_filesystem_node — error propagation (mocked lstat)
# ---------------------------------------------------------------------------


class TestInspectFilesystemNodeErrorPropagation:
    """``inspect_filesystem_node`` does NOT swallow exceptions."""

    def test_permission_error_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_permission(_: object) -> os.stat_result:
            raise PermissionError("denied")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_permission)
        with pytest.raises(PermissionError):
            filesystem_safety.inspect_filesystem_node(tmp_path)

    def test_file_not_found_error_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_not_found(_: object) -> os.stat_result:
            raise FileNotFoundError("missing")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_not_found)
        with pytest.raises(FileNotFoundError):
            filesystem_safety.inspect_filesystem_node(tmp_path)

    def test_generic_oserror_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_oserror(_: object) -> os.stat_result:
            raise OSError("boom")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_oserror)
        with pytest.raises(OSError, match="boom"):
            filesystem_safety.inspect_filesystem_node(tmp_path)

    def test_value_error_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_value(_: object) -> os.stat_result:
            raise ValueError("bad path")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_value)
        with pytest.raises(ValueError, match="bad path"):
            filesystem_safety.inspect_filesystem_node(tmp_path)


# ---------------------------------------------------------------------------
# inspect_filesystem_node — Windows reparse points (mocked)
# ---------------------------------------------------------------------------


class TestInspectFilesystemNodeWindowsReparse:
    """Windows reparse-point classification (junctions, mount points, other tags).

    These tests mock ``sys.platform`` to ``"win32"`` and ``os.lstat`` to
    return an ``st_file_attributes`` with ``FILE_ATTRIBUTE_REPARSE_POINT`` set,
    so they run on any OS. Real junction coverage lives in the Windows
    integration test.
    """

    def test_junction_classified_correctly(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,  # directory (junctions appear as dirs)
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.JUNCTION
        assert inspection.is_link_like is True

    def test_windows_symlink_tag_classified_as_symlink(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_SYMLINK,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.SYMLINK
        assert inspection.is_link_like is True

    def test_unknown_reparse_tag_classified_as_other_reparse(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0xA0000099,  # unknown tag
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.OTHER_REPARSE_POINT
        assert inspection.is_link_like is True

    def test_reparse_without_tag_attr_classified_as_other_reparse(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Even when ``st_reparse_tag`` is missing, ``st_file_attributes`` alone
        is enough to identify a reparse point.
        """

        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.OTHER_REPARSE_POINT
        assert inspection.is_link_like is True

    def test_windows_normal_directory_classified_as_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_DIRECTORY,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.DIRECTORY
        assert inspection.is_link_like is False

    def test_posix_ignores_st_file_attributes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """On POSIX, ``st_file_attributes`` is not checked even if set."""

        monkeypatch.setattr(filesystem_safety.sys, "platform", "linux")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.DIRECTORY
        assert inspection.is_link_like is False

    def test_regular_file_with_reparse_attr_on_posix_not_link_like(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On POSIX, a regular file with REPARSE_POINT attr is NOT link-like."""

        monkeypatch.setattr(filesystem_safety.sys, "platform", "linux")
        fake = _FakeStatResult(
            st_mode=0o100644,  # regular file
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        inspection = filesystem_safety.inspect_filesystem_node(tmp_path)
        assert inspection.kind is FilesystemNodeKind.REGULAR_FILE
        assert inspection.is_link_like is False


# ---------------------------------------------------------------------------
# inspect_filesystem_node — single lstat constraint
# ---------------------------------------------------------------------------


class TestInspectFilesystemNodeSingleLstat:
    """``inspect_filesystem_node`` calls ``os.lstat`` exactly once."""

    def test_only_one_lstat_call(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        call_count = 0
        real_lstat = os.lstat

        def counting_lstat(target: object) -> os.stat_result:
            nonlocal call_count
            call_count += 1
            return real_lstat(target)

        monkeypatch.setattr(filesystem_safety.os, "lstat", counting_lstat)

        filesystem_safety.inspect_filesystem_node(tmp_path)

        assert call_count == 1

    def test_inspection_does_not_resolve(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``inspect_filesystem_node`` must NOT call ``Path.resolve()``."""

        tmp_path.mkdir(parents=True, exist_ok=True)
        resolve_calls = 0
        original_resolve = Path.resolve

        def counting_resolve(self: Path, *args: object, **kwargs: object) -> Path:
            nonlocal resolve_calls
            resolve_calls += 1
            return original_resolve(self)

        monkeypatch.setattr(Path, "resolve", counting_resolve)

        filesystem_safety.inspect_filesystem_node(tmp_path)

        assert resolve_calls == 0


# ---------------------------------------------------------------------------
# FilesystemNodeInspection dataclass
# ---------------------------------------------------------------------------


class TestFilesystemNodeInspection:
    """The inspection dataclass is frozen and carries the right fields."""

    def test_is_frozen(self) -> None:
        inspection = FilesystemNodeInspection(
            path=Path("/tmp"),
            kind=FilesystemNodeKind.REGULAR_FILE,
            mode=0o100644,
        )
        with pytest.raises(AttributeError, match="kind"):
            inspection.kind = FilesystemNodeKind.DIRECTORY  # type: ignore[misc]

    def test_optional_fields_default_none(self) -> None:
        inspection = FilesystemNodeInspection(
            path=Path("/tmp"),
            kind=FilesystemNodeKind.REGULAR_FILE,
            mode=0o100644,
        )
        assert inspection.file_attributes is None
        assert inspection.reparse_tag is None

    def test_is_link_like_for_each_kind(self) -> None:
        link_like_kinds = {
            FilesystemNodeKind.SYMLINK,
            FilesystemNodeKind.JUNCTION,
            FilesystemNodeKind.OTHER_REPARSE_POINT,
        }
        non_link_like_kinds = {
            FilesystemNodeKind.REGULAR_FILE,
            FilesystemNodeKind.DIRECTORY,
            FilesystemNodeKind.OTHER,
        }
        for kind in link_like_kinds:
            inspection = FilesystemNodeInspection(path=Path("/x"), kind=kind, mode=0)
            assert inspection.is_link_like is True, f"{kind} should be link-like"
        for kind in non_link_like_kinds:
            inspection = FilesystemNodeInspection(path=Path("/x"), kind=kind, mode=0)
            assert inspection.is_link_like is False, f"{kind} should NOT be link-like"


# ---------------------------------------------------------------------------
# is_link_like_or_reparse — strict wrapper (errors propagate)
# ---------------------------------------------------------------------------


class TestIsLinkLikeOrReparseWrapper:
    """``is_link_like_or_reparse`` is a strict wrapper around inspection.

    It does NOT swallow exceptions — callers needing fail-closed or
    skip-and-log behavior must catch explicitly.
    """

    def test_normal_directory_is_not_link_like(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False

    def test_normal_file_is_not_link_like(self, tmp_path: Path) -> None:
        target = tmp_path / "file.txt"
        target.write_text("hi", encoding="utf-8")
        assert filesystem_safety.is_link_like_or_reparse(target) is False

    def test_real_symlink_is_link_like(self, tmp_path: Path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        assert filesystem_safety.is_link_like_or_reparse(link) is True

    def test_missing_path_raises_file_not_found(self, tmp_path: Path) -> None:
        """Wrapper does NOT swallow FileNotFoundError."""

        missing = tmp_path / "does-not-exist"
        with pytest.raises(FileNotFoundError):
            filesystem_safety.is_link_like_or_reparse(missing)

    def test_permission_error_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_permission(_: object) -> os.stat_result:
            raise PermissionError("denied")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_permission)
        with pytest.raises(PermissionError):
            filesystem_safety.is_link_like_or_reparse(tmp_path)

    def test_generic_oserror_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_oserror(_: object) -> os.stat_result:
            raise OSError("boom")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_oserror)
        with pytest.raises(OSError, match="boom"):
            filesystem_safety.is_link_like_or_reparse(tmp_path)

    def test_value_error_propagates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_value(_: object) -> os.stat_result:
            raise ValueError("bad path")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_value)
        with pytest.raises(ValueError, match="bad path"):
            filesystem_safety.is_link_like_or_reparse(tmp_path)

    def test_windows_reparse_point_is_link_like(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is True

    def test_posix_ignores_st_file_attributes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "linux")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.is_link_like_or_reparse(tmp_path) is False


# ---------------------------------------------------------------------------
# describe_link_like
# ---------------------------------------------------------------------------


class TestDescribeLinkLike:
    """Diagnostic string for logs."""

    def test_normal_path_is_not_link_like(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        assert filesystem_safety.describe_link_like(tmp_path) == "not-link-like"

    def test_real_symlink_described_as_symlink(self, tmp_path: Path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        assert filesystem_safety.describe_link_like(link) == "symlink"

    def test_uninspectable_returns_uninspectable(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_permission(_: object) -> os.stat_result:
            raise PermissionError("denied")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_permission)
        result = filesystem_safety.describe_link_like(tmp_path)
        assert result.startswith("uninspectable(")
        assert "PermissionError" in result

    def test_windows_junction_described(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "junction"

    def test_windows_symlink_tag_described_as_symlink(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_SYMLINK,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "symlink"

    def test_windows_unknown_reparse_tag_described(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0xA0000099,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "reparse-tag=0xa0000099"

    def test_windows_reparse_without_tag_described(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When ``st_reparse_tag`` attribute is missing entirely (e.g. Python
        3.10 ``os.stat_result`` does not always populate it), the diagnostic
        falls back to ``"reparse-point"``.
        """

        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=0,
        )
        # Remove the attribute entirely so getattr(..., None) returns None.
        del fake.st_reparse_tag
        _mock_lstat(monkeypatch, tmp_path, fake)
        assert filesystem_safety.describe_link_like(tmp_path) == "reparse-point"


# ---------------------------------------------------------------------------
# validate_managed_root_candidate
# ---------------------------------------------------------------------------


class TestValidateManagedRootCandidate:
    """The managed-root authorization entry used by resolve_runtime_paths."""

    def test_normal_directory_resolves(self, tmp_path: Path) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        result = filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")
        assert result == tmp_path.resolve()

    def test_normal_file_resolves(self, tmp_path: Path) -> None:
        target = tmp_path / "file.txt"
        target.write_text("hi", encoding="utf-8")
        result = filesystem_safety.validate_managed_root_candidate(target, source="explicit")
        assert result == target.resolve()

    def test_nonexistent_path_resolves(self, tmp_path: Path) -> None:
        """A non-existent path has no link-like node to reject; the
        repo/home/root checks in ``_reject_unsafe_log_root`` guard the
        resolved path at marker time.
        """

        missing = tmp_path / "does-not-exist"
        result = filesystem_safety.validate_managed_root_candidate(missing, source="explicit")
        assert result == missing.resolve()

    def test_real_symlink_is_rejected(self, tmp_path: Path) -> None:
        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        with pytest.raises(filesystem_safety.LinkLikeError, match="symlink"):
            filesystem_safety.validate_managed_root_candidate(link, source="explicit")

    def test_lstat_permission_error_fails_closed(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When ``lstat`` raises ``PermissionError`` for an EXISTING path,
        the helper fails closed — it does NOT silently treat the path as safe.
        """

        tmp_path.mkdir(parents=True, exist_ok=True)

        def raise_permission(_: object) -> os.stat_result:
            raise PermissionError("denied")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_permission)
        with pytest.raises(filesystem_safety.LinkLikeError, match="cannot inspect"):
            filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")

    def test_lstat_value_error_fails_closed(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``ValueError`` from ``lstat`` is also fail-closed."""

        tmp_path.mkdir(parents=True, exist_ok=True)

        def raise_value(_: object) -> os.stat_result:
            raise ValueError("bad path")

        monkeypatch.setattr(filesystem_safety.os, "lstat", raise_value)
        with pytest.raises(filesystem_safety.LinkLikeError, match="cannot inspect"):
            filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")

    def test_windows_reparse_point_rejected(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(filesystem_safety.sys, "platform", "win32")
        tmp_path.mkdir(parents=True, exist_ok=True)
        fake = _FakeStatResult(
            st_mode=0o040755,
            st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT,
            st_reparse_tag=filesystem_safety._IO_REPARSE_TAG_MOUNT_POINT,
        )
        _mock_lstat(monkeypatch, tmp_path, fake)
        with pytest.raises(filesystem_safety.LinkLikeError, match="junction"):
            filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")

    def test_source_name_appears_in_error(self, tmp_path: Path) -> None:
        """The ``source`` argument is included in the error for diagnostics."""

        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")
        with pytest.raises(filesystem_safety.LinkLikeError, match="environment"):
            filesystem_safety.validate_managed_root_candidate(link, source="environment")

    def test_expanduser_applied_before_check(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``~`` is expanded to the home directory before the lstat check."""

        real_home = tmp_path / "fakehome"
        real_home.mkdir()
        monkeypatch.setenv("USERPROFILE", str(real_home))
        monkeypatch.setenv("HOME", str(real_home))
        target = real_home / "logs"
        target.mkdir()
        result = filesystem_safety.validate_managed_root_candidate("~/logs", source="default")
        assert result == target.resolve()

    def test_resolve_failure_fails_closed(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When ``.resolve()`` raises, the helper fails closed."""

        tmp_path.mkdir(parents=True, exist_ok=True)

        def raise_on_resolve(self: Path, *args: object, **kwargs: object) -> Path:
            raise RuntimeError("symlink loop")

        monkeypatch.setattr(Path, "resolve", raise_on_resolve)
        with pytest.raises(filesystem_safety.LinkLikeError, match="cannot resolve"):
            filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")


# ---------------------------------------------------------------------------
# validate_managed_root_candidate — single lstat constraint
# ---------------------------------------------------------------------------


class TestValidateManagedRootCandidateSingleLstat:
    """One authorization decision = one ``os.lstat`` call.

    The previous implementation called ``os.lstat`` in ``validate_managed_root_candidate``
    and then again inside ``is_link_like_or_reparse`` (plus a third time in
    ``describe_link_like`` for the error message). The new implementation
    uses a single ``inspect_filesystem_node`` call.
    """

    def test_authorization_calls_lstat_once(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        tmp_path.mkdir(parents=True, exist_ok=True)
        call_count = 0
        real_lstat = os.lstat

        def counting_lstat(target: object) -> os.stat_result:
            nonlocal call_count
            call_count += 1
            return real_lstat(target)

        monkeypatch.setattr(filesystem_safety.os, "lstat", counting_lstat)

        result = filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")

        assert call_count == 1
        assert result == tmp_path.resolve()

    def test_authorization_succeeds_even_if_second_lstat_would_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the first ``lstat`` returns a regular node, authorization
        succeeds and ``lstat`` is NOT called again — even if a hypothetical
        second call would have raised.

        This proves the duplicate-lstat bug is fixed: the old code called
        ``lstat`` once in ``validate_managed_root_candidate`` and again in
        ``is_link_like_or_reparse``; if the second call raised (e.g. race
        condition), the authorization would fail despite the first call
        succeeding.
        """

        tmp_path.mkdir(parents=True, exist_ok=True)
        call_count = 0
        real_lstat = os.lstat

        def counting_lstat(target: object) -> os.stat_result:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return real_lstat(target)
            raise PermissionError("second lstat should not happen")

        monkeypatch.setattr(filesystem_safety.os, "lstat", counting_lstat)

        result = filesystem_safety.validate_managed_root_candidate(tmp_path, source="explicit")

        assert call_count == 1
        assert result == tmp_path.resolve()

    def test_symlink_rejection_calls_lstat_once(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Even when the path IS a symlink (rejected), ``lstat`` is called
        only once — the classification and the error message both use the
        same inspection result.
        """

        target = tmp_path / "target"
        target.mkdir()
        link = tmp_path / "link"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink not supported on this platform")

        call_count = 0
        real_lstat = os.lstat

        def counting_lstat(t: object) -> os.stat_result:
            nonlocal call_count
            call_count += 1
            return real_lstat(t)

        monkeypatch.setattr(filesystem_safety.os, "lstat", counting_lstat)

        with pytest.raises(filesystem_safety.LinkLikeError, match="symlink"):
            filesystem_safety.validate_managed_root_candidate(link, source="explicit")

        assert call_count == 1
