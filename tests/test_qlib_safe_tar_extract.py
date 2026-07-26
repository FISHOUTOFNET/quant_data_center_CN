"""Tests for the version-independent safe Tar extraction in Qlib sync.

Covers P0-1: ``_safe_extract_tar`` validates every archive member BEFORE any
extraction begins, so a malicious or corrupted archive is rejected atomically
and the existing ``source_dir`` is left untouched. The contract is identical
on Python 3.10 (no ``filter`` kwarg) and Python 3.11+ (``filter="data"`` as
defense-in-depth).
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

import src.sources.qlib.sync as qlib_sync_module
from src.sources.qlib.sync import (
    QLIB_ASSET_NAME,
    QlibRemoteAsset,
    UnsafeArchiveError,
    _safe_extract_tar,
    _validate_tar_member,
    download_and_extract_qlib_asset,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_member(name: str, *, content: bytes = b"x", kind: str = "file") -> tarfile.TarInfo:
    """Build a TarInfo with the requested name and kind."""

    info = tarfile.TarInfo(name=name)
    if kind == "file":
        info.type = tarfile.REGTYPE
        info.size = len(content)
        info.mode = 0o644
    elif kind == "dir":
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
    elif kind == "symlink":
        info.type = tarfile.SYMTYPE
        info.linkname = content.decode("utf-8", errors="replace")
    elif kind == "hardlink":
        info.type = tarfile.LNKTYPE
        info.linkname = content.decode("utf-8", errors="replace")
    elif kind == "fifo":
        info.type = tarfile.FIFOTYPE
    elif kind == "char":
        info.type = tarfile.CHRTYPE
    elif kind == "block":
        info.type = tarfile.BLKTYPE
    else:
        raise ValueError(f"unknown kind: {kind}")
    return info


def _write_archive(path: Path, members: list[tuple[tarfile.TarInfo, bytes | None]]) -> None:
    """Write a tar archive containing the given members.

    For file members, the second tuple element is the file content. For
    non-file members it is ignored.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as tar:
        for info, content in members:
            if info.type == tarfile.REGTYPE:
                buf = io.BytesIO(content or b"")
                tar.addfile(info, buf)
            else:
                tar.addfile(info)


def _build_safe_archive(path: Path) -> None:
    """Build a small archive that looks like a Qlib source tree."""

    cal_dir = _make_member("cn_data/calendars/", kind="dir")
    feat_dir = _make_member("cn_data/features/", kind="dir")
    instr_dir = _make_member("cn_data/instruments/", kind="dir")
    cal_file = _make_member("cn_data/calendars/day.txt", content=b"2024-01-01\n2024-01-02\n")
    instr_file = _make_member("cn_data/instruments/all.txt", content=b"sh600000\t2024-01-01\t2024-01-02\n")
    _write_archive(
        path,
        [
            (cal_dir, None),
            (feat_dir, None),
            (instr_dir, None),
            (cal_file, b"2024-01-01\n2024-01-02\n"),
            (instr_file, b"sh600000\t2024-01-01\t2024-01-02\n"),
        ],
    )


# ---------------------------------------------------------------------------
# _validate_tar_member / _ensure_path_inside_destination
# ---------------------------------------------------------------------------


class TestValidateTarMember:
    """Direct unit tests on the member validator."""

    def test_accepts_regular_file(self, tmp_path: Path) -> None:
        info = _make_member("cn_data/calendars/day.txt", content=b"hello")
        # Must not raise.
        _validate_tar_member(info, tmp_path.resolve())

    def test_accepts_directory(self, tmp_path: Path) -> None:
        info = _make_member("cn_data/calendars/", kind="dir")
        _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_dotdot_relative_path(self, tmp_path: Path) -> None:
        info = _make_member("../outside.txt", content=b"x")
        with pytest.raises(UnsafeArchiveError, match=r"\.\."):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_multilevel_dotdot(self, tmp_path: Path) -> None:
        info = _make_member("cn_data/../../outside.txt", content=b"x")
        with pytest.raises(UnsafeArchiveError, match=r"\.\."):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_posix_absolute_path(self, tmp_path: Path) -> None:
        info = _make_member("/absolute/path/file.txt", content=b"x")
        with pytest.raises(UnsafeArchiveError, match="absolute"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_windows_drive_path_backslash(self, tmp_path: Path) -> None:
        info = _make_member("C:\\outside.txt", content=b"x")
        with pytest.raises(UnsafeArchiveError, match="drive"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_windows_drive_path_forward_slash(self, tmp_path: Path) -> None:
        info = _make_member("C:/outside.txt", content=b"x")
        with pytest.raises(UnsafeArchiveError, match="drive"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_windows_unc_path(self, tmp_path: Path) -> None:
        info = _make_member("\\\\server\\share\\file.txt", content=b"x")
        with pytest.raises(UnsafeArchiveError, match="absolute"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_symlink_member(self, tmp_path: Path) -> None:
        info = _make_member("link.txt", kind="symlink", content=b"/etc/passwd")
        with pytest.raises(UnsafeArchiveError, match="symlink"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_hardlink_member(self, tmp_path: Path) -> None:
        info = _make_member("link.txt", kind="hardlink", content=b"/etc/passwd")
        with pytest.raises(UnsafeArchiveError, match="hardlink"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_fifo(self, tmp_path: Path) -> None:
        info = _make_member("pipe", kind="fifo")
        with pytest.raises(UnsafeArchiveError, match="special"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_char_device(self, tmp_path: Path) -> None:
        info = _make_member("chardev", kind="char")
        with pytest.raises(UnsafeArchiveError, match="special"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_block_device(self, tmp_path: Path) -> None:
        info = _make_member("blockdev", kind="block")
        with pytest.raises(UnsafeArchiveError, match="special"):
            _validate_tar_member(info, tmp_path.resolve())

    def test_rejects_empty_name(self, tmp_path: Path) -> None:
        info = _make_member("", content=b"x")
        with pytest.raises(UnsafeArchiveError, match="empty"):
            _validate_tar_member(info, tmp_path.resolve())


# ---------------------------------------------------------------------------
# _safe_extract_tar: validate-before-extract contract
# ---------------------------------------------------------------------------


class TestSafeExtractTarContract:
    """The validate-before-extract contract: no file is written before all
    members are validated, and a rejection leaves the destination empty.
    """

    def test_extracts_safe_archive(self, tmp_path: Path) -> None:
        archive_path = tmp_path / "safe.tar.gz"
        _build_safe_archive(archive_path)
        dest = tmp_path / "dest"
        dest.mkdir()

        with tarfile.open(archive_path, "r:gz") as tar:
            _safe_extract_tar(tar, dest)

        # The Qlib-style tree must be present.
        assert (dest / "cn_data" / "calendars" / "day.txt").read_text() == "2024-01-01\n2024-01-02\n"
        assert (dest / "cn_data" / "instruments" / "all.txt").exists()

    def test_rejection_leaves_destination_empty(self, tmp_path: Path) -> None:
        """When validation fails, NO file may have been extracted yet.

        We inject a malicious member AFTER a benign one and confirm that even
        the benign member is not on disk after the rejection. This proves the
        validate-before-extract contract is honored (no partial extraction).
        """

        archive_path = tmp_path / "mixed.tar.gz"
        benign = _make_member("benign.txt", content=b"ok")
        malicious = _make_member("../escape.txt", content=b"evil")
        _write_archive(archive_path, [(benign, b"ok"), (malicious, b"evil")])

        dest = tmp_path / "dest"
        dest.mkdir()
        with tarfile.open(archive_path, "r:gz") as tar, pytest.raises(UnsafeArchiveError):
            _safe_extract_tar(tar, dest)

        # No file from the archive may exist in dest.
        assert not (dest / "benign.txt").exists()
        assert not (dest / "escape.txt").exists()
        # And nothing escaped outside dest.
        assert not (tmp_path / "escape.txt").exists()

    def test_validation_runs_before_extraction_strategy(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """The extraction_strategy callable must NOT be invoked when
        validation fails. This is the unit-level proof that validation runs
        to completion before any extraction.
        """

        archive_path = tmp_path / "bad.tar.gz"
        malicious = _make_member("../escape.txt", content=b"evil")
        _write_archive(archive_path, [(malicious, b"evil")])

        calls: list[tuple[tarfile.TarFile, Path]] = []

        def _recording_strategy(archive: tarfile.TarFile, destination: Path) -> None:
            calls.append((archive, destination))

        dest = tmp_path / "dest"
        dest.mkdir()
        with tarfile.open(archive_path, "r:gz") as tar, pytest.raises(UnsafeArchiveError):
            _safe_extract_tar(tar, dest, extraction_strategy=_recording_strategy)

        assert calls == [], "extraction_strategy must not run when validation fails"

    def test_extraction_strategy_used_on_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """On a safe archive, the injected extraction_strategy is used."""

        archive_path = tmp_path / "safe.tar.gz"
        _build_safe_archive(archive_path)

        calls: list[tuple[tarfile.TarFile, Path]] = []

        def _recording_strategy(archive: tarfile.TarFile, destination: Path) -> None:
            calls.append((archive, destination))
            # Do nothing — we only want to confirm the strategy is invoked.

        dest = tmp_path / "dest"
        dest.mkdir()
        with tarfile.open(archive_path, "r:gz") as tar:
            _safe_extract_tar(tar, dest, extraction_strategy=_recording_strategy)

        assert len(calls) == 1


# ---------------------------------------------------------------------------
# _default_extraction_strategy: both Python branches run the same validation
# ---------------------------------------------------------------------------


class TestDefaultExtractionStrategyBothVersions:
    """Python 3.10 fallback and Python 3.11+ ``filter="data"`` both rely on
    the same up-front validation. We confirm both paths extract a safe archive
    without re-implementing the validation in the test.
    """

    def test_default_strategy_extracts_safe_archive(self, tmp_path: Path) -> None:
        archive_path = tmp_path / "safe.tar.gz"
        _build_safe_archive(archive_path)

        dest = tmp_path / "dest"
        dest.mkdir()
        with tarfile.open(archive_path, "r:gz") as tar:
            _safe_extract_tar(tar, dest, extraction_strategy=qlib_sync_module._default_extraction_strategy)

        assert (dest / "cn_data" / "calendars" / "day.txt").exists()

    def test_py310_fallback_path_runs_unified_validation(self, tmp_path: Path) -> None:
        """Simulate the Python 3.10 fallback (plain extractall) and confirm
        a malicious archive is still rejected by the up-front validation.
        """

        archive_path = tmp_path / "bad.tar.gz"
        malicious = _make_member("../escape.txt", content=b"evil")
        _write_archive(archive_path, [(malicious, b"evil")])

        def _py310_strategy(archive: tarfile.TarFile, destination: Path) -> None:
            # Mirrors the Python 3.10 branch of _default_extraction_strategy.
            archive.extractall(destination)

        dest = tmp_path / "dest"
        dest.mkdir()
        with tarfile.open(archive_path, "r:gz") as tar, pytest.raises(UnsafeArchiveError):
            _safe_extract_tar(tar, dest, extraction_strategy=_py310_strategy)

        # Nothing extracted.
        assert not (dest / "escape.txt").exists()
        assert not (tmp_path / "escape.txt").exists()

    def test_py311_filter_data_path_runs_unified_validation(self, tmp_path: Path) -> None:
        """Simulate the Python 3.11+ path (extractall with filter="data")
        and confirm a malicious archive is rejected by the up-front
        validation before the stdlib filter even sees it.
        """

        archive_path = tmp_path / "bad.tar.gz"
        malicious = _make_member("../escape.txt", content=b"evil")
        _write_archive(archive_path, [(malicious, b"evil")])

        def _py311_strategy(archive: tarfile.TarFile, destination: Path) -> None:
            # Mirrors the Python 3.11+ branch. On 3.10 the kwarg is rejected
            # by tarfile, so we fall back to plain extractall for the test
            # body — the point is that the strategy is only reached AFTER
            # validation, which is what we assert.
            try:
                archive.extractall(destination, filter="data")
            except TypeError:
                archive.extractall(destination)

        dest = tmp_path / "dest"
        dest.mkdir()
        with tarfile.open(archive_path, "r:gz") as tar, pytest.raises(UnsafeArchiveError):
            _safe_extract_tar(tar, dest, extraction_strategy=_py311_strategy)


# ---------------------------------------------------------------------------
# Atomicity: download_and_extract_qlib_asset leaves source_dir unchanged
# ---------------------------------------------------------------------------


class TestDownloadExtractAtomicity:
    """When an unsafe archive is rejected, the existing ``source_dir`` must
    survive untouched and no replacement/backup directory may be left behind.
    """

    def test_unsafe_archive_leaves_existing_source_dir_untouched(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        source_dir = tmp_path / "qlib" / "cn_data"
        (source_dir / "calendars").mkdir(parents=True)
        (source_dir / "features").mkdir()
        (source_dir / "instruments").mkdir()
        marker = source_dir / "calendars" / "day.txt"
        marker.write_text("old\n", encoding="utf-8")

        archive_path = tmp_path / "qlib" / QLIB_ASSET_NAME
        malicious = _make_member("../escape.txt", content=b"evil")
        _write_archive(archive_path, [(malicious, b"evil")])

        def fake_download(url: str, target_path: Path, *, deadline=None) -> None:
            del url, deadline
            # The test pre-wrote the archive at archive_path; just leave it.

        monkeypatch.setattr(qlib_sync_module, "_download_file", fake_download)

        with pytest.raises(UnsafeArchiveError):
            download_and_extract_qlib_asset(
                source_dir,
                QlibRemoteAsset(
                    asset_id="asset-1",
                    etag=None,
                    size=None,
                    download_url="https://example.test/qlib.tar.gz",
                ),
                force_download=True,
            )

        # Existing source_dir content is unchanged.
        assert marker.read_text(encoding="utf-8") == "old\n"
        # No replacement directory was left behind.
        assert not list(source_dir.parent.glob("cn_data.replacement.*"))
        # No backup directory was left behind.
        assert not list(source_dir.parent.glob("cn_data.backup.*"))
        # No file escaped into the parent directory.
        assert not (tmp_path / "escape.txt").exists()
        assert not (tmp_path / "qlib" / "escape.txt").exists()
