"""AkShare cross-source pipeline orchestration."""

from src.sources.akshare.pipeline.execution import AkShareUpdateRequest, update_akshare

__all__ = ["AkShareUpdateRequest", "update_akshare"]
