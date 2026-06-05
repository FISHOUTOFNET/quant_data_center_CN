"""AkShare source response models."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class AkShareResponse:
    endpoint: str
    params: dict[str, object]
    akshare_version: str
    data: pd.DataFrame

    @property
    def row_count(self) -> int:
        return len(self.data)
