from __future__ import annotations

from src.pipeline.common import resolve_codes
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


def test_resolve_codes_prefers_explicit_codes(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_stock_basic(baostock_cn_stock_basic_sample())

    codes = resolve_codes(ConfigManager(tmp_path), store, ("sz.300001",), "active")

    assert codes == ["sz.300001"]


def test_resolve_codes_uses_baostock_cn_stock_basic_modes_by_default(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_baostock_cn_stock_basic(baostock_cn_stock_basic_sample())

    history_codes = resolve_codes(ConfigManager(tmp_path), store, (), "all")
    update_codes = resolve_codes(ConfigManager(tmp_path), store, (), "active")

    assert history_codes == ["sh.000001", "sh.600000", "sz.000001"]
    assert update_codes == ["sh.600000"]

