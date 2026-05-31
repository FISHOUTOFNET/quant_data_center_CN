from __future__ import annotations

from src.pipeline.common import baostock_cn_stock_basic_codes, resolve_codes
from src.storage.parquet_store import ParquetStore
from src.utils.config_mgr import ConfigManager


def test_resolve_codes_prefers_explicit_codes(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    codes = resolve_codes(ConfigManager(tmp_path), store, ("sz.300001",), "active")

    assert codes == ["sz.300001"]


def test_resolve_codes_uses_baostock_cn_stock_basic_modes_by_default(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    history_codes = resolve_codes(ConfigManager(tmp_path), store, (), "all")
    update_codes = resolve_codes(ConfigManager(tmp_path), store, (), "active")

    assert history_codes == ["sh.000001", "sh.600000", "sz.000001"]
    assert update_codes == ["sh.000001", "sh.600000"]


def test_baostock_cn_stock_basic_codes_filters_by_security_type(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    all_codes = baostock_cn_stock_basic_codes(store, "all")
    stock_only = baostock_cn_stock_basic_codes(store, "all", security_type="1")

    assert all_codes == ["sh.000001", "sh.600000", "sz.000001"]
    assert stock_only == ["sh.600000", "sz.000001"]


def test_baostock_cn_stock_basic_codes_security_type_with_active_mode(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    codes = baostock_cn_stock_basic_codes(store, "active", security_type="1")

    assert codes == ["sh.600000"]


def test_baostock_cn_stock_basic_codes_security_type_none_returns_all(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    codes_no_filter = baostock_cn_stock_basic_codes(store, "all", security_type=None)
    codes_default = baostock_cn_stock_basic_codes(store, "all")

    assert codes_no_filter == codes_default


def test_resolve_codes_passes_security_type(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    all_codes = resolve_codes(ConfigManager(tmp_path), store, (), "all")
    stock_only = resolve_codes(ConfigManager(tmp_path), store, (), "all", security_type="1")

    assert all_codes == ["sh.000001", "sh.600000", "sz.000001"]
    assert stock_only == ["sh.600000", "sz.000001"]


def test_resolve_codes_explicit_codes_ignores_security_type(tmp_path, baostock_cn_stock_basic_sample) -> None:
    store = ParquetStore(root=tmp_path)
    store.ensure_layout()
    store.write_dataset("baostock_cn_stock_basic", baostock_cn_stock_basic_sample())

    codes = resolve_codes(ConfigManager(tmp_path), store, ("sh.000001",), "active", security_type="1")

    assert codes == ["sh.000001"]
