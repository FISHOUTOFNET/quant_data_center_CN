"""性能测试脚本：定位 valuation percentile 计算瓶颈"""

import time
import cProfile
import pstats
import io
from pathlib import Path

import pandas as pd

from src.analytics.valuation_percentile import compute_valuation_percentiles
from src.storage.parquet_store import ParquetStore
from src.pipeline.adjustments import UNADJUSTED_DAILY_DATASET
from src.pipeline.common import PipelineCheckpointLookup


def profile_single_stock(code: str = "sh.000001", iterations: int = 3):
    """测试单个股票的各环节耗时"""
    
    store = ParquetStore()
    
    print(f"\n=== 测试股票: {code} ===")
    
    start = time.perf_counter()
    source = store.read_baostock_daily_bars(UNADJUSTED_DAILY_DATASET, code)
    read_time = time.perf_counter() - start
    print(f"[{code}] 数据读取: {read_time:.3f}s, 行数: {len(source)}")
    
    start = time.perf_counter()
    from src.storage.dataset_catalog import daily_bar_definition
    definition = daily_bar_definition(UNADJUSTED_DAILY_DATASET)
    cleaned = store.clean_dataframe_for_schema(source, definition.schema)
    clean_time = time.perf_counter() - start
    print(f"[{code}] 数据清洗: {clean_time:.3f}s")
    
    start = time.perf_counter()
    result = compute_valuation_percentiles(source)
    compute_time = time.perf_counter() - start
    print(f"[{code}] 核心计算: {compute_time:.3f}s")
    
    start = time.perf_counter()
    store.write_baostock_cn_stock_valuation_percentile(code, result)
    write_time = time.perf_counter() - start
    print(f"[{code}] 数据写入: {write_time:.3f}s")
    
    store.close()
    
    return {
        "code": code,
        "rows": len(source),
        "read_time": read_time,
        "clean_time": clean_time,
        "compute_time": compute_time,
        "write_time": write_time,
        "total_time": read_time + clean_time + compute_time + write_time,
    }


def profile_multiple_stocks(codes: list[str], max_stocks: int = 10):
    """测试多个股票的累计耗时"""
    results = []
    for i, code in enumerate(codes[:max_stocks]):
        print(f"\n=== 处理第 {i+1}/{min(len(codes), max_stocks)} 个股票: {code} ===")
        result = profile_single_stock(code)
        results.append(result)
    
    df = pd.DataFrame(results)
    print("\n=== 汇总统计 ===")
    print(df.describe())
    print(f"\n预计处理 {len(codes)} 个股票总耗时: {df['total_time'].mean() * len(codes) / 60:.1f} 分钟")
    return df


def profile_compute_detailed(code: str = "sh.000001"):
    """使用 cProfile 详细分析核心计算函数"""
    store = ParquetStore()
    source = store.read_baostock_daily_bars(UNADJUSTED_DAILY_DATASET, code)
    store.close()
    
    print(f"\n=== 详细性能分析: {code} ({len(source)} 行) ===\n")
    
    pr = cProfile.Profile()
    pr.enable()
    
    result = compute_valuation_percentiles(source)
    
    pr.disable()
    
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(30)
    print(s.getvalue())
    
    return result


def profile_checkpoint_check(codes: list[str], max_stocks: int = 100):
    """测试 checkpoint 检查耗时"""
    store = ParquetStore()
    
    print("\n=== Checkpoint 检查测试 ===")
    
    start = time.perf_counter()
    lookup = PipelineCheckpointLookup.from_store(store)
    load_time = time.perf_counter() - start
    print(f"Checkpoint 数据加载: {load_time:.3f}s")
    
    check_times = []
    for code in codes[:max_stocks]:
        output_path = store.baostock_cn_stock_valuation_percentile_path(code)
        start = time.perf_counter()
        lookup.pipeline_checkpoint_succeeded(
            "update_baostock_valuation_percentile",
            "baostock_cn_stock_valuation_percentile",
            code,
            "1990-01-01",
            "2024-01-01",
            output_path,
        )
        check_times.append(time.perf_counter() - start)
    
    store.close()
    
    print(f"平均单次检查耗时: {sum(check_times)/len(check_times)*1000:.3f}ms")
    print(f"预计 {len(codes)} 个股票检查总耗时: {sum(check_times)/len(check_times)*len(codes):.3f}s")


def get_all_codes() -> list[str]:
    """获取所有股票代码"""
    store = ParquetStore()
    dataset_dir = store.parquet_dir / UNADJUSTED_DAILY_DATASET
    if not dataset_dir.exists():
        store.close()
        return []
    codes = [
        item.name[5:]
        for item in dataset_dir.iterdir()
        if item.is_dir() and item.name.startswith("code=")
    ]
    store.close()
    return sorted(codes)


if __name__ == "__main__":
    import sys
    
    codes = get_all_codes()
    print(f"共有 {len(codes)} 个股票代码")
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        mode = "quick"
    
    if mode == "quick":
        profile_single_stock()
    elif mode == "multi":
        profile_multiple_stocks(codes, max_stocks=10)
    elif mode == "profile":
        profile_compute_detailed()
    elif mode == "checkpoint":
        profile_checkpoint_check(codes)
    else:
        print(f"未知模式: {mode}")
        print("用法: python scripts/profile_valuation_percentile.py [quick|multi|profile|checkpoint]")
