"""详细性能分析：逐行测试各操作耗时"""

import time
from bisect import insort, bisect_left, bisect_right
from collections import deque

import pandas as pd
import numpy as np

from src.storage.parquet_store import ParquetStore
from src.pipeline.adjustments import UNADJUSTED_DAILY_DATASET


def test_insort_performance(n: int = 8632):
    """测试 insort 操作的性能"""
    print(f"\n=== insort 性能测试 (n={n}) ===")
    
    values = []
    start = time.perf_counter()
    for i in range(n):
        insort(values, float(i))
    elapsed = time.perf_counter() - start
    print(f"insort 逐个插入 {n} 个元素: {elapsed:.3f}s")
    print(f"平均每次插入: {elapsed/n*1000:.3f}ms")
    
    values = []
    data = list(range(n))
    np.random.shuffle(data)
    start = time.perf_counter()
    for val in data:
        insort(values, val)
    elapsed = time.perf_counter() - start
    print(f"insort 随机插入 {n} 个元素: {elapsed:.3f}s")


def test_dataframe_loc_assignment(n: int = 8632, columns: int = 25):
    """测试 DataFrame.loc 逐行赋值的性能"""
    print(f"\n=== DataFrame.loc 逐行赋值性能测试 (n={n}, cols={columns}) ===")
    
    df = pd.DataFrame(index=range(n))
    for i in range(columns):
        df[f"col_{i}"] = pd.NA
    
    start = time.perf_counter()
    for i in range(n):
        for j in range(columns):
            df.loc[i, f"col_{j}"] = 0.5
    elapsed = time.perf_counter() - start
    print(f"DataFrame.loc 逐行赋值 {n}行×{columns}列: {elapsed:.3f}s")
    print(f"平均每行赋值: {elapsed/n*1000:.3f}ms")


def test_numpy_array_assignment(n: int = 8632, columns: int = 25):
    """测试 NumPy 数组赋值的性能"""
    print(f"\n=== NumPy 数组赋值性能测试 (n={n}, cols={columns}) ===")
    
    arr = np.full((n, columns), np.nan)
    
    start = time.perf_counter()
    for i in range(n):
        for j in range(columns):
            arr[i, j] = 0.5
    elapsed = time.perf_counter() - start
    print(f"NumPy 数组逐行赋值 {n}行×{columns}列: {elapsed:.3f}s")
    print(f"平均每行赋值: {elapsed/n*1000:.3f}ms")


def test_actual_compute_breakdown(code: str = "sh.000001"):
    """测试实际计算中各部分的耗时"""
    print(f"\n=== 实际计算耗时分解: {code} ===")
    
    store = ParquetStore()
    source = store.read_baostock_daily_bars(UNADJUSTED_DAILY_DATASET, code)
    store.close()
    
    n = len(source)
    print(f"数据行数: {n}")
    
    work = source.copy()
    work["_input_order"] = range(len(work))
    work["_date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.sort_values(["code", "_date", "_input_order"]).reset_index(drop=True)
    
    result = pd.DataFrame({"date": work["date"], "code": work["code"].astype("string")})
    
    valuation_fields = ["pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm"]
    rolling_windows = [("1y", 1), ("3y", 3), ("5y", 5), ("10y", 10)]
    
    times = {
        "insort_all_history": 0.0,
        "insort_rolling": 0.0,
        "expire_before": 0.0,
        "percentile_calc": 0.0,
        "df_loc_assign": 0.0,
    }
    
    for field in valuation_fields:
        if field not in work.columns:
            continue
        
        values = work[field]
        dates = work["_date"]
        
        valid_dates = dates.loc[values.notna()]
        first_valid_date = valid_dates.min() if not valid_dates.empty else None
        all_history_values = []
        
        rolling_states = {
            window: {"years": years, "values": [], "rows": deque()}
            for window, years in rolling_windows
        }
        
        for index, row_date in dates.items():
            current = values.loc[index]
            current_value = None if pd.isna(current) else float(current)
            
            if pd.isna(row_date) or current_value is None:
                continue
            
            start = time.perf_counter()
            insort(all_history_values, current_value)
            times["insort_all_history"] += time.perf_counter() - start
            
            for window, state in rolling_states.items():
                start = time.perf_counter()
                state["rows"].append((row_date, current_value))
                insort(state["values"], current_value)
                times["insort_rolling"] += time.perf_counter() - start
                
                start = time.perf_counter()
                cutoff = row_date - pd.DateOffset(years=state["years"])
                while state["rows"] and state["rows"][0][0] < cutoff:
                    _, val = state["rows"].popleft()
                    idx = bisect_left(state["values"], val)
                    if idx < len(state["values"]):
                        state["values"].pop(idx)
                times["expire_before"] += time.perf_counter() - start
            
            start = time.perf_counter()
            start = time.perf_counter() - start
            times["percentile_calc"] += start
            
            start = time.perf_counter()
            col_name = f"{field}_percentile_all_history"
            result.loc[index, col_name] = 50.0
            times["df_loc_assign"] += time.perf_counter() - start
    
    print("\n各操作耗时:")
    total = sum(times.values())
    for name, t in sorted(times.items(), key=lambda x: -x[1]):
        pct = t / total * 100 if total > 0 else 0
        print(f"  {name}: {t:.3f}s ({pct:.1f}%)")
    print(f"  总计: {total:.3f}s")


if __name__ == "__main__":
    test_insort_performance()
    test_dataframe_loc_assignment()
    test_numpy_array_assignment()
    test_actual_compute_breakdown()
