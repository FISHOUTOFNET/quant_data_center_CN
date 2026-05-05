# 修改 `_ensure_spot_close_window` 逻辑计划

## 需求分析

**当前逻辑**：只能在 18:00 到次日 08:00 之间运行（无论是否交易日）

**新逻辑**：不能在交易日的 08:00 到 18:00 之间存储，其他时间都可以存储
- 交易日：只能在 18:00 之后或 08:00 之前运行
- 非交易日：任何时间都可以运行

## 相关代码

### 1. 交易日历判断函数
位置：`src/pipeline/common.py` 第 138-144 行

```python
def is_trading_day(calendar_df: pd.DataFrame, value: str | date) -> bool:
    """判断某天是否是交易日"""
```

### 2. 需要修改的函数
位置：`src/pipeline/update_akshare_spot.py` 第 375-389 行

```python
def _ensure_spot_close_window(config: ConfigManager, now: Callable[[], datetime] | None = None) -> None:
    """当前只检查时间窗口，不检查是否为交易日"""
```

### 3. ParquetStore 读取日历
位置：`src/storage/parquet_store.py` 第 395 行

```python
def read_calendar(self) -> pd.DataFrame:
    """读取本地存储的交易日历"""
```

## 实现步骤

### 步骤 1：修改 `_ensure_spot_close_window` 函数

1. 添加 `ParquetStore` 导入（如果尚未导入）
2. 添加 `is_trading_day` 函数导入
3. 修改函数签名，添加 `store` 参数（或从 config 创建）
4. 读取本地交易日历
5. 判断当前日期是否为交易日
6. 如果是交易日且时间在 08:00-18:00 之间，抛出异常
7. 如果是非交易日，允许任何时间运行

### 步骤 2：更新调用点

位置：`src/pipeline/update_akshare_spot.py` 第 51 行

修改调用，传入 `store` 参数。

## 修改后的代码逻辑

```python
def _ensure_spot_close_window(
    config: ConfigManager, 
    now: Callable[[], datetime] | None = None,
    store: ParquetStore | None = None,
) -> None:
    timezone_name = str(config.get("project.timezone", "Asia/Shanghai"))
    local_zone = ZoneInfo(timezone_name)
    current = now() if now is not None else datetime.now(local_zone)
    if current.tzinfo is None:
        local_now = current.replace(tzinfo=local_zone)
    else:
        local_now = current.astimezone(local_zone)
    current_time = local_now.time()
    current_date = local_now.date()
    
    # 如果时间在 18:00 之后或 08:00 之前，允许运行
    if current_time >= time(18, 0) or current_time < time(8, 0):
        return
    
    # 时间在 08:00-18:00 之间，需要检查是否为交易日
    # 如果无法获取交易日历或日历为空，默认保守策略：禁止运行
    if store is None:
        store = ParquetStore(root=config.root)
    
    try:
        calendar_df = store.read_calendar()
    except Exception:
        raise RuntimeError(
            "stock_zh_a_spot_em/stock_zh_a_spot cannot verify trading day "
            "without calendar data. Please run calendar update first."
        )
    
    # 如果是非交易日，允许运行
    if not is_trading_day(calendar_df, current_date):
        return
    
    # 是交易日且时间在 08:00-18:00 之间，禁止运行
    raise RuntimeError(
        "stock_zh_a_spot_em/stock_zh_a_spot can only write hist after 18:00 "
        "and before 08:00 Asia/Shanghai on trading days"
    )
```

## 测试场景

1. **交易日 08:00-18:00**：应抛出异常
2. **交易日 18:00 之后**：允许运行
3. **交易日 08:00 之前**：允许运行
4. **非交易日任意时间**：允许运行
5. **无交易日历数据**：抛出异常提示先更新日历
