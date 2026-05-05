# 实施计划：退市数据质量警告 + 深交所退市数据支持

## 目标
1. **数据质量警告**：将 delist 数据重复问题从拒绝写入改为警告提示
2. **深交所退市数据**：在所有 `stock_info_sh_delist` 相关位置补充 `stock_info_sz_delist`，同时获取上交所和深交所退市数据

---

## 任务清单

### 任务1：修改数据质量验证逻辑（只提示不拒绝）

**文件**: `src/quality/validators.py`

**修改内容**:
- 修改 `validate_stock_info_sh_delist` 函数
- 将 `validate_unique_columns` 的重复检查从抛出异常改为 logger.warning
- 新增 `warn_duplicate_columns` 辅助函数（或直接在验证函数中实现）

**实现细节**:
```python
def validate_stock_info_sh_delist(df: pd.DataFrame, schema: pa.Schema = STOCK_INFO_SH_DELIST_SCHEMA) -> None:
    validate_schema_matches(df, schema)
    validate_akshare_six_digit_codes(df)
    # 改为警告而非拒绝
    duplicated = df.duplicated(["snapshot_date", "market", "code"], keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, ["snapshot_date", "market", "code"]].head(5).to_dict("records")
        logger.warning("Duplicate rows found for ['snapshot_date', 'market', 'code']: {}", sample)
```

---

### 任务2：添加深交所退市数据支持

#### 2.1 更新 Schema 定义

**文件**: `src/storage/schema.py`

**修改内容**:
- 复用现有 `STOCK_INFO_SH_DELIST_SCHEMA`（字段结构相同）
- 在 `DATASET_SCHEMAS` 中添加 `stock_info_sz_delist` 映射

#### 2.2 更新 AkShare Client

**文件**: `src/api/akshare_client.py`

**修改内容**:
- 添加 `STOCK_INFO_SZ_DELIST_FIELD_ALIASES` 字段别名映射
- 添加 `fetch_stock_info_sz_delist` 方法
- 添加 `_normalize_stock_info_sz_delist` 方法

**实现细节**:
```python
STOCK_INFO_SZ_DELIST_FIELD_ALIASES = {
    "source_symbol": ("公司代码", "证券代码", "代码", "source_symbol"),
    "name": ("公司简称", "证券简称", "名称", "name"),
    "list_date": ("上市日期", "list_date"),
    "delist_date": ("终止上市日期", "退市日期", "delist_date"),
}

def fetch_stock_info_sz_delist(
    self,
    symbol: str = "全部",
    snapshot_date: str | date | None = None,
) -> AkShareResponse:
    # 类似 fetch_stock_info_sh_delist 实现
```

#### 2.3 更新 Dataset Catalog

**文件**: `src/storage/dataset_catalog.py`

**修改内容**:
- 添加 `STOCK_INFO_SZ_DELIST_DATASET` 定义
- 更新 `AKSHARE_A_STOCK_DATASET_NAMES` 包含新数据集
- 更新 `akshare_a_stock_definitions()` 返回新定义
- 更新 `DATASET_CATALOG` 字典

#### 2.4 更新 Parquet Store

**文件**: `src/storage/parquet_store.py`

**修改内容**:
- 导入 `STOCK_INFO_SZ_DELIST_DATASET`
- 添加 `stock_info_sz_delist_path` 方法
- 添加 `write_stock_info_sz_delist` 方法
- 添加 `read_stock_info_sz_delist` 方法
- 添加 `read_latest_stock_info_sz_delist` 方法
- 更新 `ensure_layout` 包含新目录

#### 2.5 更新 Pipeline

**文件**: `src/pipeline/update_akshare_delist.py`

**修改内容**:
- 重构为同时获取上交所和深交所退市数据
- 添加 `fetch_all_delist` 函数或修改现有函数支持多交易所
- 更新 metadata 记录逻辑

**实现思路**:
```python
def update_akshare_delist(
    market: str = "全部",
    exchanges: list[str] | None = None,  # 新增参数，默认 ["sh", "sz"]
    snapshot_date: str | date | None = None,
    ...
) -> list[dict[str, object]]:
    # 循环处理 sh 和 sz 两个交易所
```

#### 2.6 更新 CLI

**文件**: `src/cli.py`

**修改内容**:
- 更新 `update-akshare-delist` 命令的帮助文档
- 可选：添加 `--exchange` 参数支持单独获取某个交易所

#### 2.7 更新 AkShare Universe

**文件**: `src/pipeline/akshare_universe.py`

**修改内容**:
- 更新 `resolve_akshare_universe_codes` 函数
- 同时读取上交所和深交所退市数据

**实现细节**:
```python
def resolve_akshare_universe_codes(...):
    ...
    sh_delisted_codes = _latest_dataset_codes(store.read_latest_stock_info_sh_delist())
    sz_delisted_codes = _latest_dataset_codes(store.read_latest_stock_info_sz_delist())
    delisted_codes = list(dict.fromkeys([*sh_delisted_codes, *sz_delisted_codes]))
    ...
```

#### 2.8 更新 DuckDB Store

**文件**: `src/storage/duckdb_store.py`

**修改内容**:
- 添加 `v_stock_info_sz_delist` 视图

#### 2.9 更新配置文件

**文件**: `config/settings.yaml`

**修改内容**:
- 在 `api.akshare.endpoints` 下添加 `stock_info_sz_delist` 配置

```yaml
stock_info_sz_delist:
  source: szse
  failure_threshold: 3
  cooldown_minutes: 30
```

---

### 任务3：更新测试

**文件**:
- `tests/test_akshare_client.py` - 添加深交所退市数据测试
- `tests/test_update_akshare.py` - 更新相关测试
- `tests/test_parquet_store.py` - 添加深交所存储测试
- `tests/test_schema.py` - 添加 schema 测试
- `tests/test_dataset_catalog.py` - 添加 catalog 测试

---

## 文件修改清单

| 文件 | 修改类型 | 说明 |
|------|----------|------|
| `src/quality/validators.py` | 修改 | 重复数据改为警告 |
| `src/api/akshare_client.py` | 修改 | 添加深交所接口 |
| `src/storage/schema.py` | 修改 | 添加 schema 映射 |
| `src/storage/dataset_catalog.py` | 修改 | 添加数据集定义 |
| `src/storage/parquet_store.py` | 修改 | 添加存储方法 |
| `src/storage/duckdb_store.py` | 修改 | 添加视图 |
| `src/pipeline/update_akshare_delist.py` | 修改 | 支持双交易所 |
| `src/pipeline/akshare_universe.py` | 修改 | 包含深交所退市 |
| `src/cli.py` | 修改 | 更新命令 |
| `config/settings.yaml` | 修改 | 添加配置 |
| `tests/test_*.py` | 修改 | 更新测试 |

---

## 实施顺序

1. **第一步**：修改验证器（任务1）
2. **第二步**：添加 Schema 和 Catalog 定义
3. **第三步**：添加 AkShare Client 方法
4. **第四步**：添加 Parquet Store 方法
5. **第五步**：添加 DuckDB 视图
6. **第六步**：更新 Pipeline
7. **第七步**：更新 Universe 和 CLI
8. **第八步**：更新配置文件
9. **第九步**：更新测试
10. **第十步**：运行测试验证
