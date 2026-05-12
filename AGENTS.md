# 项目目标

本项目是本地 A 股量化数据中心，面向 Windows + PowerShell 环境运行。

目标是提供稳定、可复现、可增量更新的数据底座，为后续回测、研究、数据质量检查和本地查询服务提供统一来源。后续 Codex 工作应优先维护数据可靠性、可追溯性和兼容性，而不是追求一次性大改或风格统一。

# 核心架构

- Parquet 是主存储，真实行情、基础信息、日历和衍生数据应优先落到项目约定的 Parquet 数据集。
- DuckDB 用于本地查询、视图构建和运行元数据，不应替代 Parquet 成为事实主存储。
- Registry 使用 `catalog.json`、`inventory.parquet`、`events.jsonl` 记录数据集、文件清单和事件流水；涉及 Registry 的改动必须保持这三类文件的职责清晰。
- Registry 元数据也属于生产数据资产，`catalog.json`、`inventory.parquet`、`events.jsonl` 不应被当作临时缓存随意删除、重建或覆盖。
- Baostock 与 AkShare 数据路径应保持分离，不要把两套 provider 的 code 格式、dataset id 或落盘目录混在一起。
- 涉及 Baostock 与 AkShare 的 code 转换、join、分区路径或 dataset catalog 时，必须明确 `code_format`，避免 `sh.600000` 与 `600000` 等格式混用。
- 股票类日线数据优先按 `code` 分区，除非用户明确批准，不得改成纯时间分区或其他大范围分区策略。
- 当前项目未接入 MongoDB。后续 Codex 不得自行引入 MongoDB；如确需讨论，只能作为独立方案先征得用户批准。

# 数据分层原则

- ODS、raw、dwd 的边界尚未完全最终确定；任何分层调整都必须先给出设计计划，再做小样本验证。
- 在没有用户明确批准前，不要大规模迁移历史数据，不要批量重写既有 Parquet。
- ODS 层如需保留原始字段，应先设计 raw/ods landing 方案，明确目录、schema、字段保留规则和回滚方式，并使用测试目录或小样本验证。
- 不得随意丢弃、重命名或改变源数据字段语义；如确需规范化字段，应保留来源字段映射并说明影响范围。
- 日期字段优先使用 PyArrow `date32`；时间戳字段应明确单位和时区语义。
- schema 应优先集中在 `src/storage/schema.py` 管理，新增或调整数据集字段时先检查现有 schema、catalog 和 writer 是否一致。

# 写入安全规则

- 不得删除、清空、覆盖真实 `data` 目录。
- 任何涉及真实数据写入、修复、迁移、批量覆盖的任务，必须先提供 dry-run 或测试目录方案。
- 优先使用 `--dry-run`、`--root`、`--output-root`、`--max-codes`、`--max-tasks` 等安全参数缩小影响范围。
- 如果相关 CLI 暂时没有 dry-run 或测试 root 参数，应先建议补安全参数，而不是直接运行生产写入。
- Parquet 写入应继续使用临时文件 + 原子替换模式，避免半写入文件污染数据集。
- 涉及数据覆盖时，必须说明影响的数据集、代码范围、日期范围、文件范围和回滚方式。
- 对 Registry 元数据进行重建、压缩、归档、修复或迁移时，也必须 dry-run 优先，使用测试目录或备份，说明影响范围和回滚方式。
- 修改 DuckDB view、metadata flush 或构建流程时，应使用 `try/finally`、事务或可恢复流程，避免 Parquet 已写入但视图、inventory 或事件状态不一致。

# Codex 工作流程

1. 先审计，再计划，再修改。
2. 每次只处理一个明确目标。
3. 修改前说明计划，并说明将检查哪些文件以及检查原因。
4. 修改后说明改了哪些文件、为什么改、如何测试、风险和回滚方式。
5. 不做顺手重构。
6. 不引入大型框架。
7. 不改变公共接口，除非用户明确批准。
8. 不自动格式化全项目。
9. 不把多个无关优化混在一个提交里。

执行任务时应优先使用 `rg` 查找代码和文档；修改文件时保持范围最小。涉及数据写入、目录迁移、schema 变更、分区策略、CLI 参数或 provider 行为时，应先输出计划并等待用户认可，除非用户已经明确要求直接实施。

# 优先优化事项

1. 给写数据的 CLI 补统一 dry-run / test root / max-codes / max-tasks 安全护栏。
2. 修复 update-akshare-spot-quote 深交所退市代码未过滤问题。
3. 让 Baostock retry 真正使用配置。
4. 增加 pipeline 运行摘要，包括成功、失败、跳过和失败代码清单。
5. 为 store.close、metadata flush、DuckDB build views 增加 try/finally。
6. 澄清 raw/ods/dwd 分层。
7. 为 events.jsonl 增加 checkpoint、归档或 compaction 方案。
8. 增加 /v1/health 或等效健康检查。
9. 增加 Windows 友好的测试和运行命令。
10. 锁定依赖或补充 dev extra。

# 已知风险

- AkShare spot 写 daily bar 时可能未过滤深交所退市代码。
- Baostock retry 配置当前可能未真正生效。
- 核心更新命令缺少统一 dry-run。
- `events.jsonl` 长期线性增长。
- ODS 原始字段保留策略尚未完全落地。
- `run_update_daily.bat` 偏生产直跑。
- DuckDB view / metadata 构建失败可能导致查询视图、inventory 和事件流水状态不一致。
- Baostock 与 AkShare code 格式混用可能导致 join 错配、分区路径错误或 dataset catalog 声明失真。
- 依赖未锁定可能导致 Windows 环境复现失败或测试结果漂移。
- 测试环境存在超时风险；此前 `pytest --collect-only` 曾出现 120 秒超时。

# 测试要求

- 修改 Python 代码后至少运行相关 `pytest`，优先选择与改动文件直接相关的测试。
- 涉及 CLI 时先做小样本 smoke test，并使用受控参数限制代码数量、任务数量或输出目录。
- 涉及数据写入时必须使用测试目录或 dry-run，不得直接拿真实 `data` 目录做首次验证。
- 如果测试超时、依赖缺失或环境原因导致无法运行，必须明确说明，不能声称测试通过。
- 如果 `pytest --collect-only` 或其他测试命令超时、被中断或未完整执行，也不能声称测试通过。
- 不得把“未执行测试”写成“测试通过”。

# Windows 环境

- 项目运行环境以 Windows + PowerShell 为主。
- 路径处理应使用 `pathlib`，避免手写路径分隔符。
- 不要写只适用于 Linux/macOS 的命令、脚本或文档步骤；必要时提供 PowerShell 写法。
- 批处理脚本改动应保持 Windows 可用，并避免破坏现有 `venv\Scripts\activate.bat` 流程。

# 禁止事项

- 禁止直接删除真实数据。
- 禁止未经批准迁移历史 Parquet。
- 禁止未经批准改变分区策略。
- 禁止未经批准引入 MongoDB、Airflow、Prefect、Spark、Dask 等重型依赖。
- 禁止未经批准把所有脚本改成面向对象。
- 禁止未经批准重写整个项目。
- 禁止只为风格统一而大规模改动无关文件。
