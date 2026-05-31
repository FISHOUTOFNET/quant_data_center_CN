# ADR 0002: AkShare Update Module Interface

## Status

Accepted

## Date

2026-05-22

## Context

AkShare 更新入口原本按命令拆分：估值、退市、现货快照和日线各自维护任务规划、checkpoint 跳过、fetch、write、生命周期记录和 progress logging。理解一个 Dataset 的更新行为需要在 pipeline 入口、任务规划 helper 和数据集私有 helper 之间跳转。

这种形状让 Module 偏浅：公开入口没有提供足够 leverage，而实现细节在多个调用路径重复出现。新增 AkShare Dataset 时容易复制调度和结果记录代码。

## Decision

采用一个破坏式 AkShare CLI 入口：

```text
qdc akshare update --target valuation|daily_bar|spot_quote|delist|all
```

旧的 `update-akshare-valuation`、`update-akshare-delist`、`update-akshare-spot-quote` 和 `update-akshare-daily-bar` 不保留 alias。

内部采用执行内核加 Dataset Module：

- 执行内核只拥有 store/client/lifecycle 创建、并发 fetch 调度、progress 计数和收尾。
- 每类 AkShare Dataset Module 拥有自己的 planning、prefilter、fetch、write 和 result record。
- 新增 AkShare Dataset 时通过注册 Dataset Module 接入，不在执行内核增加 Dataset 分支。

## Consequences

正向影响：

- Dataset 更新行为集中在单条语义路径，locality 更好。
- 任务执行的并发、生命周期和 progress 规则只实现一次，调用方获得更高 leverage。
- CLI 和 Python 入口减少，旧入口错误会尽早暴露。

代价：

- 下游脚本必须迁移到 `qdc akshare update`。
- 旧 Python 入口不再作为兼容 wrapper 维护。
