# AkShare Stock History API Audit Design

## Goal

Build a standalone helper program that discovers historical stock data APIs from the AkShare stock documentation page, checks their accessibility with three separated retry rounds, finds the earliest date-like value returned by each accessible API, and regenerates a Markdown report for future AkShare feature expansion.

## Scope

The program targets the AkShare stock page at `https://akshare.akfamily.xyz/data/stock/stock.html`.

Included APIs are stock APIs that expose historical or time-series data, including daily bars, intraday history, financial history, shareholder history, margin history, dividend/allotment history, announcement history, and similar report-period data.

Excluded APIs are real-time quotes, current snapshots, symbol lists, static dictionaries, and APIs whose documented behavior only returns current state.

## Architecture

The helper will live outside the production ingestion pipeline. A reusable module under `src/tools/` will contain parsing, classification, probing, retry orchestration, earliest-date extraction, and report rendering. A small script under `scripts/` will provide a direct command-line entry point.

The tool will not write Parquet data or mutate existing pipeline metadata. Its only default output is a regenerated Markdown document at `docs/akshare_stock_history_api_accessibility.md`.

## Discovery

The discovery layer downloads the AkShare stock documentation page, splits it into endpoint sections, extracts endpoint function names from headings such as `接口: stock_zh_a_hist`, and extracts a sample `ak.<function>(...)` call from code examples when present.

Classification uses conservative keywords. It includes sections that contain history-oriented terms such as `历史`, `日线`, `分时`, `财务`, `分红`, `股东`, `龙虎榜`, `融资融券`, `报告期`, `公告`, `业绩`, `指数历史`, and excludes sections with real-time/current/list-only terms such as `实时`, `现货`, `行情快照`, `当前`, `列表`, and `代码表` unless a stronger history keyword is present.

## Probing

Each candidate is converted into a call specification:

- Start-date parameters such as `start_date`, `begin_date`, and `start` are widened to `19900101`.
- End-date parameters such as `end_date`, `end`, and `date` are set to the current date when the parameter pair clearly represents a date window.
- Common stock symbol parameters use `000001` or the documented sample value.
- Parameters already present in a documentation sample are preserved unless they are date-window parameters that should be widened.

The program calls AkShare through the installed `akshare` package with per-call timeout protection. A result is accessible when the function returns a non-empty DataFrame-like result. Empty data is reported separately from errors.

## Retry Policy

The audit runs by rounds, not by immediate per-endpoint retry. Round 1 checks every candidate. Round 2 checks only endpoints that did not return accessible data in round 1. Round 3 checks only endpoints still unresolved after round 2. After round 3, unresolved endpoints are reported as inaccessible for this run.

This matches the requirement to avoid repeatedly hitting the same endpoint back-to-back.

## Earliest Date

For accessible results, the tool scans columns and index values for date-like values. It reports the minimum parsed date as the endpoint's earliest observed available time. For period-only data, it also recognizes compact forms such as `YYYYMMDD`, `YYYY-MM-DD`, `YYYYQn`, and `YYYY年MM月DD日` where pandas can parse or the helper can normalize them.

The report labels this as earliest observed time from the audit call, because some AkShare endpoints expose history through year/report-period parameters rather than a single full-history date window.

## Report

The generated Markdown report includes:

- Generation timestamp.
- AkShare version.
- Source documentation URL.
- Candidate counts and accessibility counts.
- A table of every candidate endpoint with status, earliest observed time, row count, attempted rounds, parameters, and last error summary.

The file is overwritten on every run so later executions update the report to the latest status.

## Testing

Unit tests cover:

- Documentation parsing and history/current classification.
- Round-based retry behavior.
- Earliest observed date extraction from DataFrame columns and indexes.
- Markdown report rendering and overwrite behavior.

Network calls are not used in unit tests; tests inject fake HTML and fake endpoint callers.
