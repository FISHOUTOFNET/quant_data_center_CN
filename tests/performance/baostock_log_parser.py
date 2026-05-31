from __future__ import annotations

import argparse
import glob
import json
import re
from collections import defaultdict, deque
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from statistics import mean

LOG_LINE_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) .+ - (?P<msg>.*)$")
RUN_START_RE = re.compile(r"Daily update started run_id=(?P<run_id>\S+).+ dataset=(?P<dataset>\S+) mode=(?P<mode>\S+)")
RUN_DONE_RE = re.compile(
    r"Daily update completed run_id=(?P<run_id>\S+).+ records=(?P<records>\d+) "
    r"success=(?P<success>\d+) failed=(?P<failed>\d+) skipped=(?P<skipped>\d+)"
)
API_RE = re.compile(
    r"Baostock API (?P<event>started|completed) run_id=(?P<run_id>\S+).+ action=(?P<action>\S+)(?: code=(?P<code>\S+))?"
)
PARQUET_RE = re.compile(r"(?:Daily|Dataset) Parquet stored run_id=(?P<run_id>\S+)")
FLUSH_RE = re.compile(r"Pipeline metadata flush completed run_id=(?P<run_id>\S+).+ elapsed=(?P<elapsed>[0-9.]+)s")
FAIL_CODE_RE = re.compile(r"(?:Daily bar|Adjust factor) API failed for (?P<code>\S+)")


def analyze_lines(
    lines: Iterable[str],
    *,
    now: datetime | None = None,
    silent_gap_seconds: float = 120.0,
) -> dict[str, object]:
    starts: dict[str, datetime] = {}
    ends: dict[str, datetime] = {}
    last_log_at: dict[str, datetime] = {}
    runs: dict[str, dict[str, object]] = {}
    open_api: dict[tuple[str, str, str], deque[datetime]] = defaultdict(deque)
    duplicate_open_api_starts: list[dict[str, object]] = []
    unmatched_api_starts: list[dict[str, object]] = []
    session_error_codes: list[str] = []
    last_failed_code: str | None = None
    api_durations: dict[str, list[float]] = defaultdict(list)
    current_run_id: str | None = None

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        parsed = _parse_line(line)
        if parsed is None:
            if "用户未登录" in line and last_failed_code is not None:
                session_error_codes.append(last_failed_code)
            continue
        ts, msg = parsed

        if match := RUN_START_RE.search(msg):
            run_id = match["run_id"]
            starts[run_id] = ts
            current_run_id = run_id
            last_log_at[run_id] = ts
            runs.setdefault(run_id, _new_run_summary(run_id))
            runs[run_id].update(
                {
                    "dataset": match["dataset"],
                    "mode": match["mode"],
                    "start_time": _format_ts(ts),
                    "completed": False,
                }
            )
            continue

        run_id = _run_id_from_msg(msg)
        if run_id is not None:
            runs.setdefault(run_id, _new_run_summary(run_id))
            last_log_at[run_id] = ts

        if match := RUN_DONE_RE.search(msg):
            done_run_id = match["run_id"]
            ends[done_run_id] = ts
            runs.setdefault(done_run_id, _new_run_summary(done_run_id))
            runs[done_run_id].update(
                {
                    "completed": True,
                    "end_time": _format_ts(ts),
                    "records": int(match["records"]),
                    "success": int(match["success"]),
                    "failed": int(match["failed"]),
                    "skipped": int(match["skipped"]),
                }
            )
            if done_run_id in starts:
                runs[done_run_id]["duration_seconds"] = round((ts - starts[done_run_id]).total_seconds(), 3)
            if current_run_id == done_run_id:
                current_run_id = None
            continue

        if match := API_RE.search(msg):
            event = match["event"]
            api_run_id = match["run_id"]
            action = match["action"]
            code = match["code"] or ""
            key = (api_run_id, action, code)
            runs.setdefault(api_run_id, _new_run_summary(api_run_id))
            if event == "started":
                if open_api[key]:
                    duplicate_open_api_starts.append(_api_event(api_run_id, action, code, ts))
                open_api[key].append(ts)
            else:
                if open_api[key]:
                    started = open_api[key].popleft()
                    api_durations[api_run_id].append((ts - started).total_seconds())
            continue

        if match := PARQUET_RE.search(msg):
            runs.setdefault(match["run_id"], _new_run_summary(match["run_id"]))
            runs[match["run_id"]]["parquet_write_count"] += 1
            continue

        if match := FLUSH_RE.search(msg):
            flush_run_id = match["run_id"]
            runs.setdefault(flush_run_id, _new_run_summary(flush_run_id))
            runs[flush_run_id]["metadata_flush_count"] += 1
            runs[flush_run_id]["metadata_flush_total_seconds"] = round(
                float(runs[flush_run_id]["metadata_flush_total_seconds"]) + float(match["elapsed"]),
                3,
            )
            continue

        if "refetching from 1990-01-01" in msg:
            refetch_run_id = run_id or current_run_id
            if refetch_run_id is not None:
                runs[refetch_run_id]["full_refetch_count"] += 1
                last_log_at[refetch_run_id] = ts
            continue

        if match := FAIL_CODE_RE.search(msg):
            last_failed_code = match["code"]

    for run_id, durations in api_durations.items():
        runs.setdefault(run_id, _new_run_summary(run_id))
        runs[run_id].update(_api_summary(durations))

    for (run_id, action, code), queued in open_api.items():
        for ts in queued:
            unmatched_api_starts.append(_api_event(run_id, action, code, ts))

    unfinished_runs = [run_id for run_id in starts if run_id not in ends]
    overlapping_runs = _overlapping_runs(starts, ends)
    silent_runs = _silent_runs(unfinished_runs, last_log_at, now, silent_gap_seconds)

    return {
        "run_count": len(starts),
        "runs": runs,
        "unfinished_runs": unfinished_runs,
        "overlapping_runs": overlapping_runs,
        "duplicate_open_api_starts": duplicate_open_api_starts,
        "unmatched_api_starts": unmatched_api_starts,
        "session_error_count": len(session_error_codes),
        "session_error_codes": sorted(dict.fromkeys(session_error_codes)),
        "silent_runs": silent_runs,
    }


def _parse_line(line: str) -> tuple[datetime, str] | None:
    match = LOG_LINE_RE.match(line)
    if match is None:
        return None
    return datetime.strptime(match["ts"], "%Y-%m-%d %H:%M:%S.%f"), match["msg"]


def _new_run_summary(run_id: str) -> dict[str, object]:
    return {
        "run_id": run_id,
        "dataset": None,
        "mode": None,
        "completed": False,
        "api_call_count": 0,
        "api_total_seconds": 0.0,
        "api_avg_seconds": 0.0,
        "api_p95_seconds": 0.0,
        "api_max_seconds": 0.0,
        "parquet_write_count": 0,
        "metadata_flush_count": 0,
        "metadata_flush_total_seconds": 0.0,
        "full_refetch_count": 0,
    }


def _run_id_from_msg(msg: str) -> str | None:
    match = re.search(r"run_id=(?P<run_id>\S+)", msg)
    return match["run_id"] if match else None


def _api_summary(durations: list[float]) -> dict[str, object]:
    ordered = sorted(durations)
    return {
        "api_call_count": len(ordered),
        "api_total_seconds": round(sum(ordered), 3),
        "api_avg_seconds": round(mean(ordered), 3) if ordered else 0.0,
        "api_p95_seconds": round(_percentile(ordered, 0.95), 3) if ordered else 0.0,
        "api_max_seconds": round(max(ordered), 3) if ordered else 0.0,
    }


def _percentile(ordered: list[float], q: float) -> float:
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _api_event(run_id: str, action: str, code: str, ts: datetime) -> dict[str, object]:
    return {"run_id": run_id, "action": action, "code": code, "time": _format_ts(ts)}


def _overlapping_runs(starts: dict[str, datetime], ends: dict[str, datetime]) -> list[dict[str, str]]:
    ordered = sorted(starts.items(), key=lambda item: item[1])
    overlaps: list[dict[str, str]] = []
    for index, (run_id, _started) in enumerate(ordered[:-1]):
        next_run_id, next_started = ordered[index + 1]
        if run_id not in ends or ends[run_id] > next_started:
            overlaps.append({"previous_run_id": run_id, "next_run_id": next_run_id})
    return overlaps


def _silent_runs(
    unfinished_runs: list[str],
    last_log_at: dict[str, datetime],
    now: datetime | None,
    silent_gap_seconds: float,
) -> list[dict[str, object]]:
    if now is None:
        return []
    result: list[dict[str, object]] = []
    for run_id in unfinished_runs:
        if run_id not in last_log_at:
            continue
        seconds = round((now - last_log_at[run_id]).total_seconds(), 3)
        if seconds >= silent_gap_seconds:
            result.append({"run_id": run_id, "seconds_since_last_log": seconds})
    return result


def _format_ts(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="milliseconds")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze qdc update-baostock-daily logs.")
    parser.add_argument("paths", nargs="+", help="Log file paths or glob patterns.")
    parser.add_argument("--run-id", default=None, help="Limit JSON output to one run summary.")
    parser.add_argument("--silent-gap-seconds", type=float, default=120.0)
    args = parser.parse_args(argv)

    paths = _expand_paths(args.paths)
    lines: list[str] = []
    for path in paths:
        lines.extend(Path(path).read_text(encoding="utf-8", errors="replace").splitlines())
    report = analyze_lines(lines, now=datetime.now(), silent_gap_seconds=args.silent_gap_seconds)
    if args.run_id is not None:
        report = {**report, "runs": {args.run_id: report["runs"].get(args.run_id)}}
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _expand_paths(patterns: list[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        matched = glob.glob(pattern)
        paths.extend(matched or [pattern])
    return sorted(dict.fromkeys(paths))


if __name__ == "__main__":
    raise SystemExit(main())
