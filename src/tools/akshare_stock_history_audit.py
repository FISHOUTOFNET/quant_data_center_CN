"""Audit historical stock APIs from the AkShare stock documentation."""

from __future__ import annotations

import argparse
import ast
import html
import re
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast
from urllib.request import Request, urlopen

import pandas as pd

SOURCE_URL = "https://akshare.akfamily.xyz/data/stock/stock.html"
DEFAULT_OUTPUT = Path("docs/akshare_stock_history_api_accessibility.md")
DEFAULT_START_DATE = "19900101"

HISTORY_KEYWORDS = (
    "历史",
    "日线",
    "周线",
    "月线",
    "分钟",
    "分时",
    "tick",
    "财务",
    "分红",
    "配股",
    "股东",
    "十大流通",
    "龙虎榜",
    "融资融券",
    "报告期",
    "公告",
    "业绩",
    "估值",
    "市盈率",
    "市净率",
    "交易明细",
)
CURRENT_ONLY_KEYWORDS = (
    "实时",
    "现货",
    "快照",
    "当前",
    "当日",
    "列表",
    "字典",
    "名录",
    "代码表",
    "代码表",
    "目录",
    "查询",
)
STRONG_HISTORY_KEYWORDS = ("历史", "日线", "周线", "月线", "分时", "财务", "报告期", "公告")
TITLE_HISTORY_KEYWORDS = (
    "历史",
    "日线",
    "周线",
    "月线",
    "分钟",
    "分时",
    "日内",
    "盘前",
    "财务",
    "分红",
    "配股",
    "股东",
    "龙虎榜",
    "融资融券",
    "报告",
    "公告",
    "业绩",
    "估值",
    "商誉",
    "质押",
    "资金流",
    "停复牌",
    "股本变动",
    "主营构成",
    "机构调研",
    "增减持",
)

START_DATE_KEYS = {"start_date", "begin_date", "start", "start_year", "from_date"}
END_DATE_KEYS = {"end_date", "end", "date", "to_date"}
SYMBOL_KEYS = {"symbol", "stock", "code", "stock_code"}


@dataclass(frozen=True)
class ApiCandidate:
    endpoint: str
    title: str
    sample_call: str
    sample_kwargs: dict[str, object]
    section_text: str


@dataclass(frozen=True)
class ProbeResult:
    candidate: ApiCandidate
    status: str
    earliest_time: str | None
    row_count: int
    rounds_attempted: int
    params: dict[str, object]
    last_error: str


def fetch_html(url: str, timeout_seconds: float = 30) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
            )
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        raw = response.read()
    return raw.decode("utf-8", errors="replace")


def discover_candidates(page_html: str) -> list[ApiCandidate]:
    sections = _html_sections(page_html)
    candidates: list[ApiCandidate] = []
    seen: set[str] = set()
    for title, section_html in sections:
        section_text = _visible_text(section_html).strip()
        for match in re.finditer(r"接口\s*[:\uff1a]\s*(stock_[a-zA-Z0-9_]+)", section_text):
            endpoint = match.group(1)
            if endpoint in seen:
                continue
            sample_call = _extract_sample_call(section_text, endpoint)
            sample_kwargs = _extract_sample_kwargs(sample_call)
            if not sample_kwargs:
                sample_kwargs = _extract_documented_defaults(section_text)
            if not _is_history_candidate(title, section_text, sample_kwargs):
                continue
            candidates.append(
                ApiCandidate(
                    endpoint=endpoint,
                    title=title or _section_title(section_text, endpoint),
                    sample_call=sample_call,
                    sample_kwargs=sample_kwargs,
                    section_text=section_text,
                )
            )
            seen.add(endpoint)
    return candidates


def run_probe_rounds(
    candidates: Sequence[ApiCandidate],
    *,
    caller: Callable[[ApiCandidate, dict[str, object]], object],
    rounds: int = 3,
    today: date | None = None,
    sleep_between_rounds: float = 0,
    workers: int = 1,
    on_progress: Callable[[ProbeResult], None] | None = None,
) -> list[ProbeResult]:
    if rounds < 1:
        raise ValueError("rounds must be >= 1")
    today = today or date.today()
    unresolved = list(candidates)
    results_by_endpoint: dict[str, ProbeResult] = {}

    for round_number in range(1, rounds + 1):
        next_unresolved: list[ApiCandidate] = []
        round_results = _run_probe_round(
            unresolved,
            caller=caller,
            round_number=round_number,
            final_round=round_number == rounds,
            today=today,
            workers=workers,
        )
        for result in round_results:
            results_by_endpoint[result.candidate.endpoint] = result
            if on_progress is not None:
                on_progress(result)
            if result.status != "accessible":
                next_unresolved.append(result.candidate)
        unresolved = next_unresolved
        if not unresolved:
            break
        if sleep_between_rounds > 0 and round_number < rounds:
            time.sleep(sleep_between_rounds)

    return [results_by_endpoint[candidate.endpoint] for candidate in candidates]


def _run_probe_round(
    candidates: Sequence[ApiCandidate],
    *,
    caller: Callable[[ApiCandidate, dict[str, object]], object],
    round_number: int,
    final_round: bool,
    today: date,
    workers: int,
) -> list[ProbeResult]:
    if workers <= 1 or len(candidates) <= 1:
        return [
            _probe_candidate(candidate, caller=caller, round_number=round_number, final_round=final_round, today=today)
            for candidate in candidates
        ]
    results: list[ProbeResult] = []
    with ThreadPoolExecutor(max_workers=max(workers, 1), thread_name_prefix="akshare-audit-round") as executor:
        futures = {
            executor.submit(
                _probe_candidate,
                candidate,
                caller=caller,
                round_number=round_number,
                final_round=final_round,
                today=today,
            ): candidate
            for candidate in candidates
        }
        for future in as_completed(futures):
            results.append(future.result())
    result_by_endpoint = {result.candidate.endpoint: result for result in results}
    return [result_by_endpoint[candidate.endpoint] for candidate in candidates]


def _probe_candidate(
    candidate: ApiCandidate,
    *,
    caller: Callable[[ApiCandidate, dict[str, object]], object],
    round_number: int,
    final_round: bool,
    today: date,
) -> ProbeResult:
    params = build_call_kwargs(candidate, today)
    try:
        data = _as_dataframe(caller(candidate, params))
        if data.empty:
            return ProbeResult(
                candidate=candidate,
                status="empty",
                earliest_time=None,
                row_count=0,
                rounds_attempted=round_number,
                params=params,
                last_error="returned empty data",
            )
        return ProbeResult(
            candidate=candidate,
            status="accessible",
            earliest_time=earliest_observed_time(data),
            row_count=len(data),
            rounds_attempted=round_number,
            params=params,
            last_error="",
        )
    except Exception as exc:
        return ProbeResult(
            candidate=candidate,
            status="inaccessible" if final_round else "error",
            earliest_time=None,
            row_count=0,
            rounds_attempted=round_number,
            params=params,
            last_error=_error_summary(exc),
        )


def build_call_kwargs(candidate: ApiCandidate, today: date) -> dict[str, object]:
    kwargs = dict(candidate.sample_kwargs)
    today_compact = today.strftime("%Y%m%d")
    has_start = any(key in kwargs for key in START_DATE_KEYS)
    for key in list(kwargs):
        lowered = key.lower()
        if lowered == "start_year":
            kwargs[key] = "1990"
        elif lowered in START_DATE_KEYS:
            kwargs[key] = DEFAULT_START_DATE
        elif lowered in {"end_date", "end", "to_date"} and has_start:
            kwargs[key] = today_compact
    for key in SYMBOL_KEYS:
        if key in kwargs and _is_blank(kwargs[key]):
            kwargs[key] = "000001"
    return kwargs


def earliest_observed_time(data: pd.DataFrame) -> str | None:
    dates: list[pd.Timestamp] = []
    for column in data.columns:
        dates.extend(_parse_date_values(data[column]))
    if data.index.size:
        dates.extend(_parse_date_values(pd.Series(data.index)))
    if not dates:
        return None
    return min(dates).date().isoformat()


def render_markdown_report(
    results: Sequence[ProbeResult],
    *,
    generated_at: datetime,
    akshare_version: str,
    source_url: str = SOURCE_URL,
) -> str:
    accessible = sum(1 for item in results if item.status == "accessible")
    empty = sum(1 for item in results if item.status == "empty")
    inaccessible = len(results) - accessible - empty
    lines = [
        "# AkShare 股票历史 API 可访问性报告",
        "",
        f"- 生成时间: `{generated_at.isoformat(timespec='seconds')}`",
        f"- AkShare 版本: `{akshare_version}`",
        f"- 文档来源: {source_url}",
        f"- 候选接口数: `{len(results)}`",
        f"- 可访问: `{accessible}`",
        f"- 空数据: `{empty}`",
        f"- 不可访问: `{inaccessible}`",
        "",
        "本文件由 `scripts/audit_akshare_stock_history_apis.py` 生成。再次运行会覆盖并更新为最新探测状态。",
        "",
        "| 接口 | 标题 | 状态 | 最早观测时间 | 行数 | 轮次 | 参数 | 最后错误 |",
        "| --- | --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for result in results:
        lines.append(
            "| "
            + " | ".join(
                [
                    _md_cell(f"`{result.candidate.endpoint}`"),
                    _md_cell(result.candidate.title),
                    _md_cell(result.status),
                    _md_cell(result.earliest_time or ""),
                    str(result.row_count),
                    str(result.rounds_attempted),
                    _md_cell(_format_params(result.params)),
                    _md_cell(result.last_error),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def write_markdown_report(
    output: Path,
    results: Sequence[ProbeResult],
    *,
    generated_at: datetime,
    akshare_version: str,
    source_url: str = SOURCE_URL,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        render_markdown_report(
            results,
            generated_at=generated_at,
            akshare_version=akshare_version,
            source_url=source_url,
        ),
        encoding="utf-8",
    )


def default_akshare_caller(timeout_seconds: float) -> Callable[[ApiCandidate, dict[str, object]], pd.DataFrame]:
    import akshare as ak  # type: ignore

    def call(candidate: ApiCandidate, kwargs: dict[str, object]) -> pd.DataFrame:
        func = getattr(ak, candidate.endpoint)
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"audit-{candidate.endpoint}")
        future = executor.submit(func, **kwargs)
        try:
            return _as_dataframe(future.result(timeout=timeout_seconds))
        except FutureTimeoutError as exc:
            future.cancel()
            raise TimeoutError(f"{candidate.endpoint} timed out after {timeout_seconds:g}s") from exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    return call


def main(
    argv: Sequence[str] | None = None,
    *,
    fetch_html: Callable[[str], str] | None = None,
    caller: Callable[[ApiCandidate, dict[str, object]], object] | None = None,
    now: Callable[[], datetime] | None = None,
) -> int:
    parser = argparse.ArgumentParser(description="Audit AkShare historical stock API accessibility.")
    parser.add_argument("--source-url", default=SOURCE_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=60)
    parser.add_argument("--sleep-between-rounds", type=float, default=5)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args(argv)

    current_time = (now or datetime.now)()
    html_text = (fetch_html or (lambda url: globals()["fetch_html"](url, args.timeout_seconds)))(args.source_url)
    candidates = discover_candidates(html_text)
    probe_caller = caller or default_akshare_caller(args.timeout_seconds)
    results = run_probe_rounds(
        candidates,
        caller=probe_caller,
        rounds=args.rounds,
        today=current_time.date(),
        sleep_between_rounds=args.sleep_between_rounds,
        workers=args.workers,
        on_progress=lambda result: print(
            f"round={result.rounds_attempted} endpoint={result.candidate.endpoint} status={result.status}"
        ),
    )
    write_markdown_report(
        args.output,
        results,
        generated_at=current_time,
        akshare_version=_akshare_version(),
        source_url=args.source_url,
    )
    print(f"Wrote {args.output} with {len(results)} candidate endpoints")
    return 0


def _visible_text(page_html: str) -> str:
    text = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", page_html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|h[1-6]|pre|li|tr)>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"[ \t\r\f\v]+", " ", text)


def _html_sections(page_html: str) -> list[tuple[str, str]]:
    heading_pattern = re.compile(r"(?is)<h[1-6][^>]*>(.*?)</h[1-6]>")
    matches = list(heading_pattern.finditer(page_html))
    if not matches:
        return [("", page_html)]
    sections: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(page_html)
        title = _clean_title(_visible_text(match.group(1)).strip())
        sections.append((title, page_html[match.start() : end]))
    return sections


def _section_title(section_text: str, endpoint: str) -> str:
    for line in section_text.splitlines():
        cleaned = _clean_title(line.strip(" #\t"))
        if cleaned and endpoint not in cleaned and "接口" not in cleaned:
            return cleaned[:80]
    return endpoint


def _clean_title(value: str) -> str:
    return value.replace("\uf0c1", "").strip()


def _is_history_candidate(title: str, section_text: str, sample_kwargs: dict[str, object]) -> bool:
    title_lower = title.lower()
    combined = f"{title}\n{section_text}".lower()
    title_has_history = any(keyword.lower() in title_lower for keyword in TITLE_HISTORY_KEYWORDS)
    has_temporal_param = any(_is_temporal_param(key) for key in sample_kwargs)
    has_history = (
        title_has_history or has_temporal_param or any(keyword.lower() in combined for keyword in HISTORY_KEYWORDS)
    )
    if not has_history:
        return False
    title_has_current = any(keyword.lower() in title_lower for keyword in CURRENT_ONLY_KEYWORDS)
    if title_has_current and not title_has_history:
        return False
    has_current = any(keyword.lower() in combined for keyword in CURRENT_ONLY_KEYWORDS)
    has_strong_history = any(keyword.lower() in title_lower for keyword in STRONG_HISTORY_KEYWORDS)
    return not has_current or has_strong_history


def _is_temporal_param(key: str) -> bool:
    lowered = key.lower()
    return lowered in {*START_DATE_KEYS, *END_DATE_KEYS, "year", "start_year", "end_year", "period", "report_date"}


def _extract_sample_call(section_text: str, endpoint: str) -> str:
    pattern = rf"ak\.{re.escape(endpoint)}\((.*?)\)"
    match = re.search(pattern, section_text, flags=re.S)
    if not match:
        return ""
    return f"ak.{endpoint}({match.group(1).strip()})"


def _extract_sample_kwargs(sample_call: str) -> dict[str, object]:
    if not sample_call:
        return {}
    try:
        expression = ast.parse(sample_call, mode="eval").body
    except SyntaxError:
        return {}
    if not isinstance(expression, ast.Call):
        return {}
    kwargs: dict[str, object] = {}
    for keyword in expression.keywords:
        if keyword.arg is None:
            continue
        try:
            kwargs[keyword.arg] = ast.literal_eval(keyword.value)
        except (ValueError, SyntaxError):
            continue
    return kwargs


def _extract_documented_defaults(section_text: str) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    defaults_pattern = re.compile(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b\s+"
        r"(?:str|int|float|bool|list|dict)\s+"
        r".{0,160}?"
        r"\b\1\s*=\s*(['\"])(.*?)\2",
        flags=re.S,
    )
    for match in defaults_pattern.finditer(section_text):
        kwargs.setdefault(match.group(1), match.group(3))
    return kwargs


def _as_dataframe(value: object) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if isinstance(value, dict):
        return pd.DataFrame(value)
    if isinstance(value, Sequence):
        return pd.DataFrame(value)
    return pd.DataFrame(cast(Any, value))


def _parse_date_values(values: pd.Series) -> list[pd.Timestamp]:
    parsed: list[pd.Timestamp] = []
    for value in values.dropna().head(5000):
        normalized = _normalize_date_value(value)
        if not normalized:
            continue
        timestamp = pd.to_datetime(normalized, errors="coerce")
        if not pd.isna(timestamp) and 1900 <= pd.Timestamp(timestamp).year <= 2100:
            parsed.append(pd.Timestamp(timestamp))
    return parsed


def _normalize_date_value(value: object) -> str:
    if isinstance(value, pd.Timestamp | datetime | date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return ""
    quarter = re.fullmatch(r"(\d{4})Q([1-4])", text, flags=re.I)
    if quarter:
        month = {"1": "03", "2": "06", "3": "09", "4": "12"}[quarter.group(2)]
        return f"{quarter.group(1)}-{month}-01"
    compact = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", text)
    if compact:
        return f"{compact.group(1)}-{compact.group(2)}-{compact.group(3)}"
    chinese = re.fullmatch(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if chinese:
        return f"{chinese.group(1)}-{int(chinese.group(2)):02d}-{int(chinese.group(3)):02d}"
    if re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?$", text):
        return text
    return ""


def _format_params(params: dict[str, object]) -> str:
    if not params:
        return ""
    return ", ".join(f"{key}={value!r}" for key, value in sorted(params.items()))


def _md_cell(value: str) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").strip()


def _error_summary(exc: Exception) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}"


def _is_blank(value: object) -> bool:
    return str(value).strip() == ""


def _akshare_version() -> str:
    try:
        import akshare as ak  # type: ignore
    except ModuleNotFoundError:
        return "not installed"
    return str(getattr(ak, "__version__", "unknown"))


if __name__ == "__main__":
    raise SystemExit(main())
