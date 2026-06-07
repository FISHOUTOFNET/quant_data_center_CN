"""Typed registry for AkShare dataset modules."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from src.sources.akshare.pipeline.execution_types import AkShareDatasetModule, AkShareUpdateRequest


@dataclass(frozen=True)
class AkShareModuleSpec:
    target: str
    factory: Callable[[], AkShareDatasetModule]
    order: int
    supports_adjustment: bool = False
    supports_period: bool = False
    supports_market: bool = False
    included_in_all: bool = True


def _build_specs() -> dict[str, AkShareModuleSpec]:
    from src.sources.akshare.cninfo.modules.report_disclosure import ReportDisclosureModule
    from src.sources.akshare.eastmoney.modules.capital_structure_em import CapitalStructureEmModule
    from src.sources.akshare.eastmoney.modules.daily_bar import DailyBarModule
    from src.sources.akshare.eastmoney.modules.valuation_eastmoney import ValuationEastmoneyModule
    from src.sources.akshare.eastmoney.modules.yjyg_em import YjygEmModule
    from src.sources.akshare.eastmoney.modules.yysj_em import YysjEmModule
    from src.sources.akshare.exchange.modules.delist import DelistModule
    from src.sources.akshare.pipeline.spot_quote import SpotQuoteModule
    from src.sources.akshare.sina.modules.financial_report_sina import FinancialReportSinaModule

    specs = [
        AkShareModuleSpec("valuation", ValuationEastmoneyModule, 10),
        AkShareModuleSpec("capital_structure", CapitalStructureEmModule, 20),
        AkShareModuleSpec("delist", DelistModule, 30, supports_market=True),
        AkShareModuleSpec("spot_quote", SpotQuoteModule, 40),
        AkShareModuleSpec(
            "report_disclosure",
            ReportDisclosureModule,
            50,
            supports_period=True,
            supports_market=True,
        ),
        AkShareModuleSpec("yysj_em", YysjEmModule, 60, supports_period=True, supports_market=True),
        AkShareModuleSpec("yjyg_em", YjygEmModule, 70, supports_period=True),
        AkShareModuleSpec("financial_report", FinancialReportSinaModule, 80),
        AkShareModuleSpec("daily_bar", DailyBarModule, 90, supports_adjustment=True),
    ]
    return {spec.target: spec for spec in specs}


def module_specs() -> dict[str, AkShareModuleSpec]:
    """Return AkShare module specs keyed by target."""

    return _build_specs()


def _ordered_specs() -> list[AkShareModuleSpec]:
    return sorted(module_specs().values(), key=lambda spec: spec.order)


def target_choices(include_all: bool = True) -> list[str]:
    """Return CLI target choices in registry display order."""

    choices = [spec.target for spec in _ordered_specs()]
    if include_all:
        choices.append("all")
    return choices


def modules_for_target(target: str) -> Iterable[AkShareDatasetModule]:
    """Instantiate modules for one target or the all target."""

    specs = module_specs()
    if target == "all":
        return [spec.factory() for spec in _ordered_specs() if spec.included_in_all]
    try:
        return [specs[target].factory()]
    except KeyError as exc:
        raise ValueError(f"Unsupported AkShare update target: {target}") from exc


def spec_for_target(target: str) -> AkShareModuleSpec:
    """Return the spec for a concrete target."""

    try:
        return module_specs()[target]
    except KeyError as exc:
        raise ValueError(f"Unsupported AkShare update target: {target}") from exc


def validate_request_target_options(request: AkShareUpdateRequest) -> None:
    """Validate target-specific AkShare request options."""

    spec = None if request.target == "all" else spec_for_target(request.target)
    if request.adjustment is not None and (spec is None or not spec.supports_adjustment):
        raise ValueError("--adjustment is only valid for --target daily_bar")
    if request.period and (spec is None or not spec.supports_period):
        raise ValueError("--period is only valid for --target report_disclosure, yysj_em, or yjyg_em")
    if request.market is not None and (spec is None or not spec.supports_market):
        raise ValueError("--market is only valid for --target delist, report_disclosure, or yysj_em")
