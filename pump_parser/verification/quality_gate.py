"""Quality gates — accept/reject/warn thresholds for parse results.

Gates:
1. Minimum models per catalog (expect at least N based on page count)
2. Minimum average confidence
3. Minimum physics pass rate
4. Maximum low-confidence ratio
5. Consistency check (no extreme outliers in Q/H/P ranges)
"""

import logging
from dataclasses import dataclass, field
from enum import Enum

from pump_parser.models import PumpEntry, ParseResult

log = logging.getLogger("pump_parser.verification.quality_gate")


class GateVerdict(Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass
class GateResult:
    """Result of a single quality gate check."""
    name: str
    verdict: GateVerdict
    message: str
    value: float = 0.0
    threshold: float = 0.0


@dataclass
class QualityReport:
    """Overall quality assessment."""
    verdict: GateVerdict = GateVerdict.PASS
    gates: list[GateResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.verdict != GateVerdict.FAIL

    def summary(self) -> str:
        lines = [f"Quality: {self.verdict.value.upper()}"]
        for g in self.gates:
            icon = {"pass": "+", "warn": "!", "fail": "X"}[g.verdict.value]
            lines.append(f"  [{icon}] {g.name}: {g.message}")
        return "\n".join(lines)


# ─── Gate Thresholds ─────────────────────────────────────────────────────────

MIN_MODELS_PER_10_PAGES = 2       # expect at least 2 models per 10 data pages
MIN_AVG_CONFIDENCE = 0.40         # below = FAIL
WARN_AVG_CONFIDENCE = 0.60        # below = WARN
MIN_PHYSICS_RATE = 0.50           # below = FAIL
WARN_PHYSICS_RATE = 0.80          # below = WARN
MAX_LOW_CONF_RATIO = 0.50         # >50% entries with conf<0.4 = FAIL
WARN_LOW_CONF_RATIO = 0.30        # >30% = WARN
LOW_CONF_THRESHOLD = 0.40         # what counts as "low confidence"


def check_quality(result: ParseResult) -> QualityReport:
    """Run all quality gates on a parse result.

    Returns QualityReport with per-gate results and overall verdict.
    """
    report = QualityReport()
    entries = result.entries

    # Gate 1: Minimum models
    report.gates.append(_gate_min_models(entries, result.pages_processed))

    # Gate 2: Average confidence
    report.gates.append(_gate_avg_confidence(entries))

    # Gate 3: Physics pass rate
    report.gates.append(_gate_physics_rate(result))

    # Gate 4: Low-confidence ratio
    report.gates.append(_gate_low_conf_ratio(entries))

    # Gate 5: Data consistency
    report.gates.append(_gate_consistency(entries))

    # Determine overall verdict
    verdicts = [g.verdict for g in report.gates]
    if GateVerdict.FAIL in verdicts:
        report.verdict = GateVerdict.FAIL
    elif GateVerdict.WARN in verdicts:
        report.verdict = GateVerdict.WARN
    else:
        report.verdict = GateVerdict.PASS

    # Collect warnings
    for g in report.gates:
        if g.verdict != GateVerdict.PASS:
            report.warnings.append(f"[{g.verdict.value}] {g.name}: {g.message}")

    log.info("Quality gate: %s (%d gates, %d warnings)",
             report.verdict.value, len(report.gates), len(report.warnings))

    return report


def _gate_min_models(entries: list[PumpEntry], pages_processed: int) -> GateResult:
    """Check minimum number of models extracted."""
    if not entries:
        return GateResult(
            name="min_models",
            verdict=GateVerdict.FAIL,
            message="No models extracted",
            value=0,
        )

    expected = max(1, (pages_processed / 10) * MIN_MODELS_PER_10_PAGES)
    count = len(entries)

    if count >= expected:
        return GateResult(
            name="min_models",
            verdict=GateVerdict.PASS,
            message=f"{count} models (expected >={expected:.0f})",
            value=count,
            threshold=expected,
        )
    elif count >= expected * 0.5:
        return GateResult(
            name="min_models",
            verdict=GateVerdict.WARN,
            message=f"{count} models (expected >={expected:.0f})",
            value=count,
            threshold=expected,
        )
    else:
        return GateResult(
            name="min_models",
            verdict=GateVerdict.FAIL,
            message=f"Only {count} models (expected >={expected:.0f})",
            value=count,
            threshold=expected,
        )


def _gate_avg_confidence(entries: list[PumpEntry]) -> GateResult:
    """Check average confidence level."""
    if not entries:
        return GateResult(
            name="avg_confidence", verdict=GateVerdict.FAIL,
            message="No entries", value=0,
        )

    avg = sum(e.confidence for e in entries) / len(entries)

    if avg >= WARN_AVG_CONFIDENCE:
        verdict = GateVerdict.PASS
    elif avg >= MIN_AVG_CONFIDENCE:
        verdict = GateVerdict.WARN
    else:
        verdict = GateVerdict.FAIL

    return GateResult(
        name="avg_confidence",
        verdict=verdict,
        message=f"avg={avg:.2f}",
        value=avg,
        threshold=MIN_AVG_CONFIDENCE,
    )


def _gate_physics_rate(result: ParseResult) -> GateResult:
    """Check physics validation pass rate."""
    rate = result.report.physics_pass_rate if result.report else 0.0

    if rate >= WARN_PHYSICS_RATE:
        verdict = GateVerdict.PASS
    elif rate >= MIN_PHYSICS_RATE:
        verdict = GateVerdict.WARN
    else:
        verdict = GateVerdict.FAIL

    return GateResult(
        name="physics_rate",
        verdict=verdict,
        message=f"{rate:.0%}",
        value=rate,
        threshold=MIN_PHYSICS_RATE,
    )


def _gate_low_conf_ratio(entries: list[PumpEntry]) -> GateResult:
    """Check ratio of low-confidence entries."""
    if not entries:
        return GateResult(
            name="low_conf_ratio", verdict=GateVerdict.PASS,
            message="No entries", value=0,
        )

    low = sum(1 for e in entries if e.confidence < LOW_CONF_THRESHOLD)
    ratio = low / len(entries)

    if ratio <= WARN_LOW_CONF_RATIO:
        verdict = GateVerdict.PASS
    elif ratio <= MAX_LOW_CONF_RATIO:
        verdict = GateVerdict.WARN
    else:
        verdict = GateVerdict.FAIL

    return GateResult(
        name="low_conf_ratio",
        verdict=verdict,
        message=f"{low}/{len(entries)} ({ratio:.0%}) below {LOW_CONF_THRESHOLD}",
        value=ratio,
        threshold=MAX_LOW_CONF_RATIO,
    )


def _gate_consistency(entries: list[PumpEntry]) -> GateResult:
    """Check data consistency — no extreme outliers in Q/H/P ranges."""
    if len(entries) < 3:
        return GateResult(
            name="consistency", verdict=GateVerdict.PASS,
            message="Too few entries to check",
        )

    # Check Q range
    q_vals = [e.q_nom for e in entries if e.q_nom > 0]
    h_vals = [e.h_nom for e in entries if e.h_nom > 0]
    p_vals = [e.power_kw for e in entries if e.power_kw > 0]

    issues = []
    for name, vals in [("Q", q_vals), ("H", h_vals), ("P", p_vals)]:
        if len(vals) < 3:
            continue
        sorted_vals = sorted(vals)
        median = sorted_vals[len(sorted_vals) // 2]
        if median <= 0:
            continue
        # Check if max/min ratio is extreme (>1000x range is suspicious)
        ratio = sorted_vals[-1] / sorted_vals[0] if sorted_vals[0] > 0 else 0
        if ratio > 1000:
            issues.append(f"{name} range {sorted_vals[0]:.1f}-{sorted_vals[-1]:.1f} (×{ratio:.0f})")

    if issues:
        return GateResult(
            name="consistency",
            verdict=GateVerdict.WARN,
            message=f"Extreme ranges: {'; '.join(issues)}",
        )

    return GateResult(
        name="consistency",
        verdict=GateVerdict.PASS,
        message="OK",
    )
