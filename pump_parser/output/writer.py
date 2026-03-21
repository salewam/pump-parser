"""Output writers: JSON, CSV, detailed report."""

import csv
import json
import logging
from pathlib import Path

from pump_parser.models import ParseResult, PumpEntry, ParseReport

log = logging.getLogger("pump_parser.output")


def write_json(result: ParseResult, output_path: str) -> str:
    """Write ParseResult to JSON file. Returns path."""
    data = result.to_dict()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info("Wrote JSON: %s (%d pumps)", output_path, len(result.entries))
    return output_path


def write_csv(result: ParseResult, output_path: str) -> str:
    """Write pump entries to CSV. Returns path."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "model", "series", "manufacturer", "article",
        "q_nom", "h_nom", "power_kw", "rpm", "efficiency",
        "q_points", "h_points",
        "dn_suction", "dn_discharge", "weight_kg", "stages",
        "source_file", "source_page", "data_source", "confidence",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for entry in result.entries:
            row = entry.to_dict()
            row["q_points"] = ";".join(str(v) for v in row.get("q_points", []))
            row["h_points"] = ";".join(str(v) for v in row.get("h_points", []))
            writer.writerow(row)

    log.info("Wrote CSV: %s (%d pumps)", output_path, len(result.entries))
    return output_path


def print_summary(result: ParseResult) -> None:
    """Print human-readable summary to stdout."""
    print(f"\n{'='*60}")
    print(f"  Source: {result.source}")
    print(f"  Models: {len(result.entries)}")
    print(f"  Pages processed: {result.pages_processed}")
    print(f"  Pages skipped: {result.pages_skipped}")
    print(f"  Avg confidence: {result.avg_confidence:.2f}")
    print(f"  Time: {result.extraction_time_s:.1f}s")

    if result.report:
        r = result.report
        print(f"  Physics pass rate: {r.physics_pass_rate:.0%}")
        if r.extractor_breakdown:
            print(f"  Extractors: {r.extractor_breakdown}")

    if result.entries:
        print(f"\n  {'Model':<35} {'Q':>8} {'H':>8} {'P':>8} {'Curve':>6}")
        print(f"  {'-'*35} {'-'*8} {'-'*8} {'-'*8} {'-'*6}")
        for e in result.entries[:20]:
            curve = "yes" if e.has_curve() else "-"
            print(f"  {e.model:<35} {e.q_nom:>8.1f} {e.h_nom:>8.1f} {e.power_kw:>8.2f} {curve:>6}")
        if len(result.entries) > 20:
            print(f"  ... +{len(result.entries) - 20} more")

    print(f"{'='*60}\n")


def generate_report(result: ParseResult) -> str:
    """Generate a detailed quality report as formatted text.

    Includes:
    - Summary statistics
    - Per-page breakdown
    - Extractor usage
    - Warning summary
    - Confidence distribution
    - Data completeness
    - Top warnings
    """
    lines = []
    w = lines.append

    w(f"{'='*70}")
    w(f"  PARSE REPORT: {result.source}")
    w(f"{'='*70}")
    w("")

    # Summary
    w("  SUMMARY")
    w(f"  {'='*40}")
    w(f"  Total models:      {len(result.entries)}")
    w(f"  Pages processed:   {result.pages_processed}")
    w(f"  Pages skipped:     {result.pages_skipped}")
    w(f"  Extraction time:   {result.extraction_time_s:.1f}s")
    w(f"  Recipe used:       {result.recipe_used or 'discovery'}")
    w(f"  Avg confidence:    {result.avg_confidence:.3f}")

    if result.report:
        r = result.report
        w(f"  Physics pass rate: {r.physics_pass_rate:.0%}")
        w(f"  Self-heal applied: {'yes' if r.self_heal_applied else 'no'}")
        if r.vision_ai_pages:
            w(f"  Vision AI calls:   {r.vision_ai_pages}")
            w(f"  Vision AI cost:    ${r.vision_ai_cost_usd:.4f}")
    w("")

    # Extractor breakdown
    if result.report and result.report.extractor_breakdown:
        w("  EXTRACTORS")
        w(f"  {'='*40}")
        for ext_name, count in sorted(result.report.extractor_breakdown.items(),
                                        key=lambda x: -x[1]):
            pct = count / len(result.entries) * 100 if result.entries else 0
            bar = "#" * int(pct / 2)
            w(f"  {ext_name:<20} {count:>4} ({pct:>5.1f}%) {bar}")
        w("")

    # Confidence distribution
    if result.entries:
        w("  CONFIDENCE DISTRIBUTION")
        w(f"  {'='*40}")
        buckets = {"0.9+": 0, "0.7-0.9": 0, "0.5-0.7": 0, "0.3-0.5": 0, "<0.3": 0}
        for e in result.entries:
            if e.confidence >= 0.9:
                buckets["0.9+"] += 1
            elif e.confidence >= 0.7:
                buckets["0.7-0.9"] += 1
            elif e.confidence >= 0.5:
                buckets["0.5-0.7"] += 1
            elif e.confidence >= 0.3:
                buckets["0.3-0.5"] += 1
            else:
                buckets["<0.3"] += 1

        for label, count in buckets.items():
            pct = count / len(result.entries) * 100
            bar = "#" * int(pct / 2)
            w(f"  {label:<12} {count:>4} ({pct:>5.1f}%) {bar}")
        w("")

    # Data completeness
    if result.entries:
        w("  DATA COMPLETENESS")
        w(f"  {'='*40}")
        has_q = sum(1 for e in result.entries if e.q_nom > 0)
        has_h = sum(1 for e in result.entries if e.h_nom > 0)
        has_p = sum(1 for e in result.entries if e.power_kw > 0)
        has_curve = sum(1 for e in result.entries if e.has_curve())
        has_rpm = sum(1 for e in result.entries if e.rpm > 0)
        has_art = sum(1 for e in result.entries if e.article)
        n = len(result.entries)

        for label, count in [("Q (flow)", has_q), ("H (head)", has_h),
                             ("P (power)", has_p), ("Curve", has_curve),
                             ("RPM", has_rpm), ("Article", has_art)]:
            pct = count / n * 100
            w(f"  {label:<15} {count:>4}/{n} ({pct:>5.1f}%)")
        w("")

    # Per-page report
    if result.report and result.report.per_page:
        w("  PER-PAGE BREAKDOWN")
        w(f"  {'='*40}")
        w(f"  {'Page':>6} {'Extractor':<20} {'Models':>7} {'Conf':>7}")
        w(f"  {'-'*6} {'-'*20} {'-'*7} {'-'*7}")
        for pr in result.report.per_page:
            w(f"  {pr.page_num:>6} {pr.extractor_used:<20} {pr.models_found:>7} {pr.avg_confidence:>7.2f}")
        w("")

    # Top warnings
    all_warnings = []
    for e in result.entries:
        for warning in e.warnings:
            all_warnings.append((e.model, warning))

    if all_warnings:
        # Count warning types
        warning_types: dict[str, int] = {}
        for _, w_text in all_warnings:
            # Normalize warning for grouping
            key = w_text.split(":")[0] if ":" in w_text else w_text[:40]
            warning_types[key] = warning_types.get(key, 0) + 1

        w("  WARNING SUMMARY")
        w(f"  {'='*40}")
        w(f"  Total warnings: {len(all_warnings)}")
        w("")
        for wtype, count in sorted(warning_types.items(), key=lambda x: -x[1])[:10]:
            w(f"  {count:>4}x  {wtype}")
        w("")

    w(f"{'='*70}")

    return "\n".join(lines)


def write_report(result: ParseResult, output_path: str) -> str:
    """Write detailed report to text file. Returns path."""
    report_text = generate_report(result)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    log.info("Wrote report: %s", output_path)
    return output_path
