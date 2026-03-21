"""CLI entry point for pump_parser.

Usage:
    python -m pump_parser parse <pdf_path> [--output json|csv] [--out-dir DIR]
    python -m pump_parser batch <directory> [--output json|csv] [--out-dir DIR]
    python -m pump_parser info <pdf_path>
"""

import sys
import glob
import logging
import argparse
from pathlib import Path

from pump_parser.config import OUTPUT_DIR, LOG_DIR, LOG_FORMAT
from pump_parser.core.orchestrator import parse_pdf
from pump_parser.output.writer import write_json, write_csv, print_summary


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format=LOG_FORMAT)
    # File handler
    fh = logging.FileHandler(LOG_DIR / "parser.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger("pump_parser").addHandler(fh)


def cmd_parse(args: argparse.Namespace) -> None:
    """Parse a single PDF."""
    pdf_path = args.pdf
    if not Path(pdf_path).exists():
        print(f"Error: file not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    page_range = None
    if args.pages:
        parts = args.pages.split("-")
        start = int(parts[0])
        end = int(parts[1]) if len(parts) > 1 else start
        page_range = (start, end)

    result = parse_pdf(pdf_path, page_range=page_range, min_confidence=args.min_conf)
    print_summary(result)

    # Output
    out_dir = Path(args.out_dir) if args.out_dir else OUTPUT_DIR
    stem = Path(pdf_path).stem

    if args.output in ("json", "both"):
        path = write_json(result, str(out_dir / f"{stem}.json"))
        print(f"JSON: {path}")

    if args.output in ("csv", "both"):
        path = write_csv(result, str(out_dir / f"{stem}.csv"))
        print(f"CSV: {path}")


def cmd_batch(args: argparse.Namespace) -> None:
    """Parse all PDFs in a directory."""
    pattern = str(Path(args.directory) / "*.pdf")
    pdfs = sorted(glob.glob(pattern))
    if not pdfs:
        print(f"No PDFs found in {args.directory}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(pdfs)} PDFs")
    out_dir = Path(args.out_dir) if args.out_dir else OUTPUT_DIR

    total_models = 0
    for i, pdf_path in enumerate(pdfs, 1):
        name = Path(pdf_path).name
        print(f"\n[{i}/{len(pdfs)}] {name}")

        try:
            result = parse_pdf(pdf_path, min_confidence=args.min_conf)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        total_models += len(result.entries)
        print(f"  {len(result.entries)} models, {result.extraction_time_s:.1f}s")

        stem = Path(pdf_path).stem
        if args.output in ("json", "both"):
            write_json(result, str(out_dir / f"{stem}.json"))
        if args.output in ("csv", "both"):
            write_csv(result, str(out_dir / f"{stem}.csv"))

    print(f"\nTotal: {total_models} models from {len(pdfs)} PDFs")


def cmd_recipes_list(args: argparse.Namespace) -> None:
    """List all recipes."""
    from pump_parser.learning.recipe_store import RecipeStore
    store = RecipeStore()
    recipes = store.all()

    if not recipes:
        print("No recipes found.")
        return

    print(f"\n{'ID':<15} {'Name':<20} {'Type':<15} {'Conf':>5} {'Uses':>5} {'Rate':>6}")
    print(f"{'-'*15} {'-'*20} {'-'*15} {'-'*5} {'-'*5} {'-'*6}")
    for r in recipes:
        rate = f"{r.success_rate():.0%}" if r.uses_count > 0 else "-"
        print(f"{r.recipe_id:<15} {r.name:<20} {r.extraction.extractor_type:<15} {r.confidence:>5.2f} {r.uses_count:>5} {rate:>6}")
    print(f"\nTotal: {len(recipes)} recipes")


def cmd_recipes_show(args: argparse.Namespace) -> None:
    """Show full recipe details."""
    from pump_parser.learning.recipe_store import RecipeStore
    store = RecipeStore()
    recipe = store.load(args.recipe_id)

    if not recipe:
        print(f"Recipe '{args.recipe_id}' not found.", file=sys.stderr)
        sys.exit(1)

    print(recipe.to_json())


def cmd_recipes_delete(args: argparse.Namespace) -> None:
    """Delete a recipe."""
    from pump_parser.learning.recipe_store import RecipeStore
    store = RecipeStore()

    if store.delete(args.recipe_id):
        print(f"Deleted recipe '{args.recipe_id}'")
    else:
        print(f"Recipe '{args.recipe_id}' not found.", file=sys.stderr)
        sys.exit(1)


def cmd_recipes_seed(args: argparse.Namespace) -> None:
    """Generate seed recipes for ONIS catalogs."""
    from pump_parser.learning.seed_recipes import generate_seed_recipes
    recipes = generate_seed_recipes()
    print(f"Created {len(recipes)} seed recipes:")
    for r in recipes:
        print(f"  {r.recipe_id}: {r.name} ({r.extraction.extractor_type})")


def cmd_recipes_match(args: argparse.Namespace) -> None:
    """Test which recipe matches a PDF."""
    from pump_parser.learning.recipe_store import RecipeStore
    from pump_parser.learning.recipe_matcher import RecipeMatcher
    from pump_parser.core.ingestion import load_pdf, get_page_text, close_pdf

    store = RecipeStore()
    recipes = store.all()
    if not recipes:
        print("No recipes. Run 'recipes seed' first.")
        return

    doc = load_pdf(args.pdf)
    text = " ".join(get_page_text(doc, p) for p in range(min(3, doc.num_pages)))
    close_pdf(doc)

    matcher = RecipeMatcher()
    scored = matcher.score_all(args.pdf, recipes, text)

    print(f"\nMatching results for: {Path(args.pdf).name}\n")
    print(f"{'Recipe':<20} {'Score':>8}")
    print(f"{'-'*20} {'-'*8}")
    for r, score in scored[:10]:
        marker = " <<<" if score >= 50 else ""
        print(f"{r.name:<20} {score:>8.0f}{marker}")


def cmd_info(args: argparse.Namespace) -> None:
    """Show PDF info and page classification."""
    from pump_parser.core.ingestion import load_pdf, get_page_text, close_pdf
    from pump_parser.classifiers.page_classifier import classify_page

    doc = load_pdf(args.pdf)
    print(f"File: {doc.path}")
    print(f"Pages: {doc.num_pages}")
    print(f"Size: {doc.file_size_mb:.1f} MB")
    print(f"Scanned: {doc.is_scanned}")
    print(f"Producer: {doc.producer}")
    print()

    for p in range(doc.num_pages):
        text = get_page_text(doc, p)
        classified = classify_page(text, p)
        chars = len(text.strip())
        print(f"  p{p:3d}: {classified.page_type.value:<15} conf={classified.confidence:.2f}  chars={chars}")

    close_pdf(doc)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pump_parser",
        description="Universal PDF pump catalog parser",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command")

    # parse
    p_parse = sub.add_parser("parse", help="Parse a single PDF")
    p_parse.add_argument("pdf", help="Path to PDF file")
    p_parse.add_argument("--output", choices=["json", "csv", "both"], default="json")
    p_parse.add_argument("--out-dir", help="Output directory")
    p_parse.add_argument("--pages", help="Page range, e.g. 0-10")
    p_parse.add_argument("--min-conf", type=float, default=0.0, help="Min confidence filter")

    # batch
    p_batch = sub.add_parser("batch", help="Parse all PDFs in directory")
    p_batch.add_argument("directory", help="Directory with PDF files")
    p_batch.add_argument("--output", choices=["json", "csv", "both"], default="json")
    p_batch.add_argument("--out-dir", help="Output directory")
    p_batch.add_argument("--min-conf", type=float, default=0.0)

    # info
    p_info = sub.add_parser("info", help="Show PDF info and page classification")
    p_info.add_argument("pdf", help="Path to PDF file")

    # recipes
    p_recipes = sub.add_parser("recipes", help="Recipe management")
    rsub = p_recipes.add_subparsers(dest="recipes_command")

    rsub.add_parser("list", help="List all recipes")
    rsub.add_parser("seed", help="Generate seed recipes for ONIS catalogs")

    p_rshow = rsub.add_parser("show", help="Show recipe details")
    p_rshow.add_argument("recipe_id", help="Recipe ID")

    p_rdel = rsub.add_parser("delete", help="Delete a recipe")
    p_rdel.add_argument("recipe_id", help="Recipe ID")

    p_rmatch = rsub.add_parser("match", help="Test recipe matching for a PDF")
    p_rmatch.add_argument("pdf", help="Path to PDF file")

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.command == "parse":
        cmd_parse(args)
    elif args.command == "batch":
        cmd_batch(args)
    elif args.command == "info":
        cmd_info(args)
    elif args.command == "recipes":
        rc = getattr(args, "recipes_command", None)
        if rc == "list":
            cmd_recipes_list(args)
        elif rc == "show":
            cmd_recipes_show(args)
        elif rc == "delete":
            cmd_recipes_delete(args)
        elif rc == "seed":
            cmd_recipes_seed(args)
        elif rc == "match":
            cmd_recipes_match(args)
        else:
            p_recipes.print_help()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
