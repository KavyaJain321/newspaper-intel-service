"""
tests/test_manual.py
--------------------
CLI tool for manually testing any newspaper URL through the full pipeline.
Run from the project root:

    python tests/test_manual.py --url "https://example.com/paper.pdf" \
                                --keywords "election" "BJP" "चुनाव" \
                                --source "Dainik Jagran" \
                                --language hi

Options:
    --url           URL of the PDF or flipbook page  (required)
    --keywords      One or more keywords to search   (required)
    --source        Newspaper name                   (default: "Test Source")
    --language      Language code: hi/en/or/bn/auto  (default: auto)
    --flipbook      Flag: treat URL as a flipbook    (default: False)
    --threshold     Fuzzy match threshold 0-100      (default: 75)
    --no-upload     Skip Supabase upload (dry-run)   (default: False)
    --save-pdf      Save the downloaded PDF locally  (default: False)
    --brief-id      Brief ID for tracing             (default: "manual-test")
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

# Ensure project root is on PYTHONPATH when running from any directory.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from core.article_cropper import ArticleCropper
from core.extractor import TextExtractor
from core.keyword_matcher import KeywordMatcher
from core.layout_analyzer import LayoutAnalyzer
from core.pdf_fetcher import PDFFetcher, PDFFetchError
from utils.language_detector import detect_language
from utils.logger import log


def _fmt(val: float) -> str:
    return f"{val:.2f}s"


async def run_test(args: argparse.Namespace) -> None:
    total_start = time.perf_counter()

    print("\n" + "=" * 60)
    print("  NEWSPAPER INTEL — MANUAL TEST")
    print("=" * 60)
    print(f"  URL      : {args.url}")
    print(f"  Keywords : {args.keywords}")
    print(f"  Source   : {args.source}")
    print(f"  Language : {args.language}")
    print(f"  Flipbook : {args.flipbook}")
    print(f"  Threshold: {args.threshold}")
    print("=" * 60 + "\n")

    fetcher = PDFFetcher()
    extractor = TextExtractor()
    analyzer = LayoutAnalyzer()
    cropper = ArticleCropper()

    # ── Step 1: Fetch PDF ──────────────────────────────────────────────
    print("▶ Step 1: Fetching PDF...")
    t = time.perf_counter()
    try:
        pdf_bytes, fetch_method = await fetcher.fetch(args.url, is_flipbook=args.flipbook)
    except PDFFetchError as exc:
        print(f"  ✗ FAILED: {exc}")
        return

    elapsed = time.perf_counter() - t
    size_kb = len(pdf_bytes) / 1024
    print(f"  ✓ {size_kb:.1f} KB fetched via '{fetch_method}' in {_fmt(elapsed)}")

    # Optionally save PDF locally.
    temp_path = f"/tmp/manual_test_{int(time.time())}.pdf"
    if args.save_pdf:
        save_path = f"manual_test_{int(time.time())}.pdf"
        Path(save_path).write_bytes(pdf_bytes)
        print(f"  ✓ PDF saved to: {save_path}")

    PDFFetcher.save_temp_pdf(pdf_bytes, f"manual_test_{int(time.time())}")
    temp_path = list(Path("/tmp").glob("manual_test_*.pdf"))[-1]

    # ── Step 2: Extract text ───────────────────────────────────────────
    print("\n▶ Step 2: Extracting text...")
    t = time.perf_counter()
    blocks, extraction_method = extractor.extract(str(temp_path), args.language)
    elapsed = time.perf_counter() - t
    print(f"  ✓ {len(blocks)} blocks extracted via '{extraction_method}' in {_fmt(elapsed)}")

    if not blocks:
        print("  ✗ No text extracted. Stopping.")
        return

    # Sample: show first 3 blocks.
    print(f"\n  Sample blocks (first 3 of {len(blocks)}):")
    for b in blocks[:3]:
        preview = b["text"][:80].replace("\n", " ")
        print(f"    [p{b['page_number']}] {preview!r}")

    # ── Step 3: Layout analysis ────────────────────────────────────────
    print("\n▶ Step 3: Analysing layout...")
    t = time.perf_counter()
    articles = analyzer.analyze(blocks, str(temp_path))
    elapsed = time.perf_counter() - t
    print(f"  ✓ {len(articles)} articles detected in {_fmt(elapsed)}")

    if not articles:
        print("  ✗ No articles detected. Stopping.")
        return

    # Show article count per page.
    by_page: dict[int, int] = {}
    for a in articles:
        by_page[a["page_number"]] = by_page.get(a["page_number"], 0) + 1
    for pg, count in sorted(by_page.items()):
        print(f"    Page {pg}: {count} article(s)")

    # ── Step 4: Keyword matching ───────────────────────────────────────
    print(f"\n▶ Step 4: Matching keywords {args.keywords}...")
    t = time.perf_counter()
    matcher = KeywordMatcher(args.keywords, fuzzy_threshold=args.threshold)
    matched = matcher.find_in_articles(articles)
    elapsed = time.perf_counter() - t
    print(f"  ✓ {len(matched)} article(s) matched in {_fmt(elapsed)}")

    if not matched:
        print("  ✗ No keyword matches found.")
        print("\n  Tip: Try --threshold 50 or check that the keywords are in the PDF.")
        return

    # ── Step 5: Show results ───────────────────────────────────────────
    print(f"\n▶ Step 5: Results\n")
    print("─" * 60)

    for i, article in enumerate(matched):
        best = matcher.get_best_match(article)
        lang = detect_language(article.get("full_text", ""))
        headline = article.get("headline") or "(no headline)"
        text_preview = article.get("full_text", "")[:200].replace("\n", " ")

        print(f"  Article {i+1} — Page {article['page_number']} | Col {article.get('column_index', '?')}")
        print(f"  Headline : {headline}")
        print(f"  Keyword  : '{best['keyword']}' → score {best['score']:.3f} ({best['match_type']})")
        print(f"  Language : {lang}")
        print(f"  Text     : {text_preview!r}...")
        print(f"  BBox     : {[round(v) for v in article.get('bounding_box', [])]}")

        # ── Step 6: Crop image ─────────────────────────────────────────
        crop_bytes = cropper.crop_article(str(temp_path), article)
        if crop_bytes:
            crop_path = f"crop_article_{i+1}.jpg"
            Path(crop_path).write_bytes(crop_bytes)
            print(f"  Crop     : saved to {crop_path} ({len(crop_bytes)//1024} KB)")
        else:
            print(f"  Crop     : ✗ failed (bbox may be outside page)")

        # ── Step 7: Supabase write (optional) ─────────────────────────
        if not args.no_upload:
            try:
                from core.supabase_writer import SupabaseWriter
                writer = SupabaseWriter()
                article["image_crop_url"] = None
                article["extraction_method"] = extraction_method
                article["language_detected"] = lang
                record_id = await writer.write_article(
                    article_match=article,
                    brief_id=args.brief_id,
                    source_name=args.source,
                    pdf_url=args.url,
                    job_id=f"manual-test-{int(time.time())}",
                )
                if record_id:
                    print(f"  Supabase : ✓ written → id={record_id}")
                else:
                    print(f"  Supabase : ✗ write returned None (check logs)")
            except Exception as exc:
                print(f"  Supabase : ✗ {exc}")
                print(f"  (Use --no-upload to skip Supabase writes)")
        else:
            print(f"  Supabase : skipped (--no-upload)")

        print("─" * 60)

    total_elapsed = time.perf_counter() - total_start
    print(f"\n✓ Done in {total_elapsed:.1f}s total | {len(matched)} match(es) found")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manual end-to-end test for the Newspaper Intel pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--url", required=True, help="PDF or flipbook page URL")
    parser.add_argument("--keywords", required=True, nargs="+", help="Keywords to search")
    parser.add_argument("--source", default="Test Source", help="Newspaper name")
    parser.add_argument("--language", default="auto",
                        choices=["auto", "hi", "en", "or", "bn", "ta", "te", "mr", "gu", "pa", "ml"],
                        help="Source language (default: auto)")
    parser.add_argument("--flipbook", action="store_true", help="URL is a flipbook viewer page")
    parser.add_argument("--threshold", type=int, default=75, help="Fuzzy match threshold 0-100")
    parser.add_argument("--no-upload", action="store_true", help="Skip Supabase write (dry run)")
    parser.add_argument("--save-pdf", action="store_true", help="Save downloaded PDF locally")
    parser.add_argument("--brief-id", default="manual-test", help="Brief ID for Supabase tracing")

    args = parser.parse_args()
    asyncio.run(run_test(args))


if __name__ == "__main__":
    main()
