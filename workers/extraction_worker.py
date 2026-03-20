# workers/extraction_worker.py
# Background job processor for the newspaper PDF extraction pipeline.
# Uses Python's built-in threading (via FastAPI BackgroundTasks) instead of Redis/RQ.
# No external queue required — job status is tracked entirely in Supabase.

from __future__ import annotations

import asyncio
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

from core.article_cropper import ArticleCropper
from core.extractor import TextExtractor
from core.keyword_matcher import KeywordMatcher
from core.layout_analyzer import LayoutAnalyzer
from core.pdf_fetcher import PDFFetcher, PDFFetchError
from core.supabase_writer import SupabaseWriter
from utils.language_detector import detect_language
from utils.logger import log

# ---------------------------------------------------------------------------
# Module-level singletons — constructed once per process, shared across jobs.
# These are heavy objects (model loaders etc.) — do not recreate per job.
# ---------------------------------------------------------------------------

_fetcher = PDFFetcher()
_extractor = TextExtractor()
_analyzer = LayoutAnalyzer()
_cropper = ArticleCropper()


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------

class _Timer:
    """Lightweight wall-clock timer used to log step durations."""

    def __init__(self) -> None:
        self._t = time.perf_counter()

    def lap(self, label: str) -> float:
        elapsed = time.perf_counter() - self._t
        log.info(f"  ⏱  {label}: {elapsed:.2f}s")
        self._t = time.perf_counter()
        return elapsed

    def total_since(self, start: float) -> float:
        return time.perf_counter() - start


# ---------------------------------------------------------------------------
# Async pipeline implementation
# ---------------------------------------------------------------------------

async def _run_pipeline(job_data: dict) -> dict:
    """Execute the full extraction pipeline asynchronously.

    Returns a result dict shaped like ExtractionResponse for storage in
    the newspaper_jobs table and return to ROBIN.
    """
    job_id: str = job_data["job_id"]
    pdf_url: str = job_data["pdf_url"]
    brief_id: str = job_data["brief_id"]
    source_name: str = job_data["source_name"]
    keywords: list[str] = job_data["keywords"]
    source_language: str = job_data.get("source_language", "auto")
    fuzzy_threshold: int = job_data.get("fuzzy_threshold", 75)
    is_flipbook: bool = job_data.get("is_flipbook", False)
    # client_id: from request, or fall back to ROBIN_CLIENT_ID env var
    client_id: Optional[str] = (
        job_data.get("client_id")
        or os.environ.get("ROBIN_CLIENT_ID")
        or None
    )

    writer = SupabaseWriter()
    job_start = time.perf_counter()
    timer = _Timer()
    temp_path: Optional[str] = None

    # ── Step 0: mark as processing ────────────────────────────────────────
    await writer.update_job_status(
        job_id=job_id,
        status="processing",
        brief_id=brief_id,
        source_name=source_name,
        pdf_url=pdf_url,
    )
    log.info(f"[job:{job_id}] Pipeline started | brief={brief_id!r} | url={pdf_url!r}")

    # ── Step 1: fetch PDF ─────────────────────────────────────────────────
    log.info(f"[job:{job_id}] Step 1 — fetching PDF (is_flipbook={is_flipbook})")
    try:
        pdf_bytes, fetch_method = await _fetcher.fetch(pdf_url, is_flipbook=is_flipbook)
    except PDFFetchError as exc:
        msg = str(exc)
        if "exceeds size limit" in msg:
            log.warning(f"[job:{job_id}] PDF too large: {msg}")
        elif "HTML" in msg or "flipbook rendering" in msg:
            log.warning(f"[job:{job_id}] URL returned HTML instead of PDF: {msg}")
        else:
            log.error(f"[job:{job_id}] PDF fetch failed: {msg}")
        raise

    timer.lap(f"fetch ({fetch_method})")
    log.info(f"[job:{job_id}] Fetched {len(pdf_bytes) / 1024:.1f} KB via {fetch_method!r}")

    # ── Step 2: save to temp file ─────────────────────────────────────────
    log.info(f"[job:{job_id}] Step 2 — saving temp file")
    temp_path = PDFFetcher.save_temp_pdf(pdf_bytes, job_id)
    timer.lap("save temp")

    # ── Step 3: extract text blocks ───────────────────────────────────────
    log.info(f"[job:{job_id}] Step 3 — extracting text (lang={source_language!r})")
    blocks, extraction_method = _extractor.extract(temp_path, source_language)
    timer.lap(f"extract ({extraction_method})")

    if not blocks:
        log.warning(
            f"[job:{job_id}] All OCR engines returned 0 blocks. "
            "PDF may be empty, encrypted, or an unsupported scan format."
        )
        return _build_result(
            job_id=job_id, brief_id=brief_id, source_name=source_name,
            articles=[], extraction_method=extraction_method,
            processing_time=timer.total_since(job_start),
        )

    log.info(f"[job:{job_id}] {len(blocks)} text blocks extracted via {extraction_method!r}")

    # ── Step 4: layout analysis ───────────────────────────────────────────
    log.info(f"[job:{job_id}] Step 4 — analysing layout")
    articles = _analyzer.analyze(blocks, temp_path)
    timer.lap("layout")
    log.info(f"[job:{job_id}] {len(articles)} articles detected across all pages")

    # ── Step 5: keyword matching ──────────────────────────────────────────
    log.info(
        f"[job:{job_id}] Step 5 — matching {len(keywords)} keyword(s) "
        f"(threshold={fuzzy_threshold})"
    )
    matcher = KeywordMatcher(keywords, fuzzy_threshold=fuzzy_threshold)
    matched = matcher.find_in_articles(articles)
    timer.lap("keyword match")

    if not matched:
        log.info(
            f"[job:{job_id}] No keyword matches found in {len(articles)} articles. "
            "This is not an error — brief keywords may not appear in today's edition."
        )
        return _build_result(
            job_id=job_id, brief_id=brief_id, source_name=source_name,
            articles=[], extraction_method=extraction_method,
            processing_time=timer.total_since(job_start),
        )

    log.info(f"[job:{job_id}] {len(matched)} article(s) matched keywords")

    # ── Step 6: crop + upload + persist each matched article ──────────────
    log.info(f"[job:{job_id}] Step 6 — cropping, uploading, writing articles")
    output_articles: list[dict] = []

    for idx, article in enumerate(matched):
        article_label = (
            f"[job:{job_id}] article[{idx}] page={article.get('page_number')}"
        )

        # 6a — crop image
        crop_bytes: Optional[bytes] = None
        try:
            crop_bytes = _cropper.crop_article(temp_path, article)
        except Exception as exc:
            log.warning(f"{article_label} Crop failed (non-fatal): {exc}")

        # 6b — upload crop to Supabase Storage
        image_crop_url: Optional[str] = None
        if crop_bytes:
            try:
                image_crop_url = await writer.upload_image_crop(crop_bytes, job_id, idx)
            except Exception as exc:
                log.warning(f"{article_label} Image upload failed (non-fatal): {exc}")

        # Enrich article with resolved image URL, extraction method, and language.
        article["image_crop_url"] = image_crop_url
        article["extraction_method"] = extraction_method
        article["language_detected"] = detect_language(article.get("full_text", ""))

        # 6c — write article record
        best_match = matcher.get_best_match(article)
        record_id: Optional[str] = None
        try:
            record_id = await writer.write_article(
                article_match=article,
                brief_id=brief_id,
                source_name=source_name,
                pdf_url=pdf_url,
                job_id=job_id,
                client_id=client_id,
            )
        except Exception as exc:
            log.error(f"{article_label} DB write failed (non-fatal): {exc}")

        if record_id:
            kw_str = best_match["keyword"] if best_match else "n/a"
            score_str = f"{best_match['score']:.3f}" if best_match else "0.000"
            log.info(
                f"{article_label} persisted -> id={record_id!r} | "
                f"keyword={kw_str!r} | score={score_str}"
            )

        output_articles.append(_serialise_article(article, record_id))

    timer.lap("crop/upload/write batch")

    total_time = timer.total_since(job_start)
    log.info(
        f"[job:{job_id}] Pipeline complete in {total_time:.2f}s | "
        f"{len(output_articles)} article(s) written"
    )

    return _build_result(
        job_id=job_id, brief_id=brief_id, source_name=source_name,
        articles=output_articles, extraction_method=extraction_method,
        processing_time=total_time,
    )


# ---------------------------------------------------------------------------
# Result builders
# ---------------------------------------------------------------------------

def _serialise_article(article: dict, record_id: Optional[str]) -> dict:
    """Flatten an enriched article dict into the ExtractionResponse.articles format."""
    best_match: Optional[dict] = None
    matches: list[dict] = article.get("keyword_matches", [])
    if matches:
        best_match = max(matches, key=lambda m: m["score"])

    return {
        "record_id":           record_id,
        "keyword_matched":     best_match["keyword"] if best_match else "",
        "keyword_score":       best_match["score"] if best_match else 0.0,
        "article_text":        article.get("full_text", ""),
        "article_headline":    article.get("headline"),
        "page_number":         article.get("page_number", 0),
        "bounding_box":        article.get("bounding_box"),
        "image_crop_url":      article.get("image_crop_url"),
        "language_detected":   article.get("language_detected", "unknown"),
        "extraction_method":   article.get("extraction_method", "unknown"),
        "all_keyword_matches": article.get("keyword_matches", []),
    }


def _build_result(
    *,
    job_id: str,
    brief_id: str,
    source_name: str,
    articles: list[dict],
    extraction_method: str,
    processing_time: float,
    error: Optional[str] = None,
) -> dict:
    return {
        "job_id":                   job_id,
        "status":                   "completed" if error is None else "failed",
        "brief_id":                 brief_id,
        "source_name":              source_name,
        "total_matches":            len(articles),
        "articles":                 articles,
        "extraction_method_used":   extraction_method,
        "processing_time_seconds":  round(processing_time, 2),
        "error":                    error,
    }


# ---------------------------------------------------------------------------
# Public synchronous entry point (called by FastAPI BackgroundTasks)
# ---------------------------------------------------------------------------

def process_extraction_job(job_data: dict) -> dict:
    """Run the async pipeline synchronously inside a thread-pool thread.

    FastAPI's BackgroundTasks dispatches sync functions to a thread pool,
    so this function runs in its own OS thread with its own event loop.
    It is safe to call asyncio.run() here.

    Args:
        job_data: Dict matching ExtractionRequest fields plus a ``job_id`` key.

    Returns:
        Result dict written to Supabase and returned for logging.
    """
    job_id: str = job_data.get("job_id", "unknown")
    writer = SupabaseWriter()

    try:
        result = asyncio.run(_run_pipeline(job_data))

        asyncio.run(writer.update_job_status(
            job_id=job_id,
            status="completed",
            result=result,
        ))
        log.info(f"[job:{job_id}] Status updated to 'completed' in Supabase")
        return result

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        log.error(f"[job:{job_id}] FAILED — {error_msg}")

        try:
            asyncio.run(writer.update_job_status(
                job_id=job_id,
                status="failed",
                error=error_msg,
            ))
        except Exception as write_exc:
            log.error(
                f"[job:{job_id}] Could not write failure status to Supabase: {write_exc}"
            )
        return _build_result(
            job_id=job_id, brief_id=job_data.get("brief_id", ""),
            source_name=job_data.get("source_name", ""),
            articles=[], extraction_method="none",
            processing_time=0.0, error=error_msg,
        )

    finally:
        _cleanup_temp(job_id)


def _cleanup_temp(job_id: str) -> None:
    """Delete the temp PDF file for *job_id* if it exists."""
    path = Path(tempfile.gettempdir()) / f"{job_id}.pdf"
    if path.exists():
        try:
            path.unlink()
            log.debug(f"[cleanup] Deleted temp file: {path}")
        except Exception as exc:
            log.warning(f"[cleanup] Could not delete {path}: {exc}")
