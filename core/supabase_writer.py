# core/supabase_writer.py
# Handles all writes to the Supabase database.
# Inserts extracted article records (text, bounding boxes, language, keyword matches),
# uploads cropped article images to Supabase Storage, and updates job status rows
# throughout the processing lifecycle.

# =============================================================================
# Run this SQL in the Supabase SQL Editor before deploying:
#
# -- Job tracking table for the newspaper-intel-service
# CREATE TABLE IF NOT EXISTS newspaper_jobs (
#     job_id        TEXT PRIMARY KEY,
#     status        TEXT NOT NULL DEFAULT 'queued',
#         -- queued | processing | completed | failed
#     brief_id      TEXT,
#     source_name   TEXT,
#     pdf_url       TEXT,
#     result        JSONB,
#     error         TEXT,
#     created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
#     updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
# );
#
# -- Index for ROBIN to poll by brief
# CREATE INDEX IF NOT EXISTS idx_newspaper_jobs_brief_id
#     ON newspaper_jobs (brief_id);
#
# -- Auto-update updated_at on every row change
# CREATE OR REPLACE FUNCTION update_newspaper_jobs_updated_at()
# RETURNS TRIGGER LANGUAGE plpgsql AS $$
# BEGIN
#     NEW.updated_at = NOW();
#     RETURN NEW;
# END;
# $$;
#
# DROP TRIGGER IF EXISTS trg_newspaper_jobs_updated_at ON newspaper_jobs;
# CREATE TRIGGER trg_newspaper_jobs_updated_at
#     BEFORE UPDATE ON newspaper_jobs
#     FOR EACH ROW EXECUTE FUNCTION update_newspaper_jobs_updated_at();
#
# -- Storage bucket (also created programmatically on first upload, but
# -- creating it here ensures the RLS policies can be set in advance)
# INSERT INTO storage.buckets (id, name, public)
# VALUES ('newspaper-crops', 'newspaper-crops', true)
# ON CONFLICT (id) DO NOTHING;
# =============================================================================

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Optional

from supabase import create_client, Client
from utils.logger import log

# ---------------------------------------------------------------------------
# Module-level Supabase client (created once at import time)
# ---------------------------------------------------------------------------

_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

if not _SUPABASE_URL or not _SUPABASE_KEY:
    raise RuntimeError(
        "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in the environment. "
        "Copy .env.example to .env and fill in your Supabase credentials."
    )

_client: Client = create_client(_SUPABASE_URL, _SUPABASE_KEY)

_CROPS_BUCKET = "newspaper-crops"
_CONTENT_TABLE = "content_items"
_JOBS_TABLE = "newspaper_jobs"

# Optional ROBIN integration fields — set these in .env if you want articles
# written into the same client/source context as the rest of ROBIN's data.
# If not set, the row is inserted without client_id / source_id (nullable).
_ROBIN_CLIENT_ID: Optional[str] = os.environ.get("ROBIN_CLIENT_ID") or None
_NEWSPAPER_SOURCE_ID: Optional[str] = os.environ.get("NEWSPAPER_SOURCE_ID") or None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(value) -> Optional[str]:
    """Serialise *value* to a JSON string, returning None on failure."""
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return None


def _title_from_article(article_match: dict) -> str:
    """Return a headline or the first 100 chars of body text as a title."""
    headline = article_match.get("headline") or article_match.get("article_headline")
    if headline and headline.strip():
        return headline.strip()[:500]
    text = article_match.get("full_text") or article_match.get("article_text") or ""
    return text.strip()[:100]


def _keywords_from_matches(article_match: dict) -> list[str]:
    """Extract unique matched keyword strings from keyword_matches list."""
    matches = article_match.get("keyword_matches", [])
    seen: set[str] = set()
    result: list[str] = []
    for m in matches:
        kw = m.get("keyword", "")
        if kw and kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result


def _best_score(article_match: dict) -> float:
    matches = article_match.get("keyword_matches", [])
    if not matches:
        return 0.0
    return max(m.get("score", 0.0) for m in matches)


# ---------------------------------------------------------------------------
# SupabaseWriter
# ---------------------------------------------------------------------------

class SupabaseWriter:
    """Persist extraction results to Supabase Storage and Database."""

    def __init__(self, client: Client = _client) -> None:
        self._client = client
        self._bucket_ensured = False          # guard: check bucket only once
        self._source_id_cache: dict[str, str] = {}  # source_name → source_id

    # ------------------------------------------------------------------
    # Source auto-provisioning
    # ------------------------------------------------------------------

    def _get_or_create_source(self, source_name: str, client_id: str) -> Optional[str]:
        """Return the source_id for *source_name*, creating the row if needed.

        Results are cached in-process so repeated jobs for the same newspaper
        do not re-query Supabase.
        """
        cache_key = f"{client_id}:{source_name}"
        if cache_key in self._source_id_cache:
            return self._source_id_cache[cache_key]

        try:
            # Look for an existing source with this name under the client.
            res = (
                self._client.table("sources")
                .select("id")
                .eq("client_id", client_id)
                .eq("name", source_name)
                .limit(1)
                .execute()
            )
            if res.data:
                source_id: str = res.data[0]["id"]
                log.debug(
                    f"[get_or_create_source] Found existing source "
                    f"id={source_id!r} for {source_name!r}"
                )
            else:
                # Create a new source record for this newspaper.
                create_res = (
                    self._client.table("sources")
                    .insert({
                        "client_id":   client_id,
                        "name":        source_name,
                        "url":         f"https://newspaper-intel/{source_name.lower().replace(' ', '-')}",
                        "source_type": "pdf", # must match sources_source_type_check
                        "is_active":   True,
                        "tier":        3,
                    })
                    .execute()
                )
                source_id = create_res.data[0]["id"]
                log.info(
                    f"[get_or_create_source] Created source id={source_id!r} "
                    f"for newspaper {source_name!r}"
                )

            self._source_id_cache[cache_key] = source_id
            return source_id

        except Exception as exc:
            log.warning(
                f"[get_or_create_source] Could not get/create source for "
                f"{source_name!r}: {exc}"
            )
            return None

    # ------------------------------------------------------------------
    # Bucket bootstrap
    # ------------------------------------------------------------------

    def _ensure_bucket(self) -> None:
        """Create the crops bucket if it doesn't already exist."""
        if self._bucket_ensured:
            return
        try:
            buckets = self._client.storage.list_buckets()
            existing = {b.name for b in buckets}
            if _CROPS_BUCKET not in existing:
                self._client.storage.create_bucket(
                    _CROPS_BUCKET,
                    options={"public": True},
                )
                log.info(f"[SupabaseWriter] Created storage bucket: {_CROPS_BUCKET!r}")
            self._bucket_ensured = True
        except Exception as exc:
            # Non-fatal — the pipeline can continue without image crops.
            log.warning(f"[SupabaseWriter] Could not verify/create bucket: {exc}")

    # ------------------------------------------------------------------
    # 1. Upload article crop image
    # ------------------------------------------------------------------

    async def upload_image_crop(
        self,
        image_bytes: bytes,
        job_id: str,
        article_index: int,
    ) -> Optional[str]:
        """Upload a JPEG crop to Supabase Storage and return its public URL.

        Args:
            image_bytes:   Raw JPEG bytes from ArticleCropper.
            job_id:        RQ job ID used as the storage folder name.
            article_index: 0-based article index within the job.

        Returns:
            Public URL string, or None if the upload failed.
        """
        self._ensure_bucket()

        file_path = f"{job_id}/article_{article_index}.jpg"
        log.debug(f"[upload_image_crop] Uploading {len(image_bytes) / 1024:.1f} KB → {file_path}")

        try:
            self._client.storage.from_(_CROPS_BUCKET).upload(
                path=file_path,
                file=image_bytes,
                file_options={
                    "content-type": "image/jpeg",
                    "upsert": "true",       # overwrite if job is retried
                },
            )
            public_url = (
                self._client.storage.from_(_CROPS_BUCKET).get_public_url(file_path)
            )
            log.info(f"[upload_image_crop] Uploaded → {public_url}")
            return public_url

        except Exception as exc:
            log.warning(
                f"[upload_image_crop] Upload failed for {file_path!r}: {exc}. "
                "Continuing without image crop."
            )
            return None

    # ------------------------------------------------------------------
    # 2. Write a single matched article to content_items
    # ------------------------------------------------------------------

    async def write_article(
        self,
        article_match: dict,
        brief_id: str,
        source_name: str,
        pdf_url: str,
        job_id: str,
        client_id: Optional[str] = None,
    ) -> Optional[str]:
        """Upsert one matched article into the ``content_items`` table.

        Args:
            article_match: Article dict enriched with keyword_matches and
                           optionally image_crop_url.
            brief_id:      ROBIN brief ID for tracing.
            source_name:   Newspaper name.
            pdf_url:       URL of the source PDF.
            job_id:        RQ job ID.

        Returns:
            The ``id`` of the inserted/updated row, or None on failure.
        """
        title = _title_from_article(article_match)
        content = (
            article_match.get("full_text")
            or article_match.get("article_text")
            or ""
        ).strip()
        language = (
            article_match.get("language_detected")
            or article_match.get("language")
            or "en"
        )
        image_crop_url = article_match.get("image_crop_url")
        page_number = article_match.get("page_number")
        bbox = article_match.get("bounding_box")
        extraction_method = article_match.get("extraction_method", "unknown")
        keyword_score = _best_score(article_match)
        matched_keywords = _keywords_from_matches(article_match)

        # type_metadata mirrors the existing ROBIN schema — JSON blob for
        # newspaper-specific fields that have no dedicated column.
        type_metadata = {
            "newspaper":         source_name,
            "language":          language,
            "fuzzy_score":       round(keyword_score * 100) if keyword_score <= 1.0 else int(keyword_score),
            "source_type":       "newspaper-intel-service",
            "extracted_by":      "newspaper-intel-service",
            "bounding_box":      bbox,
            "page_number":       page_number,
            "extraction_method": extraction_method,
            "job_id":            job_id,
            "image_crop_url":    image_crop_url,
            "keyword_score":     keyword_score,
            "brief_id":          brief_id,
            "keyword_matches":   article_match.get("keyword_matches", []),
        }

        # Stable content fingerprints — required by ROBIN's dedup logic.
        content_hash = hashlib.md5(content.encode("utf-8", errors="replace")).hexdigest()
        title_hash   = hashlib.md5(title.encode("utf-8", errors="replace")).hexdigest()

        row: dict = {
            "title":            title,
            "content":          content,
            "url":              pdf_url,
            "content_type":     "newspaper",  # migration-008 added this to the CHECK constraint
            "matched_keywords": matched_keywords,
            "language":         language,
            "type_metadata":    type_metadata,
            "content_hash":     content_hash,
            "title_hash":       title_hash,
            "published_at":     _now_iso(),
            "source_tier":      3,          # tier-3 = curated/scraped source
            "is_tagged":        False,
            "analysis_status":  "pending",
        }

        # Resolve client_id: request param > env var
        resolved_client_id = client_id or _ROBIN_CLIENT_ID or None

        if resolved_client_id:
            row["client_id"] = resolved_client_id
            # Resolve or auto-create a source record for this newspaper.
            resolved_source_id = (
                _NEWSPAPER_SOURCE_ID
                or self._get_or_create_source(source_name, resolved_client_id)
            )
            if resolved_source_id:
                row["source_id"] = resolved_source_id
        else:
            log.warning(
                "[write_article] No client_id available. "
                "Set ROBIN_CLIENT_ID in .env or pass client_id in the request. "
                "Skipping content_items insert to avoid NOT NULL violation."
            )
            return None

        try:
            response = (
                self._client.table(_CONTENT_TABLE)
                .insert(row)
                .execute()
            )
            record_id: str = response.data[0]["id"]
            log.info(
                f"[write_article] Inserted content_items id={record_id!r} | "
                f"page={page_number} | score={keyword_score:.3f} | "
                f"keywords={matched_keywords}"
            )
            return record_id

        except Exception as exc:
            log.error(
                f"[write_article] Insert failed for brief={brief_id!r}, "
                f"page={page_number}: {exc}"
            )
            return None

    # ------------------------------------------------------------------
    # 3. Write a batch of matched articles
    # ------------------------------------------------------------------

    async def write_batch(
        self,
        articles: list[dict],
        brief_id: str,
        source_name: str,
        pdf_url: str,
        job_id: str,
    ) -> list[str]:
        """Persist multiple matched articles, tolerating individual failures.

        Args:
            articles:    List of article dicts from KeywordMatcher.find_in_articles().
            brief_id:    ROBIN brief ID.
            source_name: Newspaper name.
            pdf_url:     Source PDF URL.
            job_id:      RQ job ID.

        Returns:
            List of successfully inserted record IDs (may be shorter than
            *articles* if some writes failed).
        """
        inserted_ids: list[str] = []

        for article in articles:
            record_id = await self.write_article(
                article_match=article,
                brief_id=brief_id,
                source_name=source_name,
                pdf_url=pdf_url,
                job_id=job_id,
            )
            if record_id:
                inserted_ids.append(record_id)

        log.info(
            f"[write_batch] {len(inserted_ids)}/{len(articles)} articles persisted "
            f"for job={job_id!r}, brief={brief_id!r}"
        )
        return inserted_ids

    # ------------------------------------------------------------------
    # 4. Upsert job status in newspaper_jobs
    # ------------------------------------------------------------------

    async def update_job_status(
        self,
        job_id: str,
        status: str,
        brief_id: Optional[str] = None,
        source_name: Optional[str] = None,
        pdf_url: Optional[str] = None,
        result: Optional[dict] = None,
        error: Optional[str] = None,
    ) -> None:
        """Create or update a row in ``newspaper_jobs``.

        Safe to call at any pipeline stage (queued → processing → completed/failed).
        The ``updated_at`` column is maintained by a database trigger.

        Args:
            job_id:      RQ job identifier (primary key).
            status:      One of queued / processing / completed / failed.
            brief_id:    ROBIN brief ID (populated on first call).
            source_name: Newspaper name.
            pdf_url:     Source PDF URL.
            result:      Full ExtractionResponse-shaped dict (on completion).
            error:       Error message string (on failure).
        """
        row: dict = {
            "job_id":   job_id,
            "status":   status,
        }

        if brief_id is not None:
            row["brief_id"] = brief_id
        if source_name is not None:
            row["source_name"] = source_name
        if pdf_url is not None:
            row["pdf_url"] = pdf_url
        if result is not None:
            row["result"] = result          # Supabase client serialises dicts to JSONB
        if error is not None:
            row["error"] = error

        try:
            (
                self._client.table(_JOBS_TABLE)
                .upsert(row, on_conflict="job_id")
                .execute()
            )
            log.debug(f"[update_job_status] job={job_id!r} → status={status!r}")

        except Exception as exc:
            # Status update failures must never crash the pipeline.
            log.error(
                f"[update_job_status] Failed to update job={job_id!r} "
                f"to status={status!r}: {exc}"
            )
