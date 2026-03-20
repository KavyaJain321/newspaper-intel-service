# models/schemas.py
# Pydantic v2 models for the newspaper-intel-service API and internal pipeline.
# Defines request bodies (e.g. PDF submission), response shapes (e.g. job status,
# extracted articles), and internal data structures shared across core modules.

from typing import List, Literal, Optional
from pydantic import BaseModel, Field, HttpUrl, field_validator


# ---------------------------------------------------------------------------
# Inbound request from ROBIN
# ---------------------------------------------------------------------------

class ExtractionRequest(BaseModel):
    """Incoming job request submitted by ROBIN for a newspaper PDF or flipbook."""

    pdf_url: str = Field(
        ...,
        description="URL of the PDF file or flipbook page to process.",
    )
    keywords: List[str] = Field(
        ...,
        min_length=1,
        description="Keywords from the ROBIN brief to search for in extracted text.",
    )
    source_name: str = Field(
        ...,
        description="Human-readable newspaper name, e.g. 'Dainik Jagran'.",
    )
    source_language: Optional[str] = Field(
        default="auto",
        description="BCP-47 language hint for OCR engine selection. "
                    "Accepted values: 'hi', 'or', 'bn', 'en', 'auto'.",
    )
    brief_id: str = Field(
        ...,
        description="ROBIN brief ID used for end-to-end tracing and result routing.",
    )
    fuzzy_threshold: Optional[int] = Field(
        default=75,
        ge=0,
        le=100,
        description="RapidFuzz similarity threshold (0–100) for keyword matching.",
    )
    is_flipbook: Optional[bool] = Field(
        default=False,
        description="Set to True when the URL points to a browser-rendered flipbook "
                    "rather than a direct PDF download.",
    )
    client_id: Optional[str] = Field(
        default=None,
        description="ROBIN client UUID. Required to write results into content_items. "
                    "Falls back to ROBIN_CLIENT_ID env var if not provided.",
    )

    @field_validator("source_language")
    @classmethod
    def validate_language(cls, v: Optional[str]) -> Optional[str]:
        allowed = {"hi", "or", "bn", "en", "auto"}
        if v is not None and v not in allowed:
            raise ValueError(f"source_language must be one of {allowed}, got '{v}'")
        return v

    @field_validator("keywords")
    @classmethod
    def keywords_non_empty_strings(cls, v: List[str]) -> List[str]:
        for kw in v:
            if not kw.strip():
                raise ValueError("keywords must not contain empty or whitespace-only strings")
        return [kw.strip() for kw in v]


# ---------------------------------------------------------------------------
# Article-level match result
# ---------------------------------------------------------------------------

class ArticleMatch(BaseModel):
    """A single article region that matched one or more keywords."""

    keyword_matched: str = Field(
        ...,
        description="The keyword (from the brief) that triggered this match.",
    )
    keyword_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Normalised match confidence: 1.0 = exact, 0.0 = no match.",
    )
    article_text: str = Field(
        ...,
        description="Full extracted text of the matched article region.",
    )
    article_headline: Optional[str] = Field(
        default=None,
        description="Detected headline of the article, if identifiable from layout.",
    )
    page_number: int = Field(
        ...,
        ge=1,
        description="1-based page number within the PDF where the article appears.",
    )
    bounding_box: Optional[List[float]] = Field(
        default=None,
        description="Article region coordinates [x1, y1, x2, y2] in PDF-space points.",
    )
    image_crop_url: Optional[str] = Field(
        default=None,
        description="Supabase Storage public URL for the cropped article image.",
    )
    language_detected: str = Field(
        ...,
        description="ISO 639-1 language code detected in the article text, e.g. 'hi'.",
    )
    extraction_method: Literal["pymupdf", "paddleocr", "easyocr"] = Field(
        ...,
        description="OCR/extraction engine that produced this article's text.",
    )

    @field_validator("bounding_box")
    @classmethod
    def validate_bounding_box(cls, v: Optional[List[float]]) -> Optional[List[float]]:
        if v is not None and len(v) != 4:
            raise ValueError("bounding_box must contain exactly 4 values: [x1, y1, x2, y2]")
        return v


# ---------------------------------------------------------------------------
# Job-level extraction response
# ---------------------------------------------------------------------------

class ExtractionResponse(BaseModel):
    """Full result payload for a completed (or failed) extraction job."""

    job_id: str = Field(..., description="Unique RQ job identifier.")
    status: Literal["queued", "processing", "completed", "failed"] = Field(
        ...,
        description="Current lifecycle state of the extraction job.",
    )
    brief_id: str = Field(..., description="ROBIN brief ID echoed from the request.")
    source_name: str = Field(..., description="Newspaper name echoed from the request.")
    total_matches: int = Field(
        ...,
        ge=0,
        description="Total number of article regions matched across all keywords.",
    )
    articles: List[ArticleMatch] = Field(
        default_factory=list,
        description="List of individual article matches found in the PDF.",
    )
    processing_time_seconds: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Wall-clock time taken to complete the extraction, in seconds.",
    )
    error: Optional[str] = Field(
        default=None,
        description="Human-readable error message if status is 'failed'.",
    )


# ---------------------------------------------------------------------------
# Job status polling response
# ---------------------------------------------------------------------------

class JobStatusResponse(BaseModel):
    """Lightweight response returned while polling a job's status."""

    job_id: str = Field(..., description="Unique RQ job identifier.")
    status: Literal["queued", "processing", "completed", "failed"] = Field(
        ...,
        description="Current lifecycle state of the extraction job.",
    )
    progress: Optional[str] = Field(
        default=None,
        description="Human-readable progress hint, e.g. 'Processing page 3 of 12'.",
    )
    result: Optional[ExtractionResponse] = Field(
        default=None,
        description="Full extraction result, populated only when status is 'completed' or 'failed'.",
    )


# ---------------------------------------------------------------------------
# Source registry entry
# ---------------------------------------------------------------------------

class SourceConfig(BaseModel):
    """Configuration record for a registered newspaper source."""

    name: str = Field(
        ...,
        description="Unique human-readable identifier for the source, e.g. 'Dainik Jagran'.",
    )
    base_url: str = Field(
        ...,
        description="Root URL of the newspaper's website.",
    )
    pdf_url_pattern: Optional[str] = Field(
        default=None,
        description="URL template with date placeholders for constructing daily PDF URLs, "
                    "e.g. 'https://example.com/epaper/{date}.pdf'.",
    )
    is_flipbook: bool = Field(
        ...,
        description="True if the source serves pages via a browser-rendered flipbook viewer.",
    )
    language: str = Field(
        ...,
        description="Primary publication language as a BCP-47 code, e.g. 'hi', 'en', 'bn'.",
    )
    scraper_type: Literal["direct_pdf", "flipbook", "html_article"] = Field(
        ...,
        description=(
            "Fetching strategy for this source: "
            "'direct_pdf' — download PDF via HTTP; "
            "'flipbook' — render via Playwright; "
            "'html_article' — scrape individual HTML article pages."
        ),
    )
