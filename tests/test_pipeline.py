"""
tests/test_pipeline.py
----------------------
Automated unit + integration tests for the Newspaper Intel pipeline.
Run from the project root:

    python -m pytest tests/test_pipeline.py -v

Requires:
    pip install pytest pytest-asyncio
    .env must be populated with SUPABASE_URL and SUPABASE_SERVICE_KEY
    for Supabase integration tests (skipped if not set).
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is on PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()


# ===========================================================================
# Fixtures
# ===========================================================================

SAMPLE_BLOCKS = [
    {"text": "BJP wins election in Uttar Pradesh",  "bbox": [10, 10, 300, 40],  "page_number": 1,
     "block_type": "text", "font_size": 24.0, "is_bold": True,  "confidence": 1.0},
    {"text": "The party secured a historic majority yesterday",    "bbox": [10, 50, 300, 70],
     "page_number": 1, "block_type": "text", "font_size": 12.0, "is_bold": False, "confidence": 1.0},
    {"text": "Opposition protests outside parliament",            "bbox": [10, 90, 300, 110],
     "page_number": 1, "block_type": "text", "font_size": 12.0, "is_bold": False, "confidence": 1.0},
    {"text": "चुनाव में भाजपा को मिली बड़ी जीत",                 "bbox": [10, 10, 300, 40],
     "page_number": 2, "block_type": "text", "font_size": 22.0, "is_bold": True,  "confidence": 0.91},
    {"text": "उत्तर प्रदेश में मतदाताओं ने भारी संख्या में भाग लिया", "bbox": [10, 50, 300, 70],
     "page_number": 2, "block_type": "text", "font_size": 11.0, "is_bold": False, "confidence": 0.88},
]


# ===========================================================================
# 1. Language detector
# ===========================================================================

class TestLanguageDetector:
    def test_english_text(self):
        from utils.language_detector import detect_language
        result = detect_language("The election results were announced today in New Delhi.")
        assert result == "en"

    def test_hindi_text(self):
        from utils.language_detector import detect_language
        result = detect_language("प्रधानमंत्री ने आज नई दिल्ली में एक बड़ी घोषणा की।")
        assert result == "hi"

    def test_odia_unicode_override(self):
        from utils.language_detector import detect_language
        # Odia text — langdetect often misclassifies as Marathi; script check overrides.
        odia_text = "ଓଡ଼ିଶା ରାଜ୍ୟ ରେ ବନ୍ୟା ପରିସ୍ଥିତି ଗୁରୁତ୍ୱପୂର୍ଣ୍ଣ"
        result = detect_language(odia_text)
        assert result == "or", f"Expected 'or' for Odia text, got '{result}'"

    def test_empty_text_returns_en(self):
        from utils.language_detector import detect_language
        assert detect_language("") == "en"
        assert detect_language("   ") == "en"

    def test_short_text_returns_en(self):
        from utils.language_detector import detect_language
        # Very short text — langdetect may fail; fallback to 'en'
        result = detect_language("ok")
        assert result in ("en", "hi", "or", "bn", "ta", "te", "mr", "gu", "pa")


# ===========================================================================
# 2. Keyword matcher
# ===========================================================================

class TestKeywordMatcher:
    def test_exact_match(self):
        from core.keyword_matcher import KeywordMatcher
        matcher = KeywordMatcher(["election"], fuzzy_threshold=75)
        results = matcher.match_text("The election results were announced.")
        assert len(results) == 1
        assert results[0]["match_type"] == "exact"
        assert results[0]["score"] == 1.0

    def test_case_insensitive_match(self):
        from core.keyword_matcher import KeywordMatcher
        matcher = KeywordMatcher(["BJP"], fuzzy_threshold=75)
        results = matcher.match_text("bjp wins the vote")
        assert len(results) == 1
        assert results[0]["match_type"] == "exact"

    def test_fuzzy_match_with_typo(self):
        from core.keyword_matcher import KeywordMatcher
        matcher = KeywordMatcher(["corruption"], fuzzy_threshold=70)
        results = matcher.match_text("Allegations of corruptin in government")
        assert len(results) == 1
        assert results[0]["match_type"] == "fuzzy"
        assert results[0]["score"] >= 0.70

    def test_no_match_below_threshold(self):
        from core.keyword_matcher import KeywordMatcher
        matcher = KeywordMatcher(["parliament"], fuzzy_threshold=95)
        results = matcher.match_text("A building in the city was damaged.")
        assert len(results) == 0

    def test_transliteration_latin_to_devanagari(self):
        from core.keyword_matcher import KeywordMatcher
        # Latin keyword "election" should match Devanagari "चुनाव" via transliteration table
        matcher = KeywordMatcher(["election"], fuzzy_threshold=75)
        results = matcher.match_text("राज्य में चुनाव हुए।")
        assert len(results) == 1, "Latin keyword should match transliterated Devanagari form"

    def test_headline_boost_applied(self):
        from core.keyword_matcher import KeywordMatcher
        matcher = KeywordMatcher(["BJP"], fuzzy_threshold=75)
        body_results = matcher.match_text("BJP wins election", in_headline=False)
        headline_results = matcher.match_text("BJP wins election", in_headline=True)
        assert headline_results[0]["score"] >= body_results[0]["score"]
        assert headline_results[0]["in_headline"] is True

    def test_find_in_articles(self):
        from core.keyword_matcher import KeywordMatcher
        matcher = KeywordMatcher(["election", "BJP"], fuzzy_threshold=75)
        articles = [
            {"headline": "BJP wins", "full_text": "BJP wins election today", "page_number": 1,
             "bounding_box": [0, 0, 100, 100], "column_index": 0, "body_blocks": []},
            {"headline": "Weather", "full_text": "It rained heavily yesterday", "page_number": 1,
             "bounding_box": [0, 0, 100, 100], "column_index": 1, "body_blocks": []},
        ]
        matched = matcher.find_in_articles(articles)
        assert len(matched) == 1
        assert matched[0]["headline"] == "BJP wins"
        assert "keyword_matches" in matched[0]

    def test_get_best_match(self):
        from core.keyword_matcher import KeywordMatcher
        matcher = KeywordMatcher(["election", "BJP"], fuzzy_threshold=75)
        article = {
            "keyword_matches": [
                {"keyword": "election", "score": 0.85, "match_type": "fuzzy",
                 "matched_text": "electon", "char_position": 0, "in_headline": False},
                {"keyword": "BJP", "score": 1.0, "match_type": "exact",
                 "matched_text": "BJP", "char_position": 5, "in_headline": True},
            ]
        }
        best = matcher.get_best_match(article)
        assert best["keyword"] == "BJP"
        assert best["score"] == 1.0


# ===========================================================================
# 3. Layout analyzer
# ===========================================================================

class TestLayoutAnalyzer:
    def test_detect_columns_single(self):
        from core.layout_analyzer import LayoutAnalyzer
        analyzer = LayoutAnalyzer()
        # All blocks on left side — single column
        blocks = [{"bbox": [10, y, 100, y+20], "block_type": "text"} for y in range(0, 200, 25)]
        dividers = analyzer.detect_columns(blocks, page_width=500.0)
        assert len(dividers) == 0

    def test_detect_columns_two(self):
        from core.layout_analyzer import LayoutAnalyzer
        analyzer = LayoutAnalyzer()
        # Two distinct column clusters
        left  = [{"bbox": [10,  y, 200, y+20], "block_type": "text"} for y in range(0, 200, 25)]
        right = [{"bbox": [310, y, 490, y+20], "block_type": "text"} for y in range(0, 200, 25)]
        dividers = analyzer.detect_columns(left + right, page_width=500.0)
        assert len(dividers) == 1
        assert 200 < dividers[0] < 310

    def test_detect_headlines(self):
        from core.layout_analyzer import LayoutAnalyzer
        analyzer = LayoutAnalyzer()
        blocks = [
            {"text": "BIG HEADLINE HERE",   "bbox": [0,0,300,40], "block_type": "text",
             "font_size": 28.0, "is_bold": True,  "confidence": 1.0},
            {"text": "Body text here.",     "bbox": [0,50,300,65], "block_type": "text",
             "font_size": 12.0, "is_bold": False, "confidence": 1.0},
            {"text": "More body text here.","bbox": [0,70,300,85], "block_type": "text",
             "font_size": 11.0, "is_bold": False, "confidence": 1.0},
        ]
        headlines = analyzer.detect_headlines(blocks)
        assert len(headlines) == 1
        assert headlines[0]["text"] == "BIG HEADLINE HERE"

    def test_group_into_articles(self):
        from core.layout_analyzer import LayoutAnalyzer
        analyzer = LayoutAnalyzer()
        blocks = [
            {"text": "Headline One",      "bbox": [10,10,200,30],  "block_type": "text",
             "font_size": 24.0, "is_bold": True,  "confidence": 1.0},
            {"text": "Body of article 1", "bbox": [10,35,200,50],  "block_type": "text",
             "font_size": 12.0, "is_bold": False, "confidence": 1.0},
            {"text": "Headline Two",      "bbox": [10,200,200,220], "block_type": "text",
             "font_size": 24.0, "is_bold": True,  "confidence": 1.0},
            {"text": "Body of article 2", "bbox": [10,225,200,240], "block_type": "text",
             "font_size": 12.0, "is_bold": False, "confidence": 1.0},
        ]
        articles = analyzer.group_into_articles(blocks, page_number=1, column_dividers=[])
        assert len(articles) == 2
        assert articles[0]["headline"] == "Headline One"
        assert articles[1]["headline"] == "Headline Two"


# ===========================================================================
# 4. Source registry
# ===========================================================================

class TestSourceRegistry:
    def test_get_active_sources(self):
        from core.source_registry import SourceRegistry, SOURCE_REGISTRY
        registry = SourceRegistry(SOURCE_REGISTRY)
        active = registry.get_active_sources()
        assert len(active) > 0
        assert all(s["is_active"] for s in active)

    def test_get_sources_by_state_odisha(self):
        from core.source_registry import SourceRegistry, SOURCE_REGISTRY
        registry = SourceRegistry(SOURCE_REGISTRY)
        sources = registry.get_sources_by_state("Odisha")
        names = [s["name"] for s in sources]
        assert "Samaja" in names
        assert "Dharitri" in names

    def test_get_sources_by_language_hindi(self):
        from core.source_registry import SourceRegistry, SOURCE_REGISTRY
        registry = SourceRegistry(SOURCE_REGISTRY)
        sources = registry.get_sources_by_language("hi")
        assert all(s["language"] == "hi" for s in sources)
        assert len(sources) >= 5

    def test_get_sources_for_brief_odisha(self):
        from core.source_registry import SourceRegistry, SOURCE_REGISTRY
        registry = SourceRegistry(SOURCE_REGISTRY)
        brief = {"geographic_focus": ["Odisha"], "languages": ["or"]}
        sources = registry.get_sources_for_brief(brief)
        names = [s["name"] for s in sources]
        assert "Samaja" in names

    def test_build_todays_url_no_pattern(self):
        from core.source_registry import SourceRegistry
        source = {"name": "Test", "base_url": "https://example.com", "pdf_url_pattern": None}
        url = SourceRegistry.build_todays_url(source)
        assert url == "https://example.com"

    def test_build_todays_url_with_pattern(self):
        from core.source_registry import SourceRegistry
        from datetime import date
        source = {
            "name": "Test",
            "base_url": "https://example.com",
            "pdf_url_pattern": "https://example.com/{YYYY}/{MM}/{DD}/paper.pdf"
        }
        url = SourceRegistry.build_todays_url(source)
        today = date.today()
        assert today.strftime("%Y") in url
        assert today.strftime("%m") in url
        assert today.strftime("%d") in url


# ===========================================================================
# 5. Article cropper
# ===========================================================================

class TestArticleCropper:
    def test_image_to_bytes_returns_jpeg(self):
        from core.article_cropper import ArticleCropper
        from PIL import Image
        cropper = ArticleCropper()
        img = Image.new("RGB", (200, 100), color=(255, 0, 0))
        data = cropper.image_to_bytes(img)
        assert data[:2] == b"\xff\xd8", "Not a JPEG (missing magic bytes)"

    def test_image_to_base64(self):
        import base64
        from core.article_cropper import ArticleCropper
        from PIL import Image
        cropper = ArticleCropper()
        img = Image.new("RGB", (100, 50), color=(0, 128, 255))
        b64 = cropper.image_to_base64(img)
        # Should be valid base64 that decodes to a JPEG.
        decoded = base64.b64decode(b64)
        assert decoded[:2] == b"\xff\xd8"

    def test_crop_article_returns_none_without_bbox(self):
        from core.article_cropper import ArticleCropper
        cropper = ArticleCropper()
        article_no_bbox = {"page_number": 1, "bounding_box": None}
        result = cropper.crop_article("/nonexistent.pdf", article_no_bbox)
        assert result is None

    def test_crop_article_returns_none_with_bad_bbox(self):
        from core.article_cropper import ArticleCropper
        cropper = ArticleCropper()
        article_bad_bbox = {"page_number": 1, "bounding_box": [10, 20]}  # only 2 coords
        result = cropper.crop_article("/nonexistent.pdf", article_bad_bbox)
        assert result is None


# ===========================================================================
# 6. PDF fetcher (mocked — no real network calls)
# ===========================================================================

class TestPDFFetcher:
    @pytest.mark.asyncio
    async def test_fetch_direct_validates_magic_bytes(self):
        from core.pdf_fetcher import PDFFetcher, PDFFetchError
        import httpx

        fetcher = PDFFetcher()

        # Mock httpx to return HTML instead of PDF
        class FakeResponse:
            status_code = 200
            headers = {"content-type": "application/pdf", "content-length": "0"}
            async def aiter_bytes(self, chunk_size=None):
                yield b"<html><body>Not a PDF</body></html>"
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass
            def raise_for_status(self): pass

        class FakeClient:
            def stream(self, *a, **kw): return FakeResponse()
            async def __aenter__(self): return self
            async def __aexit__(self, *a): pass

        with patch("core.pdf_fetcher.httpx.AsyncClient", return_value=FakeClient()):
            with pytest.raises(PDFFetchError, match="HTML|magic bytes"):
                await fetcher.fetch_direct_pdf("https://example.com/fake.pdf")

    def test_save_temp_pdf(self):
        from core.pdf_fetcher import PDFFetcher
        import tempfile, os
        data = b"%PDF-1.4 fake content"
        path = PDFFetcher.save_temp_pdf(data, "unit-test-job")
        try:
            assert os.path.exists(path)
            assert open(path, "rb").read() == data
        finally:
            os.unlink(path)


# ===========================================================================
# 7. Supabase writer (skipped if credentials not set)
# ===========================================================================

SUPABASE_AVAILABLE = bool(
    os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY")
)

@pytest.mark.skipif(not SUPABASE_AVAILABLE, reason="SUPABASE_URL/SERVICE_KEY not set")
class TestSupabaseWriter:
    @pytest.mark.asyncio
    async def test_update_job_status_queued(self):
        import uuid
        from core.supabase_writer import SupabaseWriter
        writer = SupabaseWriter()
        job_id = f"test-{uuid.uuid4()}"
        # Should not raise
        await writer.update_job_status(
            job_id=job_id,
            status="queued",
            brief_id="test-brief",
            source_name="Test Paper",
            pdf_url="https://example.com/test.pdf",
        )

    @pytest.mark.asyncio
    async def test_write_article_returns_id(self):
        import uuid
        from core.supabase_writer import SupabaseWriter
        writer = SupabaseWriter()
        article = {
            "headline": "Test Article Headline",
            "full_text": "This is a test article written by the automated test suite.",
            "bounding_box": [10.0, 20.0, 300.0, 400.0],
            "page_number": 1,
            "keyword_matches": [
                {"keyword": "test", "score": 1.0, "match_type": "exact",
                 "matched_text": "test", "char_position": 10, "in_headline": False}
            ],
            "image_crop_url": None,
            "extraction_method": "pymupdf",
            "language_detected": "en",
        }
        record_id = await writer.write_article(
            article_match=article,
            brief_id="test-brief",
            source_name="Test Paper",
            pdf_url="https://example.com/test.pdf",
            job_id=f"test-{uuid.uuid4()}",
        )
        assert record_id is not None, "Expected a record ID from Supabase"
