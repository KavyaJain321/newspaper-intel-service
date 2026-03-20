# core/extractor.py
# Orchestrates the full OCR extraction pipeline for a newspaper PDF.
# Renders each page to an image, runs it through the configured OCR engine
# (PaddleOCR, EasyOCR, or Surya), filters results by confidence threshold,
# and returns structured text blocks with bounding boxes per page.

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

import fitz  # PyMuPDF
from PIL import Image

from utils.logger import log

if TYPE_CHECKING:
    from paddleocr import PaddleOCR as _PaddleOCRType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_OCR_CONFIDENCE_THRESHOLD = float(os.getenv("OCR_CONFIDENCE_THRESHOLD", "0.6"))
_PDF2IMAGE_DPI = 300  # kept for naming consistency; used by the PyMuPDF renderer below
# Minimum number of blocks for PaddleOCR output to be considered usable.
_PADDLE_MIN_BLOCKS = 10
# How many pages to sample when checking for a text layer.
_TEXT_LAYER_SAMPLE_PAGES = 3
_TEXT_LAYER_MIN_CHARS = 50

# ---------------------------------------------------------------------------
# Page renderer — uses PyMuPDF so poppler is NOT required
# ---------------------------------------------------------------------------

def _render_pdf_pages(pdf_path: str, dpi: int = _PDF2IMAGE_DPI) -> list[Image.Image]:
    """Render every page of *pdf_path* to a PIL RGB Image using PyMuPDF.

    Replaces pdf2image / poppler entirely.  DPI controls the output resolution;
    300 DPI gives good OCR accuracy without excessive memory usage.
    """
    images: list[Image.Image] = []
    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        log.error(f"[render_pdf_pages] Cannot open {pdf_path!r}: {exc}")
        return images

    scale = dpi / 72.0            # PyMuPDF's native unit is 72 DPI (points)
    matrix = fitz.Matrix(scale, scale)

    with doc:
        for page_index in range(len(doc)):
            try:
                page = doc[page_index]
                pix = page.get_pixmap(matrix=matrix, alpha=False)
                img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                images.append(img)
            except Exception as exc:
                log.warning(
                    f"[render_pdf_pages] Page {page_index + 1} render failed: {exc}"
                )

    log.debug(
        f"[render_pdf_pages] Rendered {len(images)} page(s) at {dpi} DPI "
        f"from {pdf_path!r}"
    )
    return images


# ---------------------------------------------------------------------------
# Lazy PaddleOCR instance cache
# Key: language code string → PaddleOCR instance
# Initialised on first use so service startup is not delayed.
# ---------------------------------------------------------------------------

_paddle_cache: dict[str, "_PaddleOCRType"] = {}


def _get_paddle(language: str) -> "_PaddleOCRType":
    """Return a cached PaddleOCR instance for *language*, creating it if needed."""
    if language not in _paddle_cache:
        log.info(f"[extractor] Initialising PaddleOCR for language={language!r} (first use)")
        from paddleocr import PaddleOCR  # deferred import — heavy startup cost

        _paddle_cache[language] = PaddleOCR(
            use_angle_cls=True,
            lang=language,
            use_gpu=False,
            show_log=False,
        )
        log.debug(f"[extractor] PaddleOCR ready for language={language!r}")
    return _paddle_cache[language]


# ---------------------------------------------------------------------------
# Block schema
# ---------------------------------------------------------------------------

def _make_block(
    text: str,
    bbox: list[float],
    page_number: int,
    block_type: str = "text",
    font_size: float = 0.0,
    is_bold: bool = False,
    confidence: float = 1.0,
) -> dict:
    """Return a normalised text-block dict shared by all extraction methods."""
    return {
        "text": text.strip(),
        "bbox": bbox,                  # [x1, y1, x2, y2]
        "page_number": page_number,
        "block_type": block_type,      # "text" | "image"
        "font_size": font_size,
        "is_bold": is_bold,
        "confidence": confidence,
    }


# ---------------------------------------------------------------------------
# EasyOCR language mapping
# PaddleOCR uses single-code strings; EasyOCR uses lists of codes.
# ---------------------------------------------------------------------------

_EASYOCR_LANG_MAP: dict[str, list[str]] = {
    "hi":  ["hi", "en"],
    "or":  ["en"],        # EasyOCR has no Odia model; fall back to English
    "bn":  ["bn", "en"],
    "ta":  ["ta", "en"],
    "te":  ["te", "en"],
    "mr":  ["hi", "en"],  # Marathi uses Devanagari — hi model is closest
    "gu":  ["gu", "en"],
    "pa":  ["en"],        # Punjabi/Gurmukhi support is limited in EasyOCR
    "ml":  ["en"],        # Malayalam not supported; degrade to English
    "en":  ["en"],
}


def _easyocr_langs(language: str) -> list[str]:
    return _EASYOCR_LANG_MAP.get(language, ["en"])


# ---------------------------------------------------------------------------
# TextExtractor
# ---------------------------------------------------------------------------

class TextExtractor:
    """Three-engine extraction pipeline: PyMuPDF → PaddleOCR → EasyOCR."""

    # ------------------------------------------------------------------
    # 1. PyMuPDF (digital text layer)
    # ------------------------------------------------------------------

    def extract_with_pymupdf(self, pdf_path: str) -> list[dict]:
        """Extract text blocks from a PDF's built-in text layer via PyMuPDF.

        Returns:
            List of normalised block dicts. Empty list if the PDF has no text.
        """
        blocks: list[dict] = []

        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            log.error(f"[pymupdf] Failed to open {pdf_path!r}: {exc}")
            return blocks

        with doc:
            for page_index in range(len(doc)):
                page = doc[page_index]
                page_number = page_index + 1

                try:
                    raw = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
                except Exception as exc:
                    log.warning(f"[pymupdf] page {page_number} get_text failed: {exc}")
                    continue

                for block in raw.get("blocks", []):
                    btype = block.get("type", 0)

                    if btype == 1:
                        # Image block — record position, no text
                        b = block.get("bbox", [0, 0, 0, 0])
                        blocks.append(_make_block(
                            text="",
                            bbox=list(b),
                            page_number=page_number,
                            block_type="image",
                        ))
                        continue

                    # Text block — aggregate spans to get font metrics
                    lines = block.get("lines", [])
                    block_text_parts: list[str] = []
                    max_font_size: float = 0.0
                    any_bold = False

                    for line in lines:
                        for span in line.get("spans", []):
                            span_text = span.get("text", "")
                            if span_text.strip():
                                block_text_parts.append(span_text)
                            size = span.get("size", 0.0)
                            if size > max_font_size:
                                max_font_size = size
                            flags = span.get("flags", 0)
                            # PyMuPDF bold flag is bit 4 (value 16)
                            if flags & 16:
                                any_bold = True

                    full_text = " ".join(block_text_parts).strip()
                    if not full_text:
                        continue

                    b = block.get("bbox", [0, 0, 0, 0])
                    blocks.append(_make_block(
                        text=full_text,
                        bbox=list(b),
                        page_number=page_number,
                        block_type="text",
                        font_size=round(max_font_size, 2),
                        is_bold=any_bold,
                        confidence=1.0,
                    ))

        log.info(f"[pymupdf] Extracted {len(blocks)} blocks from {pdf_path!r}")
        return blocks

    # ------------------------------------------------------------------
    # 2. Text-layer presence check
    # ------------------------------------------------------------------

    def has_text_layer(self, pdf_path: str) -> bool:
        """Return True if the PDF contains a usable digital text layer.

        Samples up to the first ``_TEXT_LAYER_SAMPLE_PAGES`` pages and checks
        whether any of them contain more than ``_TEXT_LAYER_MIN_CHARS`` chars.
        """
        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            log.warning(f"[has_text_layer] Cannot open {pdf_path!r}: {exc}")
            return False

        with doc:
            pages_to_check = min(_TEXT_LAYER_SAMPLE_PAGES, len(doc))
            for i in range(pages_to_check):
                text = doc[i].get_text("text")
                if len(text.strip()) >= _TEXT_LAYER_MIN_CHARS:
                    log.debug(
                        f"[has_text_layer] Page {i + 1} has {len(text.strip())} chars "
                        "— text layer confirmed"
                    )
                    return True

        log.debug(f"[has_text_layer] No usable text layer in {pdf_path!r}")
        return False

    # ------------------------------------------------------------------
    # 3. PaddleOCR
    # ------------------------------------------------------------------

    def extract_with_paddleocr(
        self, pdf_path: str, language: str = "en"
    ) -> list[dict]:
        """Run PaddleOCR on rasterised PDF pages.

        Args:
            pdf_path: Path to the PDF file.
            language:  PaddleOCR language code (e.g. "hi", "en", "bn").

        Returns:
            List of normalised block dicts.
        """
        log.info(f"[paddleocr] Starting OCR: lang={language!r}, file={pdf_path!r}")
        blocks: list[dict] = []

        images = _render_pdf_pages(pdf_path, dpi=_PDF2IMAGE_DPI)
        if not images:
            log.error(f"[paddleocr] Page rendering returned no images for {pdf_path!r}")
            return blocks

        ocr = _get_paddle(language)

        for page_index, image in enumerate(images):
            page_number = page_index + 1
            import numpy as np
            img_array = np.array(image)

            try:
                result = ocr.ocr(img_array, cls=True)
            except Exception as exc:
                log.warning(f"[paddleocr] page {page_number} OCR failed: {exc}")
                continue

            if not result or result == [None]:
                log.debug(f"[paddleocr] page {page_number}: no detections")
                continue

            # result is List[List[line]] where line = [bbox_points, (text, score)]
            for page_result in result:
                if not page_result:
                    continue
                for line in page_result:
                    if not line or len(line) < 2:
                        continue
                    bbox_points, text_conf = line
                    text, confidence = text_conf

                    if not text or not text.strip():
                        continue
                    if confidence < _OCR_CONFIDENCE_THRESHOLD:
                        log.debug(
                            f"[paddleocr] page {page_number} skipping low-confidence "
                            f"block ({confidence:.2f} < {_OCR_CONFIDENCE_THRESHOLD}): "
                            f"{text[:40]!r}"
                        )
                        continue

                    # PaddleOCR returns 4 corner points [[x,y]×4]; convert to bbox
                    xs = [pt[0] for pt in bbox_points]
                    ys = [pt[1] for pt in bbox_points]
                    bbox = [min(xs), min(ys), max(xs), max(ys)]

                    blocks.append(_make_block(
                        text=text,
                        bbox=bbox,
                        page_number=page_number,
                        confidence=round(confidence, 4),
                    ))

        log.info(f"[paddleocr] Extracted {len(blocks)} blocks from {pdf_path!r}")
        return blocks

    # ------------------------------------------------------------------
    # 4. EasyOCR (fallback)
    # ------------------------------------------------------------------

    def extract_with_easyocr(
        self, pdf_path: str, language: str = "en"
    ) -> list[dict]:
        """Run EasyOCR on rasterised PDF pages as a fallback engine.

        Args:
            pdf_path: Path to the PDF file.
            language:  Language hint mapped to an EasyOCR language list.

        Returns:
            List of normalised block dicts.
        """
        import easyocr  # deferred — heavy import

        lang_list = _easyocr_langs(language)
        log.info(
            f"[easyocr] Starting OCR: langs={lang_list!r}, file={pdf_path!r}"
        )
        blocks: list[dict] = []

        images = _render_pdf_pages(pdf_path, dpi=_PDF2IMAGE_DPI)
        if not images:
            log.error(f"[easyocr] Page rendering returned no images for {pdf_path!r}")
            return blocks

        # EasyOCR Reader construction is expensive; instantiate once per call.
        try:
            reader = easyocr.Reader(lang_list, gpu=False, verbose=False)
        except Exception as exc:
            log.error(f"[easyocr] Reader initialisation failed: {exc}")
            return blocks

        for page_index, image in enumerate(images):
            page_number = page_index + 1
            import numpy as np
            img_array = np.array(image)

            try:
                result = reader.readtext(img_array)
            except Exception as exc:
                log.warning(f"[easyocr] page {page_number} OCR failed: {exc}")
                continue

            for detection in result:
                # detection = (bbox_points, text, confidence)
                bbox_points, text, confidence = detection

                if not text or not text.strip():
                    continue
                if confidence < _OCR_CONFIDENCE_THRESHOLD:
                    log.debug(
                        f"[easyocr] page {page_number} skipping low-confidence "
                        f"block ({confidence:.2f}): {text[:40]!r}"
                    )
                    continue

                # EasyOCR returns 4 corner points [[x,y]×4]
                xs = [pt[0] for pt in bbox_points]
                ys = [pt[1] for pt in bbox_points]
                bbox = [min(xs), min(ys), max(xs), max(ys)]

                blocks.append(_make_block(
                    text=text,
                    bbox=bbox,
                    page_number=page_number,
                    confidence=round(confidence, 4),
                ))

        log.info(f"[easyocr] Extracted {len(blocks)} blocks from {pdf_path!r}")
        return blocks

    # ------------------------------------------------------------------
    # 5. Unified orchestrator
    # ------------------------------------------------------------------

    def extract(
        self,
        pdf_path: str,
        source_language: str = "auto",
    ) -> tuple[list[dict], str]:
        """Select and run the best extraction method for this PDF.

        Fallback chain:
            1. PyMuPDF  — if a digital text layer is detected (fast, perfect quality)
            2. PaddleOCR — for scanned/image PDFs
            3. EasyOCR  — if PaddleOCR yields fewer than _PADDLE_MIN_BLOCKS results
                          or raises an exception

        Args:
            pdf_path:        Path to the PDF on disk.
            source_language: Language hint from the source config or request.
                             "auto" defers to PaddleOCR's default model.

        Returns:
            Tuple of ``(blocks, method_used)`` where *method_used* is one of
            ``"pymupdf"``, ``"paddleocr"``, or ``"easyocr"``.
        """
        # Normalise "auto" → "en" for OCR engine initialisation.
        # The caller (keyword_matcher) re-runs language detection on the text.
        ocr_lang = source_language if source_language != "auto" else "en"

        # --- Step 1: try digital text layer ---
        log.info(f"[extract] Checking text layer: {pdf_path!r}")
        if self.has_text_layer(pdf_path):
            log.info("[extract] Text layer present — using PyMuPDF")
            blocks = self.extract_with_pymupdf(pdf_path)
            if blocks:
                return blocks, "pymupdf"
            # Degenerate case: has_text_layer said yes but extraction returned nothing.
            log.warning(
                "[extract] PyMuPDF returned 0 blocks despite text layer — "
                "falling through to OCR"
            )

        # --- Step 2: PaddleOCR ---
        log.info(f"[extract] No text layer (or empty) — trying PaddleOCR (lang={ocr_lang!r})")
        try:
            blocks = self.extract_with_paddleocr(pdf_path, language=ocr_lang)
        except Exception as exc:
            log.warning(f"[extract] PaddleOCR raised an exception: {exc}")
            blocks = []

        if len(blocks) >= _PADDLE_MIN_BLOCKS:
            log.info(f"[extract] PaddleOCR succeeded with {len(blocks)} blocks")
            return blocks, "paddleocr"

        log.warning(
            f"[extract] PaddleOCR returned only {len(blocks)} blocks "
            f"(threshold={_PADDLE_MIN_BLOCKS}) — falling back to EasyOCR"
        )

        # --- Step 3: EasyOCR fallback ---
        log.info(f"[extract] Trying EasyOCR (lang={ocr_lang!r})")
        try:
            blocks = self.extract_with_easyocr(pdf_path, language=ocr_lang)
        except Exception as exc:
            log.error(f"[extract] EasyOCR also failed: {exc}")
            blocks = []

        method = "easyocr" if blocks else "none"
        log.info(f"[extract] EasyOCR returned {len(blocks)} blocks (method={method!r})")
        return blocks, method
