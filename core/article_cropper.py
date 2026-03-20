# core/article_cropper.py
# Crops individual article regions from rendered newspaper page images.
# Uses bounding box data from the layout analyzer to extract sub-images,
# applies padding, and saves or returns cropped article image bytes (PIL/Pillow).

from __future__ import annotations

import base64
import io
from typing import Optional

import fitz  # PyMuPDF
from PIL import Image

from utils.logger import log

# ---------------------------------------------------------------------------
# Notes on coordinate systems
# ---------------------------------------------------------------------------
# PyMuPDF uses PDF user-space coordinates:
#   - Origin at the TOP-LEFT of the page  (fitz.Rect, fitz.Matrix)
#   - Units are PDF points (1 pt = 1/72 inch)
#   - y increases downward  ← this matches PIL, so NO y-flip is needed
#     when using page.get_pixmap() because fitz renders with a top-left origin.
#
# HOWEVER: some upstream sources (e.g. raw pdf.js / mupdf coordinates exported
# from get_text("dict")) store y values in PDF user-space which ALSO has a
# top-left origin in fitz's coordinate frame.  The classic "bottom-left" PDF
# origin only matters when working with raw PDF operators / COS coordinates —
# not when using PyMuPDF's high-level API.
#
# Conclusion: bboxes from both PyMuPDF text extraction AND from fitz.Rect are
# already in the same top-left coordinate frame.  No y-axis flip is needed.
# The scale conversion (points → pixels at the chosen DPI) is the only
# transform required.
#
# Scale factor:  pixels_per_point = dpi / 72
# ---------------------------------------------------------------------------

_JPEG_QUALITY = 85


class ArticleCropper:
    """Render PDF pages and extract article region images."""

    # ------------------------------------------------------------------
    # 1. Render a single PDF page as a PIL Image
    # ------------------------------------------------------------------

    def render_page_as_image(
        self, pdf_path: str, page_number: int, dpi: int = 150
    ) -> Image.Image:
        """Render *page_number* (1-based) from *pdf_path* to a PIL Image.

        Args:
            pdf_path:    Absolute path to the PDF file on disk.
            page_number: 1-based page index.
            dpi:         Render resolution. 150 DPI balances readability and
                         file size well for crop thumbnails.

        Returns:
            RGB PIL Image of the rendered page.

        Raises:
            ValueError:  If page_number is out of range.
            RuntimeError: If fitz cannot open or render the page.
        """
        try:
            doc = fitz.open(pdf_path)
        except Exception as exc:
            raise RuntimeError(
                f"[ArticleCropper] Cannot open PDF {pdf_path!r}: {exc}"
            ) from exc

        with doc:
            if page_number < 1 or page_number > len(doc):
                raise ValueError(
                    f"[ArticleCropper] page_number {page_number} is out of range "
                    f"(PDF has {len(doc)} page(s))"
                )

            page = doc[page_number - 1]   # fitz is 0-indexed

            # Scale matrix: 1 PDF point = 1/72 inch; dpi/72 pixels per point.
            scale = dpi / 72.0
            matrix = fitz.Matrix(scale, scale)

            pixmap = page.get_pixmap(matrix=matrix, alpha=False)

        # Convert raw pixmap bytes → PIL Image (RGB).
        img = Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
        log.debug(
            f"[render_page_as_image] page={page_number} dpi={dpi} "
            f"→ {img.width}×{img.height}px"
        )
        return img

    # ------------------------------------------------------------------
    # 2. Crop a bounding box region from a rendered page image
    # ------------------------------------------------------------------

    def crop_article_region(
        self,
        page_image: Image.Image,
        bounding_box: list[float],
        page_width_pts: float,
        page_height_pts: float,
        padding: int = 20,
        dpi: int = 150,
    ) -> Image.Image:
        """Crop an article region from a rendered page image.

        Coordinate conversion:
            pixel = point × (dpi / 72)

        Both PyMuPDF text-block bboxes and fitz.Rect use a top-left origin
        with y increasing downward, so no axis flip is required — only the
        point-to-pixel scale factor.

        Args:
            page_image:       Full-page PIL Image returned by render_page_as_image.
            bounding_box:     [x1, y1, x2, y2] in PDF points (top-left origin).
            page_width_pts:   Page width in PDF points (used for clamping only).
            page_height_pts:  Page height in PDF points (used for clamping only).
            padding:          Extra pixels added on each side of the crop box.
            dpi:              Must match the dpi used to render page_image.

        Returns:
            Cropped RGB PIL Image.

        Raises:
            ValueError: If bounding_box is malformed or produces a zero-area crop.
        """
        if not bounding_box or len(bounding_box) != 4:
            raise ValueError(
                f"[ArticleCropper] bounding_box must be [x1,y1,x2,y2], "
                f"got: {bounding_box!r}"
            )

        x1_pt, y1_pt, x2_pt, y2_pt = bounding_box

        if x1_pt >= x2_pt or y1_pt >= y2_pt:
            raise ValueError(
                f"[ArticleCropper] Degenerate bounding_box (zero or negative area): "
                f"{bounding_box}"
            )

        scale = dpi / 72.0

        # Convert PDF points → pixel coordinates.
        x1_px = int(x1_pt * scale)
        y1_px = int(y1_pt * scale)
        x2_px = int(x2_pt * scale)
        y2_px = int(y2_pt * scale)

        # Apply padding.
        x1_px -= padding
        y1_px -= padding
        x2_px += padding
        y2_px += padding

        # Clamp to image dimensions (never go out of bounds).
        img_w, img_h = page_image.size
        x1_px = max(0, x1_px)
        y1_px = max(0, y1_px)
        x2_px = min(img_w, x2_px)
        y2_px = min(img_h, y2_px)

        if x1_px >= x2_px or y1_px >= y2_px:
            raise ValueError(
                f"[ArticleCropper] Crop region collapsed to zero area after "
                f"clamping. bbox={bounding_box}, image={img_w}×{img_h}px"
            )

        crop = page_image.crop((x1_px, y1_px, x2_px, y2_px))
        log.debug(
            f"[crop_article_region] bbox_pts={bounding_box} → "
            f"px=({x1_px},{y1_px},{x2_px},{y2_px}) → "
            f"crop={crop.width}×{crop.height}px (padding={padding})"
        )
        return crop

    # ------------------------------------------------------------------
    # 3. PIL Image → base64 JPEG string
    # ------------------------------------------------------------------

    def image_to_base64(self, image: Image.Image) -> str:
        """Encode *image* as a base64 JPEG string (no data-URI prefix).

        Args:
            image: PIL Image in any mode; converted to RGB before encoding.

        Returns:
            Plain base64 string suitable for embedding in JSON or HTML.
        """
        buf = io.BytesIO()
        rgb = image.convert("RGB") if image.mode != "RGB" else image
        rgb.save(buf, format="JPEG", quality=_JPEG_QUALITY, optimize=True)
        return base64.b64encode(buf.getvalue()).decode("ascii")

    # ------------------------------------------------------------------
    # 4. PIL Image → raw JPEG bytes
    # ------------------------------------------------------------------

    def image_to_bytes(self, image: Image.Image) -> bytes:
        """Return raw JPEG bytes for *image* (for Supabase Storage upload).

        Args:
            image: PIL Image in any mode; converted to RGB before encoding.

        Returns:
            JPEG-encoded bytes at quality=_JPEG_QUALITY.
        """
        buf = io.BytesIO()
        rgb = image.convert("RGB") if image.mode != "RGB" else image
        rgb.save(buf, format="JPEG", quality=_JPEG_QUALITY, optimize=True)
        return buf.getvalue()

    # ------------------------------------------------------------------
    # 5. End-to-end: render page + crop article region → JPEG bytes
    # ------------------------------------------------------------------

    def crop_article(
        self,
        pdf_path: str,
        article: dict,
        dpi: int = 150,
        padding: int = 20,
    ) -> Optional[bytes]:
        """High-level convenience: render the article's page and return a crop.

        Args:
            pdf_path: Path to the source PDF.
            article:  Article dict from LayoutAnalyzer/KeywordMatcher with at
                      minimum ``"bounding_box"`` and ``"page_number"`` keys.
            dpi:      Render resolution passed to render_page_as_image.
            padding:  Pixel padding passed to crop_article_region.

        Returns:
            JPEG bytes of the cropped article image, or ``None`` when the
            bounding box is absent, has fewer than 4 elements, or produces a
            degenerate (zero-area) crop.
        """
        bbox: Optional[list[float]] = article.get("bounding_box")
        page_number: Optional[int] = article.get("page_number")

        # Guard: missing or malformed bounding box.
        if not bbox or len(bbox) != 4:
            log.warning(
                f"[crop_article] Skipping article on page {page_number} — "
                f"missing or invalid bounding_box: {bbox!r}"
            )
            return None

        if page_number is None:
            log.warning("[crop_article] Skipping article — page_number is None")
            return None

        # Retrieve page dimensions for clamping.
        try:
            doc = fitz.open(pdf_path)
            with doc:
                if page_number < 1 or page_number > len(doc):
                    log.warning(
                        f"[crop_article] page_number {page_number} out of range "
                        f"(PDF has {len(doc)} pages)"
                    )
                    return None
                rect = doc[page_number - 1].rect
                page_width_pts = rect.width
                page_height_pts = rect.height
        except Exception as exc:
            log.error(
                f"[crop_article] Cannot read page dimensions from {pdf_path!r}: {exc}"
            )
            return None

        # Render the page.
        try:
            page_image = self.render_page_as_image(pdf_path, page_number, dpi=dpi)
        except Exception as exc:
            log.error(f"[crop_article] Page render failed: {exc}")
            return None

        # Crop the article region.
        try:
            crop = self.crop_article_region(
                page_image,
                bbox,
                page_width_pts=page_width_pts,
                page_height_pts=page_height_pts,
                padding=padding,
                dpi=dpi,
            )
        except ValueError as exc:
            log.warning(f"[crop_article] Crop skipped: {exc}")
            return None

        jpeg_bytes = self.image_to_bytes(crop)
        log.info(
            f"[crop_article] page={page_number} bbox={[round(v) for v in bbox]} "
            f"→ {crop.width}×{crop.height}px "
            f"({len(jpeg_bytes) / 1024:.1f} KB JPEG)"
        )
        return jpeg_bytes
