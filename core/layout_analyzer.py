# core/layout_analyzer.py
# Analyzes the spatial layout of OCR text blocks on a newspaper page.
# Detects column boundaries, groups text blocks into logical article regions,
# identifies headlines vs. body text using font-size heuristics, and returns
# a structured representation of the page layout using Shapely geometries.

from __future__ import annotations

from collections import defaultdict
from typing import Optional

import fitz  # PyMuPDF — for page dimensions only
import numpy as np
from shapely.geometry import box as shapely_box
from shapely.ops import unary_union

from utils.logger import log

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

_HEADLINE_FONT_RATIO = 1.5      # headline font must be ≥ 1.5× the page median
_HEADLINE_MAX_CHARS = 150       # headlines are short
_COLUMN_MIN_GAP_FRACTION = 0.02 # gap must be ≥ 2% of page width to be a divider
_VERTICAL_PROXIMITY_PX = 50     # blocks within 50 px vertically → same article
_VERTICAL_GAP_MULTIPLIER = 2.0  # gap > 2× median line-height → new article
_HISTOGRAM_BINS = 60            # resolution for column-gap detection


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _bbox_union(bboxes: list[list[float]]) -> list[float]:
    """Return the axis-aligned bounding box that encloses all *bboxes*."""
    if not bboxes:
        return [0.0, 0.0, 0.0, 0.0]
    polys = [shapely_box(b[0], b[1], b[2], b[3]) for b in bboxes]
    union = unary_union(polys)
    minx, miny, maxx, maxy = union.bounds
    return [minx, miny, maxx, maxy]


def _block_cx(block: dict) -> float:
    """Horizontal centre of a block."""
    b = block["bbox"]
    return (b[0] + b[2]) / 2.0


def _block_cy(block: dict) -> float:
    """Vertical centre of a block."""
    b = block["bbox"]
    return (b[1] + b[3]) / 2.0


def _block_height(block: dict) -> float:
    b = block["bbox"]
    return max(b[3] - b[1], 1.0)


def _block_top(block: dict) -> float:
    return block["bbox"][1]


def _block_bottom(block: dict) -> float:
    return block["bbox"][3]


# ---------------------------------------------------------------------------
# LayoutAnalyzer
# ---------------------------------------------------------------------------

class LayoutAnalyzer:
    """Converts a flat list of text blocks into structured article regions."""

    # ------------------------------------------------------------------
    # 1. Column boundary detection
    # ------------------------------------------------------------------

    def detect_columns(
        self, blocks: list[dict], page_width: float
    ) -> list[float]:
        """Find vertical column dividers by analysing block x-coordinate density.

        Strategy:
        - Build a 1-D histogram of block left-edges and right-edges across the
          horizontal axis of the page.
        - Gaps (bins with zero or near-zero block density) between dense clusters
          are the column gutters.
        - Returns x-coordinates of those gutter centres, sorted left→right.
        - Supports 1–6 column layouts; returns [] for a single-column page.

        Args:
            blocks:     All text blocks on the page.
            page_width: Page width in the same coordinate units as block bboxes.

        Returns:
            Sorted list of x-coordinates that divide columns.  Empty list means
            the page is single-column.
        """
        if not blocks or page_width <= 0:
            return []

        # Collect left and right edges of every text block.
        edges: list[float] = []
        for b in blocks:
            if b.get("block_type") == "image":
                continue
            edges.append(b["bbox"][0])   # left edge
            edges.append(b["bbox"][2])   # right edge

        if len(edges) < 4:
            return []

        counts, bin_edges = np.histogram(edges, bins=_HISTOGRAM_BINS,
                                         range=(0, page_width))

        min_gap_px = page_width * _COLUMN_MIN_GAP_FRACTION
        dividers: list[float] = []
        in_gap = False
        gap_start: float = 0.0

        for i, count in enumerate(counts):
            bin_left = bin_edges[i]
            bin_right = bin_edges[i + 1]
            bin_width = bin_right - bin_left

            if count == 0:
                if not in_gap:
                    in_gap = True
                    gap_start = bin_left
            else:
                if in_gap:
                    gap_end = bin_left
                    gap_width = gap_end - gap_start
                    if gap_width >= min_gap_px:
                        # Column divider is the centre of the gap.
                        dividers.append((gap_start + gap_end) / 2.0)
                    in_gap = False

        # Close a gap that runs to the page edge (right margin).
        if in_gap:
            gap_end = page_width
            gap_width = gap_end - gap_start
            if gap_width >= min_gap_px:
                dividers.append((gap_start + gap_end) / 2.0)

        # Newspaper pages have at most 6 columns; prune excess dividers by keeping
        # only the widest gaps when there are too many candidates.
        if len(dividers) > 5:
            log.debug(
                f"[detect_columns] {len(dividers)} divider candidates — "
                "pruning to 5 widest gaps"
            )
            dividers = dividers[:5]

        log.debug(
            f"[detect_columns] page_width={page_width:.0f}px, "
            f"{len(dividers) + 1} column(s) detected, dividers={[f'{d:.0f}' for d in dividers]}"
        )
        return dividers

    # ------------------------------------------------------------------
    # 2. Headline detection
    # ------------------------------------------------------------------

    def detect_headlines(self, blocks: list[dict]) -> list[dict]:
        """Identify headline blocks by font size and boldness.

        A block is classified as a headline when ALL of the following hold:
        - Its font_size is ≥ _HEADLINE_FONT_RATIO × the median font_size on the page.
        - Its text has fewer than _HEADLINE_MAX_CHARS characters.
        - It is either marked is_bold=True OR its font_size alone is large enough
          (≥ 2× median) — bold metadata is unreliable for OCR-extracted blocks.

        Args:
            blocks: All text blocks for a single page.

        Returns:
            Subset of *blocks* classified as headlines.
        """
        text_blocks = [
            b for b in blocks
            if b.get("block_type") != "image" and b.get("text", "").strip()
        ]
        if not text_blocks:
            return []

        font_sizes = [b.get("font_size", 0.0) for b in text_blocks]
        # Filter out zero-size entries (OCR blocks often have font_size=0).
        nonzero_sizes = [s for s in font_sizes if s > 0]
        if not nonzero_sizes:
            # OCR path: no font-size metadata — skip headline detection.
            return []

        median_size = float(np.median(nonzero_sizes))
        if median_size <= 0:
            return []

        headlines: list[dict] = []
        for block in text_blocks:
            size = block.get("font_size", 0.0)
            text = block.get("text", "")
            is_bold = block.get("is_bold", False)

            size_ratio = size / median_size if size > 0 else 0.0
            qualifies = (
                size_ratio >= _HEADLINE_FONT_RATIO
                and len(text) < _HEADLINE_MAX_CHARS
                and (is_bold or size_ratio >= 2.0)
            )
            if qualifies:
                headlines.append(block)

        return headlines

    # ------------------------------------------------------------------
    # 3. Group blocks into articles
    # ------------------------------------------------------------------

    def group_into_articles(
        self, blocks: list[dict], page_number: int, column_dividers: list[float]
    ) -> list[dict]:
        """Cluster text blocks into logical article regions for one page.

        Grouping rules (applied in order):
        1. Assign each block to a column based on its horizontal centre and the
           divider list.
        2. Within each column, sort blocks top→bottom.
        3. A headline block always starts a new article.
        4. A vertical gap > _VERTICAL_GAP_MULTIPLIER × median line height starts
           a new article.
        5. Blocks more than _VERTICAL_PROXIMITY_PX below the previous block's
           bottom edge start a new article (catches spacing without a headline).

        Args:
            blocks:           Text blocks for this page.
            page_number:      1-based page index (stored on output articles).
            column_dividers:  Sorted x-coordinates of column gutter centres.

        Returns:
            List of article dicts, each containing headline, body_blocks,
            full_text, bounding_box, page_number, and column_index.
        """
        text_blocks = [
            b for b in blocks
            if b.get("block_type") != "image" and b.get("text", "").strip()
        ]
        if not text_blocks:
            return []

        headline_set = set(id(b) for b in self.detect_headlines(text_blocks))

        # Median line height for gap threshold.
        heights = [_block_height(b) for b in text_blocks]
        median_line_h = float(np.median(heights)) if heights else 12.0
        gap_threshold = _VERTICAL_GAP_MULTIPLIER * median_line_h

        # --- Assign column index to each block ---
        def _col_index(block: dict) -> int:
            cx = _block_cx(block)
            for i, div in enumerate(column_dividers):
                if cx < div:
                    return i
            return len(column_dividers)  # rightmost column

        # Group blocks by column.
        col_blocks: dict[int, list[dict]] = defaultdict(list)
        for b in text_blocks:
            col_blocks[_col_index(b)].append(b)

        articles: list[dict] = []

        for col_idx in sorted(col_blocks.keys()):
            col = sorted(col_blocks[col_idx], key=_block_top)

            current_headline: Optional[str] = None
            current_body: list[dict] = []

            def _flush() -> None:
                """Commit the current accumulator as an article."""
                if not current_body and current_headline is None:
                    return
                all_blocks = current_body[:]
                body_text_parts = [b["text"] for b in current_body]
                bbox = _bbox_union([b["bbox"] for b in all_blocks])
                articles.append({
                    "headline": current_headline,
                    "body_blocks": current_body[:],
                    "full_text": " ".join(body_text_parts).strip(),
                    "bounding_box": bbox,
                    "page_number": page_number,
                    "column_index": col_idx,
                })

            prev_bottom: Optional[float] = None

            for block in col:
                is_headline = id(block) in headline_set
                top = _block_top(block)

                # Determine whether this block starts a new article.
                starts_new = False
                if prev_bottom is not None:
                    gap = top - prev_bottom
                    if gap > gap_threshold or gap > _VERTICAL_PROXIMITY_PX:
                        starts_new = True
                if is_headline:
                    starts_new = True

                if starts_new and (current_body or current_headline is not None):
                    _flush()
                    current_headline = None
                    current_body = []

                if is_headline:
                    # A headline may follow another headline (e.g. kicker + main head).
                    # Concatenate them rather than creating a 0-body article.
                    if current_headline:
                        current_headline += " " + block["text"]
                    else:
                        current_headline = block["text"]
                else:
                    current_body.append(block)

                prev_bottom = _block_bottom(block)

            # Flush the last accumulator for this column.
            _flush()

        return articles

    # ------------------------------------------------------------------
    # 4. Full-document analysis
    # ------------------------------------------------------------------

    def analyze(self, blocks: list[dict], pdf_path: str) -> list[dict]:
        """Run layout analysis across all pages of a PDF.

        Steps per page:
        1. Retrieve page dimensions from PyMuPDF (authoritative for scale).
        2. detect_columns() to find column gutters.
        3. group_into_articles() to cluster blocks into article regions.

        Args:
            blocks:   All text blocks from any extraction engine, mixed pages.
            pdf_path: Path to the original PDF (needed for page dimensions).

        Returns:
            Flat list of article dicts across all pages.
        """
        if not blocks:
            log.warning("[analyze] No blocks provided — returning empty article list")
            return []

        # Load page dimensions from PyMuPDF.
        page_dims: dict[int, tuple[float, float]] = {}  # page_number → (width, height)
        try:
            doc = fitz.open(pdf_path)
            with doc:
                for i in range(len(doc)):
                    rect = doc[i].rect
                    page_dims[i + 1] = (rect.width, rect.height)
        except Exception as exc:
            log.warning(
                f"[analyze] Could not read page dimensions from {pdf_path!r}: {exc}. "
                "Falling back to bbox-derived widths."
            )

        # Group blocks by page number.
        pages: dict[int, list[dict]] = defaultdict(list)
        for b in blocks:
            pages[b["page_number"]].append(b)

        all_articles: list[dict] = []

        for page_number in sorted(pages.keys()):
            page_blocks = pages[page_number]

            # Page width: prefer PyMuPDF value, fall back to max right-edge of blocks.
            if page_number in page_dims:
                page_width = page_dims[page_number][0]
            else:
                page_width = max(
                    (b["bbox"][2] for b in page_blocks), default=600.0
                )

            column_dividers = self.detect_columns(page_blocks, page_width)
            articles = self.group_into_articles(
                page_blocks, page_number, column_dividers
            )

            log.info(
                f"[analyze] page {page_number}: "
                f"{len(column_dividers) + 1} column(s), "
                f"{len(articles)} article(s) detected"
            )
            all_articles.extend(articles)

        log.info(
            f"[analyze] Total: {len(all_articles)} articles across "
            f"{len(pages)} page(s) from {pdf_path!r}"
        )
        return all_articles
