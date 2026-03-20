# core/keyword_matcher.py
# Matches a configurable list of keywords and named entities against extracted
# article text using RapidFuzz for fuzzy matching. Returns match results with
# scores, matched positions, and contextual snippets for each keyword hit.

from __future__ import annotations

import unicodedata
from typing import Optional

from rapidfuzz import fuzz

from utils.logger import log

# ---------------------------------------------------------------------------
# Headline score boost
# When a keyword is found in the article headline the raw score is multiplied
# by this factor before being stored. Capped at 1.0 for exact matches.
# ---------------------------------------------------------------------------
_HEADLINE_BOOST = 1.2
_SCORE_CAP = 1.0

# ---------------------------------------------------------------------------
# Simple transliteration table: common Latin romanisations → Devanagari and
# vice-versa. Keys are lower-case Latin strings, values are Devanagari.
# Extend this table as new brief keywords are encountered.
# ---------------------------------------------------------------------------
_TRANSLIT_TO_DEVANAGARI: dict[str, str] = {
    # People / political terms frequently seen in ROBIN briefs
    "modi":          "मोदी",
    "kejriwal":      "केजरीवाल",
    "rahul gandhi":  "राहुल गांधी",
    "congress":      "कांग्रेस",
    "bjp":           "भाजपा",
    "aap":           "आप",
    "election":      "चुनाव",
    "vote":          "मतदान",
    "government":    "सरकार",
    "police":        "पुलिस",
    "court":         "अदालत",
    "budget":        "बजट",
    "farmer":        "किसान",
    "farmers":       "किसान",
    "protest":       "विरोध",
    "army":          "सेना",
    "attack":        "हमला",
    "accident":      "दुर्घटना",
    "flood":         "बाढ़",
    "fire":          "आग",
    "crime":         "अपराध",
    "corruption":    "भ्रष्टाचार",
    "hospital":      "अस्पताल",
    "school":        "स्कूल",
    "development":   "विकास",
    "water":         "पानी",
    "electricity":   "बिजली",
    "road":          "सड़क",
    "temple":        "मंदिर",
}

# Reverse map: Devanagari → Latin (built automatically from the table above).
_TRANSLIT_TO_LATIN: dict[str, str] = {v: k for k, v in _TRANSLIT_TO_DEVANAGARI.items()}


# ---------------------------------------------------------------------------
# Unicode normalisation helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """NFC-normalise and casefold *text*.

    NFC is required for Devanagari because some OCR engines output composed
    forms while others output decomposed sequences that look identical on screen
    but fail string equality checks.
    """
    return unicodedata.normalize("NFC", text).casefold()


def _strip_diacritics(text: str) -> str:
    """Remove combining diacritical marks from *text* after NFD decomposition.

    Used to make fuzzy matching more tolerant of OCR diacritic errors in
    Devanagari (e.g. missing anusvara / visarga strokes).
    """
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")


# ---------------------------------------------------------------------------
# KeywordMatcher
# ---------------------------------------------------------------------------

class KeywordMatcher:
    """Match a fixed keyword list against extracted newspaper article text.

    Each keyword is expanded with transliterated variants at construction time,
    so Latin keywords also match Devanagari text and vice-versa.
    """

    def __init__(self, keywords: list[str], fuzzy_threshold: int = 75) -> None:
        """
        Args:
            keywords:        Raw keyword strings from the ROBIN brief.
            fuzzy_threshold: RapidFuzz ``partial_ratio`` threshold (0–100).
                             Matches below this score are discarded.
        """
        self.fuzzy_threshold = fuzzy_threshold

        # Each entry: (original_keyword, normalised_variant, is_devanagari)
        self._variants: list[tuple[str, str, bool]] = []

        seen_variants: set[str] = set()

        for kw in keywords:
            original = kw.strip()
            if not original:
                continue
            self._add_variant(original, original, seen_variants)

            norm_kw = _normalize(original)

            # Add Devanagari equivalent for Latin keywords.
            if norm_kw in _TRANSLIT_TO_DEVANAGARI:
                deva = _TRANSLIT_TO_DEVANAGARI[norm_kw]
                self._add_variant(original, deva, seen_variants)

            # Add Latin equivalent for Devanagari keywords.
            norm_nfc = unicodedata.normalize("NFC", original)
            if norm_nfc in _TRANSLIT_TO_LATIN:
                latin = _TRANSLIT_TO_LATIN[norm_nfc]
                self._add_variant(original, latin, seen_variants)

        log.debug(
            f"[KeywordMatcher] Initialised with {len(keywords)} keyword(s), "
            f"{len(self._variants)} variant(s) total, threshold={fuzzy_threshold}"
        )

    def _add_variant(
        self, original: str, variant: str, seen: set[str]
    ) -> None:
        norm = _normalize(variant)
        if norm and norm not in seen:
            seen.add(norm)
            is_deva = any("\u0900" <= ch <= "\u097F" for ch in variant)
            self._variants.append((original, norm, is_deva))

    # ------------------------------------------------------------------
    # 1. match_text
    # ------------------------------------------------------------------

    def match_text(self, text: str, in_headline: bool = False) -> list[dict]:
        """Find all keyword occurrences in *text*.

        For each keyword variant:
        1. Exact case-insensitive substring search (score = 1.0).
        2. RapidFuzz ``partial_ratio`` fuzzy search on a sliding window of
           tokens when no exact match is found.

        Args:
            text:        The article text to search (may be mixed-language).
            in_headline: If True, apply _HEADLINE_BOOST to all scores.

        Returns:
            List of match dicts, one per keyword (deduplicated to the best
            match per original keyword).
        """
        if not text or not text.strip():
            return []

        norm_text = _normalize(text)
        # Also prepare a diacritic-stripped version for OCR-noisy Devanagari.
        stripped_text = _strip_diacritics(norm_text)

        # Best match per original keyword (avoid duplicate entries).
        best_per_keyword: dict[str, dict] = {}

        def _record(
            original: str,
            matched_text: str,
            score: float,
            match_type: str,
            position: int,
        ) -> None:
            if in_headline:
                score = min(score * _HEADLINE_BOOST, _SCORE_CAP)
            existing = best_per_keyword.get(original)
            if existing is None or score > existing["score"]:
                best_per_keyword[original] = {
                    "keyword":       original,
                    "matched_text":  matched_text,
                    "score":         round(score, 4),
                    "match_type":    match_type,
                    "char_position": position,
                    "in_headline":   in_headline,
                }

        for original, norm_variant, is_deva in self._variants:
            # ---- exact substring match ----
            search_text = stripped_text if is_deva else norm_text
            search_variant = (
                _strip_diacritics(norm_variant) if is_deva else norm_variant
            )

            pos = search_text.find(search_variant)
            if pos != -1:
                _record(original, text[pos: pos + len(search_variant)],
                        1.0, "exact", pos)
                continue

            # ---- fuzzy match on sliding token windows ----
            # Split text into tokens and test windows whose character length is
            # close to the keyword length (±50%). This is faster than running
            # partial_ratio over the entire article text.
            kw_len = len(search_variant)
            tokens = search_text.split()
            window: list[str] = []
            window_len = 0
            best_score = 0.0
            best_pos = 0
            best_window = ""

            for tok in tokens:
                window.append(tok)
                window_len += len(tok) + 1  # +1 for space

                # Drop tokens from the left when the window grows too large.
                while window_len > kw_len * 2 and window:
                    dropped = window.pop(0)
                    window_len -= len(dropped) + 1

                candidate = " ".join(window)
                ratio = fuzz.partial_ratio(search_variant, candidate) / 100.0

                if ratio > best_score:
                    best_score = ratio
                    best_window = candidate
                    # Approximate char position in original text.
                    best_pos = norm_text.find(candidate.split()[0]) if candidate else 0

            if best_score * 100 >= self.fuzzy_threshold:
                _record(original, best_window, best_score, "fuzzy", max(best_pos, 0))

        matches = list(best_per_keyword.values())
        if matches:
            log.debug(
                f"[match_text] {len(matches)} keyword match(es) "
                f"{'(headline)' if in_headline else ''}: "
                + ", ".join(f"{m['keyword']!r}@{m['score']:.2f}" for m in matches)
            )
        return matches

    # ------------------------------------------------------------------
    # 2. find_in_articles
    # ------------------------------------------------------------------

    def find_in_articles(self, articles: list[dict]) -> list[dict]:
        """Run matching across a list of article dicts from LayoutAnalyzer.

        Headline text is matched with the _HEADLINE_BOOST applied; body text
        is matched at face value. The two match lists are merged, keeping the
        best score per keyword.

        Args:
            articles: Output of ``LayoutAnalyzer.analyze()``.

        Returns:
            Articles that contain at least one keyword match, sorted by their
            highest match score descending.  Each article gains a
            ``"keyword_matches"`` key.
        """
        matched_articles: list[dict] = []

        for article in articles:
            full_text = article.get("full_text", "")
            headline = article.get("headline") or ""

            # Match headline (boosted) and body separately, then merge.
            headline_matches = self.match_text(headline, in_headline=True) if headline else []
            body_matches = self.match_text(full_text, in_headline=False)

            # Merge: for each original keyword keep the higher-scoring hit.
            merged: dict[str, dict] = {}
            for m in body_matches + headline_matches:   # headline second → overwrites if better
                kw = m["keyword"]
                if kw not in merged or m["score"] > merged[kw]["score"]:
                    merged[kw] = m

            if not merged:
                continue

            article_copy = dict(article)
            article_copy["keyword_matches"] = list(merged.values())
            matched_articles.append(article_copy)

        # Sort by best match score descending.
        matched_articles.sort(
            key=lambda a: max(m["score"] for m in a["keyword_matches"]),
            reverse=True,
        )

        log.info(
            f"[find_in_articles] {len(matched_articles)}/{len(articles)} articles "
            f"matched at least one keyword"
        )
        return matched_articles

    # ------------------------------------------------------------------
    # 3. get_best_match
    # ------------------------------------------------------------------

    def get_best_match(self, article: dict) -> Optional[dict]:
        """Return the single highest-confidence match from an article.

        Args:
            article: An article dict that contains a ``"keyword_matches"`` key
                     (as populated by ``find_in_articles``).

        Returns:
            The match dict with the highest score, or ``None`` if the article
            has no ``keyword_matches`` key or the list is empty.
        """
        matches: list[dict] = article.get("keyword_matches", [])
        if not matches:
            return None
        return max(matches, key=lambda m: m["score"])
