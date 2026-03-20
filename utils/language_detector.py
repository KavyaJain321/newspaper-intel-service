# utils/language_detector.py
# Detects the language of extracted article text using the langdetect library.
# Provides a simple interface that returns a PaddleOCR-compatible language code,
# with special handling for Odia (which langdetect often misclassifies as Marathi).

from langdetect import detect, LangDetectException

# ---------------------------------------------------------------------------
# Odia Unicode block: U+0B00 – U+0B7F
# langdetect frequently returns "mr" (Marathi) for Odia text because both
# scripts look similar to the model. We inspect the codepoints directly before
# trusting the langdetect result.
# ---------------------------------------------------------------------------
_ODIA_BLOCK_START = 0x0B00
_ODIA_BLOCK_END = 0x0B7F

# Minimum fraction of Odia-script characters required to override langdetect.
_ODIA_CHAR_THRESHOLD = 0.15


def _contains_odia_script(text: str) -> bool:
    """Return True if at least _ODIA_CHAR_THRESHOLD of the text's characters
    fall inside the Odia Unicode block (U+0B00–U+0B7F)."""
    if not text:
        return False
    odia_chars = sum(
        1 for ch in text if _ODIA_BLOCK_START <= ord(ch) <= _ODIA_BLOCK_END
    )
    return (odia_chars / len(text)) >= _ODIA_CHAR_THRESHOLD


# Mapping from ISO 639-1 codes (as returned by langdetect) to PaddleOCR lang codes.
# Keys that are absent fall through to the default ("en").
_LANG_MAP: dict[str, str] = {
    "hi": "hi",   # Hindi
    "or": "or",   # Odia  (langdetect rarely returns this; handled via script check)
    "bn": "bn",   # Bengali
    "ta": "ta",   # Tamil
    "te": "te",   # Telugu
    "mr": "mr",   # Marathi
    "gu": "gu",   # Gujarati
    "pa": "pa",   # Punjabi
    "en": "en",   # English
}

_DEFAULT_LANG = "en"


def detect_language(text: str) -> str:
    """Detect the language of *text* and return a PaddleOCR-compatible code.

    Detection order:
    1. If the text contains a significant proportion of Odia-script characters
       (Unicode block U+0B00–U+0B7F) it is classified as Odia ("or") regardless
       of what langdetect returns — this corrects the common mr→or misclassification.
    2. Otherwise, langdetect is used and the result is mapped via _LANG_MAP.
    3. If langdetect raises or the code is unknown, "en" is returned.

    Args:
        text: Raw extracted article text (may be noisy OCR output).

    Returns:
        A PaddleOCR language code string, e.g. "hi", "bn", "en".
    """
    if not text or not text.strip():
        return _DEFAULT_LANG

    # Step 1 — script-level Odia check (beats langdetect for this specific case).
    if _contains_odia_script(text):
        return "or"

    # Step 2 — langdetect-based detection with mapping.
    try:
        detected = detect(text)
        return _LANG_MAP.get(detected, _DEFAULT_LANG)
    except LangDetectException:
        return _DEFAULT_LANG
