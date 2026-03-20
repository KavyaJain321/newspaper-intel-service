# core/source_registry.py
# Single source of truth for all newspaper sources the system can scrape.
# Defines the full SOURCE_REGISTRY and the SourceRegistry class with lookup
# and URL-building helpers consumed by the extraction pipeline and ROBIN briefs.

import os
from datetime import date
from typing import Optional

# ---------------------------------------------------------------------------
# Registry data
# ---------------------------------------------------------------------------

# Each entry is a plain dict so it can be serialised to JSON / Supabase easily.
# Fields:
#   name              — display name
#   language          — ISO 639-1 code (hi/en/or/bn/ta/te/ml/mr/gu/pa)
#   scraper_type      — fetching strategy used by PDFFetcher / workers
#   base_url          — canonical epaper portal root (verified live URL)
#   aggregator_url    — (tier-1 only) third-party PDF host
#   pdf_url_pattern   — URL template; placeholders: {YYYY} {MM} {DD} {CITY}
#   cities            — available edition cities
#   requires_login    — whether credentials are needed
#   login_url         — login page (tier-3 only)
#   env_username      — env-var key holding the login email (tier-3 only)
#   env_password      — env-var key holding the login password (tier-3 only)
#   is_active         — set False to exclude without removing the entry
#   geographic_states — Indian states covered; ["all"] means national
#   notes             — operational notes for the scraping team

SOURCE_REGISTRY: list[dict] = [

    # -----------------------------------------------------------------------
    # NATIONAL ENGLISH PAPERS — Flipbook intercept
    # -----------------------------------------------------------------------

    {
        "name": "The Hindu",
        "language": "en",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.thehindu.com",
        "aggregator_url": "https://dailyepaper.in/the-hindu-epaper-free-download",
        "pdf_url_pattern": None,
        "cities": [
            "Delhi", "Mumbai", "Chennai", "Bengaluru", "Hyderabad",
            "Kolkata", "Pune", "Ahmedabad", "Lucknow", "Chandigarh", "Coimbatore",
        ],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["all"],
        "notes": "Flipbook viewer. Playwright intercepts PDF network request.",
    },
    {
        "name": "Indian Express",
        "language": "en",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.indianexpress.com",
        "aggregator_url": "https://dailyepaper.in",
        "pdf_url_pattern": None,
        "cities": ["Delhi", "Mumbai", "Pune", "Chandigarh", "Ahmedabad", "Lucknow"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["all"],
        "notes": "Flipbook viewer. Playwright intercepts PDF.",
    },
    {
        "name": "Times of India",
        "language": "en",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.timesgroup.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": [
            "Delhi", "Mumbai", "Kolkata", "Bengaluru",
            "Chennai", "Hyderabad", "Pune", "Ahmedabad",
        ],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["all"],
        "notes": "Times Group viewer. Playwright intercepts PDF network call.",
    },
    {
        "name": "Hindustan Times",
        "language": "en",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.hindustantimes.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Delhi", "Mumbai", "Chandigarh", "Lucknow", "Patna", "Kolkata"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["all"],
        "notes": "HT epaper viewer — network intercept for PDF URL",
    },
    {
        "name": "Deccan Herald",
        "language": "en",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.deccanherald.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Bengaluru", "Mysuru", "Mangaluru", "Hubli"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Karnataka"],
        "notes": "Karnataka focused English daily",
    },
    {
        "name": "The Tribune",
        "language": "en",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.tribuneindia.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Chandigarh", "Delhi", "Jalandhar", "Dehradun"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Punjab", "Haryana", "HP", "Uttarakhand"],
        "notes": "North India English daily, good for Uttarakhand coverage",
    },

    # -----------------------------------------------------------------------
    # HINDI PAPERS — Flipbook intercept
    # -----------------------------------------------------------------------

    {
        "name": "Dainik Jagran",
        "language": "hi",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.jagran.com",
        "aggregator_url": "https://epaperdaily.in/hindi-epapers/",
        "pdf_url_pattern": None,
        "cities": [
            "Delhi", "Lucknow", "Patna", "Dehradun", "Varanasi",
            "Agra", "Kanpur", "Meerut", "Allahabad", "Gorakhpur",
        ],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["UP", "Uttarakhand", "Bihar", "MP", "Jharkhand"],
        "notes": "Most widely read Hindi paper. Flipbook viewer.",
    },
    {
        "name": "Amar Ujala",
        "language": "hi",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.amarujala.com",
        "aggregator_url": "https://epaperdaily.in/hindi-epapers/",
        "pdf_url_pattern": None,
        "cities": [
            "Delhi", "Lucknow", "Dehradun", "Agra", "Meerut",
            "Chandigarh", "Shimla", "Jammu",
        ],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["UP", "Uttarakhand", "HP", "Punjab", "J&K"],
        "notes": "Strong in North India. Flipbook viewer.",
    },
    {
        "name": "Dainik Bhaskar",
        "language": "hi",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://www.bhaskar.com/epaper",
        "aggregator_url": "https://epaperdaily.in/hindi-epapers/",
        "pdf_url_pattern": None,
        "cities": [
            "Bhopal", "Indore", "Jaipur", "Ahmedabad",
            "Raipur", "Patna", "Ranchi", "Mumbai",
        ],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["MP", "Rajasthan", "Gujarat", "Chhattisgarh", "Bihar"],
        "notes": "Largest circulated Hindi daily. Flipbook viewer.",
    },
    {
        "name": "Hindustan",
        "language": "hi",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://www.livehindustan.com/epaper",
        "aggregator_url": "https://epaperdaily.in/hindi-epapers/",
        "pdf_url_pattern": None,
        "cities": ["Delhi", "Patna", "Lucknow", "Ranchi", "Muzaffarpur"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Bihar", "Jharkhand", "UP"],
        "notes": "Strong Bihar/Jharkhand coverage",
    },
    {
        "name": "Rajasthan Patrika",
        "language": "hi",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.patrika.com",
        "aggregator_url": "https://epaperdaily.in/hindi-epapers/",
        "pdf_url_pattern": None,
        "cities": ["Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer", "Bikaner"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Rajasthan"],
        "notes": "Dominant in Rajasthan",
    },
    {
        "name": "Punjab Kesari",
        "language": "hi",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://www.punjabkesari.in/epaper",
        "aggregator_url": "https://epaperdaily.in/hindi-epapers/",
        "pdf_url_pattern": None,
        "cities": [
            "Delhi", "Chandigarh", "Jalandhar", "Ludhiana", "Shimla", "Jammu",
        ],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Punjab", "Haryana", "HP", "Delhi", "J&K"],
        "notes": "North India. Flipbook viewer.",
    },
    {
        "name": "Prabhat Khabar",
        "language": "hi",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.prabhatkhabar.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Ranchi", "Patna", "Dhanbad", "Jamshedpur", "Kolkata"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Jharkhand", "Bihar", "WB"],
        "notes": "Key paper for Jharkhand intelligence",
    },

    # -----------------------------------------------------------------------
    # ODISHA — Primary focus sources
    # -----------------------------------------------------------------------

    {
        "name": "Dharitri",
        "language": "or",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://dharitriepaper.in",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Bhubaneswar", "Cuttack", "Sambalpur"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Odisha"],
        "notes": "Leading Odia daily. WORKING URL: dharitriepaper.in (NOT epaper.dharitri.com)",
    },
    {
        "name": "Pragativadi",
        "language": "or",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://pragativadi.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Bhubaneswar", "Cuttack", "Bhadrak", "Balasore"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Odisha"],
        "notes": "Popular Odia daily. Site at pragativadi.com — epaper section within main site.",
    },
    {
        "name": "Sambad",
        "language": "or",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://sambad.in",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Bhubaneswar", "Cuttack", "Sambalpur", "Berhampur"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Odisha"],
        "notes": "Widely read Odia daily.",
    },
    {
        "name": "Orissa Post",
        "language": "en",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://www.orissapost.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Bhubaneswar", "Cuttack"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Odisha"],
        "notes": "English daily from Odisha. Use www.orissapost.com (epaper.orissapost.com DNS fails).",
    },
    {
        "name": "Samaja",
        "language": "or",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.thesamaja.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Bhubaneswar", "Cuttack", "Sambalpur", "Berhampur"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": False,   # SSL cert error — disabled until TLS fixed at epaper.thesamaja.com
        "geographic_states": ["Odisha"],
        "notes": "Oldest Odia newspaper. SSL cert broken as of Mar 2026. Re-enable when fixed.",
    },
    {
        "name": "Odisha TV (OTV)",
        "language": "or",
        "scraper_type": "html_article",
        "base_url": "https://odishatv.in",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Bhubaneswar"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Odisha"],
        "notes": "HTML scrape — no PDF edition",
    },

    # -----------------------------------------------------------------------
    # OTHER REGIONAL PAPERS
    # -----------------------------------------------------------------------

    {
        "name": "Anandabazar Patrika",
        "language": "bn",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.anandabazar.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Kolkata", "Siliguri", "Asansol"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["West Bengal"],
        "notes": "Most read Bengali newspaper",
    },
    {
        "name": "Dinamalar",
        "language": "ta",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.dinamalar.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Chennai", "Madurai", "Coimbatore", "Salem", "Trichy"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Tamil Nadu"],
        "notes": "Leading Tamil daily",
    },
    {
        "name": "Eenadu",
        "language": "te",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.eenadu.net",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Hyderabad", "Vijayawada", "Visakhapatnam", "Tirupati", "Nellore"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Telangana", "Andhra Pradesh"],
        "notes": "Dominant Telugu paper",
    },
    {
        "name": "Mathrubhumi",
        "language": "ml",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.mathrubhumi.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Kozhikode", "Kochi", "Thiruvananthapuram", "Thrissur"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Kerala"],
        "notes": "Top Malayalam daily",
    },
    {
        "name": "Divya Bhaskar",
        "language": "gu",
        "scraper_type": "flipbook_intercept",
        "base_url": "https://epaper.divyabhaskar.co.in",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Ahmedabad", "Surat", "Vadodara", "Rajkot"],
        "requires_login": False,
        "login_url": None,
        "env_username": None,
        "env_password": None,
        "is_active": True,
        "geographic_states": ["Gujarat"],
        "notes": "Top Gujarati daily",
    },

    # -----------------------------------------------------------------------
    # TIER 3 — LOGIN REQUIRED (disabled until credentials configured)
    # -----------------------------------------------------------------------

    {
        "name": "The Hindu (Subscribed)",
        "language": "en",
        "scraper_type": "login_flipbook",
        "base_url": "https://epaper.thehindu.com",
        "aggregator_url": None,
        "pdf_url_pattern": None,
        "cities": ["Delhi", "Mumbai", "Chennai", "Bengaluru"],
        "requires_login": True,
        "login_url": "https://epaper.thehindu.com",
        "env_username": "HINDU_EMAIL",
        "env_password": "HINDU_PASSWORD",
        "is_active": False,
        "geographic_states": ["all"],
        "notes": "Full PDF only with subscription login",
    },
]


# ---------------------------------------------------------------------------
# SourceRegistry
# ---------------------------------------------------------------------------

class SourceRegistry:
    """Queryable wrapper around SOURCE_REGISTRY.

    All methods operate on the in-memory list; no I/O is performed.
    The ``is_active`` flag and the ``ACTIVE_SOURCES`` env variable are both
    respected so the pipeline can be narrowed at runtime without code changes.
    """

    def __init__(self, registry: list[dict] = SOURCE_REGISTRY) -> None:
        self._registry = registry
        # ACTIVE_SOURCES=all means no filtering by name; anything else is a
        # comma-separated allow-list of source names.
        active_env = os.getenv("ACTIVE_SOURCES", "all").strip()
        if active_env.lower() == "all":
            self._allowed_names: Optional[set[str]] = None
        else:
            self._allowed_names = {
                n.strip() for n in active_env.split(",") if n.strip()
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_allowed(self, source: dict) -> bool:
        """Return True if the source passes the ACTIVE_SOURCES env filter."""
        if self._allowed_names is None:
            return True
        return source["name"] in self._allowed_names

    def _covers_state(self, source: dict, state: str) -> bool:
        states = source.get("geographic_states", [])
        return "all" in states or state in states

    # ------------------------------------------------------------------
    # Public query methods
    # ------------------------------------------------------------------

    def get_active_sources(self) -> list[dict]:
        """Return every source where ``is_active=True`` and within the
        ``ACTIVE_SOURCES`` env allow-list."""
        return [
            s for s in self._registry
            if s.get("is_active", False) and self._is_allowed(s)
        ]

    def get_sources_by_state(self, state: str) -> list[dict]:
        """Return active sources whose ``geographic_states`` include *state*
        or the catch-all ``"all"``."""
        state = state.strip()
        return [
            s for s in self.get_active_sources()
            if self._covers_state(s, state)
        ]

    def get_sources_by_language(self, language: str) -> list[dict]:
        """Return active sources published in *language* (ISO 639-1)."""
        language = language.strip().lower()
        return [
            s for s in self.get_active_sources()
            if s.get("language", "").lower() == language
        ]

    def get_sources_for_brief(self, brief: dict) -> list[dict]:
        """Select the most relevant active sources for a ROBIN brief.

        The brief dict is expected to contain:
            geographic_focus: List[str]  — target Indian states
            languages:        List[str]  — language codes of interest

        Selection strategy:
        1. Gather all sources covering any of the brief's states.
        2. If language filters are provided, union in language-matching sources.
        3. De-duplicate by source name while preserving insertion order.
        4. Sources covering the exact states are returned before national ones.
        """
        target_states: list[str] = brief.get("geographic_focus", [])
        target_langs: list[str] = [
            l.strip().lower() for l in brief.get("languages", [])
        ]

        seen: set[str] = set()
        specific: list[dict] = []   # covers target states explicitly
        national: list[dict] = []   # covers "all"

        def _add(source: dict) -> None:
            n = source["name"]
            if n in seen:
                return
            seen.add(n)
            states = source.get("geographic_states", [])
            if "all" in states:
                national.append(source)
            else:
                specific.append(source)

        # State-based pass
        for state in target_states:
            for s in self.get_sources_by_state(state):
                _add(s)

        # Language-based pass (catches regional papers not tied to a state)
        for lang in target_langs:
            for s in self.get_sources_by_language(lang):
                _add(s)

        # Specific-state sources first, then national ones
        return specific + national

    # ------------------------------------------------------------------
    # URL builder
    # ------------------------------------------------------------------

    @staticmethod
    def build_todays_url(source: dict, city: Optional[str] = None) -> str:
        """Construct today's edition URL from a source's ``pdf_url_pattern``.

        Substitutes the following placeholders:
            {YYYY} — four-digit year  (e.g. 2024)
            {MM}   — zero-padded month (e.g. 03)
            {DD}   — zero-padded day   (e.g. 07)
            {CITY} — city name as provided, URL-encoded spaces → hyphens

        Falls back to ``base_url`` when no pattern is defined.

        Args:
            source: A dict from SOURCE_REGISTRY.
            city:   Edition city; used to fill {CITY} placeholder if present.

        Returns:
            A fully resolved URL string for today's edition.
        """
        pattern: Optional[str] = source.get("pdf_url_pattern")
        if not pattern:
            return source["base_url"]

        today = date.today()
        city_slug = (city or "").replace(" ", "-") if city else ""

        url = pattern
        url = url.replace("{YYYY}", today.strftime("%Y"))
        url = url.replace("{MM}",   today.strftime("%m"))
        url = url.replace("{DD}",   today.strftime("%d"))
        url = url.replace("{CITY}", city_slug)

        return url


# ---------------------------------------------------------------------------
# Module-level singleton — import this everywhere instead of constructing one
# ---------------------------------------------------------------------------

registry = SourceRegistry()
