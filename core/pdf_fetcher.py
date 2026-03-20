# core/pdf_fetcher.py
# Responsible for downloading newspaper PDF files from remote URLs.
# Validates file size against MAX_PDF_SIZE_MB, streams downloads via httpx,
# and saves files to a temporary local path for subsequent processing.

import asyncio
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import httpx
from playwright.async_api import async_playwright, Request, Response

from utils.logger import log

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_MAX_PDF_MB = float(os.getenv("MAX_PDF_SIZE_MB", "50"))
_MAX_PDF_BYTES = int(_MAX_PDF_MB * 1024 * 1024)

_DIRECT_TIMEOUT = 30.0          # seconds for httpx requests
_FLIPBOOK_TIMEOUT = 15.0        # seconds to wait for a PDF URL in network traffic
_MAX_REDIRECTS = 5

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "application/pdf,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

# URL patterns that strongly suggest a PDF download link.
_PDF_URL_PATTERNS = re.compile(
    r"(\.pdf(\?.*)?$|/download/|/file/|/document/|/epaper/|/pdf/|"
    r"epaper\.pdf|/edition/|/pages?/.*\.pdf)",
    re.IGNORECASE,
)

# Known flipbook platforms and their PDF discovery quirks.
_FLIPBOOK_HOSTS = {
    "issuu.com",
    "fliphtml5.com",
    "epapertoday.com",
    "epaper.amarujala.com",
    "epaper.jagran.com",
    "epaper.bhaskar.com",
}


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class PDFFetchError(Exception):
    """Raised when PDF acquisition fails at any stage."""


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _is_pdf_bytes(data: bytes) -> bool:
    """Return True if *data* starts with the PDF magic bytes ``%PDF``."""
    return data[:4] == b"%PDF"


def _is_pdf_content_type(content_type: str) -> bool:
    return "application/pdf" in content_type.lower()


def _is_html_content_type(content_type: str) -> bool:
    return "text/html" in content_type.lower()


def _host_of(url: str) -> str:
    return urlparse(url).netloc.lower()


# ---------------------------------------------------------------------------
# Main fetcher class
# ---------------------------------------------------------------------------

class PDFFetcher:
    """Acquires newspaper PDF bytes from direct download URLs or flipbook pages."""

    # ------------------------------------------------------------------
    # 1. Direct PDF download
    # ------------------------------------------------------------------

    async def fetch_direct_pdf(self, url: str) -> bytes:
        """Download a PDF from a direct URL using httpx.

        Args:
            url: A URL that resolves to a PDF file.

        Returns:
            Raw PDF bytes.

        Raises:
            PDFFetchError: On network error, non-PDF response, or size limit exceeded.
        """
        log.info(f"[fetch_direct_pdf] Fetching: {url}")

        try:
            async with httpx.AsyncClient(
                headers=_BROWSER_HEADERS,
                follow_redirects=True,
                max_redirects=_MAX_REDIRECTS,
                timeout=_DIRECT_TIMEOUT,
            ) as client:
                # Stream so we can check Content-Length before pulling the body.
                async with client.stream("GET", url) as response:
                    response.raise_for_status()

                    content_type = response.headers.get("content-type", "")
                    content_length = int(response.headers.get("content-length", 0))

                    log.debug(
                        f"[fetch_direct_pdf] {response.status_code} | "
                        f"content-type={content_type!r} | "
                        f"content-length={content_length}"
                    )

                    # Early size guard via Content-Length header.
                    if content_length and content_length > _MAX_PDF_BYTES:
                        raise ValueError(
                            f"PDF exceeds size limit: "
                            f"{content_length / 1024 / 1024:.1f} MB > {_MAX_PDF_MB} MB"
                        )

                    # Reject obvious non-PDF content types (HTML redirect pages, etc.)
                    # but stay permissive for octet-stream, which many servers use.
                    if content_type and not any(
                        t in content_type.lower()
                        for t in ("application/pdf", "application/octet-stream",
                                  "binary/octet-stream", "application/download")
                    ):
                        if _is_html_content_type(content_type):
                            raise PDFFetchError(
                                f"Server returned HTML instead of PDF "
                                f"(content-type: {content_type!r}). "
                                "This URL may require flipbook rendering."
                            )
                        log.warning(
                            f"[fetch_direct_pdf] Unexpected content-type {content_type!r}, "
                            "proceeding cautiously."
                        )

                    chunks: list[bytes] = []
                    downloaded = 0

                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        downloaded += len(chunk)
                        if downloaded > _MAX_PDF_BYTES:
                            raise ValueError(
                                f"PDF stream exceeded size limit of {_MAX_PDF_MB} MB "
                                f"during download."
                            )
                        chunks.append(chunk)

                    pdf_bytes = b"".join(chunks)

        except ValueError:
            raise
        except PDFFetchError:
            raise
        except httpx.TimeoutException as exc:
            raise PDFFetchError(f"Request timed out after {_DIRECT_TIMEOUT}s: {url}") from exc
        except httpx.TooManyRedirects as exc:
            raise PDFFetchError(
                f"Too many redirects (>{_MAX_REDIRECTS}) for URL: {url}"
            ) from exc
        except httpx.HTTPStatusError as exc:
            raise PDFFetchError(
                f"HTTP {exc.response.status_code} from {url}"
            ) from exc
        except httpx.RequestError as exc:
            raise PDFFetchError(f"Network error fetching {url}: {exc}") from exc

        # Magic-byte validation — catches servers that return 200 with an HTML body.
        if not _is_pdf_bytes(pdf_bytes):
            if pdf_bytes[:5].lower().startswith(b"<html") or b"<HTML" in pdf_bytes[:100]:
                raise PDFFetchError(
                    "Response body is HTML, not a PDF. "
                    "This URL may require flipbook rendering."
                )
            raise PDFFetchError(
                f"Response does not start with PDF magic bytes (%PDF). "
                f"First 8 bytes: {pdf_bytes[:8]!r}"
            )

        log.info(
            f"[fetch_direct_pdf] Downloaded {len(pdf_bytes) / 1024:.1f} KB from {url}"
        )
        return pdf_bytes

    # ------------------------------------------------------------------
    # 2. Flipbook PDF extraction via Playwright
    # ------------------------------------------------------------------

    async def fetch_flipbook_pdf(self, page_url: str) -> bytes:
        """Open a flipbook page with Playwright, intercept the underlying PDF URL,
        then download it.

        Args:
            page_url: URL of the flipbook viewer page.

        Returns:
            Raw PDF bytes.

        Raises:
            PDFFetchError: If no PDF URL is found within the timeout window.
        """
        log.info(f"[fetch_flipbook_pdf] Launching browser for: {page_url}")

        pdf_url_found: asyncio.Future[str] = asyncio.get_event_loop().create_future()

        async def _on_request(request: Request) -> None:
            url = request.url
            if pdf_url_found.done():
                return
            if _PDF_URL_PATTERNS.search(url):
                log.debug(f"[fetch_flipbook_pdf] PDF URL captured via request intercept: {url}")
                pdf_url_found.set_result(url)

        async def _on_response(response: Response) -> None:
            if pdf_url_found.done():
                return
            content_type = response.headers.get("content-type", "")
            if _is_pdf_content_type(content_type):
                log.debug(
                    f"[fetch_flipbook_pdf] PDF content-type detected in response: {response.url}"
                )
                pdf_url_found.set_result(response.url)

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            context = await browser.new_context(
                user_agent=_BROWSER_HEADERS["User-Agent"],
                accept_downloads=True,
                extra_http_headers={
                    "Accept-Language": _BROWSER_HEADERS["Accept-Language"],
                },
            )

            page = await context.new_page()
            page.on("request", _on_request)
            page.on("response", _on_response)

            try:
                log.debug(f"[fetch_flipbook_pdf] Navigating to {page_url}")
                await page.goto(
                    page_url,
                    wait_until="networkidle",
                    timeout=_FLIPBOOK_TIMEOUT * 1000,
                )

                # Platform-specific interactions to trigger PDF loading.
                host = _host_of(page_url)
                await self._platform_interactions(page, host)

                # Wait for the PDF URL future with our timeout.
                try:
                    pdf_url = await asyncio.wait_for(
                        asyncio.shield(pdf_url_found),
                        timeout=_FLIPBOOK_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    raise PDFFetchError(
                        f"No PDF URL detected in network traffic after "
                        f"{_FLIPBOOK_TIMEOUT}s on page: {page_url}. "
                        "The flipbook may use a non-standard loading strategy."
                    )

            finally:
                await context.close()
                await browser.close()
                log.debug("[fetch_flipbook_pdf] Browser closed.")

        log.info(f"[fetch_flipbook_pdf] PDF URL resolved: {pdf_url}")
        return await self.fetch_direct_pdf(pdf_url)

    async def _platform_interactions(self, page, host: str) -> None:
        """Perform platform-specific UI interactions to trigger PDF network requests."""
        try:
            if "issuu.com" in host:
                # Issuu lazy-loads pages; clicking the viewer triggers PDF chunk requests.
                await page.click(".reader__page", timeout=5000)

            elif "fliphtml5.com" in host:
                # FlipHTML5 loads the PDF src after the first page render.
                await page.wait_for_selector(".flipbook-page", timeout=5000)

            elif "epapertoday.com" in host:
                await page.wait_for_selector("iframe", timeout=5000)
                frames = page.frames
                if len(frames) > 1:
                    await frames[1].wait_for_load_state("networkidle", timeout=8000)

            elif any(h in host for h in (
                "epaper.amarujala.com", "epaper.jagran.com", "epaper.bhaskar.com"
            )):
                # These Hindi epaper portals render in an iframe; wait for it to settle.
                await page.wait_for_selector("iframe", timeout=5000)
                await page.wait_for_timeout(3000)

        except Exception as exc:
            # Interaction failures are non-fatal — the network listener may have
            # already captured the PDF URL during page load.
            log.warning(
                f"[fetch_flipbook_pdf] Platform interaction for {host!r} failed "
                f"(non-fatal): {exc}"
            )

    # ------------------------------------------------------------------
    # 3. Unified entry point
    # ------------------------------------------------------------------

    async def fetch(
        self, url: str, is_flipbook: bool = False
    ) -> tuple[bytes, str]:
        """Acquire PDF bytes from *url*, choosing the appropriate strategy.

        Strategy:
        1. If ``is_flipbook=True`` — go straight to Playwright flipbook fetcher.
        2. Otherwise try a direct HTTP download first.
        3. If the direct download returns HTML (the server wants browser rendering),
           automatically fall back to the flipbook fetcher.

        Args:
            url: Target URL (direct PDF link or flipbook viewer page).
            is_flipbook: Hint from the source config that Playwright is required.

        Returns:
            Tuple of ``(pdf_bytes, method_used)`` where *method_used* is one of
            ``"direct"`` or ``"flipbook"``.

        Raises:
            PDFFetchError: If both strategies fail.
        """
        if is_flipbook:
            log.info(f"[fetch] Using flipbook strategy for: {url}")
            pdf_bytes = await self.fetch_flipbook_pdf(url)
            return pdf_bytes, "flipbook"

        log.info(f"[fetch] Attempting direct PDF download: {url}")
        try:
            pdf_bytes = await self.fetch_direct_pdf(url)
            return pdf_bytes, "direct"

        except PDFFetchError as exc:
            # If the failure is due to an HTML response, try flipbook rendering.
            msg = str(exc)
            if "HTML" in msg or "flipbook rendering" in msg:
                log.warning(
                    f"[fetch] Direct fetch returned HTML — falling back to "
                    f"flipbook renderer for: {url}"
                )
                try:
                    pdf_bytes = await self.fetch_flipbook_pdf(url)
                    return pdf_bytes, "flipbook"
                except PDFFetchError as flipbook_exc:
                    raise PDFFetchError(
                        f"Both direct and flipbook fetch strategies failed for {url}. "
                        f"Direct error: {exc}. "
                        f"Flipbook error: {flipbook_exc}"
                    ) from flipbook_exc
            raise

    # ------------------------------------------------------------------
    # 4. Temp file persistence
    # ------------------------------------------------------------------

    @staticmethod
    def save_temp_pdf(pdf_bytes: bytes, job_id: str) -> str:
        """Write *pdf_bytes* to ``/tmp/{job_id}.pdf`` and return the path.

        Args:
            pdf_bytes: Raw PDF content.
            job_id: Unique job identifier used as the filename stem.

        Returns:
            Absolute path string to the saved file.

        Raises:
            PDFFetchError: If the file cannot be written.
        """
        path = Path(tempfile.gettempdir()) / f"{job_id}.pdf"
        try:
            path.write_bytes(pdf_bytes)
            log.debug(
                f"[save_temp_pdf] Saved {len(pdf_bytes) / 1024:.1f} KB → {path}"
            )
        except OSError as exc:
            raise PDFFetchError(
                f"Failed to write temp PDF for job {job_id!r}: {exc}"
            ) from exc
        return str(path)
