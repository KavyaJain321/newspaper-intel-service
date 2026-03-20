# app.py
# Entry point for the FastAPI application.
# Defines API routes for submitting newspaper PDF processing jobs,
# checking job status, and retrieving extracted article data.
# Uses FastAPI BackgroundTasks for async job dispatch — no Redis required.

from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from core.supabase_writer import SupabaseWriter, _client as supabase_client
from models.schemas import ExtractionRequest, ExtractionResponse, JobStatusResponse
from utils.logger import log
from workers.extraction_worker import process_extraction_job

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_VERSION = "1.0.0"
_SERVICE_SECRET_KEY = os.environ.get("SERVICE_SECRET_KEY", "")
_PADDLE_MODEL_DIR = Path.home() / ".paddleocr"

_CORS_ORIGINS: list[str] = [
    o.strip()
    for o in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8000").split(",")
    if o.strip()
]


# ---------------------------------------------------------------------------
# Startup / shutdown lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Verify external dependencies on startup and log readiness."""

    # ── Supabase ───────────────────────────────────────────────────────────
    try:
        supabase_client.table("newspaper_jobs").select("job_id").limit(1).execute()
        log.info("[startup] Supabase connection OK")
    except Exception as exc:
        log.error(f"[startup] Supabase connection FAILED: {exc}")

    # ── PaddleOCR model cache check ────────────────────────────────────────
    if _PADDLE_MODEL_DIR.exists():
        log.info(f"[startup] PaddleOCR model cache found at {_PADDLE_MODEL_DIR}")
    else:
        log.warning(
            "[startup] PaddleOCR model cache NOT found. "
            "The first OCR job will download ~500 MB of models. "
            "Pre-warm with: python -c \"from paddleocr import PaddleOCR; PaddleOCR(lang='hi')\""
        )

    log.info(f"[startup] Newspaper Intel Service v{_VERSION} is ready.")
    yield
    log.info("[shutdown] Service stopping.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Newspaper Intel Service",
    version=_VERSION,
    description="Extracts and keyword-matches articles from Indian newspaper PDFs for ROBIN.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    log.info(f"{request.method} {request.url.path} → {response.status_code} ({duration_ms:.1f}ms)")
    return response


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def _verify_key(x_service_key: Optional[str]) -> None:
    if not _SERVICE_SECRET_KEY:
        log.warning("[auth] SERVICE_SECRET_KEY is not set — endpoint is unprotected")
        return
    if x_service_key != _SERVICE_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-Service-Key header.",
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

# ── 1. Health check ────────────────────────────────────────────────────────

@app.get("/health", summary="Health check", tags=["ops"])
def health():
    return {
        "status":    "ok",
        "version":   _VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── 2. POST /extract ───────────────────────────────────────────────────────

@app.post(
    "/extract",
    summary="Submit extraction job (async)",
    description=(
        "Starts a PDF extraction job in the background and returns immediately. "
        "Poll GET /jobs/{job_id} for status and results."
    ),
    tags=["extraction"],
    status_code=status.HTTP_202_ACCEPTED,
)
async def extract(
    body: ExtractionRequest,
    background_tasks: BackgroundTasks,
    x_service_key: Optional[str] = Header(default=None),
):
    _verify_key(x_service_key)

    job_id = str(uuid.uuid4())
    job_data = {**body.model_dump(), "job_id": job_id}

    # Write "queued" status immediately so ROBIN can start polling.
    writer = SupabaseWriter()
    await writer.update_job_status(
        job_id=job_id,
        status="queued",
        brief_id=body.brief_id,
        source_name=body.source_name,
        pdf_url=body.pdf_url,
    )

    # FastAPI dispatches sync functions to a thread-pool executor automatically.
    # The full pipeline (fetch → OCR → match → write) runs in its own thread,
    # keeping the event loop free for other requests.
    background_tasks.add_task(process_extraction_job, job_data)

    log.info(
        f"[POST /extract] Job started | job_id={job_id!r} | "
        f"brief={body.brief_id!r} | source={body.source_name!r}"
    )

    return {
        "job_id":   job_id,
        "status":   "queued",
        "brief_id": body.brief_id,
    }


# ── 3. GET /jobs/{job_id} ──────────────────────────────────────────────────

@app.get(
    "/jobs/{job_id}",
    summary="Poll job status",
    response_model=JobStatusResponse,
    tags=["extraction"],
)
async def get_job_status(
    job_id: str,
    x_service_key: Optional[str] = Header(default=None),
):
    _verify_key(x_service_key)

    try:
        response = (
            supabase_client
            .table("newspaper_jobs")
            .select("*")
            .eq("job_id", job_id)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        log.error(f"[GET /jobs/{job_id}] Supabase query failed: {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not reach database. Please retry.",
        )

    if not response.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id!r} not found.",
        )

    row = response.data[0]
    raw_result = row.get("result")

    extraction_result: Optional[ExtractionResponse] = None
    if raw_result and row.get("status") in ("completed", "failed"):
        try:
            extraction_result = ExtractionResponse(**raw_result)
        except Exception as exc:
            log.warning(f"[GET /jobs/{job_id}] Could not parse stored result: {exc}")

    progress: Optional[str] = None
    if row.get("status") == "processing":
        progress = "Extraction in progress — check back in 30–60 seconds."
    elif row.get("status") == "queued":
        progress = "Job is queued and will start shortly."

    return JobStatusResponse(
        job_id=job_id,
        status=row["status"],
        progress=progress,
        result=extraction_result,
    )


# ── 4. POST /extract-sync ──────────────────────────────────────────────────

@app.post(
    "/extract-sync",
    summary="Submit extraction job (synchronous — testing only)",
    description=(
        "⚠ NOT FOR PRODUCTION. Runs extraction synchronously and waits up to 120s. "
        "Use POST /extract + GET /jobs/{job_id} in production."
    ),
    tags=["extraction", "testing"],
)
async def extract_sync(
    body: ExtractionRequest,
    x_service_key: Optional[str] = Header(default=None),
):
    _verify_key(x_service_key)

    job_id = str(uuid.uuid4())
    job_data = {**body.model_dump(), "job_id": job_id}

    log.info(f"[POST /extract-sync] Synchronous job_id={job_id!r} | brief={body.brief_id!r}")

    try:
        result = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None,
                process_extraction_job,
                job_data,
            ),
            timeout=120.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Extraction timed out after 120s (job_id={job_id!r}). Use POST /extract.",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Extraction failed: {type(exc).__name__}: {exc}",
        )

    return JSONResponse(content={
        **result,
        "_warning": "Testing endpoint only. Use POST /extract in production.",
    })
