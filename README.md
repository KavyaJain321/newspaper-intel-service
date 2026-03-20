# Newspaper Intel Service

The Newspaper Intel Service is a FastAPI microservice that ingests Indian newspaper PDFs — from direct download links, aggregator sites, or browser-rendered flipbooks — and extracts article text using a three-engine OCR pipeline (PyMuPDF → PaddleOCR → EasyOCR). It analyses page layout to identify column boundaries and article regions, runs fuzzy keyword matching against a provided brief, crops matched article images, and persists everything to Supabase. Jobs run asynchronously via an RQ/Redis background worker so the API returns immediately and results are polled.

This service is the intelligence layer for ROBIN (Rapid Open-source Brief Intelligence Network). When ROBIN creates a monitoring brief with geographic targets and keywords, it calls `/extract` with the relevant newspaper URLs and keywords. The service processes each PDF in the background, writes matched articles to the shared `content_items` Supabase table, and updates `newspaper_jobs` so ROBIN can poll for completion and surface results to analysts.

---

## Local Setup

**Prerequisites:** Python 3.11, Redis running locally, a Supabase project with the `newspaper_jobs` table created (SQL in `core/supabase_writer.py`).

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd newspaper-intel-service

# 2. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Install Playwright's Chromium browser
playwright install chromium

# 5. Pre-warm PaddleOCR models (~500 MB, one-time download)
python -c "from paddleocr import PaddleOCR; PaddleOCR(lang='hi', use_gpu=False, show_log=False)"

# 6. Configure environment
cp .env.example .env
# Edit .env and fill in SUPABASE_URL, SUPABASE_SERVICE_KEY, SERVICE_SECRET_KEY

# 7. Start Redis (separate terminal)
redis-server

# 8. Start the RQ worker (separate terminal)
python -m workers.extraction_worker

# 9. Start the API server
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

The API is now available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SUPABASE_URL` | Yes | — | Supabase project URL (e.g. `https://xyz.supabase.co`) |
| `SUPABASE_SERVICE_KEY` | Yes | — | Supabase **service role** key (not the anon key) |
| `REDIS_URL` | Yes | `redis://localhost:6379` | Redis connection string |
| `SERVICE_SECRET_KEY` | Yes | — | Shared secret sent in `X-Service-Key` header by ROBIN |
| `MAX_PDF_SIZE_MB` | No | `50` | Maximum PDF file size accepted; larger files are rejected |
| `OCR_CONFIDENCE_THRESHOLD` | No | `0.6` | Minimum OCR confidence (0–1); lower scores are discarded |
| `LOG_LEVEL` | No | `INFO` | Loguru level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `CORS_ORIGINS` | No | `http://localhost:3000` | Comma-separated list of allowed CORS origins |
| `RENDER_EXTERNAL_URL` | No | — | Public Render URL; used in self-referencing log messages |
| `ACTIVE_SOURCES` | No | `all` | Comma-separated source names to enable, or `all` |
| `HINDU_EMAIL` | No | — | Login email for The Hindu subscription (Tier 3) |
| `HINDU_PASSWORD` | No | — | Login password for The Hindu subscription (Tier 3) |

---

## API Endpoints

### `GET /health`
No authentication required. Used by UptimeRobot to prevent Render free-tier spin-down.

```bash
curl http://localhost:8000/health
```
```json
{
  "status": "ok",
  "version": "1.0.0",
  "timestamp": "2024-10-15T06:30:00.000Z"
}
```

---

### `POST /extract`
Submit a PDF extraction job. Returns immediately with a `job_id`; processing happens in the background worker. Poll `/jobs/{job_id}` for results.

**Header:** `X-Service-Key: <SERVICE_SECRET_KEY>`

```bash
curl -X POST http://localhost:8000/extract \
  -H "Content-Type: application/json" \
  -H "X-Service-Key: your-secret-key" \
  -d '{
    "pdf_url": "https://example.com/todays-paper.pdf",
    "keywords": ["election", "BJP", "चुनाव"],
    "source_name": "Dainik Jagran",
    "source_language": "hi",
    "brief_id": "brief_abc123",
    "fuzzy_threshold": 75,
    "is_flipbook": false
  }'
```
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "brief_id": "brief_abc123"
}
```

---

### `GET /jobs/{job_id}`
Poll job status. Returns `status` of `queued` → `processing` → `completed` or `failed`. The `result` field is populated on completion.

**Header:** `X-Service-Key: <SERVICE_SECRET_KEY>`

```bash
curl http://localhost:8000/jobs/550e8400-e29b-41d4-a716-446655440000 \
  -H "X-Service-Key: your-secret-key"
```
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "progress": null,
  "result": {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "completed",
    "brief_id": "brief_abc123",
    "source_name": "Dainik Jagran",
    "total_matches": 3,
    "articles": [
      {
        "keyword_matched": "चुनाव",
        "keyword_score": 0.97,
        "article_headline": "उत्तर प्रदेश में चुनाव की तैयारी",
        "article_text": "लखनऊ। प्रदेश में आगामी चुनाव को लेकर...",
        "page_number": 1,
        "bounding_box": [42.0, 310.0, 398.0, 620.0],
        "image_crop_url": "https://xyz.supabase.co/storage/v1/object/public/newspaper-crops/550e8400/article_0.jpg",
        "language_detected": "hi",
        "extraction_method": "paddleocr"
      }
    ],
    "processing_time_seconds": 47.3,
    "error": null
  }
}
```

---

### `POST /extract-sync` *(testing only)*
Runs extraction synchronously and waits up to 120 seconds. For development and debugging. **Do not call from ROBIN in production.**

```bash
curl -X POST http://localhost:8000/extract-sync \
  -H "Content-Type: application/json" \
  -H "X-Service-Key: your-secret-key" \
  -d '{
    "pdf_url": "https://example.com/sample.pdf",
    "keywords": ["flood", "बाढ़"],
    "source_name": "Hindustan",
    "source_language": "hi",
    "brief_id": "test_brief_001",
    "fuzzy_threshold": 80
  }'
```

---

## Integration with ROBIN

ROBIN integrates with this service through two flows:

**Brief-triggered extraction** — When an analyst creates or updates a monitoring brief in ROBIN, the ROBIN backend calls `POST /extract` for each relevant newspaper source identified by the `SourceRegistry` (matching the brief's geographic focus and language). It stores the returned `job_id` against the brief.

**Result polling** — ROBIN polls `GET /jobs/{job_id}` every 30 seconds until `status` is `completed` or `failed`. On completion, matched articles are already written to the shared `content_items` Supabase table (keyed by `brief_id` in the `metadata` column), so ROBIN can also query them directly via Supabase for richer filtering.

```
ROBIN                          Newspaper Intel Service           Supabase
  │                                      │                          │
  │── POST /extract ──────────────────►  │                          │
  │◄─ { job_id } ─────────────────────── │                          │
  │                                      │── fetch PDF ──────────►  │
  │── GET /jobs/{job_id} ─────────────►  │── OCR + match ────────►  │
  │◄─ { status: "processing" } ───────── │── write content_items ─► │
  │                                      │── write newspaper_jobs ─► │
  │── GET /jobs/{job_id} ─────────────►  │                          │
  │◄─ { status: "completed", result } ── │                          │
  │                                      │                          │
  │── SELECT * FROM content_items ────────────────────────────────►  │
  │   WHERE metadata->>'brief_id' = ?                               │
```

---

## Deployment on Render

1. Push this repo to GitHub.
2. In the Render dashboard, click **New → Blueprint** and point it at the repo.
3. Render reads `render.yaml` and creates three resources: web service, worker, Redis.
4. Set the four `sync: false` environment variables in the Render dashboard:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_KEY`
   - `SERVICE_SECRET_KEY`
   - `RENDER_EXTERNAL_URL` (the web service's `.onrender.com` URL)
5. Set up UptimeRobot (free) to ping `GET /health` every 5 minutes to prevent spin-down.

> **PaddleOCR models on Render:** The `Dockerfile` pre-warms Hindi and English models at build time. If using the native Python runtime (`render.yaml`), the first OCR job will download ~500 MB. Consider switching to the Docker runtime for production to avoid this.

---

## Project Structure

```
newspaper-intel-service/
├── app.py                      # FastAPI application, routes, middleware
├── requirements.txt            # Pinned Python dependencies
├── .env.example                # Environment variable template
├── render.yaml                 # Render.com deployment blueprint
├── Dockerfile                  # Docker build (alternative deployment)
├── core/
│   ├── pdf_fetcher.py          # HTTP + Playwright PDF acquisition
│   ├── extractor.py            # PyMuPDF → PaddleOCR → EasyOCR pipeline
│   ├── layout_analyzer.py      # Column detection and article grouping
│   ├── keyword_matcher.py      # Fuzzy keyword matching with transliteration
│   ├── article_cropper.py      # Page rendering and article image cropping
│   ├── source_registry.py      # Newspaper source database (22 sources)
│   └── supabase_writer.py      # Supabase Storage and Database writes
├── workers/
│   └── extraction_worker.py    # RQ worker and enqueue_job() helper
├── models/
│   └── schemas.py              # Pydantic v2 request/response schemas
└── utils/
    ├── logger.py               # Loguru configuration
    └── language_detector.py    # langdetect with Odia Unicode override
```
