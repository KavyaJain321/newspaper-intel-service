# Dockerfile
# Builds the Docker image for the newspaper-intel-service.
# Installs system dependencies required by OCR libraries (Poppler, OpenCV, etc.),
# installs Python packages from requirements.txt, and sets up Playwright browsers.
#
# Usage:
#   docker build -t newspaper-intel-service .
#   docker run -p 8000:8000 --env-file .env newspaper-intel-service

FROM python:3.11-slim

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------
# poppler-utils  — pdf2image backend (pdftoppm / pdfinfo)
# libgl1-mesa-glx / libglib2.0-0 — OpenCV runtime (PaddleOCR, EasyOCR)
# libsm6 / libxext6 / libxrender1 — additional OpenCV display libs
# wget / curl    — general-purpose download tools, used by Playwright install
# ca-certificates — TLS for HTTPS requests inside the container
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    wget \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---------------------------------------------------------------------------
# Python dependencies — installed before copying source code so that Docker
# caches this layer and doesn't re-run pip on every source change.
# ---------------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Playwright browser — Chromium only (saves ~400 MB vs full install)
# `--with-deps` installs the OS-level libraries Chromium needs.
# ---------------------------------------------------------------------------
RUN playwright install chromium --with-deps

# ---------------------------------------------------------------------------
# PaddleOCR model pre-warm (optional but recommended)
# Downloads detection + recognition models for Hindi and English at build time
# so the first OCR job doesn't trigger a ~500 MB download at runtime.
# Remove these lines if build time is more important than first-run latency.
# ---------------------------------------------------------------------------
RUN python -c "\
from paddleocr import PaddleOCR; \
PaddleOCR(use_angle_cls=True, lang='en', use_gpu=False, show_log=False); \
PaddleOCR(use_angle_cls=True, lang='hi', use_gpu=False, show_log=False); \
print('PaddleOCR models cached.')" || \
    echo "PaddleOCR pre-warm skipped (will download on first use)."

# ---------------------------------------------------------------------------
# Application source
# ---------------------------------------------------------------------------
COPY . .

# Create logs directory so Loguru can write without errors on startup.
RUN mkdir -p logs

EXPOSE 8000

# ---------------------------------------------------------------------------
# Default command — web service
# Override with `python -m workers.extraction_worker` for the worker container.
# ---------------------------------------------------------------------------
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
