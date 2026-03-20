# Dockerfile
# Builds the Docker image for the newspaper-intel-service.
# Installs system dependencies required by OCR libraries (OpenCV, etc.),
# installs Python packages from requirements.txt, and sets up Playwright browsers.
# NOTE: poppler-utils is NOT needed — page rendering uses PyMuPDF directly.
#
# Usage:
#   docker build -t newspaper-intel-service .
#   docker run -p 8000:8000 --env-file .env newspaper-intel-service

FROM python:3.11-slim

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------
# libgl1          — OpenCV runtime (replaces libgl1-mesa-glx in Debian Bookworm)
# libglib2.0-0    — OpenCV / GLib runtime
# libsm6          — X11 session manager (OpenCV headless)
# libxext6        — X11 extensions (OpenCV headless)
# libxrender1     — X rendering (OpenCV headless)
# wget / curl     — used by playwright install
# ca-certificates — TLS for HTTPS requests inside the container
#
# NOTE: python:3.11-slim is based on Debian Bookworm where libgl1-mesa-glx
# was renamed to libgl1. Using libgl1 works on both Bullseye and Bookworm.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
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
# We install Chromium's OS dependencies manually because --with-deps tries
# to install Ubuntu-era font packages (ttf-unifont, ttf-ubuntu-font-family)
# that don't exist on Debian Trixie.
# ---------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-unifont \
    fonts-liberation \
    fonts-noto-color-emoji \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

RUN playwright install chromium

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
