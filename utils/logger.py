# utils/logger.py
# Configures and exports a Loguru logger instance for use across the service.
# Sets log format, rotation policy, log level from environment, and optional
# JSON-structured output for production deployments on Render.

import io
import os
import sys
from pathlib import Path
from loguru import logger

# Ensure logs directory exists before adding the file sink.
Path("logs").mkdir(exist_ok=True)

# On Windows the default console encoding is cp1252 which cannot render
# Unicode arrows/symbols used in log messages.  Wrap stdout in a UTF-8
# writer so loguru never raises UnicodeEncodeError.
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )

# Remove the default Loguru handler so we control all sinks ourselves.
logger.remove()

_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
_LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{line} | {message}"

# Console sink — colourised, goes to stdout so Render's log drain picks it up.
logger.add(
    sys.stdout,
    format=_LOG_FORMAT,
    level=_LOG_LEVEL,
    colorize=True,
    enqueue=True,       # thread-safe async queue; safe for multi-worker setups
)

# File sink — rotates at 10 MB, keeps the 5 most recent files.
logger.add(
    "logs/service.log",
    format=_LOG_FORMAT,
    level=_LOG_LEVEL,
    rotation="10 MB",
    retention=5,
    enqueue=True,
    encoding="utf-8",
)

# Single instance re-exported for the whole service.
log = logger
