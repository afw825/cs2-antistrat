import logging
import os
from urllib.parse import urlparse


def configure_sentry() -> None:
    """Initialize Sentry SDK from environment if DSN is provided."""
    logger = logging.getLogger(__name__)
    sentry_dsn = (os.getenv("SENTRY_DSN") or "").strip()
    if not sentry_dsn:
        return

    parsed = urlparse(sentry_dsn)
    if parsed.scheme not in {"http", "https"}:
        logger.warning("Skipping Sentry init due to invalid DSN scheme")
        return

    try:
        import sentry_sdk

        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=os.getenv("APP_ENV", "development"),
            traces_sample_rate=0.0,
        )
        logger.info("Sentry initialized")
    except Exception:
        logger.exception("Failed to initialize Sentry; continuing without it")


def configure_logging() -> None:
    """Configure root logging once per process using env-driven level."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    if root.handlers:
        root.setLevel(level)
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
