import logging
import os


def configure_sentry() -> None:
    """Initialize Sentry SDK from environment if DSN is provided."""
    import sentry_sdk

    sentry_dsn = os.getenv("SENTRY_DSN")
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=os.getenv("APP_ENV", "development"),
            traces_sample_rate=0.0,
        )
        logger = logging.getLogger(__name__)
        logger.info("Sentry initialized")


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
