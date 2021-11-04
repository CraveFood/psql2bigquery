# pylint: disable=import-outside-toplevel
import logging
import os
import sys


def setup_logging():
    _setup_sentry()
    _setup_stout()


def _setup_sentry():
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if sentry_dsn:
        try:
            import sentry_sdk
            from sentry_sdk.integrations.logging import LoggingIntegration
        except ImportError as exc:
            raise ImportError(
                "Unable to find sentry-sdk dependency "
                "but SENTRY_DSN environment variable is present. "
                "Install with `pip install psql2bigquery[sentry]`."
            ) from exc

        sentry_logging = LoggingIntegration(
            level=logging.INFO,
            event_level=logging.ERROR,
        )
        sentry_sdk.init(  # pylint: disable=abstract-class-instantiated
            dsn=sentry_dsn,
            environment=os.environ.get("ENV"),
            integrations=[sentry_logging],
        )


def _setup_stout():
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.getLogger().setLevel("INFO")
