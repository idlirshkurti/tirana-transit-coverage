"""Dagster resources — MotherDuck (DuckDB) connection."""

import os
from collections.abc import Generator
from contextlib import contextmanager

import duckdb
from dagster import ConfigurableResource


class MotherDuckResource(ConfigurableResource):
    """DuckDB connection resource backed by MotherDuck.

    Connects via the ``md:`` URI scheme using a token read from the
    ``MOTHERDUCK_TOKEN`` environment variable (injected at runtime via .env).
    The spatial extension is pre-installed on MotherDuck — ``LOAD spatial``
    is called on every connection so geometry functions are always available.
    """

    database: str
    token: str

    @contextmanager
    def get_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Yield an open DuckDB connection to MotherDuck; close on exit."""
        if not self.token:
            raise RuntimeError(
                "MOTHERDUCK_TOKEN is not set. "
                "Add it to your .env file or GitHub Actions secrets."
            )
        conn = duckdb.connect(f"md:{self.database}?motherduck_token={self.token}")
        try:
            conn.execute("LOAD spatial;")
            yield conn
        finally:
            conn.close()


def get_resources() -> dict:
    # Token is optional at import time — resolved lazily at connection time.
    # This lets the module load cleanly in CI even without MOTHERDUCK_TOKEN set,
    # as long as no storage asset is materialised.
    return {
        "db": MotherDuckResource(
            database=os.getenv("MOTHERDUCK_DATABASE", "tirana_transit"),
            token=os.getenv("MOTHERDUCK_TOKEN", ""),
        ),
    }
