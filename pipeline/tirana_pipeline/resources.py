import os
from dagster import EnvVar
from dagster_postgres import PostgresRunStorage
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dagster import ConfigurableResource


class DatabaseResource(ConfigurableResource):
    """SQLAlchemy engine resource backed by PostGIS."""

    database_url: str

    def get_engine(self) -> Engine:
        return create_engine(self.database_url)


def get_resources() -> dict:
    return {
        "db": DatabaseResource(
            database_url=os.getenv("DATABASE_URL", "postgresql://dev:dev@localhost:5432/tirana")
        ),
    }
