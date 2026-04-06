import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class PostgresClient:
    """
    Reusable PostgreSQL client for connection, SQL execution, and validation.
    """

    def __init__(self) -> None:
        load_dotenv()

        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.database = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.schema = os.getenv("POSTGRES_SCHEMA", "public")

        self._validate_config()
        self.engine = self._create_engine()

    def _validate_config(self) -> None:
        required_fields = {
            "POSTGRES_HOST": self.host,
            "POSTGRES_DB": self.database,
            "POSTGRES_USER": self.user,
            "POSTGRES_PASSWORD": self.password,
        }

        missing = [key for key, value in required_fields.items() if not value]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    def _create_engine(self) -> Engine:
        connection_url = (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

        engine = create_engine(
            connection_url,
            future=True,
            pool_pre_ping=True,
        )
        return engine

    @contextmanager
    def get_connection(self):
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    def test_connection(self) -> None:
        with self.get_connection() as conn:
            result = conn.execute(text("SELECT 1 AS connection_ok"))
            row = result.fetchone()

            if row and row[0] == 1:
                print("Connection to PostgreSQL established successfully.")
            else:
                raise ConnectionError("PostgreSQL connection test failed.")

    def execute_sql(self, sql: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(sql))

    def table_exists(self, table_name: str) -> bool:
        sql = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{self.schema}'
              AND table_name = '{table_name}'
        );
        """

        with self.get_connection() as conn:
            result = conn.execute(text(sql))
            return bool(result.scalar())