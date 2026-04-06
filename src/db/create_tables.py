from src.db.ddl_catalog import DDL_CATALOG
from src.db.postgres_client import PostgresClient


class PostgresSchemaCreator:
    """
    Create PostgreSQL tables based on a DDL catalog.
    """

    def __init__(self) -> None:
        self.client = PostgresClient()

    def create_all_tables(self) -> None:
        for table_name, ddl in DDL_CATALOG.items():
            print(f"Creating table: {table_name}")
            self.client.execute_sql(ddl)
            print(f"Table created successfully: {table_name}")

    def run(self) -> None:
        self.client.test_connection()
        self.create_all_tables()
        print("All PostgreSQL tables were created successfully.")


if __name__ == "__main__":
    creator = PostgresSchemaCreator()
    creator.run()