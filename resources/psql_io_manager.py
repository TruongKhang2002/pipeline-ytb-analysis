# resources/psql_io_manager.py

from dagster import IOManager, io_manager
from sqlalchemy import create_engine


class PostgresIOManager(IOManager):
    def __init__(self, engine):
        self.engine = engine

    def handle_output(self, context, obj):
        # obj là DataFrame đã được insert ở assets
        pass

    def load_input(self, context):
        # Có thể implement nếu muốn đọc lại từ PostgreSQL
        pass


@io_manager(config_schema={"conn_str": str})
def psql_io_manager(init_context):
    engine = create_engine(init_context.resource_config["conn_str"])
    return PostgresIOManager(engine)