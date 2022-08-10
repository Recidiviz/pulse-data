"""Manages alembic and sqlalchemy environments."""
# pylint: disable=wrong-import-position

import os

# Hackity hack to get around the fact that alembic runs this file as a top-level
# module rather than a child of the recidiviz module
import sys

module_path = os.path.abspath(__file__)
# Walk up directories to reach main package
while not module_path.split("/")[-1] == "recidiviz":
    if module_path == "/":
        raise RuntimeError("Top-level recidiviz package not found")
    module_path = os.path.dirname(module_path)
# Must insert parent directory of main package
sys.path.insert(0, os.path.dirname(module_path))

from logging.config import fileConfig

from alembic import context
from sqlalchemy.schema import SchemaItem

from recidiviz.persistence.database.migrations.base_env import (
    run_migrations_offline,
    run_migrations_online,
)
from recidiviz.persistence.database.schema.pathways.schema import (
    RUN_MIGRATIONS,
    PathwaysBase,
)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

# Metadata from schema
target_metadata = PathwaysBase.metadata


def include_object(
    schema_item: SchemaItem,
    _name: str,
    object_type: str,
    _reflected: bool,
    _compare_to: SchemaItem,
) -> bool:
    if object_type == "table":
        return schema_item.info.get(RUN_MIGRATIONS, False)
    return True


if context.is_offline_mode():
    run_migrations_offline(
        target_metadata,
        # Only run migrations on objects we've configured to run them on.
        include_schemas=True,
        include_object=include_object,
    )
else:
    run_migrations_online(
        target_metadata,
        config,
        # Only run migrations on objects we've configured to run them on.
        include_schemas=True,
        include_object=include_object,
    )
