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

from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.persistence.database.migrations.base_env import (
    run_migrations_offline,
    run_migrations_online,
)

# Import anything from the two jails schema.py files to ensure the table class
# declarations are run within the Alembic environment
# pylint:disable=unused-import
from recidiviz.persistence.database.schema.aggregate.schema import CaFacilityAggregate

# pylint:disable=unused-import
from recidiviz.persistence.database.schema.county.schema import Person

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# Metadata from schema
target_metadata = JailsBase.metadata

if context.is_offline_mode():
    run_migrations_offline(target_metadata)
else:
    run_migrations_online(target_metadata, config)
