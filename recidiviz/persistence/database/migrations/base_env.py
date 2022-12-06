# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Helpers for running Alembic migrations from the Alembic env.py file."""

import os
from typing import Any

import alembic
from alembic import context
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_PORT,
    SQLALCHEMY_DB_USER,
)

# String defining database implementation used by SQLAlchemy engine
_DB_TYPE = "postgresql"


def get_sqlalchemy_url() -> str:
    """Returns string needed to connect to database."""
    # These environment variables are expected to be set up via SQLAlchemyEngineManager.update_sqlalchemy_env_vars
    # alembic commands against live databases are expected to be run by our `recidiviz/tools/migrations/` scripts,
    # and never run directly via CLI
    user = os.getenv(SQLALCHEMY_DB_USER)
    password = os.getenv(SQLALCHEMY_DB_PASSWORD)
    host = os.getenv(SQLALCHEMY_DB_HOST)
    db_name = os.getenv(SQLALCHEMY_DB_NAME)
    port = os.getenv(SQLALCHEMY_DB_PORT)

    url = URL.create(
        drivername=_DB_TYPE,
        username=user,
        password=password,
        database=db_name,
        host=host,
        port=port,
    )

    return url.render_as_string(hide_password=False)


def run_migrations_offline(target_metadata: DeclarativeMeta, **kwargs: Any) -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_sqlalchemy_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        transaction_per_migration=True,
        literal_binds=True,
        compare_type=True,
        **kwargs,
    )

    context.run_migrations()


def run_migrations_online(
    target_metadata: DeclarativeMeta, config: alembic.config.Config, **kwargs: Any
) -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Create a new connection if we don't already have one configured from Alembic
    url = get_sqlalchemy_url()
    connectable = config.attributes.get("connection", create_engine(url))

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            transaction_per_migration=True,
            compare_type=True,
            **kwargs,
        )

        context.run_migrations()
