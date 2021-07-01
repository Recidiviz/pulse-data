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
"""Helpers for running alembic migrations from the Alembic env.py file."""

import os

import alembic
from alembic import context
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeMeta

from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_USER,
    SQLALCHEMY_SSL_CERT_PATH,
    SQLALCHEMY_SSL_KEY_PATH,
    SQLALCHEMY_USE_SSL,
)

# String defining database implementation used by SQLAlchemy engine
_DB_TYPE = "postgresql"


def get_sqlalchemy_url() -> str:
    """Returns string needed to connect to database"""

    # Boolean int (0 or 1) indicating whether to use SSL to connect to the
    # database
    use_ssl = int(os.getenv(SQLALCHEMY_USE_SSL, "0"))

    if use_ssl not in {0, 1}:
        raise RuntimeError(
            "Invalid value for use_ssl: {use_ssl}".format(use_ssl=use_ssl)
        )
    return _get_sqlalchemy_url(use_ssl=bool(use_ssl))


def run_migrations_offline(target_metadata: DeclarativeMeta) -> None:
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
    )

    context.run_migrations()


def run_migrations_online(
    target_metadata: DeclarativeMeta, config: alembic.config.Config
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
        )

        context.run_migrations()


def _get_sqlalchemy_url(use_ssl: bool = True) -> str:
    """Returns string used for SQLAlchemy engine, with SSL params if |use_ssl| is True."""

    user = os.getenv(SQLALCHEMY_DB_USER)
    password = os.getenv(SQLALCHEMY_DB_PASSWORD)
    host = os.getenv(SQLALCHEMY_DB_HOST)
    db_name = os.getenv(SQLALCHEMY_DB_NAME)

    url = URL.create(
        drivername=_DB_TYPE,
        username=user,
        password=password,
        database=db_name,
        host=host,
    )
    if use_ssl:
        ssl_key_path = os.getenv(SQLALCHEMY_SSL_KEY_PATH)
        ssl_cert_path = os.getenv(SQLALCHEMY_SSL_CERT_PATH)
        url = url.update_query_dict(
            {"sslkey": ssl_key_path, "sslcert": ssl_cert_path}, append=True
        )

    return url.render_as_string(hide_password=False)
