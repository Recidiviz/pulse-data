# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Mixin for connecting to cloud sql database, either directly or through a proxy."""

from contextlib import contextmanager
from typing import Iterator, Optional

from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class CloudSqlConnectionMixin:
    """Mixin for connecting to cloud sql database, either directly or through a proxy."""

    @classmethod
    @contextmanager
    def get_session(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        with_proxy: bool,
        autocommit: bool = True,
        secret_prefix_override: Optional[str] = None
    ) -> Iterator[Session]:
        """Creates a session for the cloud sql database associated with |database_key|,
        connecting directly if |with_proxy| is False and via a cloud sql proxy which
        needs to already be running if |with_proxy| is True."""
        if with_proxy:
            with SessionFactory.for_proxy(
                database_key=database_key,
                autocommit=autocommit,
                secret_prefix_override=secret_prefix_override,
            ) as session:
                yield session
        else:
            with SessionFactory.using_database(
                database_key=database_key,
                autocommit=autocommit,
                secret_prefix_override=secret_prefix_override,
            ) as session:
                yield session
