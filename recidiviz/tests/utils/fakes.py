# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Initialize our database schema for in-memory testing via sqlite3."""
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.sqlalchemy_engine_manager import \
    SQLAlchemyEngineManager


def use_in_memory_sqlite_database(declarative_base: DeclarativeMeta) -> None:
    """Creates a new SqlDatabase object used to communicate to a fake in-memory
    sqlite database.

    This includes:
    1. Creates a new in memory sqlite database engine
    2. Create all tables in the newly created sqlite database
    3. Bind the global SessionMaker to the new fake database engine
    """

    SQLAlchemyEngineManager.init_engine_for_db_instance(
        db_url='sqlite:///:memory:',
        schema_base=declarative_base)


def use_on_disk_sqlite_database(declarative_base: DeclarativeMeta) -> None:
    """Creates a new SqlDatabase object used to communicate to an on-disk
    sqlite database.

    This includes:
    1. Creates a new on-disk sqlite database engine
    2. Create all tables in the newly created sqlite database
    3. Bind the global SessionMaker to the new database engine
    """
    SQLAlchemyEngineManager.init_engine_for_db_instance(
        db_url='sqlite:///recidiviz.db',
        schema_base=declarative_base)


def use_on_disk_postgresql_database(declarative_base: DeclarativeMeta) -> None:
    """Creates a new SqlDatabase object used to communicate to an on-disk
    PostgreSQL database.

    This includes:
    1. Create all tables in the newly created sqlite database
    2. Bind the global SessionMaker to the new database engine

    Note, this assumes the following is true:
    - A local postgres server has been started
      (`pg_ctl -D /usr/local/var/postgres start`)
    - A DB named `recidiviz_test` exists (`createdb recidiviz_test`)
    - A User `usr` has been created with no password (`createdb usr`)
    """
    url = 'postgresql://usr:@localhost:5432/recidiviz_test'
    SQLAlchemyEngineManager.init_engine_for_db_instance(
        db_url=url, schema_base=declarative_base)


def _enforce_foreign_key_constraints(connection, _) -> None:
    """Configures SQLite to enforce foreign key constraints"""
    connection.execute('PRAGMA foreign_keys = ON;')
