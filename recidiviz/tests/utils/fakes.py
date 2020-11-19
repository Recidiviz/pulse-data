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
import sqlite3
import threading
from typing import Set

from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.sqlalchemy_engine_manager import SQLAlchemyEngineManager
from recidiviz.utils import environment

_in_memory_sqlite_connection_thread_ids: Set[int] = set()


@environment.test_only
def use_in_memory_sqlite_database(declarative_base: DeclarativeMeta) -> None:
    """Creates a new SqlDatabase object used to communicate to a fake in-memory
    sqlite database.

    This includes:
    1. Creates a new in memory sqlite database engine
    2. Create all tables in the newly created sqlite database
    3. Bind the global SessionMaker to the new fake database engine

    This will assert if an engine has already been initialized for this schema - you must use
    teardown_in_memory_sqlite_databases() to do post-test cleanup, otherwise subsequent tests will fail. It will also
    assert if you attempt to create connections from multiple threads within the context of a single test - SQLite does
    not handle multi-threading well and will often lock or crash when used in a multi-threading scenario.
    """
    def connection_creator() -> sqlite3.Connection:
        global _in_memory_sqlite_connection_thread_ids

        thread_id = threading.get_ident()
        if thread_id in _in_memory_sqlite_connection_thread_ids:
            raise ValueError('Accessing SQLite in-memory database on multiple threads. Either you forgot to call '
                             'teardown_in_memory_sqlite_databases() or you should be using a persistent postgres DB.')

        _in_memory_sqlite_connection_thread_ids.add(thread_id)
        connection = sqlite3.connect('file::memory:', uri=True)

        # Configures SQLite to enforce foreign key constraints
        connection.execute('PRAGMA foreign_keys = ON;')

        return connection

    SQLAlchemyEngineManager.init_engine_for_db_instance(
        db_url='sqlite:///:memory:',
        schema_base=declarative_base,
        creator=connection_creator
    )


@environment.test_only
def teardown_in_memory_sqlite_databases():
    """Cleans up state after a test started with use_in_memory_sqlite_database() is complete."""
    global _in_memory_sqlite_connection_thread_ids
    _in_memory_sqlite_connection_thread_ids.clear()
    SQLAlchemyEngineManager.teardown_engines()


@environment.test_only
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
