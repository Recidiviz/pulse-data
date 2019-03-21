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

import sqlalchemy

import recidiviz
from recidiviz import Session
from recidiviz.persistence.database.schema import Base


def use_in_memory_sqlite_database() -> None:
    """
    Creates a new SqlDatabase object used to communicate to a fake in-memory
    sqlite database. This includes:
    1. Creates a new in memory sqlite database engine
    2. Create all tables in the newly created sqlite database
    3. Bind the global SessionMaker to the new fake database engine
    """
    recidiviz.db_engine = sqlalchemy.create_engine('sqlite:///:memory:')
    sqlalchemy.event.listen(
        recidiviz.db_engine, 'connect', _enforce_foreign_key_constraints)
    Base.metadata.create_all(recidiviz.db_engine)
    Session.configure(bind=recidiviz.db_engine)


def _enforce_foreign_key_constraints(connection, _) -> None:
    """Configures SQLite to enforce foreign key constraints"""
    connection.execute('PRAGMA foreign_keys = ON;')
