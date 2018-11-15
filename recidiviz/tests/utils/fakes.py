# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

from recidiviz.persistence.sql_database import tables
from recidiviz.persistence.sql_database.sql_database import SqlDatabase


def create_in_memory_sqlite_database():
    """
    Creates a new SqlDatabase object used to communicate to a fake in-memory
    sqlite database. This includes:
    1. Creates a new in memory sqlite database engine
    2. Create all tables in the newly created sqlite database

    :return (the fake database, sqlalchemy engine used for direct queries)
    """
    engine = sqlalchemy.create_engine('sqlite:///:memory:')
    tables.METADATA.create_all(engine)

    return SqlDatabase(engine), engine
