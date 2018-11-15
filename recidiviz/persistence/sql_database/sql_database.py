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
"""Contains logic for communicating with a SQL Database."""

from recidiviz.persistence.sql_database.tables import PEOPLE


class SqlDatabase(object):
    """Communicates with a SQL Database agnostic to the type of database."""

    def __init__(self, engine):
        """
        Use the provided sqlalchemy database engine to perform reads/writes.

        :param engine: The sqlalchemy database engine to use.
        """
        self._engine = engine

    def write_person(self, surname=None, birthdate=None):
        """
        Write the provided person as a new row to the database.

        :param surname: The name to persist.
        :param birthdate: The birthdate to persist.
        """
        connection = self._engine.connect()
        connection.execute(PEOPLE.insert(),
                           surname=surname,
                           birthdate=birthdate)
        connection.close()

    def read_people(self, surname=None, birthdate=None):
        """
        Read all people matching the optional surname and birthdate. If neither
        the surname or birthdate are provided, then read all people.

        :param surname: The surname to match against.
        :param birthdate: The birthdate to match against.
        :return: List of people matching the surname and birthdate, if provided.
        """
        query = PEOPLE.select()
        if surname is not None:
            query = query.where(PEOPLE.c.surname == surname)
        if birthdate is not None:
            query = query.where(PEOPLE.c.birthdate == birthdate)

        connection = self._engine.connect()
        result = connection.execute(query).fetchall()
        connection.close()

        return result
