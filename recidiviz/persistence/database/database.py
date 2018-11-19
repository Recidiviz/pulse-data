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
from recidiviz.persistence.database.schema import Person


def read_people(session, surname=None, birthdate=None):
    """
    Read all people matching the optional surname and birthdate. If neither
    the surname or birthdate are provided, then read all people.

    Args:
        surname: The surname to match against
        birthdate: The birthdate to match against
        session: The transaction to read from
    Returns:
        List of people matching the surname and birthdate, if provided
    """
    query = session.query(Person)
    if surname is not None:
        query = query.filter(Person.surname == surname)
    if birthdate is not None:
        query = query.filter(Person.birthdate == birthdate)

    return query.all()
