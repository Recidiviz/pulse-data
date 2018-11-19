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
"""Contains logic for communicating with the persistence layer."""

from recidiviz.persistence.database import database


class EntityMatchingError(Exception):
    """Raised when an error with entity matching is encountered."""
    pass


def get_entity_match(session, person):
    """
    Queries the database to find all matching entities for the given Person.

    Returns:
        The entity matched Person or None if no matching can be done
    """
    existing_people = database.read_people(session,
                                           person.surname,
                                           person.birthdate)

    if len(existing_people) > 1:
        raise EntityMatchingError(
            'Found multiple people who should have been matched previously')

    return existing_people[0] if existing_people else None
