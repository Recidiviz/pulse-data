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

from recidiviz import Session
from recidiviz.persistence import entity_matching
from recidiviz.persistence.database.schema import Person


def write(ingest_info):
    """
    Persist each person in the ingest_info. If a person with the given
    surname/birthday already exists, then update that person.

    Args:
         ingest_info: The IngestInfo containing each person
    """
    for ingest_info_person in ingest_info.person:
        person = _convert_person(ingest_info_person)
        session = Session()
        try:
            existing_person = entity_matching.get_entity_match(session, person)

            if existing_person is None:
                session.add(person)
            else:
                person.person_id = existing_person.person_id
                session.add(session.merge(person))

            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


# TODO: This will be done fully in another class
def _convert_person(person):
    """Converts person."""
    return Person(surname=person.surname,
                  birthdate=person.birthdate,
                  place_of_residence=person.place_of_residence)
