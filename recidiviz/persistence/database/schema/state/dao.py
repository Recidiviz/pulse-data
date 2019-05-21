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

"""Data Access Object (DAO) with logic for accessing state-level information
from a SQL Database."""

from collections import defaultdict
import logging
from typing import Dict, List

from sqlalchemy.orm import Session

from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema.state import schema


def read_people(session, full_name=None, birthdate=None) \
        -> List[entities.StatePerson]:
    """Read all people matching the optional surname and birthdate. If neither
    the surname or birthdate are provided, then read all people."""
    query = session.query(schema.StatePerson)
    if full_name is not None:
        query = query.filter(schema.StatePerson.full_name == full_name)
    if birthdate is not None:
        query = query.filter(schema.StatePerson.birthdate == birthdate)
    return _convert_and_normalize_record_trees(query.all())


def read_people_by_external_ids(session: Session,
                                _region: str,
                                ingested_people: List[entities.StatePerson]) \
        -> List[entities.StatePerson]:
    """
    Reads all people for the given |region| that have external_ids that match
    the external_ids from the |ingested_people|.
    """
    region_to_external_ids: Dict[str, List[str]] = defaultdict(list)
    for ingested_person in ingested_people:
        for external_id_info in ingested_person.external_ids:
            region_to_external_ids[external_id_info.state_code].append(
                external_id_info.external_id)

    # TODO(1625): Revert back to the proper version of this query
    state_persons: List[schema.StatePerson] = \
        session.query(schema.StatePerson).all()

    # state_persons: List[schema.StatePerson] = []
    # for state_code, external_ids in region_to_external_ids.items():
    #     query = session.query(schema.StatePerson)
    #
    #     query = query.join(schema.StatePersonExternalId) \
    #         .filter(schema.StatePersonExternalId.state_code == state_code) \
    #         .filter(schema.StatePerson.person_id ==
    #                 schema.StatePersonExternalId.person_id) \
    #         .filter(schema.StatePersonExternalId.external_id.in_(
    #             external_ids))
    #
    #     state_persons += query.all()

    return _convert_and_normalize_record_trees(state_persons)


def _convert_and_normalize_record_trees(
        people: List[schema.StatePerson]) -> List[entities.StatePerson]:
    """Converts schema record trees to persistence layer models and removes
    any duplicate people created by how SQLAlchemy handles joins
    """
    converted_people: List[entities.StatePerson] = []
    count_by_id: Dict[int, int] = defaultdict(lambda: 0)
    for person in people:
        if count_by_id[person.person_id] == 0:
            converted = converter.convert_schema_object_to_entity(person)
            if not isinstance(converted, entities.StatePerson):
                raise ValueError(
                    f"Unexpected return type [{converted.__class__}]")
            converted_people.append(converted)
        count_by_id[person.person_id] += 1

    duplicates = [(person_id, count) for person_id, count
                  in count_by_id.items() if count > 1]
    if duplicates:
        id_counts = '\n'.join(
            ['ID {} with count {}'.format(duplicate[0], duplicate[1])
             for duplicate in duplicates])
        logging.error(
            "Duplicate records returned for person IDs:\n%s", id_counts)

    return converted_people
