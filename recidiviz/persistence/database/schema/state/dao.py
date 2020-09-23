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
from typing import Dict, List, Type, Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session

from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.errors import PersistenceError


def check_not_dirty(session: Session):
    if session.dirty:
        raise PersistenceError(
            "Session unexpectedly dirty - flush before querying the database.")


def read_people_by_cls_external_ids(
        session: Session,
        state_code: str,
        schema_cls: Type[StateBase],
        cls_external_ids: Iterable[str]) -> List[schema.StatePerson]:
    """Reads all people in the given |state_code| who have an entity of type
    |schema_cls| with an external id in |cls_external_ids| somewhere in their
    entity tree.
    """
    check_not_dirty(session)

    if schema_cls == schema.StatePerson:
        schema_cls = schema.StatePersonExternalId

    logging.info("[DAO] Starting read of external ids of class [%s]",
                 schema_cls.__name__)
    person_ids_result = session.query(schema_cls.person_id) \
        .filter(schema_cls.external_id.in_(cls_external_ids)) \
        .filter(schema_cls.state_code == state_code.upper()).all()
    person_ids = [res[0] for res in person_ids_result]
    logging.info("[DAO] Finished read of external ids of class [%s]. "
                 "Found [%s] person ids.",
                 schema_cls.__name__,
                 len(person_ids))

    query = session.query(schema.StatePerson) \
        .filter(schema.StatePerson.person_id.in_(person_ids))
    schema_persons = query.all()
    logging.info("[DAO] Finished read of [%s] persons.", len(schema_persons))
    return _normalize_record_trees(schema_persons)


def read_placeholder_persons(session: Session, state_code: str) -> List[schema.StatePerson]:
    """Reads all placeholder people from the DB."""
    check_not_dirty(session)

    logging.info("[DAO] Starting read of placeholder person ids")
    person_ids_result = session.query(schema.StatePerson.person_id) \
        .outerjoin(schema.StatePersonExternalId) \
        .filter(schema.StatePerson.state_code == state_code.upper()) \
        .filter(schema.StatePersonExternalId.external_id.is_(None)).all()
    person_ids = [res[0] for res in person_ids_result]
    logging.info("[DAO] Finished read of placeholder person ids. "
                 "Found [%s] person ids.", len(person_ids))
    query = session.query(schema.StatePerson) \
        .filter(schema.StatePerson.person_id.in_(person_ids))
    schema_persons = query.all()
    logging.info("[DAO] Finished read of [%s] persons.", len(schema_persons))
    return _normalize_record_trees(schema_persons)


# TODO(#1907): Rename to read_persons.
def read_people(
        session: Session, full_name=None, birthdate=None
) -> List[schema.StatePerson]:
    """Read all people matching the optional surname and birthdate. If neither
    the surname or birthdate are provided, then read all people."""
    check_not_dirty(session)

    query = session.query(schema.StatePerson)
    if full_name is not None:
        query = query.filter(schema.StatePerson.full_name == full_name)
    if birthdate is not None:
        query = query.filter(schema.StatePerson.birthdate == birthdate)

    people = query.all()
    return _normalize_record_trees(people)


def read_external_ids_of_cls_with_external_id_match(
        session: Session,
        state_code: str,
        schema_cls: Type[StateBase]) -> List[str]:
    """Returns a list of external ids where, for the given |schema_cls|, there
    is more than one entity of that class with that external_id.
    """
    check_not_dirty(session)

    external_ids_subquery = session.query(
        schema_cls.external_id.label('external_id'),
        schema_cls.state_code.label('state_code')
    ).filter(
        schema_cls.state_code == state_code.upper()
    ).filter(
        schema_cls.external_id.isnot(None)
    ).subquery()

    external_ids_by_count = \
        session.query(
            external_ids_subquery.c.external_id,
            func.count(external_ids_subquery.c.external_id).label('cnt')
        ).group_by(external_ids_subquery.c.external_id).subquery()

    external_ids_result = \
        session.query(
            external_ids_by_count.c.external_id
        ).filter(
            external_ids_by_count.c.cnt > 1
        ).all()
    return [res[0] for res in external_ids_result]


def read_people_by_external_ids(
        session: Session,
        _region: str,
        ingested_people: List[entities.StatePerson]
) -> List[schema.StatePerson]:
    """
    Reads all people for the given |region| that have external_ids that match
    the external_ids from the |ingested_people|.
    """
    check_not_dirty(session)

    region_to_external_ids: Dict[str, List[str]] = defaultdict(list)
    for ingested_person in ingested_people:
        for external_id_info in ingested_person.external_ids:
            region_to_external_ids[external_id_info.state_code].append(
                external_id_info.external_id)

    state_persons: List[schema.StatePerson] = []
    for state_code, external_ids in region_to_external_ids.items():
        state_persons += read_people_by_cls_external_ids(
            session, state_code, schema.StatePerson, external_ids)
    return state_persons


def _normalize_record_trees(
        people: List[schema.StatePerson]) -> List[schema.StatePerson]:
    """Removes any duplicate people created by how SQLAlchemy handles joins"""
    deduped_people: List[schema.StatePerson] = []
    count_by_id: Dict[int, int] = defaultdict(lambda: 0)
    for person in people:
        if count_by_id[person.person_id] == 0:
            deduped_people.append(person)
        count_by_id[person.person_id] += 1

    duplicates = [(person_id, count) for person_id, count
                  in count_by_id.items() if count > 1]
    if duplicates:
        id_counts = '\n'.join(
            ['ID {} with count {}'.format(duplicate[0], duplicate[1])
             for duplicate in duplicates])
        logging.error(
            "Duplicate records returned for person IDs:\n%s", id_counts)

    return deduped_people
