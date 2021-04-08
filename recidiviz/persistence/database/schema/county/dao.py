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

"""Data Access Object (DAO) with logic for accessing county-level information
from a SQL Database."""
import datetime
from collections import defaultdict
import logging
from typing import Dict, List

from sqlalchemy.orm import Session, Query

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.persistence.entity.county import entities
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.schema.county.schema import Person, Booking


def read_people(
    session: Session, full_name: str = None, birthdate: datetime.date = None
) -> List[entities.Person]:
    """
    Read all people matching the optional surname and birthdate. If neither
    the surname or birthdate are provided, then read all people.

    Args:
        full_name: The full name to match against
        birthdate: The birthdate to match against
        session: The transaction to read from
    Returns:
        List of people matching the surname and birthdate, if provided
    """
    query = session.query(Person)
    if full_name is not None:
        query = query.filter(Person.full_name == full_name)
    if birthdate is not None:
        query = query.filter(Person.birthdate == birthdate)
    return _convert_and_normalize_record_trees(query.all())


def read_people_by_external_ids(
    session: Session, region: str, ingested_people: List[entities.Person]
) -> List[entities.Person]:
    """
    Reads all people for the given |region| that have external_ids that match
    the external_ids from the |ingested_people|

    Args:
        session: The transaction to read from
        region: The region to match against
        ingested_people: The ingested people to match against
    Returns: List of people that match the provided |ingested_people|
    """
    external_ids = {p.external_id for p in ingested_people}
    query = (
        session.query(Person)
        .filter(Person.region == region)
        .filter(Person.external_id.in_(external_ids))
    )
    return _convert_and_normalize_record_trees(query.all())


def read_people_with_open_bookings(
    session: Session, region: str, ingested_people: List[entities.Person]
) -> List[entities.Person]:
    """
    Reads all people for a given |region| that have open bookings and can be
    matched with the provided |ingested_people|.

    Args:
        session: The transaction to read from
        region: The region to match against
        ingested_people: The ingested people to match against
    Returns:
        List of people with open bookings matching the provided args
    """
    query = _query_people_and_open_bookings(session, region)

    full_names = {p.full_name for p in ingested_people}
    query = query.filter(Person.full_name.in_(full_names))
    return _convert_and_normalize_record_trees([person for person, _ in query.all()])


def read_people_with_open_bookings_scraped_before_time(
    session: Session, region: str, time: datetime.datetime
) -> List[entities.Person]:
    """
    Reads all people with open bookings in the given region that have a
    last_scraped_time set to a time earlier than the provided datetime.

    Args:
        session: The transaction to read from
        region: The region to match against
        time: The datetime exclusive upper bound on last_scrape_time to match
            against
    Returns:
        List of people matching the provided args
    """
    query = _query_people_and_open_bookings(session, region).filter(
        Booking.last_seen_time < time
    )
    return _convert_and_normalize_record_trees([person for person, _ in query.all()])


def _query_people_and_open_bookings(session: Session, region: str) -> Query:
    """
    Returns a list of tuples of (person, booking) for all open bookings.

    Args:
        session: Transaction to read from
        region: The region to match against.
    """
    # pylint: disable=W0143
    return (
        session.query(Person, Booking)
        .filter(Person.person_id == Booking.person_id)
        .filter(Person.region == region)
        .filter(
            Booking.custody_status.notin_(CustodyStatus.get_raw_released_statuses())
        )
    )


def _convert_and_normalize_record_trees(people: List[Person]) -> List[entities.Person]:
    """Converts schema record trees to persistence layer models and removes
    any duplicate people created by how SQLAlchemy handles joins
    """
    converted_people: List[entities.Person] = []
    count_by_id: Dict[int, int] = defaultdict(lambda: 0)
    for person in people:
        if count_by_id[person.person_id] == 0:
            converted = converter.convert_schema_object_to_entity(person)
            if not isinstance(converted, entities.Person):
                raise ValueError(f"Unexpected return type [{converted.__class__}]")
            converted_people.append(converted)
        count_by_id[person.person_id] += 1

    duplicates = [
        (person_id, count) for person_id, count in count_by_id.items() if count > 1
    ]
    if duplicates:
        id_counts = "\n".join(
            [
                "ID {} with count {}".format(duplicate[0], duplicate[1])
                for duplicate in duplicates
            ]
        )
        logging.error("Duplicate records returned for person IDs:\n%s", id_counts)

    return converted_people
