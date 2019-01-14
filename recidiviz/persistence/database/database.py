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
from typing import List

from sqlalchemy.orm import Session

from recidiviz.common.constants.mappable_enum import MappableEnum
from recidiviz.persistence import entities
from recidiviz.persistence.database import database_utils
from recidiviz.persistence.database.schema import Person, Booking


def read_people(session, full_name=None, birthdate=None):
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

    return database_utils.convert_people(query.all())


def read_bookings(session):
    """
    Reads all bookings in the db.

    Args:
        session: The transaction to read from
    Return:
        List of all bookings
    """
    return database_utils.convert_bookings(session.query(Booking).all())


def read_people_by_external_ids(
        session: Session, region: str,
        ingested_people: List[entities.Person]) -> List[entities.Person]:
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
    query = session.query(Person) \
        .filter(Person.region == region) \
        .filter(Person.external_id.in_(external_ids))

    return [database_utils.convert_person(p) for p in query.all()]


def read_people_with_open_bookings(session, region, ingested_people):
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

    return [database_utils.convert_person(person) for person, _ in query.all()]


def read_open_bookings_scraped_before_time(session, region, time):
    """
    Reads all open bookings in the given region that have a last_scraped_time
    set to a time earlier than the provided datetime.

    Args:
        session: The transaction to read from
        region: The region to match against
        time: The datetime exclusive upper bound on last_scrape_time to match
            against
    Returns:
        List of bookings matching the provided args
    """
    query = _query_people_and_open_bookings(session, region) \
        .filter(Booking.last_seen_time < time)
    return [database_utils.convert_booking(booking)
            for _, booking in query.all()]


def _query_people_and_open_bookings(session, region):
    """
    Returns a list of tuples of (person, booking) for all open bookings.

    Args:
        session: Transaction to read from
        region: The region to match against.
    """
    return session.query(Person, Booking) \
        .filter(Person.person_id == Booking.person_id) \
        .filter(Person.region == region) \
        .filter(Booking.release_date.is_(None))


def write_people(session, people):
    """
    Converts the given |people| into schema.Person objects and adds the
    schema objects into the given |session|.

    Args:
        session: (Session)
        people:  List[entities.Person]
    """
    for person in people:
        write_person(session, person)


def write_person(session, person):
    """
    Converts the given |person| into a schema.Person object and adds it into
    the given |session|.

    Args:
        session: (Session)
        person:  entities.Person
    """
    session.merge(database_utils.convert_person(person))


def update_booking(session, booking_id, **kwargs):
    """
    Finds the booking in our db from the provided |booking_id| and updates all
    fields on the booking that are provided in |**kwargs|.

    Args:
        session: (Session)
        booking_id: (int)
    """
    session.query(Booking) \
        .filter(Booking.booking_id == booking_id) \
        .update(_convert_enums_to_strings(kwargs))


def _convert_enums_to_strings(dictionary):
    result = {}
    for k, v in dictionary.items():
        if issubclass(v.__class__, MappableEnum):
            result[k] = v.value
        else:
            result[k] = v

    return result
