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
from recidiviz.persistence.database.schema import Person, Booking


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


def read_bookings(session):
    """
    Reads all bookings in the db.

    Args:
        session: The transaction to read from
    Return:
        List of all bookings
    """
    return session.query(Booking).all()


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
    query = session.query(Person, Booking)\
        .filter(Person.person_id == Booking.person_id)\
        .filter(Person.region == region)\
        .filter(Booking.release_date.is_(None))\
        .filter(Booking.last_seen_time < time)
    return [booking for _, booking in query.all()]
