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
import logging
import os
from distutils.util import strtobool  # pylint: disable=no-name-in-module

from recidiviz import Session
from recidiviz.persistence import entity_matching, converter
from recidiviz.persistence.database import database, database_utils
from recidiviz.utils import environment


class PersistenceError(Exception):
    """Raised when an error with the persistence layer is encountered."""
    pass


def infer_release_on_open_bookings(region, last_ingest_time):
    """
   Look up all open bookings whose last_seen_time is earlier than the
   provided last_ingest_time in the provided region, update those
   bookings to have an inferred release date equal to the provided
   last_ingest_time.

   Args:
       region: the region
       last_ingest_time: The last time complete data was ingested for this
           region. In the normal ingest pipeline, this is the last start time
           of a background scrape for the region.
   """

    session = Session()
    try:
        bookings = database.read_open_bookings_scraped_before_time(
            session, region, last_ingest_time)
        _infer_release_date_for_bookings(bookings, last_ingest_time)
        for booking in bookings:
            session.add(session.merge(booking))
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _infer_release_date_for_bookings(bookings, date):
    """Marks the provided bookings with an inferred release date equal to the
    provided date. Also resolves any charges associated with the provided
    bookings as 'RESOLVED_UNKNOWN_REASON'"""

    # TODO: set charge status as RESOLVED_UNKNOWN_REASON once DB enum is
    # appropriately updated

    for booking in bookings:
        if booking.release_date:
            raise PersistenceError('Attempting to mark booking {0} as '
                                   'resolved, however booking already has '
                                   'release date.'.format(booking.booking_id))

        booking.release_date = date
        booking.release_date_inferred = True


def _should_persist():
    return environment.in_prod() or strtobool((os.environ['PERSIST_LOCALLY']))


def write(ingest_info, last_seen_time):
    """
    If in prod or if 'PERSIST_LOCALLY' is set to true, persist each person in
    the ingest_info. If a person with the given surname/birthday already exists,
    then update that person.

    Otherwise, simply log the given ingest_infos for debugging

    Args:
         ingest_info: The IngestInfo containing each person
         last_seen_time: The last time this ingest_info was seen from
            its data source. In the normal ingest pipeline, this is the
            scraper_start_time.
         the scraper that produced
            the provided ingest_info
    """
    log = logging.getLogger()

    log.info(ingest_info)
    people = converter.convert(ingest_info)
    _add_last_seen_time(people, last_seen_time)

    if not _should_persist():
        return

    for person in people:
        session = Session()
        try:
            existing_person = entity_matching.get_entity_match(session, person)

            if existing_person is None:
                session.add(database_utils.convert_person_to_database(person))
            else:
                person.person_id = existing_person.person_id
                session.merge(database_utils.convert_person_to_database(person))

            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def _add_last_seen_time(people, last_seen_time):
    for person in people:
        for booking in person.bookings:
            booking.last_seen_time = last_seen_time
