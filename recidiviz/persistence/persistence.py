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

from opencensus.stats import aggregation
from opencensus.stats import measure
from opencensus.stats import view

from recidiviz import Session
from recidiviz.common.constants.booking import ReleaseReason
from recidiviz.ingest.constants import MAX_PEOPLE_TO_LOG
from recidiviz.persistence import entity_matching
from recidiviz.persistence.converter import converter
from recidiviz.persistence.database import database
from recidiviz.utils import environment, monitoring

m_people = measure.MeasureInt("persistence/num_people",
                              "The number of people persisted", "1")
people_persisted_view = view.View("recidiviz/persistence/num_people",
                                  "The sum of people persisted",
                                  [monitoring.TagKey.REGION,
                                   monitoring.TagKey.PERSISTED],
                                  m_people,
                                  aggregation.SumAggregation())
monitoring.register_views([people_persisted_view])

class PersistenceError(Exception):
    """Raised when an error with the persistence layer is encountered."""


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
        logging.info('Reading all bookings that happened before %s',
                     last_ingest_time)
        bookings = database.read_open_bookings_scraped_before_time(
            session, region, last_ingest_time)
        logging.info('Found %s bookings that will be inferred released',
                     len(bookings))
        _infer_release_date_for_bookings(session, bookings,
                                         last_ingest_time.date())
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _infer_release_date_for_bookings(session, bookings, date):
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

        logging.info('Marking booking with ID %s as inferred released',
                     booking.booking_id)
        database.update_booking(session, booking.booking_id, release_date=date,
                                release_reason=ReleaseReason.INFERRED_RELEASE)


def _should_persist():
    return bool(environment.in_prod() or \
        strtobool((os.environ.get('PERSIST_LOCALLY', 'false'))))


def write(ingest_info, metadata):
    """
    If in prod or if 'PERSIST_LOCALLY' is set to true, persist each person in
    the ingest_info. If a person with the given surname/birthday already exists,
    then update that person.

    Otherwise, simply log the given ingest_infos for debugging
    """
    mtags = {monitoring.TagKey.REGION: metadata.region,
             monitoring.TagKey.SHOULD_PERSIST: _should_persist()}
    with monitoring.measurements(mtags) as measurements:
        people = converter.convert(ingest_info, metadata)
        logging.info('Successfully converted proto(logging max 4 people):')
        loop_count = min(len(people), MAX_PEOPLE_TO_LOG)
        for i in range(loop_count):
            logging.info(people[i])
        measurements.measure_int_put(m_people, len(people))

        if not _should_persist():
            return

        persisted = False
        session = Session()
        try:
            logging.info('Starting entity matching')
            entity_matching.match_entities(session, metadata.region, people)
            logging.info('Successfully completed entity matching')
            database.write_people(session, people)
            logging.info('Successfully wrote to the database')
            session.commit()
            persisted = True
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
            mtags[monitoring.TagKey.PERSISTED] = persisted
