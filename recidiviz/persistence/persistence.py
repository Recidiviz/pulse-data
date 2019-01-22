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
import datetime
import logging
import os
from distutils.util import strtobool  # pylint: disable=no-name-in-module
from typing import List

from opencensus.stats import aggregation
from opencensus.stats import measure
from opencensus.stats import view

from recidiviz import Session
from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.common.constants.sentences import SentenceStatus
from recidiviz.ingest.constants import MAX_PEOPLE_TO_LOG
from recidiviz.persistence import entity_matching, entities
from recidiviz.persistence.converter import converter
from recidiviz.persistence.database import database
from recidiviz.utils import environment, monitoring

m_people = measure.MeasureInt("persistence/num_people",
                              "The number of people persisted", "1")
m_errors = measure.MeasureInt("persistence/num_errors",
                              "The number of errors", "1")
people_persisted_view = view.View("recidiviz/persistence/num_people",
                                  "The sum of people persisted",
                                  [monitoring.TagKey.REGION,
                                   monitoring.TagKey.PERSISTED],
                                  m_people,
                                  aggregation.SumAggregation())
errors_persisted_view = view.View("recidiviz/persistence/num_errors",
                                  "The sum of errors in the persistence layer",
                                  [monitoring.TagKey.REGION,
                                   monitoring.TagKey.ERROR],
                                  m_errors,
                                  aggregation.SumAggregation())
monitoring.register_views([people_persisted_view, errors_persisted_view])


class PersistenceError(Exception):
    """Raised when an error with the persistence layer is encountered."""


def infer_release_on_open_bookings(region, last_ingest_time, custody_status):
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
       custody_status: The custody status to be marked on the found open
           bookings. Defaults to INFERRED_RELEASE
   """

    session = Session()
    try:
        logging.info('Reading all bookings that happened before %s',
                     last_ingest_time)
        people = database.read_people_with_open_bookings_scraped_before_time(
            session, region, last_ingest_time)
        logging.info(
            'Found %s people with bookings that will be inferred released',
            len(people))
        for person in people:
            _infer_release_date_for_bookings(person.bookings, last_ingest_time,
                                             custody_status)
        database.write_people(session, people)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _infer_release_date_for_bookings(
        bookings: List[entities.Booking],
        last_ingest_time: datetime.datetime, custody_status: CustodyStatus):
    """Marks the provided bookings with an inferred release date equal to the
    provided date. Updates the custody_status to the provided custody
    status. Also updates all children of the updated booking to have status
    'UNKNOWN_REMOVED_FROM_SOURCE"""

    for booking in bookings:
        if not booking.release_date:
            logging.info('Marking booking %s as inferred release')
            booking.release_date = last_ingest_time.date()
            booking.release_date_inferred = True
            booking.custody_status = custody_status
            booking.custody_status_raw_text = None
            _mark_children_removed_from_source(booking)


def _mark_children_removed_from_source(booking: entities.Booking):
    """Marks all children of a booking with the status 'REMOVED_FROM_SOURCE'"""
    for hold in booking.holds:
        hold.status = HoldStatus.UNKNOWN_REMOVED_FROM_SOURCE
        hold.status_raw_text = None

    for charge in booking.charges:
        charge.status = ChargeStatus.UNKNOWN_REMOVED_FROM_SOURCE
        charge.status_raw_text = None
        if charge.sentence:
            charge.sentence.status = SentenceStatus.UNKNOWN_REMOVED_FROM_SOURCE
            charge.sentence.status_raw_text = None
        if charge.bond:
            charge.bond.status = BondStatus.UNKNOWN_REMOVED_FROM_SOURCE
            charge.bond.status_raw_text = None


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
        except Exception as e:
            # Record the error type that happened and increment the counter
            mtags[monitoring.TagKey.ERROR] = type(e).__name__
            measurements.measure_int_put(m_errors, 1)
            session.rollback()
            raise
        finally:
            session.close()
            mtags[monitoring.TagKey.PERSISTED] = persisted
