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
"""Contains logic for communicating with the persistence layer."""
import datetime
import logging
import os
from distutils.util import strtobool  # pylint: disable=no-name-in-module
from typing import List

from opencensus.stats import aggregation, measure, view

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.ingest.scrape.constants import MAX_PEOPLE_TO_LOG
from recidiviz.persistence import persistence_utils
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.persistence.database.schema_utils import \
    schema_base_for_system_level
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.ingest_info_validator import ingest_info_validator
from recidiviz.persistence.database.schema.county import dao as county_dao
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_validator import entity_validator
from recidiviz.persistence.database import database
from recidiviz.persistence.ingest_info_converter import ingest_info_converter
from recidiviz.persistence.ingest_info_converter.base_converter import \
    IngestInfoConversionResult
from recidiviz.utils import environment, monitoring

m_people = measure.MeasureInt("persistence/num_people",
                              "The number of people persisted", "1")
m_aborts = measure.MeasureInt("persistence/num_aborts",
                              "The number of aborted writes", "1")
m_errors = measure.MeasureInt("persistence/num_errors",
                              "The number of errors", "1")
people_persisted_view = view.View("recidiviz/persistence/num_people",
                                  "The sum of people persisted",
                                  [monitoring.TagKey.REGION,
                                   monitoring.TagKey.PERSISTED],
                                  m_people, aggregation.SumAggregation())
aborted_writes_view = view.View("recidiviz/persistence/num_aborts",
                                "The sum of aborted writes to persistence",
                                [monitoring.TagKey.REGION,
                                 monitoring.TagKey.REASON],
                                m_aborts, aggregation.SumAggregation())
errors_persisted_view = view.View("recidiviz/persistence/num_errors",
                                  "The sum of errors in the persistence layer",
                                  [monitoring.TagKey.REGION,
                                   monitoring.TagKey.ERROR],
                                  m_errors, aggregation.SumAggregation())
monitoring.register_views(
    [people_persisted_view, aborted_writes_view, errors_persisted_view])

ERROR_THRESHOLD = 0.5


def infer_release_on_open_bookings(
        region_code: str, last_ingest_time: datetime.datetime,
        custody_status: CustodyStatus):
    """
   Look up all open bookings whose last_seen_time is earlier than the
   provided last_ingest_time in the provided region, update those
   bookings to have an inferred release date equal to the provided
   last_ingest_time.

   Args:
       region_code: the region_code
       last_ingest_time: The last time complete data was ingested for this
           region. In the normal ingest pipeline, this is the last start time
           of a background scrape for the region.
       custody_status: The custody status to be marked on the found open
           bookings. Defaults to INFERRED_RELEASE
   """

    session = SessionFactory.for_schema_base(JailsBase)
    try:
        logging.info("Reading all bookings that happened before [%s]",
                     last_ingest_time)
        people = county_dao.read_people_with_open_bookings_scraped_before_time(
            session, region_code, last_ingest_time)
        logging.info(
            "Found [%s] people with bookings that will be inferred released",
            len(people))
        for person in people:
            persistence_utils.remove_pii_for_person(person)
            _infer_release_date_for_bookings(person.bookings, last_ingest_time,
                                             custody_status)
        database.write_people(session, people, IngestMetadata(
            region=region_code, jurisdiction_id='',
            ingest_time=last_ingest_time))
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _infer_release_date_for_bookings(
        bookings: List[county_entities.Booking],
        last_ingest_time: datetime.datetime, custody_status: CustodyStatus):
    """Marks the provided bookings with an inferred release date equal to the
    provided date. Updates the custody_status to the provided custody
    status. Also updates all children of the updated booking to have status
    'REMOVED_WITHOUT_INFO"""

    for booking in bookings:
        if persistence_utils.is_booking_active(booking):
            logging.info("Marking booking [%s] as inferred release",
                         booking.booking_id)
            booking.release_date = last_ingest_time.date()
            booking.release_date_inferred = True
            booking.custody_status = custody_status
            booking.custody_status_raw_text = None
            _mark_children_removed_from_source(booking)


def _mark_children_removed_from_source(booking: county_entities.Booking):
    """Marks all children of a booking with the status 'REMOVED_FROM_SOURCE'"""
    for hold in booking.holds:
        hold.status = HoldStatus.REMOVED_WITHOUT_INFO
        hold.status_raw_text = None

    for charge in booking.charges:
        charge.status = ChargeStatus.REMOVED_WITHOUT_INFO
        charge.status_raw_text = None
        if charge.sentence:
            charge.sentence.status = SentenceStatus.REMOVED_WITHOUT_INFO
            charge.sentence.status_raw_text = None
        if charge.bond:
            charge.bond.status = BondStatus.REMOVED_WITHOUT_INFO
            charge.bond.status_raw_text = None


def _should_persist():
    return bool(environment.in_gae() or
                strtobool((os.environ.get('PERSIST_LOCALLY', 'false'))))


def _should_abort(
        total_people,
        conversion_result: IngestInfoConversionResult,
        entity_matching_errors=0,
        data_validation_errors=0):
    """
    Returns true if we should abort the current attempt to persist an IngestInfo
    object, given the number of errors we've encountered.
    """
    if total_people == 0:
        logging.info("Aborting because the ingest info object contains no "
                     "people objects to persist.")
        return True

    # TODO: finalize the logic in here.
    if conversion_result.protected_class_errors:
        logging.error(
            "Aborting because there was an error regarding a protected class")
        with monitoring.measurements(
                {monitoring.TagKey.REASON: 'PROTECTED_CLASS_ERROR'}) as m:
            m.measure_int_put(m_aborts, 1)
        return True
    if (conversion_result.enum_parsing_errors +
            conversion_result.general_parsing_errors +
            entity_matching_errors +
            data_validation_errors) / total_people >= ERROR_THRESHOLD:
        logging.error(
            "Aborting because we exceeded the error threshold of [%s] with "
            "[%s] enum_parsing errors, [%s] general_parsing_errors, [%s] "
            "entity_matching_errors, and [%s] data_validation_errors",
            ERROR_THRESHOLD,
            conversion_result.enum_parsing_errors,
            conversion_result.general_parsing_errors,
            entity_matching_errors,
            data_validation_errors)
        with monitoring.measurements(
                {monitoring.TagKey.REASON: 'THRESHOLD'}) as m:
            m.measure_int_put(m_aborts, 1)
        return True
    return False


def write(ingest_info, metadata):
    """
    If in prod or if 'PERSIST_LOCALLY' is set to true, persist each person in
    the ingest_info. If a person with the given surname/birthday already exists,
    then update that person.

    Otherwise, simply log the given ingest_infos for debugging
    """
    ingest_info_validator.validate(ingest_info)

    mtags = {monitoring.TagKey.SHOULD_PERSIST: _should_persist(),
             monitoring.TagKey.PERSISTED: False}
    total_people = _get_total_people(ingest_info, metadata)
    with monitoring.measurements(mtags) as measurements:

        # Convert the people one at a time and count the errors as they happen.
        conversion_result: IngestInfoConversionResult = \
            ingest_info_converter.convert_to_persistence_entities(ingest_info,
                                                                  metadata)

        people, data_validation_errors = entity_validator.validate(
            conversion_result.people)
        logging.info("Converted [%s] people with [%s] enum_parsing_errors, [%s]"
                     " general_parsing_errors, [%s] protected_class_errors and "
                     "[%s] data_validation_errors, (logging max %d people):",
                     len(people),
                     conversion_result.enum_parsing_errors,
                     conversion_result.general_parsing_errors,
                     conversion_result.protected_class_errors,
                     data_validation_errors,
                     MAX_PEOPLE_TO_LOG)
        loop_count = min(len(people), MAX_PEOPLE_TO_LOG)
        for i in range(loop_count):
            logging.info(people[i])
        measurements.measure_int_put(m_people, len(people))

        if _should_abort(
                total_people=total_people,
                conversion_result=conversion_result,
                data_validation_errors=data_validation_errors):
            #  TODO(#1665): remove once dangling PERSIST session investigation
            #   is complete.
            logging.info("_should_abort_ was true after converting people")
            return False

        if not _should_persist():
            return True

        persisted = False

        session = SessionFactory.for_schema_base(
            schema_base_for_system_level(metadata.system_level))
        try:
            logging.info("Starting entity matching")

            entity_matching_output = entity_matching.match(
                session, metadata.region, people)
            people = entity_matching_output.people

            logging.info(
                "Completed entity matching with [%s] errors",
                entity_matching_output.error_count)
            if _should_abort(
                    total_people=total_people,
                    conversion_result=conversion_result,
                    entity_matching_errors=entity_matching_output.error_count,
                    data_validation_errors=data_validation_errors):
                #  TODO(#1665): remove once dangling PERSIST session
                #   investigation is complete.
                logging.info("_should_abort_ was true after entity matching")
                return False

            database.write_people(
                session, people, metadata,
                orphaned_entities=entity_matching_output.orphaned_entities)
            logging.info("Successfully wrote to the database")
            session.commit()

            persisted = True
            mtags[monitoring.TagKey.PERSISTED] = True
        except Exception as e:
            logging.exception("An exception was raised in write(): [%s]",
                              type(e).__name__)
            # Record the error type that happened and increment the counter
            mtags[monitoring.TagKey.ERROR] = type(e).__name__
            measurements.measure_int_put(m_errors, 1)
            session.rollback()
            raise
        finally:
            session.close()
        return persisted


def _get_total_people(ingest_info: IngestInfo, metadata: IngestMetadata) -> int:
    if metadata.system_level == SystemLevel.COUNTY:
        return len(ingest_info.people)
    return len(ingest_info.state_people)
