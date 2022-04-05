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
from typing import Callable, List, Optional, Union, Dict

import psycopg2
from psycopg2.errorcodes import SERIALIZATION_FAILURE
import sqlalchemy
from opencensus.stats import aggregation, measure, view
from opencensus.stats.measurement_map import MeasurementMap

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.common.constants.county.hold import HoldStatus
from recidiviz.common.constants.county.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence import persistence_utils
from recidiviz.persistence.database import database
from recidiviz.persistence.database.base_schema import JailsBase
from recidiviz.persistence.database.schema.county import dao as county_dao
from recidiviz.persistence.database.schema_entity_converter import \
    schema_entity_converter as converter
from recidiviz.persistence.database.schema_utils import \
    schema_base_for_system_level
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity.county import entities as county_entities
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_validator import entity_validator
from recidiviz.persistence.ingest_info_converter import ingest_info_converter
from recidiviz.persistence.ingest_info_converter.base_converter import \
    IngestInfoConversionResult
from recidiviz.persistence.ingest_info_validator import ingest_info_validator
from recidiviz.persistence.persistence_utils import should_persist
from recidiviz.utils import monitoring

m_people = measure.MeasureInt("persistence/num_people",
                              "The number of people persisted", "1")
m_aborts = measure.MeasureInt("persistence/num_aborts",
                              "The number of aborted writes", "1")
m_errors = measure.MeasureInt("persistence/num_errors",
                              "The number of errors", "1")
m_retries = measure.MeasureInt("persistence/num_transaction_retries",
                               "The number of transaction retries due to serialization failures", "1")
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
retried_transactions_view = view.View("recidiviz/persistence/num_transaction_retries",
                                      "The total number of transaction retries",
                                      [monitoring.TagKey.REGION],
                                      m_retries, aggregation.SumAggregation())
monitoring.register_views(
    [people_persisted_view, aborted_writes_view, errors_persisted_view, retried_transactions_view])

STATE_SYSTEM_LEVEL_ERROR_THRESHOLD = 0.5
COUNTY_SYSTEM_LEVEL_ERROR_THRESHOLD = 0.5


def infer_release_on_open_bookings(
        region_code: str, last_ingest_time: datetime.datetime,
        custody_status: CustodyStatus) -> None:
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
        db_people = converter.convert_entity_people_to_schema_people(people)
        database.write_people(session, db_people, IngestMetadata(
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
        last_ingest_time: datetime.datetime, custody_status: CustodyStatus) -> None:
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


def _mark_children_removed_from_source(booking: county_entities.Booking) -> None:
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


def _should_abort(
        total_root_entities: int,
        system_level: SystemLevel,
        conversion_result: IngestInfoConversionResult,
        entity_matching_errors: int = 0,
        data_validation_errors: int = 0) -> bool:
    """
    Returns true if we should abort the current attempt to persist an IngestInfo
    object, given the number of errors we've encountered.
    """
    if total_root_entities == 0:
        logging.info("Aborting because the ingest info object contains no "
                     "root entity objects to persist.")
        return True

    if conversion_result.protected_class_errors:
        logging.error(
            "Aborting because there was an error regarding a protected class")
        with monitoring.measurements(
                {monitoring.TagKey.REASON: 'PROTECTED_CLASS_ERROR'}) as m:
            m.measure_int_put(m_aborts, 1)
        return True

    error_ratio = _calculate_error_ratio(conversion_result,
                                         entity_matching_errors,
                                         data_validation_errors,
                                         total_root_entities)

    if error_ratio >= _get_system_level_error_threshold(system_level):
        logging.error(
            "Aborting because we exceeded the error threshold of [%s] with "
            "[%s] enum_parsing errors, [%s] general_parsing_errors, [%s] "
            "entity_matching_errors, and [%s] data_validation_errors",
            _get_system_level_error_threshold(system_level),
            conversion_result.enum_parsing_errors,
            conversion_result.general_parsing_errors,
            entity_matching_errors,
            data_validation_errors)
        with monitoring.measurements(
                {monitoring.TagKey.REASON: 'THRESHOLD'}) as m:
            m.measure_int_put(m_aborts, 1)
        return True
    return False


def _calculate_error_ratio(conversion_result: IngestInfoConversionResult,
                           entity_matching_errors: int,
                           data_validation_errors: int,
                           total_root_entities: int) -> float:
    """Calculates the error ratio, given the total number of errors and root entities."""
    return (conversion_result.enum_parsing_errors +
            conversion_result.general_parsing_errors +
            entity_matching_errors +
            data_validation_errors) / total_root_entities


def _get_system_level_error_threshold(system_level: SystemLevel) -> float:
    """Returns the error threshold associated with a system level."""
    if system_level == SystemLevel.COUNTY:
        return COUNTY_SYSTEM_LEVEL_ERROR_THRESHOLD
    if system_level == SystemLevel.STATE:
        return STATE_SYSTEM_LEVEL_ERROR_THRESHOLD

    raise ValueError(f'Found no error threshold associated with `system_level=[{system_level}]`')


def retry_transaction(session: Session, measurements: MeasurementMap,
                      txn_body: Callable[[Session], bool], max_retries: Optional[int]) -> bool:
    """Retries the transaction if a serialization failure occurs.

    Handles management of committing, rolling back, and closing the `session`. `txn_body` can return False to force the
    transaction to be aborted, otherwise return True.

    Returns:
        True, if the transaction succeeded.
        False, if the transaction was aborted by `txn_body`.
    """
    num_retries = 0
    try:
        while True:
            try:
                should_continue = txn_body(session)

                if not should_continue:
                    session.rollback()
                    return should_continue

                session.commit()
                return True
            except sqlalchemy.exc.DBAPIError as e:
                session.rollback()
                if max_retries and num_retries >= max_retries:
                    raise
                if isinstance(e.orig, psycopg2.OperationalError) \
                        and e.orig.pgcode == SERIALIZATION_FAILURE:
                    logging.info('Retrying transaction due to serialization failure: %s', e)
                    num_retries += 1
                    continue
                raise
            except Exception:
                session.rollback()
                raise
    finally:
        measurements.measure_int_put(m_retries, num_retries)
        session.close()


def write(ingest_info: IngestInfo, metadata: IngestMetadata,
          run_txn_fn: Callable[[Session, MeasurementMap, Callable[[Session], bool],
                                Optional[int]], bool] = retry_transaction) -> bool:
    """
    If in prod or if 'PERSIST_LOCALLY' is set to true, persist each person in
    the ingest_info. If a person with the given surname/birthday already exists,
    then update that person.

    Otherwise, simply log the given ingest_infos for debugging

    `run_txn_fn` is exposed primarily for testing and should typically be left as `retry_transaction`. `run_txn_fn`
    must handle the coordination of the transaction including, when to run the body of the transaction and when to
    commit, rollback, or close the session.
    """
    ingest_info_validator.validate(ingest_info)

    mtags: Dict[str, Union[bool, str]] = {
        monitoring.TagKey.SHOULD_PERSIST: should_persist(),
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
                     "[%s] data_validation_errors",
                     len(people),
                     conversion_result.enum_parsing_errors,
                     conversion_result.general_parsing_errors,
                     conversion_result.protected_class_errors,
                     data_validation_errors)
        measurements.measure_int_put(m_people, len(people))

        if _should_abort(
                total_root_entities=total_people,
                system_level=metadata.system_level,
                conversion_result=conversion_result,
                data_validation_errors=data_validation_errors):
            #  TODO(#1665): remove once dangling PERSIST session investigation
            #   is complete.
            logging.info("_should_abort_ was true after converting people")
            return False

        if not should_persist():
            return True

        def match_and_write_people(session: Session) -> bool:
            logging.info("Starting entity matching")

            entity_matching_output = entity_matching.match(
                session, metadata.region, people)
            output_people = entity_matching_output.people
            total_root_entities = total_people \
                if metadata.system_level == SystemLevel.COUNTY \
                else entity_matching_output.total_root_entities
            logging.info(
                "Completed entity matching with [%s] errors",
                entity_matching_output.error_count)
            logging.info("Completed entity matching and have [%s] total people "
                         "to commit to DB", len(output_people))
            if _should_abort(
                    total_root_entities=total_root_entities,
                    system_level=metadata.system_level,
                    conversion_result=conversion_result,
                    entity_matching_errors=entity_matching_output.error_count,
                    data_validation_errors=data_validation_errors):
                #  TODO(#1665): remove once dangling PERSIST session
                #   investigation is complete.
                logging.info("_should_abort_ was true after entity matching")
                return False

            database.write_people(
                session, output_people, metadata,
                orphaned_entities=entity_matching_output.orphaned_entities)
            logging.info("Successfully wrote to the database")
            return True

        try:
            if not run_txn_fn(
                    SessionFactory.for_schema_base(schema_base_for_system_level(metadata.system_level)),
                    measurements, match_and_write_people, 5):
                return False

            mtags[monitoring.TagKey.PERSISTED] = True
        except Exception as e:
            logging.exception("An exception was raised in write(): [%s]",
                              type(e).__name__)
            # Record the error type that happened and increment the counter
            mtags[monitoring.TagKey.ERROR] = type(e).__name__
            measurements.measure_int_put(m_errors, 1)
            raise
        return True


def _get_total_people(ingest_info: IngestInfo, metadata: IngestMetadata) -> int:
    if metadata.system_level == SystemLevel.COUNTY:
        return len(ingest_info.people)
    return len(ingest_info.state_people)
