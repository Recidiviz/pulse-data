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
import logging
from typing import Callable, Dict, Optional, Union

import psycopg2
import sqlalchemy
from opencensus.stats import aggregation, measure, view
from opencensus.stats.measurement_map import MeasurementMap
from psycopg2.errorcodes import SERIALIZATION_FAILURE

from recidiviz.common.ingest_metadata import (
    IngestMetadata,
    LegacyStateAndJailsIngestMetadata,
)
from recidiviz.ingest.models.ingest_info_pb2 import IngestInfo
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database_invariant_validator import (
    database_invariant_validator,
)
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_validator import entity_validator
from recidiviz.persistence.ingest_info_converter import ingest_info_converter
from recidiviz.persistence.ingest_info_converter.base_converter import (
    EntityDeserializationResult,
)
from recidiviz.persistence.ingest_info_validator import ingest_info_validator
from recidiviz.persistence.persistence_utils import should_persist
from recidiviz.utils import metadata, monitoring, trace
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

m_people = measure.MeasureInt(
    "persistence/num_people", "The number of people persisted", "1"
)
m_aborts = measure.MeasureInt(
    "persistence/num_aborts", "The number of aborted writes", "1"
)
m_errors = measure.MeasureInt("persistence/num_errors", "The number of errors", "1")
m_retries = measure.MeasureInt(
    "persistence/num_transaction_retries",
    "The number of transaction retries due to serialization failures",
    "1",
)
people_persisted_view = view.View(
    "recidiviz/persistence/num_people",
    "The sum of people persisted",
    [monitoring.TagKey.REGION, monitoring.TagKey.PERSISTED],
    m_people,
    aggregation.SumAggregation(),
)
aborted_writes_view = view.View(
    "recidiviz/persistence/num_aborts",
    "The sum of aborted writes to persistence",
    [monitoring.TagKey.REGION, monitoring.TagKey.REASON],
    m_aborts,
    aggregation.SumAggregation(),
)
errors_persisted_view = view.View(
    "recidiviz/persistence/num_errors",
    "The sum of errors in the persistence layer",
    [monitoring.TagKey.REGION, monitoring.TagKey.ERROR],
    m_errors,
    aggregation.SumAggregation(),
)
retried_transactions_view = view.View(
    "recidiviz/persistence/num_transaction_retries",
    "The total number of transaction retries",
    [monitoring.TagKey.REGION],
    m_retries,
    aggregation.SumAggregation(),
)
monitoring.register_views(
    [
        people_persisted_view,
        aborted_writes_view,
        errors_persisted_view,
        retried_transactions_view,
    ]
)

OVERALL_THRESHOLD = "overall_threshold"
ENUM_THRESHOLD = "enum_threshold"
ENTITY_MATCHING_THRESHOLD = "entity_matching_threshold"
DATABASE_INVARIANT_THRESHOLD = "database_invariant_threshold"

SYSTEM_TYPE_TO_ERROR_THRESHOLD: Dict[str, float] = {
    OVERALL_THRESHOLD: 0.5,
    ENUM_THRESHOLD: 0.0,
    ENTITY_MATCHING_THRESHOLD: 0.0,
    DATABASE_INVARIANT_THRESHOLD: 0.0,
}

STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE: Dict[str, Dict[str, float]] = {
    GCP_PROJECT_STAGING: {
        "US_ID": 0.05,
        # TODO(#5313): Decrease back to 5% once entity matching issues are resolved for ND.
        "US_ND": 0.20,
    },
    GCP_PROJECT_PRODUCTION: {
        "US_ID": 0.05,
        # TODO(#5313): Decrease back to 5% once entity matching issues are resolved for ND.
        "US_ND": 0.20,
    },
}


def _should_abort(
    total_root_entities: int,
    conversion_result: EntityDeserializationResult,
    region_code: str,
    entity_matching_errors: int = 0,
    data_validation_errors: int = 0,
    database_invariant_errors: int = 0,
) -> bool:
    """
    Returns true if we should abort the current attempt to persist an IngestInfo
    object, given the number of errors we've encountered.
    """
    if total_root_entities == 0:
        logging.info(
            "Aborting because the ingest info object contains no "
            "root entity objects to persist."
        )
        return True

    if conversion_result.protected_class_errors:
        logging.error("Aborting because there was an error regarding a protected class")
        with monitoring.measurements(
            {monitoring.TagKey.REASON: "PROTECTED_CLASS_ERROR"}
        ) as m:
            m.measure_int_put(m_aborts, 1)
        return True

    error_thresholds = _get_thresholds_for_region(region_code)

    overall_error_ratio = _calculate_overall_error_ratio(
        conversion_result,
        entity_matching_errors,
        data_validation_errors,
        total_root_entities,
    )

    if overall_error_ratio > error_thresholds[OVERALL_THRESHOLD]:
        _log_error(OVERALL_THRESHOLD, error_thresholds, overall_error_ratio)
        return True

    if (
        conversion_result.enum_parsing_errors / total_root_entities
        > error_thresholds[ENUM_THRESHOLD]
    ):
        _log_error(
            ENUM_THRESHOLD,
            error_thresholds,
            conversion_result.enum_parsing_errors / total_root_entities,
        )
        return True

    if (
        entity_matching_errors / total_root_entities
        > error_thresholds[ENTITY_MATCHING_THRESHOLD]
    ):
        _log_error(
            ENTITY_MATCHING_THRESHOLD,
            error_thresholds,
            entity_matching_errors / total_root_entities,
        )
        return True

    if database_invariant_errors > error_thresholds[DATABASE_INVARIANT_THRESHOLD]:
        _log_error(
            DATABASE_INVARIANT_THRESHOLD,
            error_thresholds,
            database_invariant_errors / total_root_entities,
        )
        return True

    return False


def _calculate_overall_error_ratio(
    conversion_result: EntityDeserializationResult,
    entity_matching_errors: int,
    data_validation_errors: int,
    total_root_entities: int,
) -> float:
    """Calculates the error ratio, given the total number of errors and root entities."""
    return (
        conversion_result.enum_parsing_errors
        + conversion_result.general_parsing_errors
        + entity_matching_errors
        + data_validation_errors
    ) / total_root_entities


def _get_thresholds_for_region(region_code: str) -> Dict[str, float]:
    """Returns the dictionary of error thresholds for a given system level."""
    error_thresholds = SYSTEM_TYPE_TO_ERROR_THRESHOLD

    state_code: str = region_code.upper()

    # Override the entity matching threshold from the default value, if applicable.
    project_id = metadata.project_id()
    if (
        not project_id
        or project_id not in STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE
    ):
        raise ValueError(
            f"Unexpected project id [{project_id}] - must be one of "
            f"{STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE.keys()}."
        )

    thresholds_for_project = STATE_CODE_TO_ENTITY_MATCHING_THRESHOLD_OVERRIDE[
        project_id
    ]
    if state_code in thresholds_for_project:
        state_specific_threshold = thresholds_for_project[state_code]
        if state_specific_threshold is None:
            raise ValueError(
                f"Override unexpectedly None for state_code [{state_code}]."
            )
        error_thresholds[ENTITY_MATCHING_THRESHOLD] = state_specific_threshold
    return error_thresholds


def _log_error(
    threshold_type: str, error_thresholds: Dict[str, float], error_ratio: float
) -> None:
    logging.error(
        "Aborting because we exceeded the [%s] threshold of [%s] with an error ratio of [%s]",
        threshold_type,
        error_thresholds[threshold_type],
        error_ratio,
    )
    with monitoring.measurements({monitoring.TagKey.REASON: threshold_type}) as m:
        m.measure_int_put(m_aborts, 1)


def retry_transaction(
    session: Session,
    measurements: MeasurementMap,
    txn_body: Callable[[Session], bool],
    max_retries: Optional[int],
) -> bool:
    """Retries the transaction if a serialization failure occurs.

    Handles management of committing and rolling back the `session`, without
    closing it. `txn_body` can return False to force the transaction to be aborted,
    otherwise return True.

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
                if (
                    isinstance(e.orig, psycopg2.OperationalError)
                    and e.orig.pgcode == SERIALIZATION_FAILURE
                ):
                    logging.info(
                        "Retrying transaction due to serialization failure: %s", e
                    )
                    num_retries += 1
                    continue
                raise
            except Exception:
                session.rollback()
                raise
    finally:
        measurements.measure_int_put(m_retries, num_retries)


# TODO(#8905): Once direct ingest regions have migrated to ingest mappings overhaul,
#  delete this function.
@trace.span
def write_ingest_info(
    ingest_info: IngestInfo,
    ingest_metadata: LegacyStateAndJailsIngestMetadata,
    run_txn_fn: Callable[
        [Session, MeasurementMap, Callable[[Session], bool], Optional[int]], bool
    ] = retry_transaction,
) -> bool:
    """
    If should_persist(), persist each person in the ingest_info. If a person with the
    given surname/birthday already exists, then update that person.

    Otherwise, simply log the given ingest_infos for debugging

    `run_txn_fn` is exposed primarily for testing and should typically be left as
    `retry_transaction`. `run_txn_fn` must handle the coordination of the transaction
    including, when to run the body of the transaction and when to commit or rollback
    the session.
    """
    ingest_info_validator.validate(ingest_info)

    # Convert the people one at a time and count the errors as they happen.
    conversion_result: EntityDeserializationResult = (
        ingest_info_converter.convert_to_persistence_entities(
            ingest_info, ingest_metadata
        )
    )
    total_people = len(ingest_info.state_people)

    return write_entities(
        conversion_result,
        ingest_metadata,
        total_people,
        run_txn_fn,
    )


@trace.span
def write_entities(
    conversion_result: EntityDeserializationResult,
    ingest_metadata: IngestMetadata,
    total_people: int,
    run_txn_fn: Callable[
        [Session, MeasurementMap, Callable[[Session], bool], Optional[int]], bool
    ] = retry_transaction,
) -> bool:
    """If should_persist(), persist each object in the |conversion_result|. If an object
    representing any given entity already exists, that object is merely updated via the
    entity matching process.

    Otherwise, if should_persist() is false, goes through the motions without
    actually committing anything to the database.

    `run_txn_fn` is exposed primarily for testing and should typically be left as
    `retry_transaction`. `run_txn_fn` must handle the coordination of the transaction
    including, when to run the body of the transaction and when to commit or rollback
    the session.
    """

    mtags: Dict[str, Union[bool, str]] = {
        monitoring.TagKey.SHOULD_PERSIST: should_persist(),
        monitoring.TagKey.PERSISTED: False,
    }
    with monitoring.measurements(mtags) as measurements:
        people, data_validation_errors = entity_validator.validate(
            conversion_result.people
        )
        logging.info(
            "Converted [%s] people with [%s] enum_parsing_errors, [%s]"
            " general_parsing_errors, [%s] protected_class_errors and "
            "[%s] data_validation_errors",
            len(people),
            conversion_result.enum_parsing_errors,
            conversion_result.general_parsing_errors,
            conversion_result.protected_class_errors,
            data_validation_errors,
        )
        measurements.measure_int_put(m_people, len(people))

        if _should_abort(
            total_root_entities=total_people,
            conversion_result=conversion_result,
            region_code=ingest_metadata.region,
            data_validation_errors=data_validation_errors,
        ):
            #  TODO(#1665): remove once dangling PERSIST session investigation
            #   is complete.
            logging.info("_should_abort_ was true after converting people")
            return False

        if not should_persist():
            return True

        @trace.span
        def match_and_write_people(session: Session) -> bool:
            logging.info("Starting entity matching")

            entity_matching_output = entity_matching.match(
                session, ingest_metadata.region, people, ingest_metadata
            )
            output_people = entity_matching_output.people
            total_root_entities = entity_matching_output.total_root_entities
            logging.info(
                "Completed entity matching with [%s] errors",
                entity_matching_output.error_count,
            )
            logging.info(
                "Completed entity matching and have [%s] total people "
                "to commit to DB",
                len(output_people),
            )
            if _should_abort(
                total_root_entities=total_root_entities,
                conversion_result=conversion_result,
                region_code=ingest_metadata.region,
                entity_matching_errors=entity_matching_output.error_count,
            ):
                #  TODO(#1665): remove once dangling PERSIST session
                #   investigation is complete.
                logging.info("_should_abort_ was true after entity matching")
                return False

            database_invariant_errors = (
                database_invariant_validator.validate_invariants(
                    session,
                    ingest_metadata.region,
                    output_people,
                )
            )

            if _should_abort(
                total_root_entities=total_root_entities,
                conversion_result=conversion_result,
                region_code=ingest_metadata.region,
                database_invariant_errors=database_invariant_errors,
            ):
                logging.info(
                    "_should_abort_ was true after database invariant validation"
                )
                return False

            if entity_matching_output.error_count:
                logging.warning(
                    "Proceeding with persist step even though there are [%s] entity "
                    "matching errors ([%s] error ratio).",
                    entity_matching_output.error_count,
                    entity_matching_output.error_count / total_root_entities,
                )
            if database_invariant_errors:
                logging.warning(
                    "Proceeding with persist step even though there are [%s] database "
                    "invariant errors ([%s] error ratio).",
                    database_invariant_errors,
                    database_invariant_errors / total_root_entities,
                )
            return True

        try:
            with SessionFactory.using_database(
                ingest_metadata.database_key, autocommit=False
            ) as session:
                if not run_txn_fn(session, measurements, match_and_write_people, 5):
                    return False
            mtags[monitoring.TagKey.PERSISTED] = True
        except Exception as e:
            logging.exception(
                "An exception was raised in write(): [%s]", type(e).__name__
            )
            # Record the error type that happened and increment the counter
            mtags[monitoring.TagKey.ERROR] = type(e).__name__
            measurements.measure_int_put(m_errors, 1)
            raise

        logging.info("Successfully wrote to the database")
        return True
