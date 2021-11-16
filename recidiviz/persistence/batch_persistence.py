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
"""Contains logic for communicating with the batch persistence layer."""
import datetime
import json
import logging
from http import HTTPStatus
from typing import Dict, List, Optional, Set, Tuple

from flask import Blueprint, request, url_for
from opencensus.stats import aggregation, measure, view

from recidiviz.common.ingest_metadata import (
    LegacyStateAndJailsIngestMetadata,
    SystemLevel,
)
from recidiviz.ingest.models import ingest_info_pb2, serialization
from recidiviz.ingest.models.ingest_info import IngestInfo, Person
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import scrape_phase, sessions
from recidiviz.ingest.scrape.constants import ScrapeType
from recidiviz.ingest.scrape.scraper_cloud_task_manager import ScraperCloudTaskManager
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.persistence import datastore_ingest_info, persistence
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.datastore_ingest_info import BatchIngestInfoData
from recidiviz.persistence.ingest_info_validator import ingest_info_validator
from recidiviz.utils import monitoring, regions
from recidiviz.utils.auth.gae import requires_gae_auth

FAILED_TASK_THRESHOLD = 0.1

batch_blueprint = Blueprint("batch", __name__)

m_batch_count = measure.MeasureInt(
    "persistence/batch_persistence/batch_count",
    "The count of batch persistence calls",
    "1",
)

count_view = view.View(
    "recidiviz/persistence/batch_persistence/batch_count",
    "The sum of batch persistence calls that occurred",
    [monitoring.TagKey.REGION, monitoring.TagKey.STATUS, monitoring.TagKey.PERSISTED],
    m_batch_count,
    aggregation.SumAggregation(),
)
monitoring.register_views([count_view])


class BatchPersistError(Exception):
    """Raised when there was an error with batch persistence."""

    def __init__(self, region: str, scrape_type: ScrapeType):
        super().__init__(f"Error when running '{scrape_type}' for region {region}")


class DatastoreError(Exception):
    """Raised when there was an error with Datastore."""

    def __init__(self, region: str, call_type: str):
        super().__init__(
            f"Error when calling '{call_type}' for region {region} in Datastore."
        )


def _get_proto_from_batch_ingest_info_data_list(
    batch_ingest_info_data_list: List[BatchIngestInfoData],
) -> Tuple[ingest_info_pb2.IngestInfo, Dict[int, BatchIngestInfoData]]:
    """Merges an ingest_info_proto from all of the batched ingest_infos.

    Args:
        batch_ingest_info_data_list: A list of BatchIngestInfoData.
    Returns:
        an IngestInfo proto with data from all of the messages.
    """
    logging.info("Starting generation of proto")
    ingest_infos: List[IngestInfo] = []
    successful_tasks: Set[int] = set()
    failed_tasks: Dict[int, BatchIngestInfoData] = {}
    for batch_ingest_info_datum in batch_ingest_info_data_list:
        # We do this because dicts are not hashable in python and we want to
        # avoid an n2 operation to see which tasks have been seen previously
        # which can be on the order of a million operations.
        task_hash = batch_ingest_info_datum.task_hash
        if not batch_ingest_info_datum.error and task_hash not in successful_tasks:
            successful_tasks.add(task_hash)
            if task_hash in failed_tasks:
                del failed_tasks[task_hash]
            if batch_ingest_info_datum.ingest_info:
                ingest_infos.append(batch_ingest_info_datum.ingest_info)
        else:
            # We only add to failed if we didn't see a successful one. This is
            # because its possible a task ran 3 times before passing, meaning
            # we don't want to fail on that when we see the failed ones.
            if task_hash not in successful_tasks:
                failed_tasks[task_hash] = batch_ingest_info_datum

    deduped_ingest_info = _dedup_people(ingest_infos)
    base_proto = serialization.convert_ingest_info_to_proto(deduped_ingest_info)
    ingest_info_validator.validate(base_proto)
    logging.info("Generated proto for [%s] people", len(base_proto.people))
    return base_proto, failed_tasks


def _get_batch_ingest_info_list(
    region_code: str, session_start_time: datetime.datetime
) -> List[BatchIngestInfoData]:
    """Reads all of the messages from Datastore for the region.
    Args:
        region_code (str): The region code of the scraper.
        session_start_time (datetime): The start time of the scraper.
    Returns:
        A list of BatchIngestInfoData.
    """
    return datastore_ingest_info.batch_get_ingest_infos_for_region(
        region_code, session_start_time
    )


def _dedup_people(ingest_infos: List[IngestInfo]) -> IngestInfo:
    """Combines a list of IngestInfo objects into a single IngestInfo with
    duplicate People objects removed."""

    unique_people: List[Person] = []
    duplicate_people: List[Person] = []

    for ingest_info in ingest_infos:
        for person in ingest_info.people:
            # Sort deeply so that repeated fields are compared in a consistent
            # order.
            person.sort()
            if person not in unique_people:
                unique_people.append(person)
            elif person not in duplicate_people:
                duplicate_people.append(person)
    if duplicate_people:
        logging.info(
            "Removed %d duplicate people: %s", len(duplicate_people), duplicate_people
        )
    return IngestInfo(people=unique_people)


def _should_abort(failed_tasks: int, total_people: int) -> bool:
    if total_people and (failed_tasks / total_people) >= FAILED_TASK_THRESHOLD:
        return True
    return False


def write(ingest_info: IngestInfo, scrape_key: ScrapeKey, task: Task) -> None:
    session = sessions.get_current_session(scrape_key)
    if not session:
        raise DatastoreError(scrape_key.region_code, "write")
    datastore_ingest_info.write_ingest_info(
        region=scrape_key.region_code,
        session_start_time=session.start,
        ingest_info=ingest_info,
        task_hash=hash(json.dumps(task.to_serializable(), sort_keys=True)),
    )


def write_error(
    error: str, trace_id: Optional[str], task: Task, scrape_key: ScrapeKey
) -> None:
    session = sessions.get_current_session(scrape_key)
    if not session:
        raise DatastoreError(scrape_key.region_code, "write_error")

    datastore_ingest_info.write_error(
        region=scrape_key.region_code,
        error=error,
        trace_id=trace_id,
        task_hash=hash(json.dumps(task.to_serializable(), sort_keys=True)),
        session_start_time=session.start,
    )


def persist_to_database(
    region_code: str, session_start_time: datetime.datetime
) -> bool:
    """Reads all of the ingest infos from Datastore for a region and persists
    them to the database.
    """
    region = regions.get_region(region_code)
    overrides = region.get_scraper_enum_overrides()

    ingest_info_data_list = _get_batch_ingest_info_list(region_code, session_start_time)

    logging.info("Received %s total ingest infos", len(ingest_info_data_list))
    if ingest_info_data_list:
        proto, failed_tasks = _get_proto_from_batch_ingest_info_data_list(
            ingest_info_data_list
        )

        if not proto.people:
            logging.error("Scrape session returned 0 people.")
            return False

        for batch_ingest_info_datum in failed_tasks.values():
            logging.error(
                "Task with trace_id %s failed with error %s",
                batch_ingest_info_datum.trace_id,
                batch_ingest_info_datum.error,
            )
        if _should_abort(len(failed_tasks), len(proto.people)):
            logging.error(
                "Too many scraper tasks failed(%s), aborting write", len(failed_tasks)
            )
            return False

        metadata = LegacyStateAndJailsIngestMetadata(
            region=region_code,
            jurisdiction_id=region.jurisdiction_id,
            ingest_time=session_start_time,
            facility_id=region.facility_id,
            enum_overrides=overrides,
            system_level=SystemLevel.COUNTY,
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
        )

        did_write = persistence.write_ingest_info(proto, metadata)
        if did_write:
            datastore_ingest_info.batch_delete_ingest_infos_for_region(region_code)

        return did_write

    logging.error("No ingest infos received from Datastore")
    return False


@batch_blueprint.route("/read_and_persist")
@requires_gae_auth
def read_and_persist() -> Tuple[str, HTTPStatus]:
    """Reads all of the messages from Datastore for a region and persists
    them to the database.
    """

    region = request.args.get("region")

    if not isinstance(region, str):
        raise ValueError(f"Expected string region, found [{region}]")

    batch_tags = {
        monitoring.TagKey.STATUS: "COMPLETED",
        monitoring.TagKey.PERSISTED: False,
    }
    # Note: measurements must be second so it receives the region tag.
    with monitoring.push_tags(
        {monitoring.TagKey.REGION: region}
    ), monitoring.measurements(batch_tags) as measurements:
        measurements.measure_int_put(m_batch_count, 1)

        session = sessions.get_most_recent_completed_session(
            region, ScrapeType.BACKGROUND
        )

        if not session:
            raise ValueError(
                f"Most recent session for region [{region}] is unexpectedly None"
            )

        scrape_type = session.scrape_type

        try:
            did_persist = persist_to_database(region, session.start)
            batch_tags[monitoring.TagKey.PERSISTED] = did_persist
        except Exception as e:
            logging.exception(
                "An exception occurred in read and persist: %s", type(e).__name__
            )
            batch_tags[monitoring.TagKey.STATUS] = f"ERROR: {type(e).__name__}"
            sessions.update_phase(session, scrape_phase.ScrapePhase.DONE)
            raise BatchPersistError(region, scrape_type) from e

        if did_persist:
            next_phase = scrape_phase.next_phase(request.endpoint)
            sessions.update_phase(session, scrape_phase.ScrapePhase.RELEASE)
            if next_phase:
                logging.info("Enqueueing %s for region %s.", next_phase, region)
                ScraperCloudTaskManager().create_scraper_phase_task(
                    region_code=region, url=url_for(next_phase)
                )
            return "", HTTPStatus.OK

        sessions.update_phase(session, scrape_phase.ScrapePhase.DONE)
        return "", HTTPStatus.ACCEPTED
