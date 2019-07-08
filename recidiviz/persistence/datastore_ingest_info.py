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

"""Utilities for managing ingest infos stored on Datastore."""
from datetime import datetime
from typing import List, Optional
import logging
import attr
import cattr

from google.cloud import datastore


from recidiviz.common.common_utils import get_trace_id_from_flask
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.common.common_utils import retry_grpc
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.utils import environment

_ds = None


class DatastoreWriteIngestInfoError(Exception):
    """Raised when there was an error with writing an ingest info to
     Datastore."""

    def __init__(self, ingest_info: IngestInfo, region: str):
        msg_template = "Error when writing to Datastore ingest info '{}' for" \
                       " region {}. Trace id: {}"
        msg = msg_template.format(ingest_info, region,
                                  get_trace_id_from_flask())
        super(DatastoreWriteIngestInfoError, self).__init__(msg)


class DatastoreErrorWriteError(Exception):
    """Raised when there was an error with writing an error to
     Datastore."""

    def __init__(self, error: str, region: str):
        msg_template = "Error when writing to Datastore error '{}' for" \
                       " region {}. Trace id: {}"
        msg = msg_template.format(error, region,
                                  get_trace_id_from_flask())
        super(DatastoreErrorWriteError, self).__init__(msg)


class DatastoreBatchGetError(Exception):
    """Raised when there was an error batch getting ingest infos for a region
    from Datastore."""

    def __init__(self, region: str):
        msg_template = "Error when batch getting ingest_infos for region {}"
        msg = msg_template.format(region)
        super(DatastoreBatchGetError, self).__init__(msg)


class DatastoreBatchDeleteError(Exception):
    """Raised when there was an error batch deleting ingest infos for a region
    from Datastore."""

    def __init__(self, region: str):
        msg_template = "Error when batch deleting ingest_infos for region {}"
        msg = msg_template.format(region)
        super(DatastoreBatchDeleteError, self).__init__(msg)


def ds():
    global _ds
    if not _ds:
        _ds = environment.get_datastore_client()
    return _ds


@environment.test_only
def clear_ds():
    global _ds
    _ds = None


NUM_GRPC_RETRIES = 2


@attr.s(frozen=True)
class BatchIngestInfoData:
    """A wrapper around ingest info data so we can batch up writes.
       This is the object that is serialized and written to Datastore.
       """

    # The task which published this message.  We use this to dedupe messages
    # That failed some number of times before finally passing, or to not
    # double count tasks that failed more than once.
    task: Task = attr.ib()

    # The ingest info object that was batched up for a write.
    ingest_info: Optional[IngestInfo] = attr.ib(default=None)

    # The error type of the task if it ended in failure.
    error: Optional[str] = attr.ib(default=None)

    # The trace id of the failing request if it failed.  Used for debugging.
    trace_id: Optional[str] = attr.ib(default=None)

    def to_serializable(self):
        return cattr.unstructure(self)

    @classmethod
    def from_serializable(cls, serializable):
        return cattr.structure(serializable, cls)


class _DatastoreIngestInfo:
    """Datastore model to describe an ingest info."""

    @classmethod
    def get_batch_ingest_info_data(cls, entity):
        batch_ingest_info_data_serialized = \
            cls(entity).__dict__['_entity']['batch_ingest_info_data']
        batch_ingest_info_data = BatchIngestInfoData.from_serializable(
            batch_ingest_info_data_serialized)
        return batch_ingest_info_data

    @classmethod
    def new(cls, key, scraper_start_time=None,
            region=None, ingest_info=None, task=None, error=None,
            trace_id=None):
        new_ingest_info = cls(datastore.Entity(key))
        batch_ingest_info_data = BatchIngestInfoData(task=task,
                                                     ingest_info=ingest_info,
                                                     error=error,
                                                     trace_id=trace_id)
        # pylint: disable=protected-access
        new_ingest_info._entity['region']: str = region
        new_ingest_info._entity['batch_ingest_info_data']: str = \
            batch_ingest_info_data.to_serializable()
        new_ingest_info._entity['scraper_start_time']: datetime = \
            scraper_start_time
        return new_ingest_info

    def __init__(self, entity):
        self._entity = entity

    def to_entity(self):
        return self._entity


INGEST_INFO_KIND = 'DatastoreIngestInfo'


def write_ingest_info(region: str, task: Task, scraper_start_time: datetime,
                      ingest_info: IngestInfo) -> BatchIngestInfoData:
    """Writes a new ingest info for a given region.

    Args:
        region: (string) The region the ingest info is getting added for
        task: (Task) the task associated with the ingest info
        scraper_start_time: (datetime) The start time of the scraper that got
          the ingest info
        ingest_info: (IngestInfo) The ingest info data
    """
    logging.info("Writing a new ingest info for region: [%s]", region)

    new_ingest_info_entity = _DatastoreIngestInfo.new(
        key=ds().key(INGEST_INFO_KIND),
        scraper_start_time=scraper_start_time,
        region=region,
        ingest_info=ingest_info,
        task=task).to_entity()

    try:
        retry_grpc(
            NUM_GRPC_RETRIES,
            ds().put,
            new_ingest_info_entity
        )
    except Exception:
        raise DatastoreWriteIngestInfoError(ingest_info, region)

    return _DatastoreIngestInfo.get_batch_ingest_info_data(
        new_ingest_info_entity)


def write_error(region: str, scraper_start_time: datetime, error: str,
                trace_id: Optional[str], task: Task) -> BatchIngestInfoData:
    """Writes a new ingest info for a given region.


       Args:
           region: (string) The region the ingest info is getting added for
           scraper_start_time: (datetime) The start time of the scraper that
            got the ingest info
           error: (string) the error message
           trace_id: (string) the trace id used to debug
           ingest_info: (IngestInfo) The ingest info data
           task: (Task) the task associated with the error
       """
    logging.info("Writing a new failure for region: [%s]", region)

    new_ingest_info_entity = _DatastoreIngestInfo.new(
        key=ds().key(INGEST_INFO_KIND),
        scraper_start_time=scraper_start_time,
        region=region,
        task=task,
        error=error,
        trace_id=trace_id).to_entity()

    try:
        retry_grpc(
            NUM_GRPC_RETRIES,
            ds().put,
            new_ingest_info_entity
        )
    except Exception:
        raise DatastoreErrorWriteError(error, region)

    return _DatastoreIngestInfo.get_batch_ingest_info_data(
        new_ingest_info_entity)


def batch_get_ingest_infos_for_region(region: str,
                                      scraper_start_time: datetime) ->\
        List[BatchIngestInfoData]:
    """Batch retrieves ingest infos for a particular region.

    Args:
        region: (string) Region to fetch ingest_infos for
        scraper_start_time: (datetime) Start time for the scraper

    Returns:
        A list of BatchIngestInfoData
    """
    return _batch_ingest_info_data_from_entities(
        _get_ingest_info_entities_for_region(region, scraper_start_time))


def batch_delete_ingest_infos_for_region(region: str):
    """Batch deletes ingest infos for a particular region.

        Args:
            region: (string) Region to delete ingest infos for
        """
    results = _get_ingest_info_entities_for_region(region)
    try:
        retry_grpc(
            NUM_GRPC_RETRIES, ds().delete_multi,
            [result.key for result in results]
        )
    except Exception:
        raise DatastoreBatchDeleteError(region)


def _batch_ingest_info_data_from_entities(
        entity_list: List[datastore.Entity]) -> List[BatchIngestInfoData]:
    return [_DatastoreIngestInfo.get_batch_ingest_info_data(entity) for entity
            in entity_list]


def _get_ingest_info_entities_for_region(region: str, scraper_start_time=None) \
        -> List[datastore.Entity]:
    session_query = ds().query(kind=INGEST_INFO_KIND)
    session_query.add_filter('region', '=', region)
    if scraper_start_time:
        session_query.add_filter('scraper_start_time', '=', scraper_start_time)

    results = None
    try:
        results = retry_grpc(
            NUM_GRPC_RETRIES, session_query.fetch, limit=None)

    except Exception:
        raise DatastoreBatchGetError(region)

    return list(results)
