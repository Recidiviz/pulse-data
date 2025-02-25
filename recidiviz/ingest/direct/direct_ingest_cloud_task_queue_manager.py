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
"""Class for interacting with the direct ingest cloud task queues."""
import abc
import logging
import os
import uuid
from enum import Enum
from typing import Dict, Generator, List, Optional, TypedDict
from urllib.parse import urlencode

import attr
from google.cloud import tasks_v2

from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
)
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.types.cloud_task_args import (
    CloudTaskArgs,
    GcsfsRawDataBQImportArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata

SCHEDULER_TASK_ID_TAG = "scheduler"
HANDLE_NEW_FILES_TASK_ID_TAG = "handle_new_files"

_TASK_LOCATION = "us-east1"
QUEUE_STATE_ENUM = tasks_v2.Queue.State

# TODO(#28239) remove all code once raw data import dag is fully rolled out
class IngestQueueState(TypedDict):
    name: str
    state: QUEUE_STATE_ENUM


def _build_task_id(
    region_code: str,
    ingest_instance: DirectIngestInstance,
    task_id_tag: Optional[str],
    prefix_only: bool = False,
) -> str:
    """Creates a task id for a task running for a particular region. Ids take
    the form:
    <region_code>(-<task_id_tag>)(-<uuid>).

    For example:
    _build_task_id('us_nd', DirectIngestInstance.PRIMARY, 'elite_offenders', prefix_only=False) ->
        'us_nd-primary-elite_offenders-d1315074-8cea-4848-afe1-af20af07e275

    _build_task_id('us_nd', DirectIngestInstance.SECONDARY, None, prefix_only=False) ->
        'us_nd-secondary-d1315074-8cea-4848-afe1-af20af07e275

    _build_task_id('us_nd', DirectIngestInstance.PRIMARY, 'elite_offenders', prefix_only=True) ->
        'us_nd-primary-elite_offenders

    """
    task_id_parts = [region_code.lower(), ingest_instance.value.lower()]
    if task_id_tag:
        task_id_parts.append(task_id_tag)
    if not prefix_only:
        task_id_parts.append(str(uuid.uuid4()))

    return "-".join(task_id_parts)


def build_scheduler_task_id(
    region: DirectIngestRegion,
    ingest_instance: DirectIngestInstance,
    prefix_only: bool = False,
) -> str:
    return _build_task_id(
        region.region_code,
        ingest_instance,
        task_id_tag=SCHEDULER_TASK_ID_TAG,
        prefix_only=prefix_only,
    )


def build_handle_new_files_task_id(
    region: DirectIngestRegion,
    ingest_instance: DirectIngestInstance,
    prefix_only: bool = False,
) -> str:
    return _build_task_id(
        region.region_code,
        ingest_instance,
        task_id_tag=HANDLE_NEW_FILES_TASK_ID_TAG,
        prefix_only=prefix_only,
    )


def build_raw_data_import_task_id(
    region: DirectIngestRegion,
    data_import_args: GcsfsRawDataBQImportArgs,
) -> str:
    return _build_task_id(
        region.region_code,
        data_import_args.ingest_instance(),
        task_id_tag=data_import_args.task_id_tag(),
        prefix_only=False,
    )


class DirectIngestQueueType(Enum):
    SCHEDULER = "scheduler"
    RAW_DATA_IMPORT = "raw-data-import"


def _build_direct_ingest_queue_name(
    region_code: str,
    queue_type: DirectIngestQueueType,
    ingest_instance: DirectIngestInstance,
) -> str:
    """Creates a cloud task queue name for a specific task for a particular region.
    Names take the form: direct-ingest-<region_code>-<queue_type><optional instance suffix>.

    For example:
         _build_direct_ingest_queue_name('us_nd', DirectIngestQueueType.SCHEDULER, DirectIngestInstance.SECONDARY) ->
        'direct-ingest-state-us-nd-scheduler-secondary'
    """

    if ingest_instance == DirectIngestInstance.PRIMARY:
        instance_suffix = ""
    elif ingest_instance == DirectIngestInstance.SECONDARY:
        instance_suffix = "-secondary"
    else:
        raise ValueError(f"Unexpected ingest instance [{ingest_instance}]")
    return (
        f"direct-ingest-state-{region_code.lower().replace('_', '-')}"
        f"-{queue_type.value}{instance_suffix}"
    )


def _queue_name_for_queue_type(
    queue_type: DirectIngestQueueType,
    region_code: str,
    ingest_instance: DirectIngestInstance,
) -> str:
    return _build_direct_ingest_queue_name(region_code, queue_type, ingest_instance)


def get_direct_ingest_queues_for_state(state_code: StateCode) -> List[str]:
    queue_names = []
    for queue_type in DirectIngestQueueType:
        for ingest_instance in DirectIngestInstance:
            queue_names.append(
                _queue_name_for_queue_type(
                    queue_type, state_code.value, ingest_instance
                )
            )
    return queue_names


@attr.s
class DirectIngestCloudTaskQueueInfo(CloudTaskQueueInfo):
    def _task_names_for_instance(
        self, region_code: str, ingest_instance: DirectIngestInstance
    ) -> Generator[str, None, None]:
        instance_prefix = _build_task_id(
            region_code,
            ingest_instance,
            task_id_tag=None,
            prefix_only=True,
        )
        yield from self.task_names_for_task_id_prefix(instance_prefix)

    def task_names_for_task_id_prefix(
        self, task_prefix: str
    ) -> Generator[str, None, None]:
        for task_name in self.task_names:
            _, task_id = os.path.split(task_name)
            if task_id.startswith(task_prefix):
                yield task_name

    def has_any_tasks_for_instance(
        self, region_code: str, ingest_instance: DirectIngestInstance
    ) -> bool:
        return any(self._task_names_for_instance(region_code, ingest_instance))


@attr.s
class SchedulerCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    pass


@attr.s
class RawDataImportCloudTaskQueueInfo(DirectIngestCloudTaskQueueInfo):
    """Class containing information about tasks in a given region's raw data import task
    queue.
    """

    @staticmethod
    def _is_raw_data_import_task(task_name: str) -> bool:
        return "raw_data_import" in task_name

    def is_raw_data_import_task_already_queued(
        self, task_args: GcsfsRawDataBQImportArgs
    ) -> bool:
        return any(
            self._is_raw_data_import_task(task_name)
            and task_args.task_id_tag() in task_name
            for task_name in self.task_names
        )

    def has_raw_data_import_jobs_queued(self) -> bool:
        return any(
            self._is_raw_data_import_task(task_name) for task_name in self.task_names
        )


class DirectIngestCloudTaskQueueManager:
    """Abstract interface for a class that interacts with Cloud Task queues."""

    @abc.abstractmethod
    def get_raw_data_import_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> RawDataImportCloudTaskQueueInfo:
        """Returns information about the tasks in the raw data import queue for
        the given region."""

    @abc.abstractmethod
    def get_scheduler_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        """Returns information about the tasks in the job scheduler queue for
        the given region."""

    @abc.abstractmethod
    def create_direct_ingest_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> None:
        """Creates a scheduler task for direct ingest for a given region.
        Scheduler tasks should be short-running and queue other tasks if
        there is more work to do.

        Args:
            region: `Region` direct ingest region.
            ingest_instance: The ingest instance that work should be scheduled for.
        """

    @abc.abstractmethod
    def create_direct_ingest_handle_new_files_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        can_start_ingest: bool,
    ) -> None:
        """Creates a Cloud Task for for a given region that identifies and registers
        new files that have been added to the provided ingest bucket.

        Args:
            region: `Region` direct ingest region.
            ingest_instance: The ingest instance to look for new files in.
            can_start_ingest: True if the controller can proceed with scheduling
                more jobs to process the data once the files have been properly named
                and registered.
        """

    @abc.abstractmethod
    def create_direct_ingest_raw_data_import_task(
        self,
        region: DirectIngestRegion,
        raw_data_source_instance: DirectIngestInstance,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        pass

    @abc.abstractmethod
    def delete_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        task_name: str,
    ) -> None:
        """Deletes a single task from the region's scheduler queue with the
        fully-qualified |task_name| which follows this format:
           '/projects/{project}/locations/{location}/queues/{queue}/tasks/{task_id}'
        """

    @staticmethod
    def json_to_cloud_task_args(json_data: dict) -> Optional[CloudTaskArgs]:
        if "cloud_task_args" in json_data and "args_type" in json_data:
            args_type = json_data["args_type"]
            cloud_task_args_dict = json_data["cloud_task_args"]
            if args_type == GcsfsRawDataBQImportArgs.__name__:
                return GcsfsRawDataBQImportArgs.from_serializable(cloud_task_args_dict)
            logging.error("Unexpected args_type in json_data: %s", args_type)
        return None

    @staticmethod
    def _get_body_from_args(cloud_task_args: CloudTaskArgs) -> Dict:
        body = {
            "cloud_task_args": cloud_task_args.to_serializable(),
            "args_type": cloud_task_args.__class__.__name__,
        }
        return body

    def _get_all_ingest_instance_queues(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> List[DirectIngestCloudTaskQueueInfo]:
        """Returns all ingest instance related queue information."""
        ingest_queue_info: List[DirectIngestCloudTaskQueueInfo] = [
            self.get_scheduler_queue_info(region, ingest_instance),
            self.get_raw_data_import_queue_info(region, ingest_instance),
        ]

        return ingest_queue_info

    def all_ingest_instance_queues_are_empty(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> bool:
        """Returns whether all ingest instance related queues are empty."""
        ingest_queue_info = self._get_all_ingest_instance_queues(
            region, ingest_instance
        )
        return all(queue_info.is_empty() for queue_info in ingest_queue_info)


class DirectIngestCloudTaskQueueManagerImpl(DirectIngestCloudTaskQueueManager):
    """Real implementation of the DirectIngestCloudTaskManager that interacts
    with actual GCP Cloud Task queues."""

    def __init__(self) -> None:
        self.cloud_tasks_client = tasks_v2.CloudTasksClient()

    def _get_scheduler_queue_manager(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> SingleCloudTaskQueueManager[SchedulerCloudTaskQueueInfo]:
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.SCHEDULER, region.region_code, ingest_instance
        )

        return SingleCloudTaskQueueManager(
            queue_info_cls=SchedulerCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def _get_raw_data_import_queue_manager(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> SingleCloudTaskQueueManager[RawDataImportCloudTaskQueueInfo]:
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.RAW_DATA_IMPORT,
            region.region_code,
            ingest_instance,
        )

        return SingleCloudTaskQueueManager(
            queue_info_cls=RawDataImportCloudTaskQueueInfo,
            queue_name=queue_name,
            cloud_tasks_client=self.cloud_tasks_client,
        )

    def get_scheduler_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> SchedulerCloudTaskQueueInfo:
        return self._get_scheduler_queue_manager(
            region, ingest_instance
        ).get_queue_info(task_id_prefix=region.region_code)

    def get_raw_data_import_queue_info(
        self, region: DirectIngestRegion, ingest_instance: DirectIngestInstance
    ) -> RawDataImportCloudTaskQueueInfo:
        return self._get_raw_data_import_queue_manager(
            region, ingest_instance
        ).get_queue_info(task_id_prefix=region.region_code)

    def create_direct_ingest_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
    ) -> None:
        task_id = build_scheduler_task_id(region, ingest_instance)

        params = {
            "region": region.region_code.lower(),
            "ingest_instance": ingest_instance.value.lower(),
        }

        relative_uri = f"/direct/scheduler?{urlencode(params)}"

        self._get_scheduler_queue_manager(region, ingest_instance).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body={},
        )

    def create_direct_ingest_handle_new_files_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        can_start_ingest: bool,
    ) -> None:
        task_id = build_handle_new_files_task_id(region, ingest_instance)

        params = {
            "region": region.region_code.lower(),
            "ingest_instance": ingest_instance.value.lower(),
            "can_start_ingest": can_start_ingest,
        }
        relative_uri = f"/direct/handle_new_files?{urlencode(params)}"

        self._get_scheduler_queue_manager(region, ingest_instance).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body={},
        )

    def create_direct_ingest_raw_data_import_task(
        self,
        region: DirectIngestRegion,
        raw_data_source_instance: DirectIngestInstance,
        data_import_args: GcsfsRawDataBQImportArgs,
    ) -> None:
        task_id = build_raw_data_import_task_id(region, data_import_args)

        params = {
            "region": region.region_code.lower(),
            "file_path": data_import_args.raw_data_file_path.abs_path(),
        }
        relative_uri = f"/direct/raw_data_import?{urlencode(params)}"

        body = self._get_body_from_args(data_import_args)

        self._get_raw_data_import_queue_manager(
            region, raw_data_source_instance
        ).create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body=body,
        )

    def delete_scheduler_queue_task(
        self,
        region: DirectIngestRegion,
        ingest_instance: DirectIngestInstance,
        task_name: str,
    ) -> None:
        self._get_scheduler_queue_manager(region, ingest_instance).delete_task(
            task_name=task_name
        )

    def update_ingest_queue_states_str(
        self, state_code: StateCode, new_queue_state_str: str
    ) -> None:
        self.update_ingest_queue_states(
            state_code,
            # The Queue.State proto enum specifies __members__ for key-index lookups, but mypy does not recognize that
            QUEUE_STATE_ENUM[new_queue_state_str],  # type: ignore[misc, valid-type]
        )

    def get_scheduler_queue_state(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> tasks_v2.Queue.State:
        """Retrieves the state of the state code and instance specific scheduler queue."""
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.SCHEDULER, state_code.value, ingest_instance
        )

        queue_path = self.cloud_tasks_client.queue_path(
            metadata.project_id(), _TASK_LOCATION, queue_name
        )
        queue = self.cloud_tasks_client.get_queue(name=queue_path)
        return queue.state

    def update_scheduler_queue_state(
        self,
        state_code: StateCode,
        ingest_instance: DirectIngestInstance,
        new_queue_state: tasks_v2.Queue.State,
    ) -> None:
        """Updates the state of the state code and instance specific scheduler queue."""
        queue_name = _queue_name_for_queue_type(
            DirectIngestQueueType.SCHEDULER, state_code.value, ingest_instance
        )

        queue_path = self.cloud_tasks_client.queue_path(
            metadata.project_id(), _TASK_LOCATION, queue_name
        )

        if new_queue_state not in [
            QUEUE_STATE_ENUM.RUNNING,
            QUEUE_STATE_ENUM.PAUSED,
        ]:
            logging.error(
                "Received an invalid queue state: %s. This method should only be used "
                "to update queue states to PAUSED or RUNNING",
                new_queue_state,
            )
            raise ValueError(
                f"Invalid queue state [{new_queue_state}] received",
            )

        if new_queue_state == QUEUE_STATE_ENUM.PAUSED:
            self.cloud_tasks_client.pause_queue(name=queue_path)
        else:
            self.cloud_tasks_client.resume_queue(name=queue_path)

    def purge_queue(
        self,
        queue_name: str,
    ) -> None:
        """Purges all tasks from the queue with the specified queue_name."""
        # Build full queue path
        full_queue_path = self.cloud_tasks_client.queue_path(
            metadata.project_id(), _TASK_LOCATION, queue_name
        )
        self.cloud_tasks_client.purge_queue(name=full_queue_path)

    def update_ingest_queue_states(
        self, state_code: StateCode, new_queue_state: tasks_v2.Queue.State
    ) -> None:
        """
         It updates the state of the following queues by either pausing or resuming the
        queues:
         - direct-ingest-state-<region_code>-scheduler
         - direct-ingest-state-<region_code>-scheduler-secondary
         - direct-ingest-state-<region_code>-raw-data-import
         - direct-ingest-state-<region_code>-raw-data-import-secondary

        Requires:
        - state_code: (required) State code to pause queues for
        - new_state: (required) Either 'PAUSED' or 'RUNNING'
        """
        queues_to_update = sorted(get_direct_ingest_queues_for_state(state_code))

        if new_queue_state not in [
            QUEUE_STATE_ENUM.RUNNING,
            QUEUE_STATE_ENUM.PAUSED,
        ]:
            logging.error(
                "Received an invalid queue state: %s. This method should only be used "
                "to update queue states to PAUSED or RUNNING",
                new_queue_state,
            )
            raise ValueError(
                f"Invalid queue state [{new_queue_state}] received",
            )

        for queue in queues_to_update:
            queue_path = self.cloud_tasks_client.queue_path(
                metadata.project_id(), _TASK_LOCATION, queue
            )

            if new_queue_state == QUEUE_STATE_ENUM.PAUSED:
                logging.info("Pausing queue: %s", new_queue_state)
                self.cloud_tasks_client.pause_queue(name=queue_path)
            else:
                logging.info("Resuming queue: %s", new_queue_state)
                self.cloud_tasks_client.resume_queue(name=queue_path)

    def get_ingest_queue_states(self, state_code: StateCode) -> List[IngestQueueState]:
        """Returns a list of dictionaries that contain the name and states of direct ingest queues for a given region"""
        ingest_queue_states: List[IngestQueueState] = []
        queues_for_state = sorted(get_direct_ingest_queues_for_state(state_code))

        for queue_name in queues_for_state:
            queue_path = self.cloud_tasks_client.queue_path(
                metadata.project_id(), _TASK_LOCATION, queue_name
            )
            queue = self.cloud_tasks_client.get_queue(name=queue_path)
            queue_state: IngestQueueState = {
                "name": queue_name,
                "state": queue.state,
            }
            ingest_queue_states.append(queue_state)

        return ingest_queue_states
