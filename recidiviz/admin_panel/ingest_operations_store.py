# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Store used to keep information related to direct ingest operations"""
import logging
from typing import List, Optional, Dict

from google.cloud import tasks_v2

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
    get_direct_ingest_states_with_sftp_queue,
)
from recidiviz.utils import metadata

_START_DIRECT_INGEST_RUN_URL = (
    "https://{}.appspot.com/direct/scheduler?region={}&bucket={}"
)

_BQ_IMPORT_EXPORT_QUEUE = "direct-ingest-state-{}-bq-import-export"
_PROCESS_JOB_QUEUE = "direct-ingest-state-{}-process-job-queue"
_SCHEDULER_QUEUE = "direct-ingest-state-{}-scheduler"
_SFTP_QUEUE = "direct-ingest-state-{}-sftp-queue"

INGEST_QUEUES: List[str] = [
    _BQ_IMPORT_EXPORT_QUEUE,
    _PROCESS_JOB_QUEUE,
    _SCHEDULER_QUEUE,
]

_TASK_LOCATION = "us-east1"
_CLOUD_TASKS_URL = "https://console.cloud.google.com/cloudtasks?project={}"

QUEUE_STATE_ENUM = tasks_v2.enums.Queue.State


class IngestOperationsStore:
    """
    A store for tracking the current state of direct ingest.
    """

    def __init__(self, override_project_id: Optional[str] = None) -> None:
        self._override_project_id = override_project_id

    @property
    def project_id(self) -> str:
        return (
            metadata.project_id()
            if self._override_project_id is None
            else self._override_project_id
        )

    @property
    def region_codes_launched_in_env(self) -> List[StateCode]:
        return get_direct_ingest_states_launched_in_env()

    @staticmethod
    def get_queues_for_region(region_code: StateCode) -> List[str]:
        """Returns the list of formatted direct ingest queues for given region"""
        formatted_region_code = region_code.value.lower().replace("_", "-")
        queues: List[str] = [
            queue.format(formatted_region_code) for queue in INGEST_QUEUES
        ]

        if region_code in get_direct_ingest_states_with_sftp_queue():
            queues.append(_SFTP_QUEUE.format(formatted_region_code))

        return queues

    def start_ingest_run(self, region_code: StateCode) -> None:
        """This function is called through the Ingest Operations UI in the admin panel.
        It calls to start a direct ingest run for the given region_code
        Requires:
        - region_code: (required) Region code to start ingest for (i.e. "US_ID")
        """
        # TODO(#6975): Implement on a bucket-level basis
        raise NotImplementedError

    def update_ingest_queues_state(
        self, region_code: StateCode, new_queue_state: str
    ) -> None:
        """This function is called through the Ingest Operations UI in the admin panel.
        It updates the state of the following queues by either pausing or resuming the queues:
         - direct-ingest-state-<region_code>-bq-import-export
         - direct-ingest-state-<region_code>-process-job-queue
         - direct-ingest-state-<region_code>-scheduler
         - direct-ingest-state-<region_code>-sftp-queue    (for select regions)

        Requires:
        - region_code: (required) Region code to pause queues for
        - new_state: (required) Either 'PAUSED' or 'RUNNING'
        """
        client = tasks_v2.CloudTasksClient()
        queues_to_update = self.get_queues_for_region(region_code)

        if new_queue_state not in [
            QUEUE_STATE_ENUM.RUNNING.name,
            QUEUE_STATE_ENUM.PAUSED.name,
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
            queue_path = client.queue_path(self.project_id, _TASK_LOCATION, queue)

            if new_queue_state == QUEUE_STATE_ENUM.PAUSED.name:
                logging.info("Pausing queue: %s", new_queue_state)
                client.pause_queue(name=queue_path)
            else:
                logging.info("Resuming queue: %s", new_queue_state)
                client.resume_queue(name=queue_path)

    def get_ingest_queue_states(self, region_code: StateCode) -> List[Dict[str, str]]:
        """Returns a list of dictionaries that contain the name and states of direct ingest queues for a given region"""
        client = tasks_v2.CloudTasksClient()
        ingest_queue_states: List[Dict[str, str]] = []
        queues_to_update = self.get_queues_for_region(region_code)

        for queue_name in queues_to_update:
            queue_path = client.queue_path(self.project_id, _TASK_LOCATION, queue_name)
            queue = client.get_queue(name=queue_path)
            queue_state = {
                "name": queue_name,
                "state": QUEUE_STATE_ENUM(queue.state).name,
            }
            ingest_queue_states.append(queue_state)

        return ingest_queue_states
