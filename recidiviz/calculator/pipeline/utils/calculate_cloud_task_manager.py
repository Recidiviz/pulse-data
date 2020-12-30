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
"""Class for interacting with the calculation pipeline cloud task queues."""

import datetime
import uuid

from recidiviz.common.google_cloud.cloud_task_queue_manager import CloudTaskQueueManager, CloudTaskQueueInfo
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    JOB_MONITOR_QUEUE_V2


class CalculateCloudTaskManager:
    """Class for interacting with the calculation pipeline cloud task queues."""
    def __init__(self) -> None:
        self.job_monitor_cloud_task_queue_manager = CloudTaskQueueManager(queue_info_cls=CloudTaskQueueInfo,
                                                                          queue_name=JOB_MONITOR_QUEUE_V2)

    def create_dataflow_monitor_task(self,
                                     job_id: str,
                                     location: str,
                                     topic: str) -> None:
        """Create a task to monitor the progress of a Dataflow job.

        Args:
            job_id: The unique id of the Dataflow job
            location: The region where the job is being run
            topic: Pub/Sub topic where a message will be published if the job
                completes successfully
        """
        body = {'project_id': self.job_monitor_cloud_task_queue_manager.cloud_task_client.project_id,
                'job_id': job_id,
                'location': location,
                'topic': topic}
        task_id = '{}-{}-{}'.format(
            job_id, str(datetime.datetime.utcnow().date()), uuid.uuid4())

        self.job_monitor_cloud_task_queue_manager.create_task(
            task_id=task_id,
            relative_uri='/dataflow_monitor/monitor',
            body=body,
            schedule_delay_seconds=300,  # 5-minute delay
        )
