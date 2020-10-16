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
"""Class for interacting with cloud tasks and queues related to BigQuery
exports.
"""

import datetime
import uuid
from typing import Optional

from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import \
    BIGQUERY_QUEUE_V2, JOB_MONITOR_QUEUE_V2
from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import \
    GoogleCloudTasksClientWrapper
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    CloudTaskQueueInfo


class BQExportCloudTaskManager:
    """Class for interacting with cloud tasks and queues related to BigQuery
    exports.
    """

    def __init__(self, project_id: Optional[str] = None):
        self.cloud_task_client = \
            GoogleCloudTasksClientWrapper(project_id=project_id)

    def _get_queue_info(self,
                        queue_name: str) -> CloudTaskQueueInfo:
        tasks_list = \
            self.cloud_task_client.list_tasks_with_prefix(
                queue_name=queue_name,
                task_id_prefix='')
        task_names = [task.name for task in tasks_list] if tasks_list else []
        return CloudTaskQueueInfo(queue_name=queue_name,
                                  task_names=task_names)

    def get_bq_queue_info(self) -> CloudTaskQueueInfo:
        return self._get_queue_info(BIGQUERY_QUEUE_V2)

    def create_bq_task(self, table_name: str, schema_type: str) -> None:
        """Create a BigQuery table export path.

        Args:
            table_name: Cloud SQL table to export to BQ. Must be defined in
                the *_TABLES_TO_EXPORT for the given schema.
            schema_type: The schema of the table being exported, either 'jails'
                or 'state'.
            url: App Engine worker URL.
        """
        body = {'table_name': table_name, 'schema_type': schema_type}
        task_id = '{}-{}-{}-{}'.format(
            table_name,
            schema_type,
            str(datetime.datetime.utcnow().date()),
            uuid.uuid4())

        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=BIGQUERY_QUEUE_V2,
            relative_uri='/export_manager/export',
            body=body,
        )

    def create_bq_monitor_task(self,
                               topic: str,
                               message: str) -> None:
        """Create a task to monitor the progress of an export to BQ.

        Args:
            topic: Pub/Sub topic where a message will be published when the BQ
                export tasks are complete.
            message: The message that will be sent to the topic.
        """

        body = {'topic': topic, 'message': message}
        task_topic = topic.replace('.', '-')
        task_id = '{}-{}-{}'.format(
            task_topic, str(datetime.datetime.utcnow().date()), uuid.uuid4())

        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=JOB_MONITOR_QUEUE_V2,
            relative_uri='/export_manager/bq_monitor',
            body=body,
            schedule_delay_seconds=60,  # 1-minute delay
        )
