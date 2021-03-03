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

from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    BIGQUERY_QUEUE_V2,
    JOB_MONITOR_QUEUE_V2,
)
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType


class BQRefreshCloudTaskManager:
    """Class for interacting with cloud tasks and queues related to BigQuery
    exports.
    """

    def __init__(self) -> None:
        self.bq_cloud_task_queue_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo, queue_name=BIGQUERY_QUEUE_V2
        )
        self.job_monitor_cloud_task_queue_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo, queue_name=JOB_MONITOR_QUEUE_V2
        )

    def get_bq_queue_info(self) -> CloudTaskQueueInfo:
        return self.bq_cloud_task_queue_manager.get_queue_info()

    def create_refresh_bq_table_task(
        self, table_name: str, schema_type: SchemaType
    ) -> None:
        """Create a BigQuery table export path.

        Args:
            table_name: Cloud SQL table to export to BQ. Must be defined in
                one of the base_schema SchemaTypes.
            schema_type: The SchemaType of the table being exported.
            url: App Engine worker URL.
        """
        body = {"table_name": table_name, "schema_type": schema_type.value}
        task_id = "{}-{}-{}-{}".format(
            table_name,
            schema_type.value,
            str(datetime.datetime.utcnow().date()),
            uuid.uuid4(),
        )

        self.bq_cloud_task_queue_manager.create_task(
            task_id=task_id,
            relative_uri="/cloud_sql_to_bq/refresh_bq_table",
            body=body,
        )

    def create_bq_refresh_monitor_task(
        self, schema: str, topic: str, message: str
    ) -> None:
        """Create a task to monitor the progress of an export to BQ.

        Args:
            topic: Pub/Sub topic where a message will be published when the BQ
                export tasks are complete.
            message: The message that will be sent to the topic.
            schema: Which schema the export is for
        """
        task_topic = topic.replace(".", "-")
        body = {"schema": schema, "topic": topic, "message": message}
        task_id = "{}-{}-{}".format(
            task_topic, str(datetime.datetime.utcnow().date()), uuid.uuid4()
        )

        self.job_monitor_cloud_task_queue_manager.create_task(
            task_id=task_id,
            relative_uri="/cloud_sql_to_bq/monitor_refresh_bq_tasks",
            body=body,
            schedule_delay_seconds=60,  # 1-minute delay
        )
