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

import pytz

from recidiviz.cloud_functions.cloudsql_to_bq_refresh_utils import (
    PIPELINE_RUN_TYPE_REQUEST_ARG,
    UPDATE_MANAGED_VIEWS_REQUEST_ARG,
)
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    CLOUD_SQL_TO_BQ_REFRESH_QUEUE,
    CLOUD_SQL_TO_BQ_REFRESH_SCHEDULER_QUEUE,
)
from recidiviz.persistence.database.schema_utils import SchemaType


class BQRefreshCloudTaskManager:
    """Class for interacting with cloud tasks and queues related to BigQuery
    exports.
    """

    def __init__(self) -> None:
        self.bq_cloud_task_queue_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo, queue_name=CLOUD_SQL_TO_BQ_REFRESH_QUEUE
        )

        self.job_monitor_cloud_task_queue_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo,
            queue_name=CLOUD_SQL_TO_BQ_REFRESH_SCHEDULER_QUEUE,
        )

    def get_bq_queue_info(self) -> CloudTaskQueueInfo:
        return self.bq_cloud_task_queue_manager.get_queue_info()

    def create_reattempt_create_refresh_tasks_task(
        self,
        schema: str,
        lock_id: str,
        pipeline_run_type: Optional[str],
        update_managed_views: Optional[str],
    ) -> None:
        """Schedules a task that will reattempt to create BQ refresh tasks in 1 minute.

        Args:
            lock_id: The id of the currently held BQ refresh lock.
            schema: Which schema the export is for
            pipeline_run_type: Which pipeline run should be triggered after the
                refresh, if any
            update_managed_views: Whether the managed views should be updated after
                the refresh
        """
        task_id = "-".join(
            [
                "reenqueue_wait_task",
                str(datetime.datetime.now(tz=pytz.UTC).date()),
                str(uuid.uuid4()),
            ]
        )
        body = {"lock_id": lock_id}
        if pipeline_run_type:
            body[PIPELINE_RUN_TYPE_REQUEST_ARG] = pipeline_run_type
        if update_managed_views:
            body[UPDATE_MANAGED_VIEWS_REQUEST_ARG] = update_managed_views

        self.job_monitor_cloud_task_queue_manager.create_task(
            task_id=task_id,
            body=body,
            relative_uri=f"/cloud_sql_to_bq/create_refresh_bq_schema_task/{schema}",
            schedule_delay_seconds=60,
        )

    def create_refresh_bq_schema_task(
        self,
        schema_type: SchemaType,
        pipeline_run_type: Optional[str],
        update_managed_views: Optional[str],
    ) -> None:
        """Queues a task to refresh the given schema in BQ.

        Args:
            schema_type: The SchemaType of the table being exported.
            pipeline_run_type: Which pipeline run should be triggered after the
                refresh, if any
            update_managed_views: Whether the managed views should be updated after
                the refresh
        """
        task_id = "-".join(
            [
                schema_type.value,
                str(datetime.datetime.now(tz=pytz.UTC).date()),
                str(uuid.uuid4()),
            ]
        )

        body = {}
        if pipeline_run_type:
            body[PIPELINE_RUN_TYPE_REQUEST_ARG] = pipeline_run_type
        if update_managed_views:
            body[UPDATE_MANAGED_VIEWS_REQUEST_ARG] = update_managed_views

        self.bq_cloud_task_queue_manager.create_task(
            task_id=task_id,
            relative_uri=f"/cloud_sql_to_bq/refresh_bq_schema/{schema_type.value}",
            body=body,
        )

    # TODO(#11437): Support for this type of task here is a **temporary** solution,
    #  and will be deleted once we put the BigQuery view update into the DAG.
    def create_update_managed_views_task(self) -> None:
        """Queues a task to update all managed views in BigQuery."""
        task_id = "-".join(
            [
                "update-all-managed-views",
                str(datetime.datetime.now(tz=pytz.UTC).date()),
                str(uuid.uuid4()),
            ]
        )

        self.bq_cloud_task_queue_manager.create_task(
            task_id=task_id,
            relative_uri="/view_update/update_all_managed_views",
            body={},
        )
