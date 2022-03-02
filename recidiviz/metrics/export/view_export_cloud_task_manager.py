# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Class for interacting with cloud tasks related to BQ view export."""

import datetime
import uuid
from typing import Optional

import pytz

from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    METRIC_VIEW_EXPORT_QUEUE,
)


class ViewExportCloudTaskManager:
    """Class for interacting with cloud tasks related to BQ view export."""

    def __init__(self) -> None:
        self.cloud_task_queue_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo, queue_name=METRIC_VIEW_EXPORT_QUEUE
        )

    def create_metric_view_data_export_task(
        self,
        export_job_name: str,
        state_code: Optional[str] = None,
    ) -> None:
        """Create a BigQuery table export path.

        Args:
            export_job_name: Name of job to initiate export for (e.g. LANTERN).
            state_code: (Optional) State code to initiate export for (e.g. US_MO)
        """

        if state_code:
            uri = f"/export/metric_view_data?export_job_name={export_job_name}&state_code={state_code}"

            task_id = "-".join(
                [
                    "view_export",
                    export_job_name,
                    state_code,
                    str(datetime.datetime.now(tz=pytz.UTC).date()),
                    str(uuid.uuid4()),
                ]
            )
        else:
            uri = f"/export/metric_view_data?export_job_name={export_job_name}"

            task_id = "-".join(
                [
                    "view_export",
                    export_job_name,
                    str(datetime.datetime.now(tz=pytz.UTC).date()),
                    str(uuid.uuid4()),
                ]
            )

        self.cloud_task_queue_manager.create_task(
            task_id=task_id,
            relative_uri=uri,
            body={},
        )
