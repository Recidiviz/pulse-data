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
"""Class for interacting with cloud tasks and queues related to updating state raw data latest views
"""

import datetime
import uuid

import pytz

from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueManager,
    CloudTaskQueueInfo,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    BIGQUERY_QUEUE_V2,
)


class DirectIngestRawUpdateCloudTaskManager:
    """Class for interacting with cloud tasks and queues related to updating
    state raw data latest views.
    """

    def __init__(self) -> None:
        self.cloud_task_queue_manager = CloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo, queue_name=BIGQUERY_QUEUE_V2
        )

    def create_raw_data_latest_view_update_task(self, region_code: str) -> None:

        relative_uri = (
            f"/direct/update_raw_data_latest_views_for_state?region={region_code}"
        )

        task_id = "{}-update_raw_data_latest_views-{}-{}".format(
            region_code, str(datetime.datetime.now(tz=pytz.UTC).date()), uuid.uuid4()
        )

        self.cloud_task_queue_manager.create_task(
            task_id=task_id,
            relative_uri=relative_uri,
            body={},
        )
