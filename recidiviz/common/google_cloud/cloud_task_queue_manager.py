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
"""Various helper classes for interacting with a cloud task queue."""

from typing import List, Type, TypeVar, Generic, Dict, Optional

import attr
from google.cloud import tasks_v2

from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    GoogleCloudTasksClientWrapper,
)


@attr.s
class CloudTaskQueueInfo:
    """Holds info about a Cloud Task queue."""

    queue_name: str = attr.ib()

    # Task names for tasks in queue, in order.
    # pylint:disable=not-an-iterable
    task_names: List[str] = attr.ib(factory=list)

    def size(self) -> int:
        """Number of tasks currently queued in the queue for the given region.
        If this is generated from the queue itself, it will return at least 1.
        """
        return len(self.task_names)


QueueInfoType = TypeVar("QueueInfoType", bound=CloudTaskQueueInfo)


class CloudTaskQueueManager(Generic[QueueInfoType]):
    """Class with helpers for interacting with a single CloudTask queue."""

    def __init__(
        self,
        queue_info_cls: Type[QueueInfoType],
        queue_name: str,
        cloud_tasks_client: Optional[tasks_v2.CloudTasksClient] = None,
    ):
        self.cloud_task_client = GoogleCloudTasksClientWrapper(cloud_tasks_client)
        self.queue_info_cls = queue_info_cls
        self.queue_name = queue_name

    def get_queue_info(self, *, task_id_prefix: str = "") -> QueueInfoType:
        tasks_list = self.cloud_task_client.list_tasks_with_prefix(
            queue_name=self.queue_name, task_id_prefix=task_id_prefix
        )
        task_names = [task.name for task in tasks_list] if tasks_list else []
        return self.queue_info_cls(queue_name=self.queue_name, task_names=task_names)

    def create_task(
        self,
        *,
        relative_uri: str,
        body: Dict[str, str],
        task_id: Optional[str] = None,
        schedule_delay_seconds: int = 0
    ) -> None:
        self.cloud_task_client.create_task(
            task_id=task_id,
            queue_name=self.queue_name,
            relative_uri=relative_uri,
            body=body,
            schedule_delay_seconds=schedule_delay_seconds,
        )
