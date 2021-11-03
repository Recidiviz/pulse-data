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
"""Class for interacting with the scraper cloud task queues."""

import uuid
from typing import Any, Dict, List, Optional

from google.cloud import tasks_v2

from recidiviz.common.google_cloud.google_cloud_tasks_client_wrapper import (
    GoogleCloudTasksClientWrapper,
    HttpMethod,
)
from recidiviz.common.google_cloud.google_cloud_tasks_shared_queues import (
    SCRAPER_PHASE_QUEUE_V2,
)


class ScraperCloudTaskManager:
    """Class for interacting with the scraper cloud task queues."""

    def __init__(self, project_id: Optional[str] = None):
        self.cloud_task_client = GoogleCloudTasksClientWrapper(project_id=project_id)

    def _format_scrape_task_id(self, region_code: str, rest: str):
        return f"{region_code}-{rest}"

    def purge_scrape_tasks(self, *, region_code: str, queue_name: str):
        """Purge scrape tasks for a given region from its queue.

        Args:
            region_code: `str` region code.
            queue_name: `str` queue name.
        """
        for task in self.list_scrape_tasks(
            region_code=region_code, queue_name=queue_name
        ):
            self.cloud_task_client.delete_task(task)

    def list_scrape_tasks(
        self, *, region_code: str, queue_name: str
    ) -> List[tasks_v2.types.task_pb2.Task]:
        """List scrape tasks for the given region and queue"""
        region_task_id_prefix = self._format_scrape_task_id(region_code, "")
        return self.cloud_task_client.list_tasks_with_prefix(
            queue_name, region_task_id_prefix
        )

    def create_scrape_task(
        self, *, region_code: str, queue_name: str, url: str, body: Dict[str, Any]
    ):
        """Create a scrape task in a queue.

        Args:
            region_code: `str` region code.
            queue_name: `str` queue name.
            url: `str` App Engine worker url.
            body: `dict` task body to be passed to worker.
        """
        self.cloud_task_client.create_task(
            task_id=self._format_scrape_task_id(region_code, str(uuid.uuid4())),
            queue_name=queue_name,
            relative_uri=url,
            body=body,
        )

    def create_scraper_phase_task(self, *, region_code: str, url: str):
        """Add a task to trigger the next phase of a scrape.

        This triggers the phase at the given url for an individual region,
        passing the `region_code` as a url parameter. For example, this can
        trigger stopping a scraper or inferring release for a particular region.
        """
        self.cloud_task_client.create_task(
            task_id=self._format_scrape_task_id(region_code, str(uuid.uuid4())),
            queue_name=SCRAPER_PHASE_QUEUE_V2,
            relative_uri=f"{url}?region={region_code}",
            http_method=HttpMethod.GET,
        )
