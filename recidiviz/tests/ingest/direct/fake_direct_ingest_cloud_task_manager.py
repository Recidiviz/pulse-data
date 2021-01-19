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
"""Base class for fake implementations of DirectIngestCloudTaskManager."""

import abc
from typing import Optional

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    DirectIngestCloudTaskManager, ProcessIngestJobCloudTaskQueueInfo, SchedulerCloudTaskQueueInfo, \
    BQImportExportCloudTaskQueueInfo
from recidiviz.utils.regions import Region


class FakeDirectIngestCloudTaskManager(DirectIngestCloudTaskManager):
    """Base class for fake implementations of DirectIngestCloudTaskManager."""

    def __init__(self) -> None:
        self.controller: Optional[BaseDirectIngestController] = None

    def set_controller(self, controller: BaseDirectIngestController) -> None:
        self.controller = controller

    @abc.abstractmethod
    def get_process_job_queue_info(self, region: Region) -> ProcessIngestJobCloudTaskQueueInfo:
        pass

    @abc.abstractmethod
    def get_scheduler_queue_info(self, region: Region) -> SchedulerCloudTaskQueueInfo:
        pass

    @abc.abstractmethod
    def get_bq_import_export_queue_info(self, region: Region) -> BQImportExportCloudTaskQueueInfo:
        pass

    @abc.abstractmethod
    def create_direct_ingest_process_job_task(self, region: Region,
                                              ingest_args: IngestArgs) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_scheduler_queue_task(self, region: Region,
                                                  just_finished_job: bool,
                                                  delay_sec: int) -> None:
        pass

    @abc.abstractmethod
    def create_direct_ingest_handle_new_files_task(self,
                                                   region: Region,
                                                   can_start_ingest: bool) -> None:
        pass
