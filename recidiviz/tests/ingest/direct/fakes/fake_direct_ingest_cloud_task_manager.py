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
from typing import Dict, Optional

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_queue_manager import (
    DirectIngestCloudTaskQueueManager,
    DirectIngestQueueType,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class FakeDirectIngestCloudTaskQueueManager(DirectIngestCloudTaskQueueManager, abc.ABC):
    """Base class for fake implementations of DirectIngestCloudTaskManager."""

    _queue_type_to_name: Dict[DirectIngestQueueType, str] = {
        DirectIngestQueueType.SFTP_QUEUE: "sftp_download",
        DirectIngestQueueType.MATERIALIZE_INGEST_VIEW: "ingest_view_materialization",
        DirectIngestQueueType.RAW_DATA_IMPORT: "raw_data_import",
        DirectIngestQueueType.SCHEDULER: "schedule",
        DirectIngestQueueType.EXTRACT_AND_MERGE: "process",
    }

    def __init__(self) -> None:
        self.controllers: Dict[
            DirectIngestInstance, Optional[BaseDirectIngestController]
        ] = {}

    def set_controller(
        self,
        ingest_instance: DirectIngestInstance,
        controller: BaseDirectIngestController,
    ) -> None:
        self.controllers[ingest_instance] = controller

    def queue_name_for_type(
        self, queue_type: DirectIngestQueueType, instance: DirectIngestInstance
    ) -> str:
        return self._queue_type_to_name[queue_type] + (
            "_secondary" if instance == DirectIngestInstance.SECONDARY else ""
        )
