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

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManager,
)


class FakeDirectIngestCloudTaskManager(DirectIngestCloudTaskManager, abc.ABC):
    """Base class for fake implementations of DirectIngestCloudTaskManager."""

    def __init__(self) -> None:
        self.controller: Optional[BaseDirectIngestController] = None

    def set_controller(self, controller: BaseDirectIngestController) -> None:
        self.controller = controller
