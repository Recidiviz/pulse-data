# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A fake implementation of DirectIngestInstanceStatusManager for use in tests."""
import datetime
from typing import List, Optional

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus


class FakeDirectIngestInstanceStatusManager(DirectIngestInstanceStatusManager):
    """A fake implementation of DirectIngestInstanceStatusManager for use in tests."""

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        super().__init__(region_code=region_code, ingest_instance=ingest_instance)
        self.statuses: List[DirectIngestInstanceStatus] = []

    def get_raw_data_source_instance(self) -> DirectIngestInstance:
        """Returns the current raw data source of the ingest instance associated with
        this status manager.
        """
        return self.ingest_instance

    def change_status_to(self, new_status: DirectIngestStatus) -> None:
        """Change status to the passed in status."""
        self.validate_transition(
            self.ingest_instance,
            current_status=(self.statuses[-1].status if self.statuses else None),
            new_status=new_status,
        )

        new_ingest_instance_status = DirectIngestInstanceStatus(
            region_code=self.region_code,
            instance=self.ingest_instance,
            timestamp=datetime.datetime.now(),
            status=new_status,
        )
        self.statuses.append(new_ingest_instance_status)

    def get_current_status(self) -> Optional[DirectIngestStatus]:
        """Get current status."""
        return self.statuses[-1].status if self.statuses else None

    def get_all_statuses(self) -> List[DirectIngestInstanceStatus]:
        """Return all statuses."""
        return self.statuses
