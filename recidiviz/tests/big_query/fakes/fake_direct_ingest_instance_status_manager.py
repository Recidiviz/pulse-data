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
    DirectIngestInstanceStatusChangeListener,
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus


class FakeDirectIngestInstanceStatusManager(DirectIngestInstanceStatusManager):
    """A fake implementation of DirectIngestInstanceStatusManager for use in tests."""

    def __init__(
        self,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        initial_statuses: List[DirectIngestStatus],
        change_listener: Optional[DirectIngestInstanceStatusChangeListener] = None,
    ):
        super().__init__(
            region_code=region_code,
            ingest_instance=ingest_instance,
        )
        self.statuses: List[DirectIngestInstanceStatus] = [
            DirectIngestInstanceStatus(
                region_code=self.region_code,
                instance=self.ingest_instance,
                status_timestamp=datetime.datetime.now(),
                status=status,
            )
            for status in initial_statuses
        ]
        self.change_listener = change_listener

    def get_raw_data_source_instance(self) -> Optional[DirectIngestInstance]:
        """Returns the current raw data source of the ingest instance associated with
        this status manager.
        """
        for status in reversed(self.statuses):
            if status.status == DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED:
                return self.ingest_instance
            if status.status == DirectIngestStatus.STANDARD_RERUN_STARTED:
                return DirectIngestInstance.PRIMARY

        raise ValueError("Did not find a valid start rerun status")

    def change_status_to(self, new_status: DirectIngestStatus) -> None:
        """Change status to the passed in status."""
        self.validate_transition(
            self.ingest_instance,
            current_status=self.statuses[-1].status,
            new_status=new_status,
        )

        new_ingest_instance_status = DirectIngestInstanceStatus(
            region_code=self.region_code,
            instance=self.ingest_instance,
            status_timestamp=datetime.datetime.now(),
            status=new_status,
        )
        self.statuses.append(new_ingest_instance_status)
        if self.change_listener is not None:
            self.change_listener.on_raw_data_source_instance_change(
                self.get_raw_data_source_instance()
            )

    def get_current_status(self) -> DirectIngestStatus:
        """Get current status."""
        return self.statuses[-1].status

    def get_all_statuses(self) -> List[DirectIngestInstanceStatus]:
        """Return all statuses."""
        return self.statuses

    def add_instance_status(self, status: DirectIngestStatus) -> None:
        """Add a status (without any validations)."""
        new_ingest_instance_status = DirectIngestInstanceStatus(
            region_code=self.region_code,
            instance=self.ingest_instance,
            status_timestamp=datetime.datetime.now(),
            status=status,
        )
        self.statuses.append(new_ingest_instance_status)

    def get_current_status_info(self) -> DirectIngestInstanceStatus:
        """Get current status and associated information."""
        return self.statuses[-1]
