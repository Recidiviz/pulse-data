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
"""Implementation of the IngestViewMaterializationArgsGeneratorDelegate for use on
states using BQ-based ingest view materialization.
"""
import datetime
from typing import List, Optional

from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_args_generator_delegate import (
    IngestViewMaterializationArgsGeneratorDelegate,
    RegisteredMaterializationJob,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestViewMaterializationMetadata,
)


class BQBasedMaterializationArgsGeneratorDelegate(
    IngestViewMaterializationArgsGeneratorDelegate[BQIngestViewMaterializationArgs]
):
    """Implementation of the IngestViewMaterializationArgsGeneratorDelegate for use on
    states using file-based ingest view materialization.
    """

    def __init__(
        self,
        *,
        metadata_manager: DirectIngestViewMaterializationMetadataManager,
    ):
        self.metadata_manager = metadata_manager

    def get_registered_jobs_pending_completion(
        self,
    ) -> List[BQIngestViewMaterializationArgs]:
        metadata_pending_export = self.metadata_manager.get_jobs_pending_completion()
        return self._materialization_args_from_metadata(metadata_pending_export)

    def build_new_args(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        lower_bound_datetime_exclusive: Optional[datetime.datetime],
    ) -> BQIngestViewMaterializationArgs:
        return BQIngestViewMaterializationArgs(
            ingest_view_name=ingest_view_name,
            lower_bound_datetime_exclusive=lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=upper_bound_datetime_inclusive,
            ingest_instance_=self.metadata_manager.ingest_instance,
        )

    def register_new_job(self, args: BQIngestViewMaterializationArgs) -> None:
        self.metadata_manager.register_ingest_materialization_job(args)

    def get_most_recent_registered_job(
        self, ingest_view_name: str
    ) -> Optional[RegisteredMaterializationJob]:
        metadata_row = self.metadata_manager.get_most_recent_registered_job(
            ingest_view_name
        )

        if not metadata_row:
            return None

        return RegisteredMaterializationJob(
            job_creation_time=metadata_row.job_creation_time,
            upper_bound_datetime_inclusive=metadata_row.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=metadata_row.lower_bound_datetime_exclusive,
        )

    @staticmethod
    def _materialization_args_from_metadata(
        metadata_list: List[DirectIngestViewMaterializationMetadata],
    ) -> List[BQIngestViewMaterializationArgs]:
        return [
            BQIngestViewMaterializationArgs(
                ingest_view_name=metadata.ingest_view_name,
                lower_bound_datetime_exclusive=metadata.lower_bound_datetime_exclusive,
                upper_bound_datetime_inclusive=metadata.upper_bound_datetime_inclusive,
                ingest_instance_=DirectIngestInstance(metadata.instance),
            )
            for metadata in metadata_list
        ]
