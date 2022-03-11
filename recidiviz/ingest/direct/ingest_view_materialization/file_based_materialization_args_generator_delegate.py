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
states using file-based ingest view materialization.

TODO(#11424): Delete this whole class/file once all states have shipped to BQ-based
 ingest view materialization.
"""
import datetime
from typing import List, Optional

from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_args_generator_delegate import (
    IngestViewMaterializationArgsGeneratorDelegate,
    RegisteredMaterializationJob,
)
from recidiviz.ingest.direct.metadata.direct_ingest_file_metadata_manager import (
    DirectIngestIngestFileMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsIngestViewExportArgs
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestIngestFileMetadata,
)


class FileBasedMaterializationArgsGeneratorDelegate(
    IngestViewMaterializationArgsGeneratorDelegate[GcsfsIngestViewExportArgs]
):
    """Implementation of the IngestViewMaterializationArgsGeneratorDelegate for use on
    states using file-based ingest view materialization.
    """

    def __init__(
        self,
        *,
        output_bucket_name: str,
        ingest_file_metadata_manager: DirectIngestIngestFileMetadataManager,
    ):
        self.output_bucket_name = output_bucket_name
        self.ingest_file_metadata_manager = ingest_file_metadata_manager

    def get_registered_jobs_pending_completion(
        self,
    ) -> List[GcsfsIngestViewExportArgs]:
        metadata_pending_export = (
            self.ingest_file_metadata_manager.get_ingest_view_metadata_pending_export()
        )
        return self._export_args_from_metadata(metadata_pending_export)

    def build_new_args(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        lower_bound_datetime_exclusive: Optional[datetime.datetime],
    ) -> GcsfsIngestViewExportArgs:
        return GcsfsIngestViewExportArgs(
            ingest_view_name=ingest_view_name,
            upper_bound_datetime_prev=lower_bound_datetime_exclusive,
            upper_bound_datetime_to_export=upper_bound_datetime_inclusive,
            output_bucket_name=self.output_bucket_name,
        )

    def register_new_job(self, args: GcsfsIngestViewExportArgs) -> None:
        self.ingest_file_metadata_manager.register_ingest_file_export_job(args)

    def get_most_recent_registered_job(
        self, ingest_view_name: str
    ) -> Optional[RegisteredMaterializationJob]:
        metadata_row = self.ingest_file_metadata_manager.get_ingest_view_metadata_for_most_recent_valid_job(
            ingest_view_name
        )

        if not metadata_row:
            return None

        return RegisteredMaterializationJob(
            job_creation_time=metadata_row.job_creation_time,
            upper_bound_datetime_inclusive=metadata_row.datetimes_contained_upper_bound_inclusive,
            lower_bound_datetime_exclusive=metadata_row.datetimes_contained_lower_bound_exclusive,
        )

    def _export_args_from_metadata(
        self,
        metadata_list: List[DirectIngestIngestFileMetadata],
    ) -> List[GcsfsIngestViewExportArgs]:
        return [
            GcsfsIngestViewExportArgs(
                ingest_view_name=metadata.file_tag,
                upper_bound_datetime_prev=metadata.datetimes_contained_lower_bound_exclusive,
                upper_bound_datetime_to_export=metadata.datetimes_contained_upper_bound_inclusive,
                output_bucket_name=self.output_bucket_name,
            )
            for metadata in metadata_list
        ]
