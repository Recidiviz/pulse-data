# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains a fake implementation of DirectIngestViewMaterializationMetadataManager"""


import datetime
from typing import Dict, List, Optional

import pytz

from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
    IngestViewMaterializationSummary,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestViewMaterializationMetadata,
)


class NullDirectIngestViewMaterializationMetadataManager(
    DirectIngestViewMaterializationMetadataManager
):
    """Ignores writing metadata about pending and completed ingest view materialization
    jobs, and returns nothing on reads.
    """

    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance):
        self._region_code = region_code.upper()
        self._ingest_instance = ingest_instance

    @property
    def region_code(self) -> str:
        return self._region_code

    @property
    def ingest_instance(self) -> DirectIngestInstance:
        return self._ingest_instance

    def register_ingest_materialization_job(
        self, job_args: IngestViewMaterializationArgs
    ) -> DirectIngestViewMaterializationMetadata:
        return DirectIngestViewMaterializationMetadata(
            region_code=self.region_code,
            instance=self.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            job_creation_time=datetime.datetime.now(tz=pytz.UTC),
            materialization_time=None,
            is_invalidated=False,
        )

    def get_job_completion_time_for_args(
        self, job_args: IngestViewMaterializationArgs
    ) -> Optional[datetime.datetime]:
        return None

    def mark_ingest_view_materialized(
        self, job_args: IngestViewMaterializationArgs
    ) -> None:
        return None

    def get_most_recent_registered_job(
        self, ingest_view_name: str
    ) -> Optional[DirectIngestViewMaterializationMetadata]:
        return None

    def get_jobs_pending_completion(
        self,
    ) -> List[DirectIngestViewMaterializationMetadata]:
        return []

    def mark_instance_data_invalidated(self) -> None:
        return None

    def transfer_metadata_to_new_instance(
        self,
        new_instance_manager: "DirectIngestViewMaterializationMetadataManager",
    ) -> None:
        return None

    def get_instance_summaries(self) -> Dict[str, IngestViewMaterializationSummary]:
        return {}
