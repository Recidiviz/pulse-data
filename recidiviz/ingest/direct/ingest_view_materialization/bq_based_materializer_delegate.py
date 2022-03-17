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
"""Implementation of the IngestViewMaterializerDelegate for use on
states using BQ-based ingest view materialization.
"""

import datetime
from typing import Optional

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer_delegate import (
    IngestViewMaterializerDelegate,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
)


# TODO(#9717): Write tests for the IngestViewMaterializer that use this version of the
#  delegate once materialize_query_results is implemented.
class BQBasedMaterializerDelegate(
    IngestViewMaterializerDelegate[IngestViewMaterializationArgs]
):
    """Implementation of the IngestViewMaterializerDelegate for use on
    states using bQ-based ingest view materialization.
    """

    def __init__(
        self,
        metadata_manager: DirectIngestViewMaterializationMetadataManager,
        big_query_client: BigQueryClient,
    ):
        self.metadata_manager = metadata_manager
        self.big_query_client = big_query_client

    def get_job_completion_time_for_args(
        self, args: IngestViewMaterializationArgs
    ) -> Optional[datetime.datetime]:
        return self.metadata_manager.get_job_completion_time_for_args(args)

    def prepare_for_job(self, args: IngestViewMaterializationArgs) -> None:
        # No work to do
        pass

    def materialize_query_results(
        self,
        args: IngestViewMaterializationArgs,
        ingest_view: DirectIngestPreProcessedIngestView,
        query: str,
    ) -> None:
        raise NotImplementedError(
            "TODO(#9717): BQ-based materialization not yet implemented."
        )

    def mark_job_complete(self, args: IngestViewMaterializationArgs) -> None:
        self.metadata_manager.mark_ingest_view_materialized(args)
