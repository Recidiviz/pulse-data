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

from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer_delegate import (
    IngestViewMaterializerDelegate,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    InstanceIngestViewContents,
)
from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
)


class BQBasedMaterializerDelegate(
    IngestViewMaterializerDelegate[IngestViewMaterializationArgs]
):
    """Implementation of the IngestViewMaterializerDelegate for use on
    states using bQ-based ingest view materialization.
    """

    def __init__(
        self,
        metadata_manager: DirectIngestViewMaterializationMetadataManager,
        ingest_view_contents: InstanceIngestViewContents,
    ):
        self.metadata_manager = metadata_manager
        self.ingest_view_contents = ingest_view_contents

    def temp_dataset_id(self) -> str:
        return self.ingest_view_contents.temp_results_dataset

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
        self.ingest_view_contents.save_query_results(
            ingest_view_name=args.ingest_view_name,
            upper_bound_datetime_inclusive=args.upper_bound_datetime_inclusive,
            lower_bound_datetime_exclusive=args.lower_bound_datetime_exclusive,
            query_str=query,
            order_by_cols_str=ingest_view.order_by_cols,
        )

    def mark_job_complete(self, args: IngestViewMaterializationArgs) -> None:
        self.metadata_manager.mark_ingest_view_materialized(args)
