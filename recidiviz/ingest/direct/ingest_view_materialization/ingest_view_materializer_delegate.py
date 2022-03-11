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
"""Delegate object that isolates ingest view materialization logic that
is specific to the file-based implementation of materialization.
"""

import abc
import datetime
from typing import Generic, Optional

from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_args_generator_delegate import (
    IngestViewMaterializationArgsT,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
)


# TODO(#9717): Update the naming of this dataset to include "temp" or something to
#  more clearly indicate that it's meant for ephemeral storage.
def ingest_view_materialization_temp_dataset(
    view: DirectIngestPreProcessedIngestView, ingest_instance: DirectIngestInstance
) -> str:
    return f"{view.dataset_id}_{ingest_instance.value.lower()}"


class IngestViewMaterializerDelegate(Generic[IngestViewMaterializationArgsT]):
    """Delegate object that isolates ingest view materialization logic that
    is specific to the file-based implementation of materialization.

    TODO(#11424): Merge the BQ-based implementation of this interface back into the
      IngestViewMaterializer once all states have been migrated to use
      BQ-based materialization.
    """

    @abc.abstractmethod
    def get_job_completion_time_for_args(
        self, args: IngestViewMaterializationArgsT
    ) -> Optional[datetime.datetime]:
        """Returns the time that the materialization job for the given set of args was
        completed, or None if the job is still outstanding. May throw if these args
        were not properly registered in the database.
        """

    @abc.abstractmethod
    def prepare_for_job(self, args: IngestViewMaterializationArgsT) -> None:
        """Does work needed to prepare any databases for the materialization job
        represented by |args|. Note: this will be empty for the BQ-based implementation.
        """

    @abc.abstractmethod
    def materialize_query_results(
        self,
        args: IngestViewMaterializationArgsT,
        ingest_view: DirectIngestPreProcessedIngestView,
        query: str,
    ) -> None:
        """Materialized the results of |query| to the appropriate location."""

    @abc.abstractmethod
    def mark_job_complete(self, args: IngestViewMaterializationArgsT) -> None:
        """Marks a materialization job complete in the appropriate datastore."""
