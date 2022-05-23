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
from typing import Optional

from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestView,
)


class IngestViewMaterializerDelegate:
    """Delegate object that isolates ingest view materialization logic that
    is specific to the file-based implementation of materialization.

    TODO(#11424): Merge the BQ-based implementation of this interface back into the
      IngestViewMaterializer once all states have been migrated to use
      BQ-based materialization.
    """

    @abc.abstractmethod
    def temp_dataset_id(self) -> str:
        """Returns a name of the dataset that should be used to store intermediate /
        ephemeral results from any materialization job.
        """

    @abc.abstractmethod
    def get_job_completion_time_for_args(
        self, args: IngestViewMaterializationArgs
    ) -> Optional[datetime.datetime]:
        """Returns the time that the materialization job for the given set of args was
        completed, or None if the job is still outstanding. May throw if these args
        were not properly registered in the database.
        """

    @abc.abstractmethod
    def prepare_for_job(self, args: IngestViewMaterializationArgs) -> None:
        """Does work needed to prepare any databases for the materialization job
        represented by |args|. Note: this will be empty for the BQ-based implementation.
        """

    @abc.abstractmethod
    def materialize_query_results(
        self,
        args: IngestViewMaterializationArgs,
        ingest_view: DirectIngestPreProcessedIngestView,
        query: str,
    ) -> None:
        """Materialized the results of |query| to the appropriate location."""

    @abc.abstractmethod
    def mark_job_complete(self, args: IngestViewMaterializationArgs) -> None:
        """Marks a materialization job complete in the appropriate datastore."""
