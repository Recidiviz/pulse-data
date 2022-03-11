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
"""Delegate object that isolates ingest view materialization args generation logic that
is specific to the file-based implementation of materialization.
"""
import abc
import datetime
from typing import Generic, List, Optional, TypeVar

import attr

from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs

IngestViewMaterializationArgsT = TypeVar(
    "IngestViewMaterializationArgsT", bound=IngestViewMaterializationArgs
)


@attr.s(frozen=True)
class RegisteredMaterializationJob:
    # Time the materialization job is first scheduled for these time bounds
    job_creation_time: datetime.datetime = attr.ib()

    # Date bounds for this materialization job
    upper_bound_datetime_inclusive: datetime.datetime = attr.ib()
    lower_bound_datetime_exclusive: Optional[datetime.datetime] = attr.ib()


class IngestViewMaterializationArgsGeneratorDelegate(
    Generic[IngestViewMaterializationArgsT]
):
    """Delegate object that isolates ingest view materialization args generation logic
    that is specific to the file-based implementation of materialization.

    TODO(#11424): Merge the BQ-based implementation of this interface back into the
      IngestViewMaterializationArgsGenerator once all states have been migrated to use
      BQ-based materialization.
    """

    @abc.abstractmethod
    def get_registered_jobs_pending_completion(
        self,
    ) -> List[IngestViewMaterializationArgsT]:
        """Returns a lists of materialization jobs that have been registered via
        |register_new_job| but have not completed.
        """

    @abc.abstractmethod
    def build_new_args(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        lower_bound_datetime_exclusive: Optional[datetime.datetime],
    ) -> IngestViewMaterializationArgsT:
        """Builds a new set of |IngestViewMaterializationArgs| of the appropriate type,
        given the input information about the materialization job.
        """

    @abc.abstractmethod
    def register_new_job(self, args: IngestViewMaterializationArgsT) -> None:
        """Register the materialization job represented by these |args| in the
        appropriate datastore.
        """

    @abc.abstractmethod
    def get_most_recent_registered_job(
        self, ingest_view_name: str
    ) -> Optional[RegisteredMaterializationJob]:
        """Returns the materialization job with the highest
        upper_bound_datetime_inclusive value that has been registered for this ingest
        view via |register_new_job|. May return jobs that have not yet completed.
        """
