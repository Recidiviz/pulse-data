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
"""Defines an interface for a class that handles logic for deciding which extract and
merge job should run next given the desired data import ordering.
"""
import abc
from datetime import datetime
from typing import Generic, List, Optional, TypeVar

import pytz

from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    InstanceIngestViewContents,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    ExtractAndMergeArgs,
    NewExtractAndMergeArgs,
)

ExtractAndMergeArgsT = TypeVar("ExtractAndMergeArgsT", bound=ExtractAndMergeArgs)


class ExtractAndMergeJobPrioritizer(Generic[ExtractAndMergeArgsT]):
    """Interface for a class that handles logic for deciding which extract and merge
    job should run next given the desired data import ordering.
    """

    @abc.abstractmethod
    def get_next_job_args(
        self,
    ) -> Optional[ExtractAndMergeArgsT]:
        """Returns a set of args defining the next chunk of data to process."""


class ExtractAndMergeJobPrioritizerImpl(
    ExtractAndMergeJobPrioritizer[NewExtractAndMergeArgs]
):
    """Implementation of the ExtractAndMergeJobPrioritizer interface, for use in
    regions where BQ-based ingest view materialization is enabled.
    """

    def __init__(
        self,
        ingest_view_contents: InstanceIngestViewContents,
        ingest_view_rank_list: List[str],
    ):
        self.ingest_view_contents = ingest_view_contents
        self.ingest_view_rank_list = ingest_view_rank_list

    def get_next_job_args(
        self,
    ) -> Optional[NewExtractAndMergeArgs]:

        highest_pri_batch = None

        # Iterate over ingest view names in the order that they should be processed
        # *within* a given date.
        for ingest_view_name in self.ingest_view_rank_list:
            next_batch_for_view = (
                self.ingest_view_contents.get_next_unprocessed_batch_info(
                    ingest_view_name
                )
            )
            if next_batch_for_view is None:
                continue

            if highest_pri_batch is None or (
                next_batch_for_view.upper_bound_datetime_inclusive.date()
                < highest_pri_batch.upper_bound_datetime_inclusive.date()
            ):
                highest_pri_batch = next_batch_for_view

        if not highest_pri_batch:
            return None

        return NewExtractAndMergeArgs(
            ingest_time=datetime.now(tz=pytz.UTC),
            ingest_instance=self.ingest_view_contents.ingest_instance,
            ingest_view_name=highest_pri_batch.ingest_view_name,
            upper_bound_datetime_inclusive=highest_pri_batch.upper_bound_datetime_inclusive,
            batch_number=highest_pri_batch.batch_number,
        )
