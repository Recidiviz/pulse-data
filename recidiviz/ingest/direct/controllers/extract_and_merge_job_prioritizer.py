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
from typing import Generic, List, Optional, TypeVar

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.types.cloud_task_args import ExtractAndMergeArgs

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


# TODO(#9717): Write tests for this class.
class ExtractAndMergeJobPrioritizerImpl(
    ExtractAndMergeJobPrioritizer[ExtractAndMergeArgs]
):
    """Implementation of the ExtractAndMergeJobPrioritizer interface, for use in
    regions where BQ-based ingest view materialization is enabled.
    """

    def __init__(
        self,
        bq_client: BigQueryClient,
        ingest_view_rank_list: List[str],
    ):
        self.bq_client = bq_client
        self.ingest_view_rank_list = ingest_view_rank_list

    def get_next_job_args(
        self,
    ) -> Optional[ExtractAndMergeArgs]:
        raise NotImplementedError(
            "TODO(#9717): BQ-based job prioritization not yet implemented."
        )
