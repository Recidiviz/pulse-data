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
"""Defines a class that can be used to collect view builders of type
TaskCompletionEventBigQueryViewBuilder.
"""
from typing import List

from recidiviz.big_query.big_query_view_collector import (
    BigQueryViewCollector,
    filename_matches_view_id_validator,
)
from recidiviz.task_eligibility import completion_events
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventBigQueryViewBuilder,
)


class TaskCompletionEventBigQueryViewCollector(
    BigQueryViewCollector[TaskCompletionEventBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of type
    TaskCompletionEventBigQueryViewBuilder.
    """

    def collect_view_builders(self) -> List[TaskCompletionEventBigQueryViewBuilder]:
        """Returns a list of all defined TaskCriteriaBigQueryViewBuilder."""
        return self.collect_view_builders_in_module(
            builder_type=TaskCompletionEventBigQueryViewBuilder,
            view_dir_module=completion_events,
            validate_builder_fn=filename_matches_view_id_validator,
        )
