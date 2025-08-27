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
from recidiviz.task_eligibility.completion_events import (
    general as general_completion_events_module,
)
from recidiviz.task_eligibility.completion_events import (
    state_specific as state_specific_completion_event_module,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventBigQueryViewBuilder,
)


# TODO(#46985): Move this collector to a Workflows specific module
class TaskCompletionEventBigQueryViewCollector(
    BigQueryViewCollector[TaskCompletionEventBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of type
    TaskCompletionEventBigQueryViewBuilder.
    """

    def collect_view_builders(self) -> List[TaskCompletionEventBigQueryViewBuilder]:
        """Returns a list of all defined TaskCompletionEventBigQueryViewBuilder, both the
        StateAgnosticTaskCompletionEventBigQueryViewBuilders which contain general queries that
        can be used for any state and the StateSpecificTaskCompletionEventBigQueryViewBuilders
        which apply logic specific to a particular state.
        """
        view_builders = self.collect_view_builders_in_module(
            builder_type=StateAgnosticTaskCompletionEventBigQueryViewBuilder,
            view_dir_module=general_completion_events_module,
            validate_builder_fn=filename_matches_view_id_validator,
        )

        for state_completion_event_module in self.get_submodules(
            state_specific_completion_event_module, submodule_name_prefix_filter=None
        ):
            view_builders.extend(
                self.collect_view_builders_in_module(
                    builder_type=StateSpecificTaskCompletionEventBigQueryViewBuilder,
                    view_dir_module=state_completion_event_module,
                    validate_builder_fn=filename_matches_view_id_validator,
                )
            )

        return view_builders
