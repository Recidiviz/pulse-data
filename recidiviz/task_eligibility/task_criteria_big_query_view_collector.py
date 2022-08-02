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
TaskCriteriaBigQueryViewBuilder.
"""
from typing import List

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.task_eligibility.criteria import general as general_criteria_module
from recidiviz.task_eligibility.criteria import (
    state_specific as state_specific_criteria_module,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)


class TaskCriteriaBigQueryViewCollector(
    BigQueryViewCollector[TaskCriteriaBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of type
    TaskCriteriaBigQueryViewBuilder.
    """

    def collect_view_builders(self) -> List[TaskCriteriaBigQueryViewBuilder]:
        """Returns a list of all defined TaskCriteriaBigQueryViewBuilder, both the
        StateAgnosticTaskCriteriaBigQueryViewBuilders which contain general queries that
        can be used for any state and the StateSpecificTaskCriteriaBigQueryViewBuilders
        which apply logic specific to a particular state.
        """
        view_builders = self.collect_view_builders_in_module(
            StateAgnosticTaskCriteriaBigQueryViewBuilder, general_criteria_module
        )

        for state_criteria_module in self.get_submodules(
            state_specific_criteria_module, submodule_name_prefix_filter=None
        ):
            view_builders.extend(
                self.collect_view_builders_in_module(
                    StateSpecificTaskCriteriaBigQueryViewBuilder, state_criteria_module
                )
            )

        return view_builders
