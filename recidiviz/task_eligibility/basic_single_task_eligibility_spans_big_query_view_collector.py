# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
BasicSingleTaskEligibilitySpansBigQueryViewBuilder derived from a
SingleTaskEligibilitySpansBigQueryViewBuilder.
"""
from collections import defaultdict
from typing import Dict, List

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)


class BasicSingleTaskEligibilitySpansBigQueryViewCollector(
    BigQueryViewCollector[BasicSingleTaskEligibilitySpansBigQueryViewBuilder]
):
    """A class that can be used to collect view builders created from
    the BasicSingleTaskEligibilitySpansBigQueryViewBuilder when the separate view
    builder is required.
    """

    def __init__(
        self,
        single_task_view_builders: list[SingleTaskEligibilitySpansBigQueryViewBuilder],
    ) -> None:
        self.basic_task_eligibility_view_builders = (
            BasicSingleTaskEligibilitySpansBigQueryViewCollector._init_view_builders(
                single_task_view_builders
            )
        )

    @staticmethod
    def _init_view_builders(
        single_task_view_builders: list[SingleTaskEligibilitySpansBigQueryViewBuilder],
    ) -> List[BasicSingleTaskEligibilitySpansBigQueryViewBuilder]:
        """Returns a list of all SimpleBigQueryViewBuilder created
        to supplement single task eligibility spans across all states.
        """
        candidate_criteria_eligibility_view_builders = []
        for vb in single_task_view_builders:
            # If the TES view builder has an almost eligible condition, we need to break
            # out the basic eligibility spans logic into a separate view so it can be
            # referenced by the view that produces almost_eligible info.
            if vb.almost_eligible_condition:
                candidate_criteria_eligibility_view_builders.append(
                    BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                        state_code=vb.state_code,
                        task_name=vb.task_name,
                        candidate_population_view_builder=vb.candidate_population_view_builder,
                        criteria_spans_view_builders=vb.criteria_spans_view_builders,
                    )
                )

        return candidate_criteria_eligibility_view_builders

    def collect_view_builders(
        self,
    ) -> List[BasicSingleTaskEligibilitySpansBigQueryViewBuilder]:
        """Returns a list of all SimpleBigQueryViewBuilder created
        to supplement single task eligibility spans across all states.
        """
        return self.basic_task_eligibility_view_builders

    def collect_view_builders_by_state(
        self,
    ) -> Dict[StateCode, List[BasicSingleTaskEligibilitySpansBigQueryViewBuilder]]:
        """Returns a map of state to the SimpleBigQueryViewBuilder
        defined for that state.
        """
        view_builders_by_state = defaultdict(list)
        for vb in self.collect_view_builders():
            view_builders_by_state[vb.state_code].append(vb)
        return view_builders_by_state
