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
AlmostEligibleSpansBigQueryViewCollector.
"""
from collections import defaultdict
from typing import Dict, List

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.almost_eligible_spans_big_query_view_builder import (
    AlmostEligibleSpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.single_task_eligibility_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)


class AlmostEligibleSpansBigQueryViewCollector(
    BigQueryViewCollector[AlmostEligibleSpansBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of type
    AlmostEligibleSpansBigQueryViewBuilder.
    """

    def __init__(
        self,
        basic_single_task_view_builders: list[
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder
        ],
        single_task_view_builders: list[SingleTaskEligibilitySpansBigQueryViewBuilder],
    ) -> None:
        self.almost_eligible_spans_view_builders = (
            AlmostEligibleSpansBigQueryViewCollector._init_view_builders(
                basic_single_task_view_builders,
                single_task_view_builders,
            )
        )

    @staticmethod
    def _init_view_builders(
        basic_single_task_view_builders: list[
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder
        ],
        single_task_view_builders: list[SingleTaskEligibilitySpansBigQueryViewBuilder],
    ) -> List[AlmostEligibleSpansBigQueryViewBuilder]:
        """Returns a list of all AlmostEligibleSpansBigQueryViewBuilder created
        to supplement single task eligibility spans across all states.
        """
        basic_tes_vbs_by_state_and_task_name = {
            (vb.state_code, vb.task_name): vb for vb in basic_single_task_view_builders
        }

        almost_eligible_view_builders = []
        for tes_vb in single_task_view_builders:
            if tes_vb.almost_eligible_condition:
                # If there is an almost_eligible_condition, we always expect a basic TES
                # builder to exist.
                basic_tes_vb = basic_tes_vbs_by_state_and_task_name[
                    (tes_vb.state_code, tes_vb.task_name)
                ]
                almost_eligible_view_builders.append(
                    AlmostEligibleSpansBigQueryViewBuilder(
                        basic_tes_view_builder=basic_tes_vb,
                        almost_eligible_condition=tes_vb.almost_eligible_condition,
                    )
                )
        return almost_eligible_view_builders

    def collect_view_builders(
        self,
    ) -> List[AlmostEligibleSpansBigQueryViewBuilder]:
        return self.almost_eligible_spans_view_builders

    def collect_view_builders_by_state(
        self,
    ) -> Dict[StateCode, List[AlmostEligibleSpansBigQueryViewBuilder]]:
        """Returns a map of state to the AlmostEligibleSpansBigQueryViewBuilder
        defined for that state.
        """
        view_builders_by_state = defaultdict(list)
        for vb in self.collect_view_builders():
            view_builders_by_state[vb.state_code].append(vb)
        return view_builders_by_state
