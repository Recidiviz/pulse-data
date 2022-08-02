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
SingleTaskEligibilitySpansBigQueryViewBuilder.
"""
from collections import defaultdict
from typing import Dict, List

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility import eligibility_spans
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)


# TODO(#14309): Write tests for this class
class SingleTaskEligibilityBigQueryViewCollector(
    BigQueryViewCollector[SingleTaskEligibilitySpansBigQueryViewBuilder]
):
    """A class that can be used to collect view builders of type
    SingleTaskEligibilitySpansBigQueryViewBuilder.
    """

    def collect_view_builders(
        self,
    ) -> List[SingleTaskEligibilitySpansBigQueryViewBuilder]:
        """Returns a list of all SingleTaskEligibilitySpansBigQueryViewBuilder defined
        across all states.
        """
        view_builders = []
        for state_tasks_module in self.get_submodules(
            eligibility_spans, submodule_name_prefix_filter=None
        ):
            view_builders.extend(
                self.collect_view_builders_in_module(
                    SingleTaskEligibilitySpansBigQueryViewBuilder,
                    state_tasks_module,
                )
            )

        return view_builders

    def collect_view_builders_by_state(
        self,
    ) -> Dict[StateCode, List[SingleTaskEligibilitySpansBigQueryViewBuilder]]:
        """Returns a map of state to the SingleTaskEligibilitySpansBigQueryViewBuilders
        defined for that state.
        """
        view_builders_by_state = defaultdict(list)
        for vb in self.collect_view_builders():
            view_builders_by_state[vb.state_code].append(vb)
        return view_builders_by_state
