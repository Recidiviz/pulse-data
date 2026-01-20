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
"""View builder that auto-generates task spans with the `is_eligible` flag
from component criteria and candidate population views.
"""
import datetime
from typing import List

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_query_builder import (
    BasicSingleTaskEligibilitySpansBigQueryQueryBuilder,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    TaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)


class BasicSingleTaskEligibilitySpansBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that auto-generates task spans with the `is_eligible` flag
    from component criteria and candidate population views.
    """

    def __init__(
        self,
        *,
        state_code: StateCode,
        task_name: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
        policy_start_date: datetime.date | None = None,
        policy_end_date: datetime.date | None = None,
    ) -> None:

        query_builder = BasicSingleTaskEligibilitySpansBigQueryQueryBuilder(
            state_code=state_code,
            task_name=task_name,
            candidate_population_view_builder=candidate_population_view_builder,
            criteria_spans_view_builders=criteria_spans_view_builders,
            policy_start_date=policy_start_date,
            policy_end_date=policy_end_date,
        )
        address = self._address_for_task_name(state_code, task_name)
        super().__init__(
            dataset_id=address.dataset_id,
            view_id=address.table_id,
            description="Task eligibility view collapsing criteria components with the candidate population to compute the `is_eligible` flag",
            view_query_template=query_builder.query_template,
            should_materialize=True,
            materialized_address_override=self.materialized_table_for_task_name(
                state_code, task_name
            ),
        )
        self.state_code = state_code
        self.task_name = task_name
        self.criteria_spans_view_builders = criteria_spans_view_builders
        self.query_builder = query_builder

    @classmethod
    def _address_for_task_name(
        cls, state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=task_eligibility_spans_state_specific_dataset(state_code),
            table_id=f"{task_name.lower()}__basic",
        )

    @classmethod
    def materialized_table_for_task_name(
        cls, state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        view_address = cls._address_for_task_name(state_code, task_name)
        return cls.build_standard_materialized_address(
            dataset_id=view_address.dataset_id, view_id=view_address.table_id
        )
