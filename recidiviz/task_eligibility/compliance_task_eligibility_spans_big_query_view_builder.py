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
"""View builder that adds compliance and due date information to the 
BasicSingleTaskEligibilitySpansBigQueryViewBuilder. Captures information about
a compliance-oriented task (e.g., contacts, assessments).
"""
from typing import List, Optional, Sequence

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.tasks.compliance_type import ComplianceType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.dataset_config import (
    compliance_task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    TaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.reasons_field_query_fragments import (
    extract_reasons_from_criteria,
)
from recidiviz.utils.string_formatting import fix_indent


class ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder
):
    """View builder that adds compliance and due date information to the
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder. Captures information about
    a compliance-oriented task (e.g., contacts, assessments).
    """

    def __init__(
        self,
        *,
        state_code: StateCode,
        task_name: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
        compliance_type: ComplianceType,
        due_date_field: str,
        due_date_criteria_builder: Optional[TaskCriteriaBigQueryViewBuilder] = None,
    ) -> None:
        super().__init__(
            state_code=state_code,
            task_name=task_name,
            candidate_population_view_builder=candidate_population_view_builder,
            criteria_spans_view_builders=criteria_spans_view_builders,
        )

        if due_date_criteria_builder is None and len(criteria_spans_view_builders) != 1:
            raise ValueError(
                "Must specify due_date_criteria_builder when providing multiple criteria_spans_view_builders."
            )

        if (
            due_date_criteria_builder is not None
            and due_date_criteria_builder not in criteria_spans_view_builders
        ):
            raise ValueError(
                f"The due_date_criteria_builder {due_date_criteria_builder.criteria_name} not found among criteria_spans_view_builders."
            )

        self.compliance_type = compliance_type
        self.due_date_field = due_date_field

        # Use the provided due_date_criteria_builder or default to the only criteria builder
        # in criteria_spans_view_builders
        if due_date_criteria_builder is None:
            due_date_criteria_builder = criteria_spans_view_builders[0]
        self.due_date_criteria_builder = due_date_criteria_builder

        self.view_query_template = self._wrap_query_template(
            base_tes_builder=self,
            due_date_field=self.due_date_field,
            due_date_criteria_builder=due_date_criteria_builder,
        )

    @classmethod
    def _address_for_task_name(
        cls, state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=compliance_task_eligibility_spans_state_specific_dataset(
                state_code
            ),
            table_id=task_name.lower(),
        )

    @classmethod
    def _wrap_query_template(
        cls,
        base_tes_builder: BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
        due_date_field: str,
        due_date_criteria_builder: TaskCriteriaBigQueryViewBuilder,
    ) -> str:
        return f"""
WITH base_tes AS (
{fix_indent(base_tes_builder.view_query_template, indent_level=4)}
)
,
-- Extract the due date from the specified criteria builder
due_date_extract AS (
{fix_indent(
    extract_reasons_from_criteria(
        criteria_reason_fields={due_date_criteria_builder: [due_date_field]},
        tes_view_builder=base_tes_builder,
        tes_table_name="base_tes",
        index_columns=["state_code", "person_id", "start_date"]
    ), 
    indent_level=4
)}
)
-- Join due date information back to base task eligibility spans
SELECT
    base_tes.state_code,
    base_tes.person_id,
    base_tes.start_date,
    base_tes.end_date,
    base_tes.is_eligible,
    base_tes.reasons_v2 AS reasons,
    base_tes.reasons_v2,
    base_tes.ineligible_criteria,
    due_date_extract.{due_date_field} AS due_date,
FROM base_tes
LEFT JOIN due_date_extract
USING (state_code, person_id, start_date)
"""

    @classmethod
    def materialized_table_for_task_name(
        cls, state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        view_address = cls._address_for_task_name(state_code, task_name)
        return cls.build_standard_materialized_address(
            dataset_id=view_address.dataset_id, view_id=view_address.table_id
        )

    def all_descendant_criteria_builders(
        self,
    ) -> Sequence[TaskCriteriaBigQueryViewBuilder]:
        all_descendants: set[TaskCriteriaBigQueryViewBuilder] = set()
        for criteria_builder in self.criteria_spans_view_builders:
            all_descendants.add(criteria_builder)
            all_descendants |= criteria_builder.get_descendant_criteria()

        return sorted(all_descendants, key=lambda c: c.criteria_name)
