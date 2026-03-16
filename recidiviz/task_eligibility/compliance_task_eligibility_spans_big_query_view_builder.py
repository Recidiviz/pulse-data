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
from recidiviz.big_query.big_query_view_column import BigQueryViewColumn, Bool, Date
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.views.tasks.compliance_type import (
    CadenceType,
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
    basic_single_task_eligibility_span_schema,
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


def compliance_task_eligibility_span_schema() -> list[BigQueryViewColumn]:
    """Builds the schema for a compliance task eligibility span view."""
    return [
        *basic_single_task_eligibility_span_schema(),
        Date(
            name="due_date",
            description="The due date for the compliance task, extracted from the criteria reason fields.",
            mode="NULLABLE",
        ),
        Date(
            name="last_task_completed_date",
            description="The date when the compliance task was last completed, extracted from the criteria reason fields.",
            mode="NULLABLE",
        ),
        Bool(
            name="is_overdue",
            description="Boolean indicating whether the span falls after the due date for the compliance task.",
            mode="REQUIRED",
        ),
    ]


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
        cadence_type: CadenceType,
        due_date_field: str,
        last_task_completed_date_field: str,
        due_date_criteria_builder: Optional[TaskCriteriaBigQueryViewBuilder] = None,
        last_task_completed_date_criteria_builder: Optional[
            TaskCriteriaBigQueryViewBuilder
        ] = None,
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
            last_task_completed_date_criteria_builder is None
            and len(criteria_spans_view_builders) != 1
        ):
            raise ValueError(
                "Must specify last_task_completed_date_criteria_builder when providing multiple criteria_spans_view_builders."
            )

        if (
            due_date_criteria_builder is not None
            and due_date_criteria_builder not in criteria_spans_view_builders
        ):
            raise ValueError(
                f"The due_date_criteria_builder {due_date_criteria_builder.criteria_name} not found among criteria_spans_view_builders."
            )

        if (
            last_task_completed_date_criteria_builder is not None
            and last_task_completed_date_criteria_builder
            not in criteria_spans_view_builders
        ):
            raise ValueError(
                f"The last_task_completed_date_criteria_builder {last_task_completed_date_criteria_builder.criteria_name} not found among criteria_spans_view_builders."
            )

        self.compliance_type = compliance_type
        self.cadence_type = cadence_type
        self.due_date_field = due_date_field
        self.last_task_completed_date_field = last_task_completed_date_field
        self.candidate_population_view_builder = candidate_population_view_builder
        # Use the provided due_date_criteria_builder or default to the only criteria builder
        # in criteria_spans_view_builders
        if due_date_criteria_builder is None:
            due_date_criteria_builder = criteria_spans_view_builders[0]
        self.due_date_criteria_builder = due_date_criteria_builder

        # Use the provided last_task_completed_date_criteria_builder or default to the only criteria builder
        # in criteria_spans_view_builders
        if last_task_completed_date_criteria_builder is None:
            last_task_completed_date_criteria_builder = criteria_spans_view_builders[0]
        self.last_task_completed_date_criteria_builder = (
            last_task_completed_date_criteria_builder
        )

        self.view_query_template = self._wrap_query_template(
            base_tes_builder=self,
            cadence_type=self.cadence_type,
            due_date_field=self.due_date_field,
            last_task_completed_date_field=self.last_task_completed_date_field,
            due_date_criteria_builder=due_date_criteria_builder,
            last_task_completed_date_criteria_builder=last_task_completed_date_criteria_builder,
        )
        self.schema = compliance_task_eligibility_span_schema()

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
        cadence_type: CadenceType,
        due_date_field: str,
        due_date_criteria_builder: TaskCriteriaBigQueryViewBuilder,
        last_task_completed_date_field: str,
        last_task_completed_date_criteria_builder: TaskCriteriaBigQueryViewBuilder,
    ) -> str:
        """Wraps the base TES query with compliance fields (due_date,
        last_task_completed_date, is_overdue). For rolling cadences, splits
        spans at the due_date to compute is_overdue."""

        # If both reason fields correspond to the same criteria builder, we pass
        # consolidate `due_date_field` and `last_task_completed_date_field` into a
        # single dict that can be passed into extract_reasons_from_criteria.
        if last_task_completed_date_criteria_builder == due_date_criteria_builder:
            criteria_reason_fields_dict = {
                due_date_criteria_builder: [
                    due_date_field,
                    last_task_completed_date_field,
                ]
            }
        else:
            criteria_reason_fields_dict = {
                due_date_criteria_builder: [due_date_field],
                last_task_completed_date_criteria_builder: [
                    last_task_completed_date_field
                ],
            }

        nonnull_end_date = nonnull_end_date_clause("joined.end_date")

        base_ctes = f"""
WITH base_tes AS (
{fix_indent(base_tes_builder.view_query_template, indent_level=4)}
)
,
-- Extract the due date and last contact date from the specified criteria builder
reasons_extract AS (
{fix_indent(
    extract_reasons_from_criteria(
        criteria_reason_fields=criteria_reason_fields_dict,
        tes_view_builder=base_tes_builder,
        tes_table_name="base_tes",
        index_columns=["state_code", "person_id", "start_date"]
    ),
    indent_level=4
)}
)
,
-- Join due date information back to base task eligibility spans
joined AS (
    SELECT
        base_tes.state_code,
        base_tes.person_id,
        base_tes.start_date,
        base_tes.end_date,
        base_tes.is_eligible,
        base_tes.reasons_v2 AS reasons,
        base_tes.reasons_v2,
        base_tes.ineligible_criteria,
        reasons_extract.{due_date_field} AS due_date,
        reasons_extract.{last_task_completed_date_field} AS last_task_completed_date,
    FROM base_tes
    LEFT JOIN reasons_extract
    USING (state_code, person_id, start_date)
)"""

        if cadence_type == CadenceType.RECURRING_FIXED:
            return f"""{base_ctes}
SELECT
    *,
    FALSE AS is_overdue,
FROM joined
"""

        # For nonrecurring and recurring rolling cadences, split spans the day
        # after the due date to compute is_overdue. The due_date is inclusive
        # (i.e. the person is not yet overdue on the due_date itself; they
        # become overdue the following day).
        # Case 1: Span is not eligible, or due_date is NULL, or due_date >= end_date
        #         -> keep span as-is, is_overdue = FALSE
        # Case 2: due_date < start_date (entire span is overdue)
        #         -> keep span as-is, is_overdue = TRUE
        # Case 3: start_date <= due_date < end_date (split the day after due_date)
        #         -> [start_date, due_date + 1) with is_overdue = FALSE
        #         -> [due_date + 1, end_date) with is_overdue = TRUE
        return f"""{base_ctes}
,
-- Spans that are not overdue at all (cases 1 & first half of case 3)
not_overdue AS (
    SELECT
        state_code, person_id,
        start_date,
        -- For case 3: truncate end_date to the day after due_date
        CASE
            WHEN is_eligible AND due_date IS NOT NULL
                AND due_date >= start_date
                AND due_date < {nonnull_end_date}
            THEN DATE_ADD(due_date, INTERVAL 1 DAY)
            ELSE end_date
        END AS end_date,
        is_eligible, reasons, reasons_v2, ineligible_criteria,
        due_date, last_task_completed_date,
        -- Case 2: entire span is overdue
        CASE
            WHEN is_eligible AND due_date IS NOT NULL
                AND due_date < start_date
            THEN TRUE
            ELSE FALSE
        END AS is_overdue,
    FROM joined
)
,
-- Overdue portion for case 3 only (day after due_date splits the span)
overdue_splits AS (
    SELECT
        state_code, person_id,
        DATE_ADD(due_date, INTERVAL 1 DAY) AS start_date,
        end_date,
        is_eligible, reasons, reasons_v2, ineligible_criteria,
        due_date, last_task_completed_date,
        TRUE AS is_overdue,
    FROM joined
    WHERE
        is_eligible
        AND due_date IS NOT NULL
        AND due_date >= start_date
        AND DATE_ADD(due_date, INTERVAL 1 DAY) < {nonnull_end_date}
)
SELECT * FROM not_overdue
UNION ALL
SELECT * FROM overdue_splits
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

            # Add criteria from candidate_population_view_builder if available
            candidate_criteria = (
                self.candidate_population_view_builder.get_descendant_criteria()
            )
            all_descendants |= candidate_criteria
        return sorted(all_descendants, key=lambda c: c.criteria_name)
