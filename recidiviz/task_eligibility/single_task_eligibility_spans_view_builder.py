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
"""View builder that auto-generates the task eligibility spans view from component criteria,
task completion/decarceral transition events, and candidate population views, along with optional
almost-eligible conditions.
"""
import datetime
from textwrap import indent
from typing import List, Optional, Sequence

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.almost_eligible_spans_big_query_view_builder import (
    AlmostEligibleSpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_query_builder import (
    BasicSingleTaskEligibilitySpansBigQueryQueryBuilder,
    build_policy_date_check,
)
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria_condition import CriteriaCondition
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    TaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.string_formatting import fix_indent
from recidiviz.utils.types import assert_type

# CTE that can be formatted with completion-event-specific naming. Once formatted,
# the string will look like a standard view query template string:
#     candidate_population AS (
#         SELECT
#             * EXCEPT(completion_date),
#             completion_date AS end_date,
#         FROM `{project_id}.task_eligibility_completion_events_general.my_event_materialized`
#         WHERE state_code = "US_XX"
#     )
TASK_COMPLETION_EVENT_CTE = """task_completion_events AS (
    SELECT
        * EXCEPT(completion_event_date),
        completion_event_date AS end_date,
    FROM `{{project_id}}.{completion_events_dataset_id}.{completion_events_view_id}`
    WHERE state_code = "{state_code}"
)"""


# TODO(#46985): Rename this object and move it to a Workflows specific module
class SingleTaskEligibilitySpansBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that auto-generates the task eligibility spans view from component
    criteria, task completion/decarceral transition events, and candidate population views,
    along with (optional) almost-eligible conditions.
    """

    def __init__(
        self,
        *,
        state_code: StateCode,
        task_name: str,
        description: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
        completion_event_builder: TaskCompletionEventBigQueryViewBuilder,
        almost_eligible_condition: Optional[CriteriaCondition] = None,
        policy_start_date: datetime.date | None = None,
        policy_end_date: datetime.date | None = None,
    ) -> None:
        self._validate_builder_state_codes(
            state_code,
            completion_event_builder,
        )
        view_query_template = self._build_query_template(
            state_code=state_code,
            task_name=task_name,
            candidate_population_view_builder=candidate_population_view_builder,
            criteria_spans_view_builders=criteria_spans_view_builders,
            completion_event_builder=completion_event_builder,
            include_almost_eligible=bool(almost_eligible_condition),
            policy_start_date=policy_start_date,
            policy_end_date=policy_end_date,
        )
        view_address = self._view_address_for_task_name(state_code, task_name)

        super().__init__(
            dataset_id=view_address.dataset_id,
            view_id=view_address.table_id,
            description=description,
            view_query_template=view_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
        )
        self.state_code = state_code
        self.task_name = task_name
        self.completion_event_builder = completion_event_builder
        self.almost_eligible_condition = almost_eligible_condition
        self.candidate_population_view_builder = candidate_population_view_builder
        self.criteria_spans_view_builders = criteria_spans_view_builders
        self.policy_start_date = policy_start_date
        self.policy_end_date = policy_end_date

    @staticmethod
    def _view_address_for_task_name(
        state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=task_eligibility_spans_state_specific_dataset(state_code),
            table_id=task_name.lower(),
        )

    @classmethod
    def materialized_table_for_task_name(
        cls, *, state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        """Returns the table that you should query to get task eligibility spans for the
        given state's task.
        """
        view_address = cls._view_address_for_task_name(state_code, task_name)
        return assert_type(
            cls._build_materialized_address(
                dataset_id=view_address.dataset_id,
                view_id=view_address.table_id,
                should_materialize=True,
                materialized_address_override=None,
            ),
            BigQueryAddress,
        )

    @staticmethod
    def _build_query_template(
        *,
        state_code: StateCode,
        task_name: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
        completion_event_builder: TaskCompletionEventBigQueryViewBuilder,
        include_almost_eligible: bool,
        policy_start_date: datetime.date | None = None,
        policy_end_date: datetime.date | None = None,
    ) -> str:
        """Builds the view query template that combines span collapsing logic to generate
        task eligibility spans from component criteria, population spans, and optionally,
        almost eligible conditions.
        """
        if not completion_event_builder.materialized_address:
            raise ValueError(
                f"Expected materialized_address for view [{completion_event_builder.address}]"
            )

        basic_tes_query_builder = BasicSingleTaskEligibilitySpansBigQueryQueryBuilder(
            state_code=state_code,
            task_name=task_name,
            candidate_population_view_builder=candidate_population_view_builder,
            criteria_spans_view_builders=criteria_spans_view_builders,
            policy_start_date=policy_start_date,
            policy_end_date=policy_end_date,
        )

        # Build policy date check for is_almost_eligible.
        # We apply the same policy date check to is_almost_eligible as we do to is_eligible
        # so that spans outside the policy date range have is_almost_eligible=FALSE.
        policy_date_check = build_policy_date_check(policy_start_date, policy_end_date)

        completion_event_span_cte = StrictStringFormatter().format(
            TASK_COMPLETION_EVENT_CTE,
            state_code=state_code.value,
            completion_events_dataset_id=completion_event_builder.materialized_address.dataset_id,
            completion_events_view_id=completion_event_builder.materialized_address.table_id,
        )
        if include_almost_eligible:
            basic_eligibility_spans_clause = BasicSingleTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                state_code, task_name
            ).select_query_template()
            almost_eligible_spans_clause = (
                AlmostEligibleSpansBigQueryViewBuilder.materialized_table_for_task_name(
                    state_code, task_name
                ).select_query_template()
            )
            almost_eligible_cte = f"""
            WITH almost_eligible_spans AS (
                {almost_eligible_spans_clause}
            ),
            almost_eligible_and_eligible_intersection AS ({indent(create_intersection_spans(
                table_1_name="basic_eligibility_spans",
                table_2_name="almost_eligible_spans",
                index_columns=["state_code", "person_id"],
                use_left_join=True,
                table_1_columns=["is_eligible", "reasons", "reasons_v2", "ineligible_criteria"],
                table_2_columns=["is_almost_eligible"],
                table_1_end_date_field_name="end_date",
                table_2_end_date_field_name="end_date",
            ), " " * 8)})
            SELECT
                * EXCEPT(end_date_exclusive, is_almost_eligible),
                end_date_exclusive AS end_date,
                (IFNULL(is_almost_eligible, FALSE){policy_date_check}) AS is_almost_eligible
            FROM almost_eligible_and_eligible_intersection
            """
        else:
            basic_eligibility_spans_clause = basic_tes_query_builder.query_template
            almost_eligible_cte = (
                "SELECT *, FALSE AS is_almost_eligible FROM basic_eligibility_spans"
            )

        eligibility_sub_spans_with_almost_eligible_cte = f"""
        eligibility_sub_spans_with_almost_eligible AS (
            {almost_eligible_cte}
        )
        """

        return f"""
WITH
basic_eligibility_spans AS (
{fix_indent(basic_eligibility_spans_clause, indent_level=4)}
),
{completion_event_span_cte},
{eligibility_sub_spans_with_almost_eligible_cte},
eligibility_sub_spans_with_id AS
/*
Create an eligibility_span_id to help collapse adjacent spans with the same
eligibility type. The id is incremented if there is a date gap or if the eligibility
type switches.
*/
(
    SELECT
        *,
        SUM(
            IF(is_date_gap OR is_eligibility_change OR is_almost_eligibility_change
               OR is_ineligible_criteria_change OR is_reasons_change,
               1,
               0
            )
        ) OVER (PARTITION BY state_code, person_id ORDER BY start_date ASC) AS task_eligibility_span_id,
    FROM (
        SELECT
        * EXCEPT(ineligible_reasons_str, full_reasons_str),
        COALESCE(start_date != LAG(end_date) OVER state_person_window, TRUE) AS is_date_gap,
        COALESCE(is_eligible != LAG(is_eligible) OVER state_person_window, TRUE) AS is_eligibility_change,
        COALESCE(is_almost_eligible != LAG(is_almost_eligible) OVER state_person_window, TRUE) AS is_almost_eligibility_change,
        COALESCE(ineligible_reasons_str != LAG(ineligible_reasons_str) OVER state_person_window, TRUE)
            AS is_ineligible_criteria_change,
        COALESCE(full_reasons_str != LAG(full_reasons_str) OVER state_person_window, TRUE) AS is_reasons_change
        FROM (
            SELECT 
                *,
                COALESCE(ARRAY_TO_STRING(ineligible_criteria, ","), "") AS ineligible_reasons_str,
                #TODO(#25910): Sort all arrays in reasons json to avoid non-determinism in sub-sessionization  
                TO_JSON_STRING(reasons) AS full_reasons_str
            FROM eligibility_sub_spans_with_almost_eligible
        )
        WINDOW state_person_window AS (PARTITION BY state_code, person_id
            ORDER BY start_date ASC)
    )
)
SELECT
    state_code,
    person_id,
    "{task_name}" AS task_name,
    task_eligibility_span_id,
    MIN(start_date) OVER (PARTITION BY state_code, person_id, task_eligibility_span_id) AS start_date,
    {revert_nonnull_end_date_clause("eligibility_sub_spans_with_id.end_date")} AS end_date,
    is_eligible,
    is_almost_eligible,
    reasons,
    reasons_v2,
    ineligible_criteria,
    -- Infer the task eligibility span end reason
    CASE
        -- No end reason if the span is still open
        WHEN {revert_nonnull_end_date_clause("eligibility_sub_spans_with_id.end_date")} IS NULL THEN NULL
        -- Mark completed if there is a completion event on the span end date
        WHEN task_completion_events.end_date IS NOT NULL THEN "TASK_COMPLETED"
        -- Left candidate population if there is a gap between spans
        WHEN end_date < {nonnull_end_date_clause("LEAD(start_date) OVER w")} THEN "LEFT_CANDIDATE_POPULATION"
        WHEN NOT is_eligible THEN
            CASE
                -- Became eligible
                WHEN LEAD(is_eligible) OVER w THEN "BECAME_ELIGIBLE"
                -- Became almost eligible
                WHEN NOT is_almost_eligible AND LEAD(is_almost_eligible) OVER w THEN "BECAME_ALMOST_ELIGIBLE"
                WHEN LEAD(is_ineligible_criteria_change) over w THEN "INELIGIBLE_CRITERIA_CHANGED"
                WHEN LEAD(is_reasons_change) OVER w THEN "INELIGIBLE_REASONS_CHANGED"
                ELSE "UNKNOWN_NOT_ELIGIBLE"
            END
        WHEN is_eligible THEN
            CASE
                -- Became almost eligible
                WHEN LEAD(is_almost_eligible) OVER w THEN "BECAME_ALMOST_ELIGIBLE"
                -- Became ineligible
                WHEN NOT LEAD(is_eligible) OVER w THEN "BECAME_INELIGIBLE"
                WHEN LEAD(is_reasons_change) OVER w THEN "ELIGIBLE_REASONS_CHANGED"
                ELSE "UNKNOWN_IS_ELIGIBLE"
            END
        -- Edge case that should never occur
        ELSE "UNKNOWN" 
    END AS end_reason,
FROM eligibility_sub_spans_with_id
LEFT JOIN task_completion_events
    USING (state_code, person_id, end_date)
-- Pick the row with the latest end date for each task_eligibility_span_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, task_eligibility_span_id
    ORDER BY {nonnull_end_date_clause("end_date")} DESC
) = 1
WINDOW w AS (PARTITION BY state_code, person_id ORDER BY start_date ASC)
"""

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

    @staticmethod
    def _validate_builder_state_codes(
        task_state_code: StateCode,
        completion_event_builder: TaskCompletionEventBigQueryViewBuilder,
    ) -> None:
        """Validates that the state code for this task eligibility view matches the
        state code on the task completion event (if one
        exists).
        """
        if isinstance(
            completion_event_builder,
            StateSpecificTaskCompletionEventBigQueryViewBuilder,
        ):
            if completion_event_builder.state_code != task_state_code:
                raise ValueError(
                    f"Found completion event "
                    f"[{completion_event_builder.completion_event_type.name}] with "
                    f"state_code [{completion_event_builder.state_code}] which"
                    f"does not match the task state_code [{task_state_code}]"
                )
