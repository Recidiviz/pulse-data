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
"""Query builder that generates the eligibility query (not a view) for task
eligibility spans from component criteria and candidate population views.
"""
from typing import List

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
    TaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.string import StrictStringFormatter

# Query fragment that can be formatted with criteria-specific naming. Once formatted,
# the string will look like a standard view query template string:
#         SELECT
#             *,
#             "my_criteria" AS criteria_name
#         FROM `{project_id}.task_eligibility_criteria_xxx.my_criteria_materialized`
#         WHERE state_code = "US_XX"
#     )
STATE_SPECIFIC_CRITERIA_FRAGMENT = """
    SELECT
        *,
        "{criteria_name}" AS criteria_name
    FROM `{{project_id}}.{criteria_dataset_id}.{criteria_view_id}`
    WHERE state_code = "{state_code}"
"""

# CTE that can be formatted with population-specific naming. Once formatted,
# the string will look like a standard view query template string:
#     candidate_population AS (
#         SELECT
#             *,
#         FROM `{project_id}.task_eligibility_candidates_general.my_population_materialized`
#         WHERE state_code = "US_XX"
#     )
STATE_SPECIFIC_POPULATION_CTE = """candidate_population AS (
    SELECT
        *
    FROM `{{project_id}}.{population_dataset_id}.{population_view_id}`
    WHERE state_code = "{state_code}"
)"""

CRITERIA_INFO_STRUCT_FRAGMENT = """STRUCT(
            "{state_code}" AS state_code,
            "{criteria_name}" AS criteria_name,
            {meets_criteria_default} AS meets_criteria_default
        )"""


class BasicSingleTaskEligibilitySpansBigQueryQueryBuilder:
    """Query builder that generates the eligibility query for task eligibility spans
    from component criteria and candidate population views.
    """

    def __init__(
        self,
        *,
        state_code: StateCode,
        task_name: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> None:
        self._validate_builder_state_codes(
            state_code,
            candidate_population_view_builder,
            criteria_spans_view_builders,
        )
        self.query_template = self._build_query_template(
            state_code=state_code,
            candidate_population_view_builder=candidate_population_view_builder,
            criteria_spans_view_builders=criteria_spans_view_builders,
        )
        self.state_code = state_code
        self.task_name = task_name
        self.candidate_population_view_builder = candidate_population_view_builder
        self.criteria_spans_view_builders = criteria_spans_view_builders

    @staticmethod
    def _build_query_template(
        *,
        state_code: StateCode,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> str:
        """Builds the view query template that does span collapsing logic to generate
        task eligibility spans from component criteria and population spans views.
        """
        if not candidate_population_view_builder.materialized_address:
            raise ValueError(
                f"Expected materialized_address for view [{candidate_population_view_builder.address}]"
            )
        population_span_cte = StrictStringFormatter().format(
            STATE_SPECIFIC_POPULATION_CTE,
            state_code=state_code.value,
            population_dataset_id=candidate_population_view_builder.materialized_address.dataset_id,
            population_view_id=candidate_population_view_builder.materialized_address.table_id,
        )
        criteria_info_structs = []
        criteria_span_ctes = []

        for criteria_view_builder in criteria_spans_view_builders:
            if not criteria_view_builder.materialized_address:
                raise ValueError(
                    f"Expected materialized_address for view [{criteria_view_builder.address}]"
                )

            criteria_query_fragment = STATE_SPECIFIC_CRITERIA_FRAGMENT

            criteria_span_ctes.append(
                StrictStringFormatter().format(
                    criteria_query_fragment,
                    state_code=state_code.value,
                    criteria_name=criteria_view_builder.criteria_name,
                    criteria_dataset_id=criteria_view_builder.materialized_address.dataset_id,
                    criteria_view_id=criteria_view_builder.materialized_address.table_id,
                )
            )
            criteria_info_structs.append(
                StrictStringFormatter().format(
                    CRITERIA_INFO_STRUCT_FRAGMENT,
                    state_code=state_code.value,
                    criteria_name=criteria_view_builder.criteria_name,
                    meets_criteria_default=str(
                        criteria_view_builder.meets_criteria_default
                    ).upper(),
                )
            )

        all_criteria_spans_cte_str = (
            f"""criteria_spans AS ({"UNION ALL".join(criteria_span_ctes)})"""
        )
        criteria_info_structs_str = ",\n\t\t".join(criteria_info_structs)

        return f"""
WITH
all_criteria AS (
    SELECT state_code, criteria_name, meets_criteria_default,
    FROM UNNEST([
        {criteria_info_structs_str}
    ])
),
{all_criteria_spans_cte_str},
{population_span_cte},
combined_cte AS (
    SELECT * EXCEPT(meets_criteria, reason, reason_v2) FROM criteria_spans
    UNION ALL
    -- Add an indicator for the population spans so the population sub-sessions can be
    -- used as the base spans in the span collapsing logic below
    SELECT *, "POPULATION" AS criteria_name FROM candidate_population
),
/*
Split the candidate population spans into sub-spans separated on every criteria boundary
*/
{create_sub_sessions_with_attributes(table_name="combined_cte", use_magic_date_end_dates=True)}
/*
Combine all overlapping criteria for each population sub-span to determine if the
sub-span represents an eligible or ineligible period for each individual.
*/
SELECT
    spans.state_code,
    spans.person_id,
    spans.start_date,
    {revert_nonnull_end_date_clause("spans.end_date")} AS end_date,
    -- Only set the eligibility to TRUE if all criteria are TRUE. Use the criteria 
    -- default value for criteria spans that do not overlap this sub-span.
    LOGICAL_AND(COALESCE(criteria.meets_criteria, all_criteria.meets_criteria_default)) AS is_eligible,
    -- Assemble the reasons array from all the overlapping criteria reasons
    TO_JSON(ARRAY_AGG(
        TO_JSON(STRUCT(all_criteria.criteria_name AS criteria_name, reason AS reason))
        -- Make the array order deterministic
        ORDER BY all_criteria.criteria_name
    )) AS reasons,
    -- Assemble the reasons array from all the overlapping criteria reasons
    TO_JSON(ARRAY_AGG(
        TO_JSON(STRUCT(all_criteria.criteria_name AS criteria_name, reason_v2 AS reason))
        -- Make the array order deterministic
        ORDER BY all_criteria.criteria_name
    )) AS reasons_v2,
    -- Aggregate all of the FALSE criteria into an array
    ARRAY_AGG(
        IF(NOT COALESCE(criteria.meets_criteria, all_criteria.meets_criteria_default), all_criteria.criteria_name, NULL)
        IGNORE NULLS
        ORDER BY all_criteria.criteria_name
    ) AS ineligible_criteria,
FROM (
    -- Form the eligibility spans around all of the population sub-spans
    SELECT * FROM sub_sessions_with_attributes
    WHERE criteria_name = "POPULATION"
) spans
-- Add 1 row per criteria type to ensure all are included in the results
INNER JOIN all_criteria
    ON spans.state_code = all_criteria.state_code
-- Join all criteria spans that overlap this sub-span
LEFT JOIN criteria_spans criteria
    ON spans.state_code = criteria.state_code
    AND spans.person_id = criteria.person_id
    AND spans.start_date BETWEEN criteria.start_date
        AND DATE_SUB({nonnull_end_date_clause("criteria.end_date")}, INTERVAL 1 DAY)
    AND all_criteria.criteria_name = criteria.criteria_name
GROUP BY 1,2,3,4
"""

    @staticmethod
    def _validate_builder_state_codes(
        task_state_code: StateCode,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> None:
        """Validates that the state code for this task eligibility view matches the
        state codes on all component criteria / population view builders (if one
        exists).
        """
        if isinstance(
            candidate_population_view_builder,
            StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
        ):
            if candidate_population_view_builder.state_code != task_state_code:
                raise ValueError(
                    f"Found candidate population "
                    f"[{candidate_population_view_builder.population_name}] with "
                    f"state_code [{candidate_population_view_builder.state_code}] which"
                    f"does not match the task state_code [{task_state_code}]"
                )

        for criteria_view_builder in criteria_spans_view_builders:
            if isinstance(
                criteria_view_builder,
                StateSpecificTaskCriteriaBigQueryViewBuilder,
            ):
                if criteria_view_builder.state_code != task_state_code:
                    raise ValueError(
                        f"Found criteria [{criteria_view_builder.criteria_name}] with "
                        f"state_code [{criteria_view_builder.state_code}] which does "
                        f"not match the task state_code [{task_state_code}]"
                    )
