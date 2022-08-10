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
"""View builder that auto-generates task eligiblity spans view from component criteria
and candidate population views.
"""
from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
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
#             * EXCEPT(end_date),
#             COALESCE(end_date, "9999-12-31") AS end_date,
#             'my_criteria' AS criteria_name
#         FROM `{project_id}.{task_eligibility_criteria_xxx_dataset}.my_criteria`
#         WHERE state_code = 'US_XX'
#     )
STATE_SPECIFIC_CRITERIA_FRAGMENT = """
    SELECT
        * EXCEPT(end_date),
        {nonnull_end_date} AS end_date,
        '{criteria_name}' AS criteria_name
    FROM `{{project_id}}.{{{criteria_dataset_id}_dataset}}.{criteria_view_id}`
    WHERE state_code = '{state_code}'
"""

# CTE that can be formatted with population-specific naming. Once formatted,
# the string will look like a standard view query template string:
#     candidate_population AS (
#         SELECT
#             * EXCEPT(end_date),
#             COALESCE(end_date, "9999-12-31") AS end_date,
#         FROM `{project_id}.{task_eligibility_candidates_xxx_dataset}.my_population`
#         WHERE state_code = 'US_XX'
#     )
STATE_SPECIFIC_POPULATION_CTE = """candidate_population AS (
    SELECT
        * EXCEPT(end_date),
        {nonnull_end_date} AS end_date,
    FROM `{{project_id}}.{{{population_dataset_id}_dataset}}.{population_view_id}`
    WHERE state_code = '{state_code}'
)"""


# TODO(#14310): Write tests for this class
class SingleTaskEligibilitySpansBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that auto-generates task eligiblity spans view from component
    criteria and candidate population views.
    """

    def __init__(
        self,
        state_code: StateCode,
        task_name: str,
        description: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> None:
        self._validate_builder_state_codes(
            state_code, candidate_population_view_builder, criteria_spans_view_builders
        )
        view_query_template = self._build_query_template(
            state_code,
            task_name,
            candidate_population_view_builder,
            criteria_spans_view_builders,
        )
        query_format_kwargs = self._dataset_query_format_args(
            candidate_population_view_builder, criteria_spans_view_builders
        )
        super().__init__(
            dataset_id=f"task_eligibility_spans_{state_code.value.lower()}",
            view_id=task_name.lower(),
            description=description,
            view_query_template=view_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            should_deploy_predicate=None,
            clustering_fields=None,
            **query_format_kwargs,
        )
        self.state_code = state_code

    @staticmethod
    def _build_query_template(
        state_code: StateCode,
        task_name: str,
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
            nonnull_end_date=nonnull_end_date_clause("end_date"),
        )
        criteria_span_ctes = []
        for criteria_view_builder in criteria_spans_view_builders:
            if not criteria_view_builder.materialized_address:
                raise ValueError(
                    f"Expected materialized_address for view [{criteria_view_builder.address}]"
                )
            criteria_span_ctes.append(
                StrictStringFormatter().format(
                    STATE_SPECIFIC_CRITERIA_FRAGMENT,
                    state_code=state_code.value,
                    criteria_name=criteria_view_builder.criteria_name,
                    criteria_dataset_id=criteria_view_builder.materialized_address.dataset_id,
                    criteria_view_id=criteria_view_builder.materialized_address.table_id,
                    nonnull_end_date=nonnull_end_date_clause("end_date"),
                )
            )
        all_criteria_spans_cte_str = (
            f"criteria_spans AS ({'UNION ALL'.join(criteria_span_ctes)})"
        )
        return f"""
WITH {all_criteria_spans_cte_str},
{population_span_cte},
combined_cte AS (
    SELECT * EXCEPT(meets_criteria, reason, criteria_name) FROM criteria_spans
    UNION ALL
    SELECT * FROM candidate_population
),
start_dates AS
/*
Start creating the new smaller sub-spans with boundaries for every criteria span
date. Generate the full list of the new sub-span start dates including all unique
criteria span start dates and any end dates that overlap another span.
*/
(
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
    FROM combined_cte
    UNION DISTINCT
    SELECT DISTINCT
        orig.state_code,
        orig.person_id,
        new_start_dates.end_date AS start_date,
    FROM combined_cte orig
    INNER JOIN combined_cte new_start_dates
        ON orig.state_code = new_start_dates.state_code
        AND orig.person_id = new_start_dates.person_id
        AND new_start_dates.end_date
            BETWEEN orig.start_date AND DATE_SUB(orig.end_date, INTERVAL 1 DAY)
),
end_dates AS
/*
Generate the full list of the new sub-span end dates including all unique criteria
span end dates and any start dates that overlap another span.
*/
(
    SELECT DISTINCT
        state_code,
        person_id,
        end_date,
    FROM combined_cte
    UNION DISTINCT
    SELECT DISTINCT
        orig.state_code,
        orig.person_id,
        new_end_dates.start_date AS end_date,
    FROM combined_cte orig
    INNER JOIN combined_cte new_end_dates
        ON orig.state_code = new_end_dates.state_code
        AND orig.person_id = new_end_dates.person_id
        AND new_end_dates.start_date
            BETWEEN orig.start_date AND DATE_SUB(orig.end_date, INTERVAL 1 DAY)
),
sub_spans AS
/*
Join start and end dates together to create smaller sub-spans. Each start date gets
matched to the closest following end date for each person.
*/
(
    SELECT
        start_dates.state_code,
        start_dates.person_id,
        start_dates.start_date,
        MIN(end_dates.end_date) AS end_date,
    FROM start_dates
    INNER JOIN end_dates
        ON start_dates.state_code = end_dates.state_code
        AND start_dates.person_id = end_dates.person_id
        AND start_dates.start_date < end_dates.end_date
    GROUP BY state_code, person_id, start_date
),
eligibility_sub_spans AS
/*
Combine all overlapping criteria for each sub-span to determine if the sub-span
represents an eligible or ineligible period for each individual.
*/
(
    SELECT
        spans.state_code,
        spans.person_id,
        spans.start_date,
        spans.end_date,
        -- Only set the eligibility to TRUE if all criteria are TRUE
        LOGICAL_AND(COALESCE(criteria.meets_criteria, FALSE)) AS is_eligible,
        -- Assemble the reasons array from all the overlapping criteria reasons
        TO_JSON(ARRAY_AGG(
            TO_JSON(STRUCT(all_criteria.criteria_name AS criteria_name, reason AS reason))
            -- Make the array order deterministic
            ORDER BY all_criteria.criteria_name
        )) AS reasons,
        -- Aggregate all of the FALSE criteria into an array
        ARRAY_AGG(
            IF(NOT COALESCE(criteria.meets_criteria, FALSE), all_criteria.criteria_name, NULL)
            IGNORE NULLS
        ) AS ineligible_criteria,
    FROM sub_spans spans
    -- Limit to spans that overlap with the candidate population
    INNER JOIN candidate_population pop
        ON spans.state_code = pop.state_code
        AND spans.person_id = pop.person_id
        AND spans.start_date BETWEEN pop.start_date AND DATE_SUB(pop.end_date, INTERVAL 1 DAY)
    -- Add 1 row per criteria type to ensure all are included in the results
    INNER JOIN (
        SELECT DISTINCT state_code, criteria_name
        FROM criteria_spans
    ) all_criteria
        ON spans.state_code = all_criteria.state_code
    -- Join all criteria spans that overlap this sub-span
    LEFT JOIN criteria_spans criteria
        ON spans.state_code = criteria.state_code
        AND spans.person_id = criteria.person_id
        AND spans.start_date
            BETWEEN criteria.start_date AND DATE_SUB(criteria.end_date, INTERVAL 1 DAY)
        AND all_criteria.criteria_name = criteria.criteria_name
    GROUP BY 1,2,3,4
),
eligibility_sub_spans_with_id AS
/*
Create an eligibility_span_id to help collapse adjacent spans with the same
eligibility type. The id is incremented if there is a date gap or if the eligibility
type switches.
*/
(
    SELECT
        * EXCEPT(new_span),
        SUM(IF(new_span, 1, 0)) OVER (PARTITION BY state_code, person_id
            ORDER BY start_date ASC) AS task_eligibility_span_id,
    FROM (
        SELECT
        *,
        -- TODO(#14445): split spans when the `reasons` array changes
        COALESCE(
            start_date != LAG(end_date) OVER state_person_window
                OR is_eligible != LAG(is_eligible) OVER state_person_window,
            TRUE
        ) AS new_span,
        FROM eligibility_sub_spans
        WINDOW state_person_window AS (PARTITION BY state_code, person_id
            ORDER BY start_date ASC)
    )
)
SELECT
    state_code,
    person_id,
    '{task_name}' AS task_name,
    task_eligibility_span_id,
    MIN(start_date) OVER (PARTITION BY state_code, person_id, task_eligibility_span_id) AS start_date,
    {revert_nonnull_end_date_clause('end_date')} AS end_date,
    is_eligible,
    reasons,
    ineligible_criteria,
FROM eligibility_sub_spans_with_id
-- Pick the row with the latest end date for each task_eligibility_span_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, task_eligibility_span_id
    ORDER BY {nonnull_end_date_clause('end_date')} DESC
) = 1
"""

    @staticmethod
    def _dataset_query_format_args(
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> Dict[str, str]:
        """Returns the query format args for the datasets in the query template. All
        datasets are injected as query format args so that these views work when
        deployed to sandbox datasets.
        """
        datasets = [candidate_population_view_builder.dataset_id] + [
            vb.dataset_id for vb in criteria_spans_view_builders
        ]

        return {f"{dataset_id}_dataset": dataset_id for dataset_id in datasets}

    @staticmethod
    def _validate_builder_state_codes(
        task_state_code: StateCode,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
    ) -> None:
        """Validates that the state code for this task eligiblity view matches the
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
