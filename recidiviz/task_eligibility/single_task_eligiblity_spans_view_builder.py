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
from typing import Dict, List, Sequence, Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
    TaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    TaskCompletionEventBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaGroupBigQueryViewBuilder,
)
from recidiviz.utils.string import StrictStringFormatter

# Query fragment that can be formatted with criteria-specific naming. Once formatted,
# the string will look like a standard view query template string:
#         SELECT
#             *,
#             "my_criteria" AS criteria_name
#         FROM `{project_id}.{task_eligibility_criteria_xxx_dataset}.my_criteria_materialized`
#         WHERE state_code = "US_XX"
#     )
STATE_SPECIFIC_CRITERIA_FRAGMENT = """
    SELECT
        *,
        "{criteria_name}" AS criteria_name
    FROM `{{project_id}}.{{{criteria_dataset_id}_dataset}}.{criteria_view_id}`
    WHERE state_code = "{state_code}"
"""

# CTE that can be formatted with population-specific naming. Once formatted,
# the string will look like a standard view query template string:
#     candidate_population AS (
#         SELECT
#             *,
#         FROM `{project_id}.{task_eligibility_candidates_xxx_dataset}.my_population_materialized`
#         WHERE state_code = "US_XX"
#     )
STATE_SPECIFIC_POPULATION_CTE = """candidate_population AS (
    SELECT
        *
    FROM `{{project_id}}.{{{population_dataset_id}_dataset}}.{population_view_id}`
    WHERE state_code = "{state_code}"
)"""

CRITERIA_INFO_STRUCT_FRAGMENT = """STRUCT(
            "{state_code}" AS state_code,
            "{criteria_name}" AS criteria_name,
            {meets_criteria_default} AS meets_criteria_default
        )"""

# CTE that can be formatted with completion-event-specific naming. Once formatted,
# the string will look like a standard view query template string:
#     candidate_population AS (
#         SELECT
#             * EXCEPT(completion_date),
#             completion_date AS end_date,
#         FROM `{project_id}.{task_eligibility_completion_events_dataset}.my_event_materialized`
#         WHERE state_code = "US_XX"
#     )
TASK_COMPLETION_EVENT_CTE = """task_completion_events AS (
    SELECT
        * EXCEPT(completion_event_date),
        completion_event_date AS end_date,
    FROM `{{project_id}}.{{{completion_events_dataset_id}_dataset}}.{completion_events_view_id}`
    WHERE state_code = "{state_code}"
)"""


def get_all_descendant_criteria_builders(
    criteria_builders: List[
        Union[
            TaskCriteriaBigQueryViewBuilder,
            TaskCriteriaGroupBigQueryViewBuilder,
            InvertedTaskCriteriaBigQueryViewBuilder,
        ]
    ]
) -> List[
    Union[
        TaskCriteriaBigQueryViewBuilder,
        TaskCriteriaGroupBigQueryViewBuilder,
        InvertedTaskCriteriaBigQueryViewBuilder,
    ]
]:
    """Recursive function to collect all view builders representing task
    criteria at all levels of criteria dependency tree.
    """
    view_builders = []
    for builder in criteria_builders:
        view_builders.append(builder)
        if isinstance(builder, TaskCriteriaGroupBigQueryViewBuilder):
            view_builders += get_all_descendant_criteria_builders(
                builder.sub_criteria_list
            )
        elif isinstance(builder, InvertedTaskCriteriaBigQueryViewBuilder):
            view_builders.append(builder.sub_criteria)
    return view_builders


# TODO(#16091): Write tests for this class
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
        criteria_spans_view_builders: List[
            Union[
                TaskCriteriaBigQueryViewBuilder,
                TaskCriteriaGroupBigQueryViewBuilder,
                InvertedTaskCriteriaBigQueryViewBuilder,
            ]
        ],
        completion_event_builder: TaskCompletionEventBigQueryViewBuilder,
    ) -> None:
        self._validate_builder_state_codes(
            state_code, candidate_population_view_builder, criteria_spans_view_builders
        )
        view_query_template = self._build_query_template(
            state_code,
            task_name,
            candidate_population_view_builder,
            criteria_spans_view_builders,
            completion_event_builder,
        )
        query_format_kwargs = self._dataset_query_format_args(
            candidate_population_view_builder,
            criteria_spans_view_builders,
            completion_event_builder,
        )
        super().__init__(
            dataset_id=task_eligibility_spans_state_specific_dataset(state_code),
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
        self.task_name = task_name
        self.candidate_population_view_builder = candidate_population_view_builder
        self.criteria_spans_view_builders = criteria_spans_view_builders
        self.completion_event_builder = completion_event_builder

    @staticmethod
    def _build_query_template(
        state_code: StateCode,
        task_name: str,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[
            Union[
                TaskCriteriaBigQueryViewBuilder,
                TaskCriteriaGroupBigQueryViewBuilder,
                InvertedTaskCriteriaBigQueryViewBuilder,
            ]
        ],
        completion_event_builder: TaskCompletionEventBigQueryViewBuilder,
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
            criteria_span_ctes.append(
                StrictStringFormatter().format(
                    STATE_SPECIFIC_CRITERIA_FRAGMENT,
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

        if not completion_event_builder.materialized_address:
            raise ValueError(
                f"Expected materialized_address for view [{completion_event_builder.address}]"
            )
        completion_event_span_cte = StrictStringFormatter().format(
            TASK_COMPLETION_EVENT_CTE,
            state_code=state_code.value,
            completion_events_dataset_id=completion_event_builder.materialized_address.dataset_id,
            completion_events_view_id=completion_event_builder.materialized_address.table_id,
        )

        return f"""
WITH
all_criteria AS (
    SELECT state_code, criteria_name, meets_criteria_default
    FROM UNNEST([
        {criteria_info_structs_str}
    ])
),
{all_criteria_spans_cte_str},
{population_span_cte},
{completion_event_span_cte},
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
{create_sub_sessions_with_attributes(table_name="combined_cte", use_magic_date_end_dates=True)},
eligibility_sub_spans AS
/*
Combine all overlapping criteria for each population sub-span to determine if the
sub-span represents an eligible or ineligible period for each individual.
*/
(
    SELECT
        spans.state_code,
        spans.person_id,
        spans.start_date,
        spans.end_date,
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
            TO_JSON(STRUCT(all_criteria.criteria_name AS criteria_name, reason_v2 AS reason_v2))
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
),
eligibility_sub_spans_with_id AS
/*
Create an eligibility_span_id to help collapse adjacent spans with the same
eligibility type. The id is incremented if there is a date gap or if the eligibility
type switches.
*/
(
    SELECT
        *,
        SUM(IF(is_date_gap OR is_eligibility_change OR is_ineligible_criteria_change OR is_reasons_change, 1, 0)) OVER 
            (PARTITION BY state_code, person_id ORDER BY start_date ASC) AS task_eligibility_span_id,
    FROM (
        SELECT
        * EXCEPT(ineligible_reasons_str, full_reasons_str),
        COALESCE(start_date != LAG(end_date) OVER state_person_window, TRUE) AS is_date_gap,
        COALESCE(is_eligible != LAG(is_eligible) OVER state_person_window, TRUE) AS is_eligibility_change,
        COALESCE(ineligible_reasons_str != LAG(ineligible_reasons_str) OVER state_person_window, TRUE)
            AS is_ineligible_criteria_change,
        COALESCE(full_reasons_str != LAG(full_reasons_str) OVER state_person_window, TRUE) AS is_reasons_change
        FROM (
            SELECT 
                *,
                COALESCE(ARRAY_TO_STRING(ineligible_criteria, ","), "") AS ineligible_reasons_str,
                #TODO(#25910): Sort all arrays in reasons json to avoid non-determinism in sub-sessionization  
                TO_JSON_STRING(reasons) AS full_reasons_str
            FROM eligibility_sub_spans
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
                WHEN LEAD(is_ineligible_criteria_change) over w THEN "INELIGIBLE_CRITERIA_CHANGED"
                WHEN LEAD(is_reasons_change) OVER w THEN "INELIGIBLE_REASONS_CHANGED"
                ELSE "UNKNOWN_NOT_ELIGIBLE"
            END
        WHEN is_eligible THEN
            CASE
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

    @staticmethod
    def _dataset_query_format_args(
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[
            Union[
                TaskCriteriaBigQueryViewBuilder,
                TaskCriteriaGroupBigQueryViewBuilder,
                InvertedTaskCriteriaBigQueryViewBuilder,
            ]
        ],
        completion_event_builder: TaskCompletionEventBigQueryViewBuilder,
    ) -> Dict[str, str]:
        """Returns the query format args for the datasets in the query template. All
        datasets are injected as query format args so that these views work when
        deployed to sandbox datasets.
        """
        datasets = (
            [candidate_population_view_builder.dataset_id]
            + [vb.dataset_id for vb in criteria_spans_view_builders]
            + [completion_event_builder.dataset_id]
        )

        return {f"{dataset_id}_dataset": dataset_id for dataset_id in datasets}

    def all_descendant_criteria_builders(
        self,
    ) -> Sequence[
        Union[
            TaskCriteriaBigQueryViewBuilder,
            TaskCriteriaGroupBigQueryViewBuilder,
            InvertedTaskCriteriaBigQueryViewBuilder,
        ]
    ]:
        return get_all_descendant_criteria_builders(self.criteria_spans_view_builders)

    @staticmethod
    def _validate_builder_state_codes(
        task_state_code: StateCode,
        candidate_population_view_builder: TaskCandidatePopulationBigQueryViewBuilder,
        criteria_spans_view_builders: List[
            Union[
                TaskCriteriaBigQueryViewBuilder,
                TaskCriteriaGroupBigQueryViewBuilder,
                InvertedTaskCriteriaBigQueryViewBuilder,
            ]
        ],
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
