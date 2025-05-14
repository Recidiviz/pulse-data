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
"""View builder that auto-generates task eligibility spans view from component criteria
and candidate population views.
"""
from collections import defaultdict
from textwrap import indent
from typing import Dict, List, Optional, Sequence, Union

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.criteria_condition import (
    ComparatorCriteriaCondition,
    CriteriaCondition,
    DateComparatorCriteriaCondition,
    EligibleCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
)
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
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

# Query fragment that can be formatted with criteria-specific naming. Once formatted,
# the string will look like a standard view query template string:
#         SELECT
#             *,
#             "my_criteria" AS criteria_name
#         FROM `{project_id}.task_eligibility_criteria_xxx.my_criteria_materialized`
#         WHERE state_code = "US_XX"
#     )
# Use this query fragment for criteria that do not have an almost eligible
# criteria condition.
# Otherwise, use `get_almost_eligible_condition_query_fragment(CriteriaCondition)`.
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


def get_critical_date_parsing_fragments_by_criteria(
    condition: CriteriaCondition,
) -> Optional[Dict[str, List[str]]]:
    """Return any critical date parsing queries that need to be applied to the criteria spans"""
    if isinstance(condition, DateComparatorCriteriaCondition):
        reason_column = _parse_reason_field_query_fragment(
            condition=condition, json_reasons_column="reason_v2"
        )
        return {
            condition.criteria.criteria_name: [
                _get_date_comparator_critical_date_query_fragment(
                    condition, reason_column
                )
            ]
        }
    if isinstance(condition, PickNCompositeCriteriaCondition):
        # Collect all the criteria critical dates across the sub conditions
        critical_date_fragments_map: Dict[str, List[str]] = defaultdict(list)
        for sub_condition in condition.sub_conditions_list:
            sub_condition_fragments_map = (
                get_critical_date_parsing_fragments_by_criteria(sub_condition)
            )
            if not sub_condition_fragments_map:
                continue
            for criteria_name, fragments in sub_condition_fragments_map.items():
                critical_date_fragments_map[criteria_name].extend(fragments)

        if critical_date_fragments_map:
            return critical_date_fragments_map

    return None


def _parse_reason_field_query_fragment(
    condition: Union[ComparatorCriteriaCondition, DateComparatorCriteriaCondition],
    json_reasons_column: str,
) -> str:
    """Return the query fragment that parses and casts the criteria condition reason field"""
    if json_reasons_column not in ["reason_v2", "criteria_reason"]:
        raise NotImplementedError(f"Unsupported reason column: {json_reasons_column}")
    # Prepend "reason." to the json field name string if the reasons blob is nested -
    # - criteria_reason: from the nested eligibility spans, unnested from reasons_v2
    # - reason_v2: from the non-nested criteria spans
    object_column = (
        f"reason.{condition.reasons_field.name}"
        if json_reasons_column == "criteria_reason"
        else condition.reasons_field.name
    )
    return extract_object_from_json(
        json_column=json_reasons_column,
        object_column=object_column,
        object_type=condition.reasons_field.type.name,
    )


def _get_date_comparator_critical_date_query_fragment(
    condition: DateComparatorCriteriaCondition, reason_column: str
) -> str:
    """Return a query fragment with the critical date computed from the criteria reason column"""
    return StrictStringFormatter().format(
        condition.critical_date_condition_query_template,
        reasons_date=reason_column,
    )


def _get_comparator_criteria_almost_eligible_query(
    condition: Union[ComparatorCriteriaCondition, DateComparatorCriteriaCondition],
) -> str:
    """Return the query fragment used within the task eligibility spans view to
    label the spans that are considered to be almost eligible for comparator style
    conditions (ComparatorCriteriaCondition, DateComparatorCriteriaCondition).
    The fragment should select from the potential_almost_eligible CTE, which contains
    eligibility spans that might be almost eligible (no unsupported ineligible
    criteria) and adds the boolean column is_almost_eligible which is TRUE if the
    span is considered to be almost eligible according to the comparator logic
    and FALSE otherwise.
    """
    reason_field_column = _parse_reason_field_query_fragment(
        condition=condition,
        json_reasons_column="criteria_reason",
    )
    if isinstance(condition, DateComparatorCriteriaCondition):
        # Spans that have passed the condition critical date are considered almost eligible,
        # spans with no critical date are considered not almost eligible
        almost_eligible_criteria = f"""IFNULL(
            {_get_date_comparator_critical_date_query_fragment(condition, reason_field_column)} <= start_date,
            FALSE
        )"""
    elif isinstance(condition, ComparatorCriteriaCondition):
        # Compare the criteria reason field to the condition value
        almost_eligible_criteria = (
            f"{reason_field_column} {condition.comparison_operator} {condition.value}"
        )
    else:
        raise NotImplementedError(f"Unexpected condition type: {type(condition)}")

    return f"""
    SELECT
        * EXCEPT(criteria_reason),
        -- Apply the almost eligible criteria to the criteria reason field
        {almost_eligible_criteria} AS is_almost_eligible,
    FROM potential_almost_eligible,
    UNNEST(JSON_QUERY_ARRAY(reasons_v2)) AS criteria_reason
    WHERE "{condition.criteria.criteria_name}" IN UNNEST(ineligible_criteria)
        AND {extract_object_from_json(
    json_column="criteria_reason",
    object_column="criteria_name",
    object_type="STRING",
)} = "{condition.criteria.criteria_name}"
"""


def _get_pick_n_composite_criteria_almost_eligible_query(
    condition: PickNCompositeCriteriaCondition,
) -> str:
    """Return the query fragment used within the task eligibility spans view to
    label the spans that are considered to be almost eligible for the combined
    PickNCompositeCriteriaCondition conditions.
    The fragment should select from the potential_almost_eligible CTE, which contains
    eligibility spans that might be almost eligible (no unsupported ineligible
    criteria) and add the boolean column is_almost_eligible which is TRUE for spans where:
    - total sub conditions met is between at_least_n_conditions_true and at_most_n_conditions_true
    - total sub conditions not met is less than (at_most_n_conditions_true - at_least_n_conditions_true)
    """
    composite_criteria_query = "\n    UNION ALL\n".join(
        [
            indent(
                f"""({get_almost_eligible_criteria_query(condition)})""",
                " " * 4,
            )
            for condition in condition.sub_conditions_list
        ]
    )
    return indent(
        f"""
WITH composite_criteria_condition AS (
{composite_criteria_query}
)
SELECT
    state_code, person_id, start_date, end_date,
    -- Use ANY_VALUE for these span attributes since they are the same across every span with the same start/end date
    ANY_VALUE(is_eligible) AS is_eligible,
    ANY_VALUE(reasons) AS reasons,
    ANY_VALUE(reasons_v2) AS reasons_v2,
    ANY_VALUE(ineligible_criteria) AS ineligible_criteria,
    -- Almost eligible if number of almost eligible criteria count is in the set range
    -- and the almost eligible criteria not-met does not exceed the number allowed
    COUNTIF(is_almost_eligible) BETWEEN {condition.at_least_n_conditions_true} AND {condition.at_most_n_conditions_true}
        AND COUNTIF(NOT is_almost_eligible) <= {condition.at_most_n_conditions_true - condition.at_least_n_conditions_true} AS is_almost_eligible,
FROM composite_criteria_condition
GROUP BY 1, 2, 3, 4
""",
        " " * 4,
    )


def get_almost_eligible_criteria_query(condition: CriteriaCondition) -> str:
    """Return the query fragment used within the task eligibility spans view to
    label the spans that are considered to be almost eligible.
    The fragment should select from the potential_almost_eligible CTE, which contains
    eligibility spans that might be almost eligible (no unsupported ineligible
    criteria) and adds the boolean column is_almost_eligible which is TRUE if the
    span is considered to be almost eligible and FALSE otherwise.
    """
    if isinstance(condition, PickNCompositeCriteriaCondition):
        return _get_pick_n_composite_criteria_almost_eligible_query(condition)

    if isinstance(
        condition, (DateComparatorCriteriaCondition, ComparatorCriteriaCondition)
    ):
        return _get_comparator_criteria_almost_eligible_query(condition)

    if isinstance(
        condition,
        (NotEligibleCriteriaCondition, EligibleCriteriaCondition),
    ):
        if isinstance(condition, NotEligibleCriteriaCondition):
            filter_fragment = f""""{condition.criteria.criteria_name}" IN UNNEST(ineligible_criteria)"""
        elif isinstance(condition, EligibleCriteriaCondition):
            filter_fragment = f""""{condition.criteria.criteria_name}" NOT IN UNNEST(ineligible_criteria)"""
        else:
            raise ValueError(f"Unexpected condition type: {type(condition)}")

        return f"""
    SELECT
        *,
        TRUE AS is_almost_eligible,
    FROM potential_almost_eligible
    WHERE {filter_fragment}
"""
    raise NotImplementedError(f"Unexpected condition type: {type(condition)}")


class SingleTaskEligibilitySpansBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that auto-generates task eligibility spans view from component
    criteria and candidate population views.
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
    ) -> None:
        self._validate_builder_state_codes(
            state_code,
            candidate_population_view_builder,
            criteria_spans_view_builders,
        )
        self._validate_almost_eligible_condition(
            state_code,
            task_name,
            criteria_spans_view_builders,
            almost_eligible_condition,
        )
        view_query_template = self._build_query_template(
            state_code=state_code,
            task_name=task_name,
            candidate_population_view_builder=candidate_population_view_builder,
            criteria_spans_view_builders=criteria_spans_view_builders,
            completion_event_builder=completion_event_builder,
            almost_eligible_criteria_condition=almost_eligible_condition,
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
        self.candidate_population_view_builder = candidate_population_view_builder
        self.criteria_spans_view_builders = criteria_spans_view_builders
        self.completion_event_builder = completion_event_builder
        # TODO(#31443): split almost eligible spans into a separate view
        self.almost_eligible_condition = almost_eligible_condition

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
        almost_eligible_criteria_condition: Optional[CriteriaCondition],
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
        # TODO(#31443): split almost eligible spans into a separate view
        # Pull out any query fragments for parsing critical dates required to split
        # criteria spans for the almost eligible criteria conditions
        if almost_eligible_criteria_condition:
            criteria_to_critical_date_query_fragment_map = (
                get_critical_date_parsing_fragments_by_criteria(
                    almost_eligible_criteria_condition
                )
            )
        else:
            criteria_to_critical_date_query_fragment_map = None
        for criteria_view_builder in criteria_spans_view_builders:
            if not criteria_view_builder.materialized_address:
                raise ValueError(
                    f"Expected materialized_address for view [{criteria_view_builder.address}]"
                )

            criteria_query_fragment = STATE_SPECIFIC_CRITERIA_FRAGMENT

            # If there is an almost eligible condition for this criteria then apply it to the criteria query fragment
            # TODO(#31443): split almost eligible spans into a separate view
            if almost_eligible_criteria_condition:
                # If the criteria is covered by the almost eligible criteria condition
                if (
                    criteria_view_builder
                    in almost_eligible_criteria_condition.get_criteria_builders()
                ):
                    # If there is a time dependent condition on this criteria then split the criteria spans along
                    # the relevant critical date
                    if criteria_to_critical_date_query_fragment_map and (
                        criteria_view_builder.criteria_name
                        in criteria_to_critical_date_query_fragment_map
                    ):
                        criteria_query_fragment = SingleTaskEligibilitySpansBigQueryViewBuilder.split_criteria_by_critical_date_query_fragment(
                            criteria_to_critical_date_query_fragment_map[
                                criteria_view_builder.criteria_name
                            ]
                        )

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

        # Create the query fragments to insert the almost eligible logic into the eligibility spans query
        # TODO(#31443): split almost eligible spans into a separate view
        total_almost_eligible_query_fragment = """
    SELECT
        *,
        FALSE AS is_almost_eligible
    FROM eligibility_sub_spans
"""
        if almost_eligible_criteria_condition:
            ae_query_fragment = get_almost_eligible_criteria_query(
                almost_eligible_criteria_condition
            )
            supported_almost_eligible_criteria_names = {
                criteria.criteria_name
                for criteria in almost_eligible_criteria_condition.get_criteria_builders()
            }
            ae_supported_criteria_name_string = (
                "-- Include all criteria that are part of an almost eligible criteria condition\n"
                + ",\n".join(
                    indent(f'"{criteria_name}"', " " * 24)
                    for criteria_name in sorted(
                        supported_almost_eligible_criteria_names
                    )
                )
            )

            total_almost_eligible_query_fragment = f"""
    WITH potential_almost_eligible AS (
        SELECT
            eligibility_sub_spans.*
        FROM eligibility_sub_spans
        INNER JOIN (
            SELECT
                state_code, person_id, start_date,
                -- Spans are potentially almost eligible if all the ineligible criteria
                -- are supported by criteria conditions
                LOGICAL_AND(
                    criteria_name IN UNNEST([
                        {ae_supported_criteria_name_string}
                    ])
                ) AS no_unsupported_criteria,
            FROM eligibility_sub_spans,
            UNNEST(ineligible_criteria) AS criteria_name
            GROUP BY 1, 2, 3
        )
        USING (state_code, person_id, start_date)
        WHERE
            NOT is_eligible
            AND no_unsupported_criteria
    ),
    spans_with_almost_eligible_flag AS ({indent(ae_query_fragment, " " * 4)}
    ),
    almost_eligible_and_eligible_intersection AS ({indent(create_intersection_spans(
            table_1_name="eligibility_sub_spans",
            table_2_name="spans_with_almost_eligible_flag",
            index_columns=["state_code", "person_id"],
            use_left_join=True,
            table_1_columns=["is_eligible", "reasons", "reasons_v2", "ineligible_criteria"],
            table_2_columns=["is_almost_eligible"],
            table_1_end_date_field_name="end_date",
            table_2_end_date_field_name="end_date",
        ), " " * 4)}
    )
    SELECT
        * EXCEPT(end_date_exclusive, is_almost_eligible),
        end_date_exclusive AS end_date,
        IFNULL(is_almost_eligible, FALSE) AS is_almost_eligible
    FROM almost_eligible_and_eligible_intersection
"""

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
),
-- TODO(#31443): split almost eligible spans into a separate view
eligibility_sub_spans_with_almost_eligible AS (
    {total_almost_eligible_query_fragment}
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
        -- TODO(#31443): split almost eligible spans into a separate view
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

    @staticmethod
    def split_criteria_by_critical_date_query_fragment(
        critical_date_extract_strings: List[str],
    ) -> str:
        """Return a query fragment that splits the criteria spans across any overlapping a critical dates"""
        critical_date_extract_fragment = (
            "\n"
            + indent(",\n".join(sorted(set(critical_date_extract_strings))), " " * 12)
            + "\n"
        )
        return f"""
(
    -- Split the criteria spans across any overlapping critical date boundaries
    WITH criteria_spans AS (
        SELECT
            *
        FROM `{{{{project_id}}}}.{{criteria_dataset_id}}.{{criteria_view_id}}` criteria
        WHERE state_code = "{{state_code}}"
    ),
    criteria_boundary_dates AS (
        -- Fetch the set of unique new start dates taken as the combination of the
        -- original start date and all overlapping critical dates
        SELECT DISTINCT
            state_code,
            person_id,
            start_date AS original_start_date,
            end_date AS original_end_date,
            new_start_date,
        FROM criteria_spans,
        -- Unnest 1 row for each new start dates including the original criteria span start date
        -- along with all the critical dates that fall within the criteria span interval
        UNNEST([start_date, {critical_date_extract_fragment}
        ]) AS new_start_date
        WHERE
            -- Only include the dates that fall within the criteria span (start date inclusive)
            new_start_date BETWEEN criteria_spans.start_date
                AND {nonnull_end_date_exclusive_clause("criteria_spans.end_date")}
    ),
    new_criteria_boundary_spans AS (
        -- Create the new spans representing the intervals
        -- [new start date, coalesce(next new start date, original end date)]
        SELECT
            state_code,
            person_id,
            new_start_date AS start_date,
            IFNULL(
                -- Pick the next new_start_date as this span's new span end_date
                LEAD(new_start_date) OVER (
                    PARTITION BY state_code, person_id, original_start_date, original_end_date
                    ORDER BY new_start_date
                ),
                -- Use the original span end_date if there is no next new_start_date
                original_end_date
            ) AS end_date,
        FROM criteria_boundary_dates
    )
    -- Join the new span boundaries back to the overlapping criteria span for
    -- the criteria span metadata columns (reason, meets_criteria)
    SELECT
        criteria_spans.state_code,
        criteria_spans.person_id,
        new_spans.start_date,
        new_spans.end_date,
        criteria_spans.meets_criteria,
        criteria_spans.reason,
        criteria_spans.reason_v2,
        "{{criteria_name}}" AS criteria_name
    FROM new_criteria_boundary_spans new_spans
    INNER JOIN criteria_spans
        ON new_spans.state_code = criteria_spans.state_code
        AND new_spans.person_id = criteria_spans.person_id
        AND new_spans.start_date < {nonnull_end_date_clause("criteria_spans.end_date")}
        AND criteria_spans.start_date < {nonnull_end_date_clause("new_spans.end_date")}
)
"""

    def all_descendant_criteria_builders(
        self,
    ) -> Sequence[TaskCriteriaBigQueryViewBuilder]:
        all_descendants: set[TaskCriteriaBigQueryViewBuilder] = set()
        for criteria_builder in self.criteria_spans_view_builders:
            all_descendants.add(criteria_builder)
            all_descendants |= criteria_builder.get_descendant_criteria()

        return sorted(all_descendants, key=lambda c: c.criteria_name)

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

    @staticmethod
    def _validate_almost_eligible_condition(
        state_code: StateCode,
        task_name: str,
        criteria_spans_view_builders: List[TaskCriteriaBigQueryViewBuilder],
        almost_eligible_condition: Optional[CriteriaCondition],
    ) -> None:
        """Validate the almost eligible condition is only applied to criteria in the top-level of the
        criteria spans view builders list, and do not reference a criteria within a task criteria group.
        """
        if almost_eligible_condition is None:
            return

        unsupported_almost_eligible_conditions = set()
        for (
            almost_eligible_related_criteria
        ) in almost_eligible_condition.get_criteria_builders():
            if almost_eligible_related_criteria not in criteria_spans_view_builders:
                unsupported_almost_eligible_conditions.add(
                    almost_eligible_related_criteria.criteria_name
                )

        if len(unsupported_almost_eligible_conditions) > 0:
            eligibility_criteria_name_string = ", ".join(
                [criteria.criteria_name for criteria in criteria_spans_view_builders]
            )
            raise ValueError(
                f"Cannot create task eligibility spans for {state_code.value}-{task_name} with an almost eligible"
                f" condition for criteria [{', '.join(unsupported_almost_eligible_conditions)}] as it is not in the"
                f" top-level of eligibility criteria: [{eligibility_criteria_name_string}]"
            )
