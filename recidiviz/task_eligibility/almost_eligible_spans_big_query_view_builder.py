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
"""View builder that auto-generates task spans with the `is_almost_eligible` flag
by combining basic task eligibility spans with almost eligible criteria conditions.
"""
from collections import defaultdict
from textwrap import indent
from typing import Dict, List, Optional, Union

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
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
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.string import StrictStringFormatter


def _get_critical_date_parsing_fragments_by_criteria(
    condition: CriteriaCondition,
) -> Optional[Dict[str, List[str]]]:
    """Return any critical date parsing queries that need to be applied to the criteria spans"""
    if isinstance(condition, DateComparatorCriteriaCondition):
        reason_column = _parse_reason_field_query_fragment(
            condition=condition,
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
                _get_critical_date_parsing_fragments_by_criteria(sub_condition)
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
) -> str:
    """Return the query fragment that parses and casts the criteria condition reason field"""
    return extract_object_from_json(
        json_column="criteria_reason",
        object_column=f"reason.{condition.reasons_field.name}",
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
    FROM eligibility_spans_split_by_critical_date,
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
    The fragment should select from the eligibility_spans_split_by_critical_date CTE, which contains
    eligibility spans that might be almost eligible (no unsupported ineligible
    criteria) and add the boolean column is_almost_eligible which is TRUE for spans where:
    - total sub conditions met is between at_least_n_conditions_true and at_most_n_conditions_true
    - total sub conditions not met is less than (at_most_n_conditions_true - at_least_n_conditions_true)
    """
    composite_criteria_query = "\n    UNION ALL\n".join(
        [
            indent(
                f"""({_get_almost_eligible_criteria_query(condition)})""",
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
    ANY_VALUE(reasons_v2) AS reasons_v2,
    ANY_VALUE(ineligible_criteria) AS ineligible_criteria,
    -- Almost eligible if the number of almost eligible criteria met is in the set range
    -- and the almost eligible criteria not-met does not exceed the number allowed
    COUNTIF(is_almost_eligible) BETWEEN {condition.at_least_n_conditions_true} AND {condition.at_most_n_conditions_true}
        AND COUNTIF(NOT is_almost_eligible) <= {condition.at_most_n_conditions_true - condition.at_least_n_conditions_true} AS is_almost_eligible,
FROM composite_criteria_condition
GROUP BY 1, 2, 3, 4
""",
        " " * 4,
    )


def _get_almost_eligible_criteria_query(condition: CriteriaCondition) -> str:
    """Return the query fragment used within the task eligibility spans view to
    label the spans that are considered to be almost eligible.
    The fragment should select from the eligibility_spans_split_by_critical_date CTE, which contains
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
    FROM eligibility_spans_split_by_critical_date
    WHERE {filter_fragment}
"""
    raise NotImplementedError(f"Unexpected condition type: {type(condition)}")


class AlmostEligibleSpansBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """View builder that auto-generates task spans with the `is_almost_eligible` flag
    by combining basic task eligibility spans with almost eligible criteria conditions.
    """

    def __init__(
        self,
        *,
        basic_tes_view_builder: BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
        almost_eligible_condition: CriteriaCondition,
    ) -> None:
        state_code = basic_tes_view_builder.state_code
        task_name = basic_tes_view_builder.task_name
        criteria_spans_view_builders = (
            basic_tes_view_builder.criteria_spans_view_builders
        )

        self._validate_almost_eligible_condition(
            state_code,
            task_name,
            criteria_spans_view_builders,
            almost_eligible_condition,
        )
        view_query_template = self._build_query_template(
            basic_tes_view_builder=basic_tes_view_builder,
            almost_eligible_criteria_condition=almost_eligible_condition,
        )
        view_address = self._address_for_task_name(state_code, task_name)

        super().__init__(
            dataset_id=view_address.dataset_id,
            view_id=view_address.table_id,
            description="Task eligibility spans intermediate view with the `is_almost_eligible` flag defined",
            view_query_template=view_query_template,
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
        )
        self.state_code = state_code
        self.task_name = task_name
        self.almost_eligible_condition = almost_eligible_condition
        self.criteria_spans_view_builders = criteria_spans_view_builders

    @staticmethod
    def _address_for_task_name(
        state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=task_eligibility_spans_state_specific_dataset(state_code),
            table_id=f"{task_name.lower()}__with_is_almost_eligible",
        )

    @classmethod
    def materialized_table_for_task_name(
        cls, state_code: StateCode, task_name: str
    ) -> BigQueryAddress:
        view_address = cls._address_for_task_name(state_code, task_name)
        return cls.build_standard_materialized_address(
            dataset_id=view_address.dataset_id, view_id=view_address.table_id
        )

    @staticmethod
    def _build_query_template(
        basic_tes_view_builder: BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
        almost_eligible_criteria_condition: CriteriaCondition,
    ) -> str:
        """Return the query template used to generate the task eligibility spans almost eligible
        view containing the almost eligible flag column. These spans are split by changes in the
        `is_almost_eligible` column and so they are not guaranteed to align with the basic eligibility
        span boundaries.
        """

        # Create the query fragments to compute the almost eligible logic from the eligibility spans
        ae_query_fragment = _get_almost_eligible_criteria_query(
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
                for criteria_name in sorted(supported_almost_eligible_criteria_names)
            )
        )

        # Pull out any query fragments for parsing critical dates required to split
        # criteria spans for the almost eligible criteria conditions
        criteria_to_critical_date_query_fragment_map = (
            _get_critical_date_parsing_fragments_by_criteria(
                almost_eligible_criteria_condition
            )
        )

        if criteria_to_critical_date_query_fragment_map:
            split_spans_by_critical_date_cte = AlmostEligibleSpansBigQueryViewBuilder.split_basic_eligibility_spans_by_critical_date_query_fragment(
                criteria_to_critical_date_query_fragment_map
            )
        else:
            split_spans_by_critical_date_cte = "SELECT * FROM potential_almost_eligible"

        eligibility_source_table = f"`{basic_tes_view_builder.table_for_query.format_address_for_query_template()}`"

        return f"""
WITH 
-- Pull out the spans that may qualify as almost eligible:
-- not eligible
-- only missing criteria that are covered by the almost eligible conditions 
potential_almost_eligible AS (
    SELECT
        eligibility_spans.*
    FROM {eligibility_source_table} eligibility_spans
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
        FROM {eligibility_source_table},
        UNNEST(ineligible_criteria) AS criteria_name
        GROUP BY 1, 2, 3
    )
    USING (state_code, person_id, start_date)
    WHERE
        NOT is_eligible
        AND no_unsupported_criteria
),
eligibility_spans_split_by_critical_date AS ({split_spans_by_critical_date_cte}
),
spans_with_almost_eligible_flag AS ({ae_query_fragment}
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    IFNULL(is_almost_eligible, FALSE) AS is_almost_eligible,
FROM ({aggregate_adjacent_spans(table_name="spans_with_almost_eligible_flag", attribute="is_almost_eligible")})
"""

    @staticmethod
    def split_basic_eligibility_spans_by_critical_date_query_fragment(
        criteria_to_critical_date_query_fragment_map: Dict[str, List[str]],
    ) -> str:
        """Return a query fragment that splits the basic eligibility spans across any overlapping a critical dates"""
        critical_date_extract_fragment = (
            "\n"
            + indent(
                ",\n".join(
                    sorted(
                        set(
                            f"""IF(
                    JSON_VALUE(criteria_reason, '$.criteria_name') = '{criteria_name}',
                    {critical_date_extract_string},
                    NULL
                )"""
                            for criteria_name, critical_date_extract_string_list in criteria_to_critical_date_query_fragment_map.items()
                            for critical_date_extract_string in critical_date_extract_string_list
                        )
                    )
                ),
                " " * 12,
            )
            + "\n"
        )
        return f"""
    (
        -- Split the criteria spans across any overlapping critical date boundaries
        WITH basic_eligibility_spans_with_new_boundary_dates AS (
            -- Fetch the set of unique new start dates taken as the combination of the
            -- original start date and any overlapping critical date across all
            -- time based criteria conditions
            SELECT DISTINCT
                state_code,
                person_id,
                start_date AS original_start_date,
                end_date AS original_end_date,
                new_start_date,
            FROM potential_almost_eligible,
            UNNEST(JSON_QUERY_ARRAY(reasons_v2)) AS criteria_reason,
            -- Unnest 1 row for each new start dates including the original criteria span start date
            -- along with all the critical dates that fall within the criteria span interval
            UNNEST([start_date, {critical_date_extract_fragment}
            ]) AS new_start_date
            WHERE
                -- Only include the dates that fall within the criteria span (start date inclusive)
                new_start_date BETWEEN potential_almost_eligible.start_date
                    AND {nonnull_end_date_exclusive_clause("potential_almost_eligible.end_date")}
        ),
        new_eligibility_boundary_spans AS (
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
            FROM basic_eligibility_spans_with_new_boundary_dates
        )
        -- Join the new span boundaries back to the original overlapping span
        -- to get the `reasons` array
        SELECT
            original_spans.state_code,
            original_spans.person_id,
            new_spans.start_date,
            new_spans.end_date,
            original_spans.reasons_v2,
            original_spans.ineligible_criteria,
        FROM new_eligibility_boundary_spans new_spans
        INNER JOIN potential_almost_eligible AS original_spans
            ON new_spans.state_code = original_spans.state_code
            AND new_spans.person_id = original_spans.person_id
            AND new_spans.start_date < {nonnull_end_date_clause("original_spans.end_date")}
            AND original_spans.start_date < {nonnull_end_date_clause("new_spans.end_date")}
    )
    """

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
