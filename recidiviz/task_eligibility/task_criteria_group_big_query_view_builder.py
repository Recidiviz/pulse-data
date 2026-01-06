# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines BigQueryViewBuilder that can be used to define some boolean logic on
inputted criteria span views.
"""

import abc
from collections import defaultdict
from enum import Enum
from textwrap import indent
from typing import List, Sequence

import attr
from google.cloud import bigquery
from more_itertools import one

from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)

FOUR_SPACES_INDENT = "    "


def _deduplicated_reasons_fields(
    *,
    criteria_group_name: str,
    sub_criteria_list: Sequence[TaskCriteriaBigQueryViewBuilder],
    allowed_duplicate_reasons_keys: List[str],
    reason_fields_with_aggregate_function_override: set[str],
) -> List[ReasonsField]:
    """Returns the list of ALL reasons attributes based on a recursive
    search of the sub-criteria.

    Args:
        criteria_group_name: The name of the group criteria these sub-criteria belong
            to.
        sub_criteria_list: The list of sub-criteria to pull reasons from.
        allowed_duplicate_reasons_keys: List of reasons fields that we expect to
            appear in multiple sub-criteria reasons blobs. If duplicate is allowed
            for a given key, we assume that values agree and select one of the
            values deterministically
        reason_fields_with_aggregate_function_override: Set of reason fields that
            have a defined aggregate function override in
            reasons_aggregate_function_override. If duplicate ARRAY type fields are
            found in sub-criteria, the field MUST have an override.

    """
    reasons_fields_by_name = defaultdict(list)
    for sub_criteria in sub_criteria_list:
        for field in sub_criteria.reasons_fields:
            reasons_fields_by_name[field.name].append(
                (field, sub_criteria.criteria_name)
            )

    for field_name, fields_list in reasons_fields_by_name.items():
        # Skip if there is only 1 criterion with the reason field name
        if len(fields_list) == 1:
            continue

        sub_criteria_names = ", ".join(
            [sub_criteria_name for _, sub_criteria_name in fields_list]
        )
        if field_name not in allowed_duplicate_reasons_keys:
            raise ValueError(
                f"Found reason fields with name [{field_name}] in multiple "
                f"sub-criteria of criteria group [{criteria_group_name}]: "
                f"{sub_criteria_names}. If you expect the values for this reason to "
                f"always be the same in these two sub-criteria and are ok with "
                f"arbitrarily picking one, add this reasons field to "
                f"`allowed_duplicate_reasons_keys`."
            )

        # Check that none of the duplicate reasons fields have ARRAY type
        # without a corresponding entry in `reasons_aggregate_function_override`
        for reasons_field, sub_criteria_name in fields_list:
            if (
                reasons_field.type == bigquery.enums.StandardSqlTypeNames.ARRAY
                and field_name not in reason_fields_with_aggregate_function_override
            ):
                raise ValueError(
                    f"Found ARRAY type reason fields with the name [{field_name}] "
                    f"in multiple sub-criteria of criteria group "
                    f"[{criteria_group_name}]: {sub_criteria_names}. Criteria "
                    f"groups do not support duplicate reasons keys "
                    f"with type ARRAY by default because they cannot be easily "
                    f"deduplicated. Please specify a "
                    f"reasons_aggregate_function_override for this field."
                )

    reasons_fields_list = [
        fields_list[0][0] for fields_list in reasons_fields_by_name.values()
    ]
    return sorted(reasons_fields_list, key=lambda x: x.name)


class TaskCriteriaGroupQueryBuilder:
    """Class with logic for generating a BQ query that will compose multiple
    sub-criteria into a single set of criteria results, according to the
    provided logic.
    """

    @staticmethod
    def _general_criteria_state_code_filter(
        state_code: StateCode | None,
        sub_criteria: TaskCriteriaBigQueryViewBuilder,
    ) -> str:
        """Returns the query fragment to filter a table to a specific state code if the sub-criteria is state-agnostic."""
        if state_code and isinstance(
            sub_criteria, StateAgnosticTaskCriteriaBigQueryViewBuilder
        ):
            return f'\nWHERE state_code = "{state_code.name}"'
        return ""

    @classmethod
    def _flatten_reasons_blob_clause(
        cls,
        *,
        criteria_group_name: str,
        sub_criteria_list: Sequence[TaskCriteriaBigQueryViewBuilder],
        allowed_duplicate_reasons_keys: List[str],
        reasons_aggregate_function_override: dict[str, str],
        reasons_aggregate_function_use_ordering_clause: set[str],
    ) -> str:
        """Returns query fragment that combines all fields across sub-criteria
        reason blobs into a single flat json, with an aggregation function that
        deterministically de-dupes across any duplicate reasons keys
        """

        reasons_fields = _deduplicated_reasons_fields(
            criteria_group_name=criteria_group_name,
            sub_criteria_list=sub_criteria_list,
            allowed_duplicate_reasons_keys=allowed_duplicate_reasons_keys,
            reason_fields_with_aggregate_function_override=set(
                reasons_aggregate_function_override
            ),
        )

        criteria_group_reasons_field_names = [reason.name for reason in reasons_fields]
        for reason_field_name in reasons_aggregate_function_override.keys():
            if reason_field_name not in criteria_group_reasons_field_names:
                field_names_str = ", ".join(criteria_group_reasons_field_names)
                raise ValueError(
                    f"Cannot override aggregate function for reason '{reason_field_name}' since it is not in the "
                    f"{criteria_group_name} reasons fields list: {field_names_str}"
                )
        reasons_query_fragment = ", ".join(
            [
                f"{cls._get_reason_aggregate_function(reason, reasons_aggregate_function_override)}("
                + extract_object_from_json(
                    reason.name, str(reason.type.value), "reason_v2"
                )
                + f"""{cls._get_reason_aggregate_ordering_clause(reason, reasons_aggregate_function_use_ordering_clause)}) AS {reason.name}"""
                for reason in reasons_fields
            ]
        )
        return reasons_query_fragment

    @staticmethod
    def _get_reason_aggregate_function(
        reason: ReasonsField, reasons_aggregate_function_override: dict[str, str]
    ) -> str:
        """Return the aggregate function to use for de-duping the provided reasons field:
        - Use the function in `reasons_aggregate_function_override` if set
        - Use ANY_VALUE for ARRAY reason types since ARRAY duplicates are not allowed across sub-criteria
        - Use MAX is used for all other reason types
        """
        if reason.name in reasons_aggregate_function_override:
            return reasons_aggregate_function_override[reason.name]
        if reason.type == bigquery.enums.StandardSqlTypeNames.ARRAY:
            return "ANY_VALUE"
        return "MAX"

    @staticmethod
    def _get_reason_aggregate_ordering_clause(
        reason: ReasonsField,
        reasons_aggregate_function_use_ordering_clause: set[str],
    ) -> str:
        """Return the ordering clause used to aggregate the reasons fields when provided"""
        if reason.name in reasons_aggregate_function_use_ordering_clause:
            if reason.type == bigquery.enums.StandardSqlTypeNames.STRING:
                return f" ORDER BY {extract_object_from_json(reason.name, reason.type.value, 'reason_v2')}"
            if reason.type == bigquery.enums.StandardSqlTypeNames.ARRAY:
                return f" ORDER BY ARRAY_TO_STRING({extract_object_from_json(reason.name, reason.type.value, 'reason_v2')}, ',')"
            raise ValueError(
                f"Unsupported type {reason.type} for ordering clause. "
                f"Only STRING and ARRAY types are supported for reasons_aggregate_function_use_ordering_clause."
            )
        return ""

    @classmethod
    def get_query_template(
        cls,
        *,
        criteria_group_name: str,
        sub_criteria_list: Sequence[TaskCriteriaBigQueryViewBuilder],
        state_code: StateCode | None,
        meets_criteria_aggregator_clause: str,
        allowed_duplicate_reasons_keys: List[str],
        reasons_aggregate_function_override: dict[str, str],
        reasons_aggregate_function_use_ordering_clause: set[str],
    ) -> str:
        """Returns a query template that performs the appropriate aggregation
        over component criteria.
        """
        # Use the static list of criteria in this group to unnest
        # 1 row per sub-session & criteria in order to coalesce the
        # criteria default value
        criteria_info_structs_str = (",\n" + " " * 8).join(
            [
                f"""STRUCT(
            "{sub_criteria.criteria_name}" AS criteria_name,
            {sub_criteria.meets_criteria_default} AS meets_criteria_default
        )"""
                for sub_criteria in sub_criteria_list
            ]
        )

        # Filter all general/state-agnostic criteria to one state for state-specific criteria groups
        criteria_queries = [
            f"""
SELECT *, "{sub_criteria.criteria_name}" AS criteria_name,
FROM `{{project_id}}.{sub_criteria.table_for_query.to_str()}`{cls._general_criteria_state_code_filter(state_code, sub_criteria)}
"""
            for sub_criteria in sub_criteria_list
        ]
        criteria_query_union_fragment = "UNION ALL".join(criteria_queries)

        flatten_reasons_blob_clause = cls._flatten_reasons_blob_clause(
            criteria_group_name=criteria_group_name,
            sub_criteria_list=sub_criteria_list,
            allowed_duplicate_reasons_keys=allowed_duplicate_reasons_keys,
            reasons_aggregate_function_override=reasons_aggregate_function_override,
            reasons_aggregate_function_use_ordering_clause=reasons_aggregate_function_use_ordering_clause,
        )

        reasons_fields = _deduplicated_reasons_fields(
            criteria_group_name=criteria_group_name,
            sub_criteria_list=sub_criteria_list,
            allowed_duplicate_reasons_keys=allowed_duplicate_reasons_keys,
            reason_fields_with_aggregate_function_override=set(
                reasons_aggregate_function_override
            ),
        )

        # Mimic the query logic in the SingleTaskEligibilityViewBuilder to combine multiple
        # criteria that may not be perfectly overlapping
        return f"""
WITH unioned_criteria AS ({indent(criteria_query_union_fragment, FOUR_SPACES_INDENT)})
,
{create_sub_sessions_with_attributes("unioned_criteria", use_magic_date_end_dates=True)}
SELECT
    spans.state_code,
    spans.person_id,
    spans.start_date,
    {revert_nonnull_end_date_clause("spans.end_date")} AS end_date,
    {meets_criteria_aggregator_clause}(
        -- Use the `meets_criteria_default` if the sub-criteria doesn't overlap this sub-session
        COALESCE(criteria.meets_criteria, all_criteria.meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT({flatten_reasons_blob_clause})) AS reason,
{f"    {flatten_reasons_blob_clause}," if len(reasons_fields) > 0 else ""}
-- Start with the DISTINCT sub-sessions where at least 1 sub-criteria is non-null
FROM (
    SELECT DISTINCT
        state_code, person_id, start_date, end_date
    FROM sub_sessions_with_attributes
) AS spans,
-- Unnest every sub-criteria in the group so there is 1 row for each sub-criteria
-- per sub-session, along with the associated sub-criteria `meets_criteria_default` value
UNNEST
    ([
        {criteria_info_structs_str}
    ]) all_criteria
-- Left join all the sub-criteria sub-sessions back in to hydrate the `meets_criteria` column
-- and the reasons fields for cases where the sub-criteria overlaps each sub-session
LEFT JOIN
    sub_sessions_with_attributes AS criteria
USING
    (state_code, person_id, start_date, end_date, criteria_name)
GROUP BY 1, 2, 3, 4
"""


class TaskCriteriaGroupLogicType(Enum):
    OR = "OR"
    AND = "AND"


class TaskCriteriaGroupLogicConfig:
    """Class providing configuration for how to combine sub-criteria."""

    @abc.abstractmethod
    def meets_criteria_default_for_sub_criteria(
        self,
        sub_criteria_list: Sequence[TaskCriteriaBigQueryViewBuilder],
    ) -> bool:
        """Returns boolean value for meets criteria default based on the
        provided sub-criteria.
        """

    @property
    @abc.abstractmethod
    def meets_criteria_aggregator_clause(self) -> str:
        """SQL aggregator function to use on meets_criteria field to aggregate
        eligibility.
        """

    @property
    @abc.abstractmethod
    def boolean_logic_description(self) -> str:
        """
        Returns a string describing the type of boolean logic being applied
        by the group (AND, OR).
        """

    @classmethod
    def for_logic_type(
        cls, logic_type: TaskCriteriaGroupLogicType
    ) -> "TaskCriteriaGroupLogicConfig":
        if logic_type is TaskCriteriaGroupLogicType.AND:
            return _AndTaskCriteriaGroupLogicConfig()
        if logic_type is TaskCriteriaGroupLogicType.OR:
            return _OrTaskCriteriaGroupLogicConfig()

        raise ValueError(f"Unexpected logic_type [{logic_type}]")


def task_criteria_group_description(
    sub_criteria_list: Sequence[TaskCriteriaBigQueryViewBuilder],
    logic_config: TaskCriteriaGroupLogicConfig,
) -> str:
    sub_criteria_descriptions = []
    for criteria in sub_criteria_list:
        criteria_description = (
            # Adds an additional indent for every layer of nested criteria groups,
            # for readability
            indent(criteria.description, FOUR_SPACES_INDENT)
            if isinstance(
                criteria,
                (
                    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
                    StateSpecificTaskCriteriaGroupBigQueryViewBuilder,
                ),
            )
            else criteria.description
        )
        sub_criteria_descriptions.append(
            f" - {criteria.criteria_name}: {criteria_description}"
        )

    sub_criteria_descriptions_str = "\n".join(sub_criteria_descriptions)
    return f"""
Combines the following criteria queries using {logic_config.boolean_logic_description} logic:
{sub_criteria_descriptions_str}"""


@attr.define
class _OrTaskCriteriaGroupLogicConfig(TaskCriteriaGroupLogicConfig):
    """Class that represents an OR boolean relationship between sub-criteria"""

    def meets_criteria_default_for_sub_criteria(
        self,
        sub_criteria_list: Sequence[TaskCriteriaBigQueryViewBuilder],
    ) -> bool:
        # Meets criteria default is true if default is true for at least one
        # sub-criteria.
        return any(criteria.meets_criteria_default for criteria in sub_criteria_list)

    @property
    def meets_criteria_aggregator_clause(self) -> str:
        return "LOGICAL_OR"

    @property
    def boolean_logic_description(self) -> str:
        return "OR"


@attr.define
class _AndTaskCriteriaGroupLogicConfig(TaskCriteriaGroupLogicConfig):
    """Class that represents AND boolean relationship between sub-criteria"""

    def meets_criteria_default_for_sub_criteria(
        self,
        sub_criteria_list: Sequence[TaskCriteriaBigQueryViewBuilder],
    ) -> bool:
        # Meets criteria default is true only if default is true for all sub-criteria
        return all(criteria.meets_criteria_default for criteria in sub_criteria_list)

    @property
    def meets_criteria_aggregator_clause(self) -> str:
        return "LOGICAL_AND"

    @property
    def boolean_logic_description(self) -> str:
        return "AND"


class StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
    StateAgnosticTaskCriteriaBigQueryViewBuilder
):
    """
    A builder for a view that defines spans of time during which someone satisfies a
    collection of state-agnostic criteria specified by sub_criteria_list.

    Users must specify whether all criteria need to be met (AND logic) or
    at least one (OR logic) for the criteria to be satisfied.
    """

    def __init__(
        self,
        *,
        criteria_name: str,
        logic_type: TaskCriteriaGroupLogicType,
        sub_criteria_list: list[StateAgnosticTaskCriteriaBigQueryViewBuilder],
        allowed_duplicate_reasons_keys: List[str] | None = None,
        reasons_aggregate_function_override: dict[str, str] | None = None,
        reasons_aggregate_function_use_ordering_clause: set[str] | None = None,
    ) -> None:
        """
        Args:
            criteria_name: The name of this criteria view
            logic_type: Specifies the logic we use to combine sub-criteria.
            sub_criteria_list: The list of criteria to combine.
            allowed_duplicate_reasons_keys: List of reasons fields that we expect to
                appear in multiple sub-criteria reasons blobs. If duplicate is allowed
                for a given key, we assume that values agree and select one of the
                values deterministically.
            reasons_aggregate_function_override: Map of reasons field name to aggregate
                function string for de-duplicating the reasons field across multiple
                criteria. Used to override the default aggregation function:
                `ANY_VALUE` for arrays, `MAX` for all other types
            reasons_aggregate_function_use_ordering_clause: Map of reasons field name to
                ordering clauses for ordering aggregation functions when necessary to
                ensure the views are deterministic
        """

        allowed_duplicate_reasons_keys = allowed_duplicate_reasons_keys or []
        reasons_aggregate_function_override = reasons_aggregate_function_override or {}
        reasons_aggregate_function_use_ordering_clause = (
            reasons_aggregate_function_use_ordering_clause or set()
        )

        logic_config = TaskCriteriaGroupLogicConfig.for_logic_type(logic_type)
        query_template = TaskCriteriaGroupQueryBuilder.get_query_template(
            criteria_group_name=criteria_name,
            sub_criteria_list=sub_criteria_list,
            state_code=None,
            meets_criteria_aggregator_clause=logic_config.meets_criteria_aggregator_clause,
            allowed_duplicate_reasons_keys=allowed_duplicate_reasons_keys,
            reasons_aggregate_function_override=reasons_aggregate_function_override,
            reasons_aggregate_function_use_ordering_clause=reasons_aggregate_function_use_ordering_clause,
        )
        super().__init__(
            criteria_name=criteria_name,
            description=task_criteria_group_description(
                sub_criteria_list, logic_config
            ),
            reasons_fields=_deduplicated_reasons_fields(
                criteria_group_name=criteria_name,
                sub_criteria_list=sub_criteria_list,
                allowed_duplicate_reasons_keys=allowed_duplicate_reasons_keys,
                reason_fields_with_aggregate_function_override=set(
                    reasons_aggregate_function_override
                ),
            ),
            meets_criteria_default=logic_config.meets_criteria_default_for_sub_criteria(
                sub_criteria_list
            ),
            criteria_spans_query_template=query_template,
        )
        self.sub_criteria_list = sub_criteria_list
        self.logic_config = logic_config

    def get_descendant_criteria(
        self,
    ) -> set[StateAgnosticTaskCriteriaBigQueryViewBuilder]:
        all_descendant_criteria = set(self.sub_criteria_list)
        for sub_criteria in self.sub_criteria_list:
            all_descendant_criteria.update(sub_criteria.get_descendant_criteria())

        return all_descendant_criteria


class StateSpecificTaskCriteriaGroupBigQueryViewBuilder(
    StateSpecificTaskCriteriaBigQueryViewBuilder
):
    """
    A builder for a view that defines spans of time during which someone satisfies a
    collection of criteria specified by sub_criteria_list, where at least one
    sub-criteria is a state-specific criteria.

    Users must specify whether all criteria need to be met (AND logic) or
    at least one (OR logic) for the criteria to be satisfied.
    """

    def __init__(
        self,
        *,
        criteria_name: str,
        logic_type: TaskCriteriaGroupLogicType,
        sub_criteria_list: list[TaskCriteriaBigQueryViewBuilder],
        allowed_duplicate_reasons_keys: List[str] | None = None,
        reasons_aggregate_function_override: dict[str, str] | None = None,
        reasons_aggregate_function_use_ordering_clause: set[str] | None = None,
    ) -> None:
        """
        Args:
            criteria_name: The name of this criteria view
            logic_type: Specifies the logic we use to combine sub-criteria.
            sub_criteria_list: The list of criteria to combine.
            allowed_duplicate_reasons_keys: List of reasons fields that we expect to
                appear in multiple sub-criteria reasons blobs. If duplicate is allowed
                for a given key, we assume that values agree and select one of the
                values deterministically.
            reasons_aggregate_function_override: Map of reasons field name to aggregate
                function string for de-duplicating the reasons field across multiple
                criteria. Used to override the default aggregation function:
                `ANY_VALUE` for arrays, `MAX` for all other types
            reasons_aggregate_function_use_ordering_clause: Map of reasons field name to
                ordering clauses for ordering aggregation functions when necessary to
                ensure the views are deterministic
        """

        state_codes = {
            criteria.state_code
            for criteria in sub_criteria_list
            if isinstance(criteria, StateSpecificTaskCriteriaBigQueryViewBuilder)
        }

        if len(state_codes) > 1:
            raise ValueError(
                "Can not combine state-specific criteria from more than one state code."
            )
        if len(state_codes) == 0:
            raise ValueError(
                f"Did not provide any state-specific criteria to "
                f"StateSpecificTaskCriteriaGroupBigQueryViewBuilder [{criteria_name}]. "
                f"If this composite criteria does not have any state-specific "
                f"sub-criteria, use StateAgnosticTaskCriteriaGroupBigQueryViewBuilder "
                f"instead."
            )

        state_code = one(state_codes)

        allowed_duplicate_reasons_keys = allowed_duplicate_reasons_keys or []
        reasons_aggregate_function_override = reasons_aggregate_function_override or {}
        reasons_aggregate_function_use_ordering_clause = (
            reasons_aggregate_function_use_ordering_clause or set()
        )

        logic_config = TaskCriteriaGroupLogicConfig.for_logic_type(logic_type)
        query_template = TaskCriteriaGroupQueryBuilder.get_query_template(
            criteria_group_name=criteria_name,
            sub_criteria_list=sub_criteria_list,
            state_code=state_code,
            meets_criteria_aggregator_clause=logic_config.meets_criteria_aggregator_clause,
            allowed_duplicate_reasons_keys=allowed_duplicate_reasons_keys,
            reasons_aggregate_function_override=reasons_aggregate_function_override,
            reasons_aggregate_function_use_ordering_clause=reasons_aggregate_function_use_ordering_clause,
        )
        super().__init__(
            criteria_name=criteria_name,
            description=task_criteria_group_description(
                sub_criteria_list, logic_config
            ),
            reasons_fields=_deduplicated_reasons_fields(
                criteria_group_name=criteria_name,
                sub_criteria_list=sub_criteria_list,
                allowed_duplicate_reasons_keys=allowed_duplicate_reasons_keys,
                reason_fields_with_aggregate_function_override=set(
                    reasons_aggregate_function_override
                ),
            ),
            meets_criteria_default=logic_config.meets_criteria_default_for_sub_criteria(
                sub_criteria_list
            ),
            criteria_spans_query_template=query_template,
            state_code=state_code,
        )
        self.sub_criteria_list = sub_criteria_list
        self.logic_config = logic_config

    def get_descendant_criteria(self) -> set[TaskCriteriaBigQueryViewBuilder]:
        all_descendant_criteria = set(self.sub_criteria_list)
        for sub_criteria in self.sub_criteria_list:
            all_descendant_criteria.update(sub_criteria.get_descendant_criteria())

        return all_descendant_criteria
