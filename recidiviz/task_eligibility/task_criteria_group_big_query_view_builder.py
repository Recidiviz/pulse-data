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
from functools import cached_property
from textwrap import indent
from typing import List, Optional, Union

import attr
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
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


@attr.define
class TaskCriteriaGroupBigQueryViewBuilder:
    """
    A builder for a view that defines spans of time during which
    someone satisfies a collection of criteria specified by sub_criteria_list.
    Subclasses define whether all criteria need to be met (AND logic) or
    at least one (OR logic) for the criteria to be satisfied.
    """

    # Name of criteria group - this name will appear as the criteria name
    # for any downstream task eligibility queries.
    criteria_name: str

    # List of all criteria or boolean criteria blocks that make up the group
    sub_criteria_list: List[
        Union[
            TaskCriteriaBigQueryViewBuilder,
            "TaskCriteriaGroupBigQueryViewBuilder",
            "InvertedTaskCriteriaBigQueryViewBuilder",
        ]
    ]

    # List of reasons fields that we expect to appear in multiple sub-criteria
    # reasons blobs. If duplicate is allowed for a given key, we assume that
    # values agree and select one of the values deterministically
    allowed_duplicate_reasons_keys: List[str]

    @property
    @abc.abstractmethod
    def meets_criteria_aggregator_clause(self) -> str:
        """SQL aggregator function to use on meets_criteria field to aggregate
        eligibility.
        """

    @property
    @abc.abstractmethod
    def meets_criteria_default(self) -> bool:
        """Returns boolean value for meets criteria default based on the
        underlying sub-criteria.
        """

    @property
    @abc.abstractmethod
    def boolean_logic_description(self) -> str:
        """
        Returns a string describing the type of boolean logic being applied
        by the group (AND, OR).
        """

    @property
    def reasons_fields(self) -> List[ReasonsField]:
        """Returns the list of ALL reasons attributes based on a recursive
        search of the sub-criteria.
        """
        reasons_fields_by_name = defaultdict(list)
        for sub_criteria in self.sub_criteria_list:
            # TODO(#29664): Remove this if-check once reasons_fields is non-optional
            if sub_criteria.reasons_fields is not None:
                for field in sub_criteria.reasons_fields:
                    reasons_fields_by_name[field.name].append(
                        (field, sub_criteria.criteria_name)
                    )

        for field_name, fields_list in reasons_fields_by_name.items():
            if (
                len(fields_list) == 1
                or field_name in self.allowed_duplicate_reasons_keys
            ):
                continue

            sub_criteria_names = [
                sub_criteria_name for _field, sub_criteria_name in fields_list
            ]
            raise ValueError(
                f"Found reason fields with name [{field_name}] in multiple "
                f"sub-criteria of criteria group [{self.criteria_name}]: "
                f"{sub_criteria_names}. If you expect the values for this reason to "
                f"always be the same in these two sub-criteria and are ok with "
                f"arbitrarily picking one, add this reasons field to "
                f"`allowed_duplicate_reasons_keys`."
            )
        reasons_fields_list = [
            fields_list[0][0] for fields_list in reasons_fields_by_name.values()
        ]
        return sorted(reasons_fields_list, key=lambda x: x.name)

    def flatten_reasons_blob_clause(self) -> str:
        """Returns query fragment that combines all fields across sub-criteria
        reason blobs into a single flat json, with an aggregation function that
        deterministically dedupes across any duplicate reasons keys.
        """
        reasons_query_fragment = ", ".join(
            [
                f"""MAX({extract_object_from_json(reason.name, reason.type.value, "reason_v2")}) AS {reason.name}"""
                for reason in self.reasons_fields
            ]
        )
        return reasons_query_fragment

    def general_criteria_state_code_filter(
        self,
        sub_criteria: Union[
            TaskCriteriaBigQueryViewBuilder,
            "TaskCriteriaGroupBigQueryViewBuilder",
            "InvertedTaskCriteriaBigQueryViewBuilder",
        ],
    ) -> str:
        """Returns the query fragment to filter a table to a specific state code if the sub-criteria is state-agnostic."""
        if self.state_code and isinstance(
            sub_criteria, StateAgnosticTaskCriteriaBigQueryViewBuilder
        ):
            return f'\nWHERE state_code = "{self.state_code.name}"'
        return ""

    def get_query_template(self) -> str:
        """Returns a query template that performs the appropriate aggregation
        over component criteria.
        """
        # Filter all general/state-agnostic criteria to one state for state-specific criteria groups
        criteria_queries = [
            f"""
SELECT *, {sub_criteria.meets_criteria_default} AS meets_criteria_default
FROM `{{project_id}}.{sub_criteria.table_for_query.to_str()}`{self.general_criteria_state_code_filter(sub_criteria)}
"""
            for sub_criteria in self.sub_criteria_list
        ]
        criteria_query_union_fragment = "UNION ALL".join(criteria_queries)
        return f"""
WITH unioned_criteria AS ({indent(criteria_query_union_fragment, FOUR_SPACES_INDENT)})
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    {self.meets_criteria_aggregator_clause}(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT({self.flatten_reasons_blob_clause()})) AS reason,
{f"    {self.flatten_reasons_blob_clause()}," if len(self.reasons_fields) > 0 else ""}
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""

    def sub_criteria(self) -> List[TaskCriteriaBigQueryViewBuilder]:
        return [
            criteria
            if isinstance(
                criteria,
                (
                    StateSpecificTaskCriteriaBigQueryViewBuilder,
                    StateAgnosticTaskCriteriaBigQueryViewBuilder,
                ),
            )
            else criteria.as_criteria_view_builder
            for criteria in self.sub_criteria_list
        ]

    @property
    def state_code(self) -> Optional[StateCode]:
        """Returns the value of the state_code associated with this
        TaskCriteriaGroup. A state_code will only be returned if 1) there is at
        least one state-specific criteria in the dependency tree, and 2) there
        is no more than one unique state_code value across all state-specific
        criteria in the dependency tree. If multiple states are found, raise an
        error.
        """

        state_codes = {
            criteria.state_code
            for criteria in self.sub_criteria()
            if isinstance(criteria, StateSpecificTaskCriteriaBigQueryViewBuilder)
        }

        if len(state_codes) > 1:
            raise ValueError(
                "Can not combine state-specific criteria from more than one state code."
            )
        if len(state_codes) == 1:
            return one(state_codes)
        return None

    @property
    def description(self) -> str:
        sub_criteria_descriptions = []
        for criteria in self.sub_criteria_list:
            criteria_description = (
                # Adds an additional indent for every layer of nested criteria groups,
                # for readability
                indent(criteria.description, FOUR_SPACES_INDENT)
                if isinstance(criteria, TaskCriteriaGroupBigQueryViewBuilder)
                else criteria.description
            )
            sub_criteria_descriptions.append(
                f" - {criteria.criteria_name}: {criteria_description}"
            )

        sub_criteria_descriptions_str = "\n".join(sub_criteria_descriptions)
        return f"""
Combines the following criteria queries using {self.boolean_logic_description} logic:
{sub_criteria_descriptions_str}"""

    @cached_property
    def as_criteria_view_builder(self) -> TaskCriteriaBigQueryViewBuilder:
        """Returns a TaskCriteriaBigQueryViewBuilder that represents the
        aggregation of the task criteria group.
        """
        if self.state_code:
            return StateSpecificTaskCriteriaBigQueryViewBuilder(
                criteria_name=self.criteria_name,
                description=self.description,
                state_code=self.state_code,
                criteria_spans_query_template=self.get_query_template(),
                meets_criteria_default=self.meets_criteria_default,
                reasons_fields=self.reasons_fields,
            )

        return StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name=self.criteria_name,
            description=self.description,
            criteria_spans_query_template=self.get_query_template(),
            meets_criteria_default=self.meets_criteria_default,
            reasons_fields=self.reasons_fields,
        )

    @property
    def table_for_query(self) -> BigQueryAddress:
        return self.as_criteria_view_builder.table_for_query

    @property
    def materialized_address(self) -> Optional[BigQueryAddress]:
        return self.as_criteria_view_builder.materialized_address

    @property
    def address(self) -> BigQueryAddress:
        """The (dataset_id, table_id) address for this view"""
        return self.as_criteria_view_builder.address

    @property
    def dataset_id(self) -> str:
        return self.address.dataset_id


@attr.define
class OrTaskCriteriaGroup(TaskCriteriaGroupBigQueryViewBuilder):
    """Class that represents an OR boolean relationship between sub-criteria"""

    @property
    def meets_criteria_default(self) -> bool:
        # Meets criteria default is true if default is true for at least one
        # sub-criteria.
        return any(
            criteria.meets_criteria_default for criteria in self.sub_criteria_list
        )

    @property
    def meets_criteria_aggregator_clause(self) -> str:
        return "LOGICAL_OR"

    @property
    def boolean_logic_description(self) -> str:
        return "OR"


@attr.define
class AndTaskCriteriaGroup(TaskCriteriaGroupBigQueryViewBuilder):
    """Class that represents AND boolean relationship between sub-criteria"""

    @property
    def meets_criteria_default(self) -> bool:
        # Meets criteria default is true only if default is true for all sub-criteria
        return all(
            criteria.meets_criteria_default for criteria in self.sub_criteria_list
        )

    @property
    def meets_criteria_aggregator_clause(self) -> str:
        return "LOGICAL_AND"

    @property
    def boolean_logic_description(self) -> str:
        return "AND"


@attr.define
class InvertedTaskCriteriaBigQueryViewBuilder:
    """Class that represents an inversion of a sub-criteria (NOT boolean
    logic).
    """

    sub_criteria: Union[
        TaskCriteriaBigQueryViewBuilder,
        TaskCriteriaGroupBigQueryViewBuilder,
    ]

    @property
    def criteria_name(self) -> str:
        """Converts the sub-criteria name into an inverted name with NOT
        prepended.

        Examples:
            HAS_POSITIVE_DRUG_SCREEN => NOT_HAS_POSITIVE_DRUG_SCREEN
            US_XX_HAS_POSITIVE_DRUG_SCREEN => US_XX_NOT_HAS_POSITIVE_DRUG_SCREEN
        """
        if isinstance(self.sub_criteria, StateSpecificTaskCriteriaBigQueryViewBuilder):
            state_code_prefix = f"{self.sub_criteria.state_code.value}_"
            base_name = self.sub_criteria.criteria_name.removeprefix(state_code_prefix)
            return f"{state_code_prefix}NOT_{base_name}"
        return f"NOT_{self.sub_criteria.criteria_name}"

    @property
    def meets_criteria_default(self) -> bool:
        """Returns the opposite of the meets_criteria_default value for the
        sub-criteria.
        """
        return not self.sub_criteria.meets_criteria_default

    @property
    def reasons_fields(self) -> Optional[List[ReasonsField]]:
        """Returns the reasons fields of the inverted sub-criteria"""
        return self.sub_criteria.reasons_fields

    @property
    def state_code(self) -> Optional[StateCode]:
        """Returns the value of the state_code associated with this
        InvertedTaskCriteriaBigQueryViewBuilder. A state_code will only be
        returned if the inverted task criteria is state-specific.
        """
        if isinstance(self.sub_criteria, StateSpecificTaskCriteriaBigQueryViewBuilder):
            return self.sub_criteria.state_code
        return None

    @property
    def description(self) -> str:
        return (
            f"A criteria that is met for every period of time when the "
            f"{self.sub_criteria.criteria_name} criteria is not met, and vice versa."
        )

    @cached_property
    def as_criteria_view_builder(self) -> TaskCriteriaBigQueryViewBuilder:
        """Returns a TaskCriteriaBigQueryViewBuilder that represents the
        aggregation of the task criteria group.
        """
        sub_criteria = self._sub_criteria_as_view_builder()
        if isinstance(sub_criteria, StateSpecificTaskCriteriaBigQueryViewBuilder):
            return StateSpecificTaskCriteriaBigQueryViewBuilder(
                criteria_name=self.criteria_name,
                description=self.description,
                state_code=sub_criteria.state_code,
                criteria_spans_query_template=self.get_query_template(),
                meets_criteria_default=self.meets_criteria_default,
                reasons_fields=self.sub_criteria.reasons_fields,
            )

        return StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name=self.criteria_name,
            description=self.description,
            criteria_spans_query_template=self.get_query_template(),
            meets_criteria_default=self.meets_criteria_default,
            reasons_fields=self.sub_criteria.reasons_fields,
        )

    @property
    def table_for_query(self) -> BigQueryAddress:
        return self.as_criteria_view_builder.table_for_query

    @property
    def materialized_address(self) -> Optional[BigQueryAddress]:
        return self.as_criteria_view_builder.materialized_address

    @property
    def address(self) -> BigQueryAddress:
        """The (dataset_id, table_id) address for this view"""
        return self.as_criteria_view_builder.address

    @property
    def dataset_id(self) -> str:
        return self.address.dataset_id

    def get_query_template(self) -> str:
        """Returns a query template that inverts the meets criteria values."""
        sub_criteria = self._sub_criteria_as_view_builder()

        # TODO(#29664): Remove this if-check once reasons_fields is non-optional
        if sub_criteria.reasons_fields is not None:
            reason_columns = (
                "\n    "
                + ",\n    ".join(
                    [
                        f'{extract_object_from_json(reason.name, reason.type.value, "reason_v2")} AS {reason.name}'
                        for reason in sub_criteria.reasons_fields
                    ]
                )
                + ","
            )
        else:
            reason_columns = ""
        return f"""
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,{reason_columns}
FROM
    `{{project_id}}.{sub_criteria.table_for_query.to_str()}`
"""

    def _sub_criteria_as_view_builder(self) -> TaskCriteriaBigQueryViewBuilder:
        if isinstance(
            self.sub_criteria,
            (
                StateSpecificTaskCriteriaBigQueryViewBuilder,
                StateAgnosticTaskCriteriaBigQueryViewBuilder,
            ),
        ):
            return self.sub_criteria
        if isinstance(self.sub_criteria, TaskCriteriaGroupBigQueryViewBuilder):
            return self.sub_criteria.as_criteria_view_builder

        raise TypeError(
            f"Inverted sub_criteria is not of a supported type: "
            f"[{type(self.sub_criteria)}]"
        )
