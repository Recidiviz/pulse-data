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
"""Class that represents an inversion of a sub-criteria (NOT boolean logic)."""
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)


def inverted_criteria_name(sub_criteria: TaskCriteriaBigQueryViewBuilder) -> str:
    """Converts the sub-criteria name into an inverted name with NOT
    prepended.

    Examples:
        HAS_POSITIVE_DRUG_SCREEN => NOT_HAS_POSITIVE_DRUG_SCREEN
        US_XX_HAS_POSITIVE_DRUG_SCREEN => US_XX_NOT_HAS_POSITIVE_DRUG_SCREEN
    """
    if isinstance(sub_criteria, StateSpecificTaskCriteriaBigQueryViewBuilder):
        state_code_prefix = f"{sub_criteria.state_code.value}_"
        base_name = sub_criteria.criteria_name.removeprefix(state_code_prefix)
        return f"{state_code_prefix}NOT_{base_name}"
    return f"NOT_{sub_criteria.criteria_name}"


def inverted_criteria_description(sub_criteria: TaskCriteriaBigQueryViewBuilder) -> str:
    return (
        f"A criteria that is met for every period of time when the "
        f"{sub_criteria.criteria_name} criteria is not met, and vice versa."
    )


def _inverted_meets_criteria_default(
    sub_criteria: TaskCriteriaBigQueryViewBuilder,
    invert_meets_criteria_default: bool,
) -> bool:
    """Returns the opposite of the meets_criteria_default value for the
    sub-criteria if invert_meets_criteria_default is True, otherwise returns
    the same meets_criteria_default value as the sub-criteria.
    """
    meets_criteria_default = (
        not sub_criteria.meets_criteria_default
        if invert_meets_criteria_default
        else sub_criteria.meets_criteria_default
    )
    return meets_criteria_default


def inverted_criteria_query_template(
    sub_criteria: TaskCriteriaBigQueryViewBuilder,
) -> str:
    """Returns a query template that inverts the meets criteria values."""
    reason_columns = "\n    ".join(
        [
            f'{extract_object_from_json(reason.name, reason.type.value, "reason_v2")} AS {reason.name},'
            for reason in sub_criteria.reasons_fields
        ]
    )

    return f"""
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,
{f"    {reason_columns}" if len(sub_criteria.reasons_fields) > 0 else ""}
FROM
    `{{project_id}}.{sub_criteria.table_for_query.to_str()}`
"""


class StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
    StateAgnosticTaskCriteriaBigQueryViewBuilder
):
    """Criteria view builder that represents an inversion of a state-agnostic
    sub-criteria (NOT boolean logic).
    The invert_meets_criteria_default option can be set to False if the inverted criteria
    should use the same meets_criteria_default value as the sub-criteria for any eligibility
    spans without an overlapping inverted criteria span.
    """

    def __init__(
        self,
        *,
        sub_criteria: StateAgnosticTaskCriteriaBigQueryViewBuilder,
        invert_meets_criteria_default: bool = True,
    ) -> None:
        meets_criteria_default = _inverted_meets_criteria_default(
            sub_criteria, invert_meets_criteria_default
        )
        super().__init__(
            criteria_name=inverted_criteria_name(sub_criteria),
            description=inverted_criteria_description(sub_criteria),
            reasons_fields=sub_criteria.reasons_fields,
            meets_criteria_default=meets_criteria_default,
            criteria_spans_query_template=inverted_criteria_query_template(
                sub_criteria
            ),
        )
        self.sub_criteria = sub_criteria

    def get_descendant_criteria(
        self,
    ) -> set[StateAgnosticTaskCriteriaBigQueryViewBuilder]:
        return self.sub_criteria.get_descendant_criteria() | {self.sub_criteria}


class StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
    StateSpecificTaskCriteriaBigQueryViewBuilder
):
    """Criteria view builder that represents an inversion of a state-specific
    sub-criteria (NOT boolean logic).
    The invert_meets_criteria_default option can be set to False if the inverted criteria
    should use the same meets_criteria_default value as the sub-criteria for any eligibility
    spans without an overlapping inverted criteria span.
    """

    def __init__(
        self,
        *,
        sub_criteria: StateSpecificTaskCriteriaBigQueryViewBuilder,
        invert_meets_criteria_default: bool = True,
    ) -> None:
        meets_criteria_default = _inverted_meets_criteria_default(
            sub_criteria, invert_meets_criteria_default
        )
        super().__init__(
            criteria_name=inverted_criteria_name(sub_criteria),
            description=inverted_criteria_description(sub_criteria),
            reasons_fields=sub_criteria.reasons_fields,
            meets_criteria_default=meets_criteria_default,
            criteria_spans_query_template=inverted_criteria_query_template(
                sub_criteria
            ),
            state_code=sub_criteria.state_code,
        )
        self.sub_criteria = sub_criteria

    def get_descendant_criteria(self) -> set[TaskCriteriaBigQueryViewBuilder]:
        return self.sub_criteria.get_descendant_criteria() | {self.sub_criteria}
