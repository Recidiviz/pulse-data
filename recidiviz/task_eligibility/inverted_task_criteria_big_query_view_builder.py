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
from typing import Union

from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    TaskCriteriaGroupBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)


def inverted_criteria_name(
    sub_criteria: Union[
        TaskCriteriaBigQueryViewBuilder,
        "TaskCriteriaGroupBigQueryViewBuilder",
    ]
) -> str:
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


def inverted_criteria_description(
    sub_criteria: Union[
        TaskCriteriaBigQueryViewBuilder,
        "TaskCriteriaGroupBigQueryViewBuilder",
    ]
) -> str:
    return (
        f"A criteria that is met for every period of time when the "
        f"{sub_criteria.criteria_name} criteria is not met, and vice versa."
    )


def inverted_meets_criteria_default(
    sub_criteria: Union[
        TaskCriteriaBigQueryViewBuilder,
        "TaskCriteriaGroupBigQueryViewBuilder",
    ]
) -> bool:
    """Returns the opposite of the meets_criteria_default value for the
    sub-criteria.
    """
    return not sub_criteria.meets_criteria_default


def inverted_criteria_query_template(
    sub_criteria: Union[
        TaskCriteriaBigQueryViewBuilder,
        "TaskCriteriaGroupBigQueryViewBuilder",
    ]
) -> str:
    """Returns a query template that inverts the meets criteria values."""
    sub_criteria = _sub_criteria_as_view_builder(sub_criteria)

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


# TODO(#41711): This function should go away entirely once all criteria groups and
#  inverted criteria are subclasses of StateSpecificTaskCriteriaBigQueryViewBuilder
#  and StateAgnosticTaskCriteriaBigQueryViewBuilder
def _sub_criteria_as_view_builder(
    sub_criteria: Union[
        TaskCriteriaBigQueryViewBuilder,
        "TaskCriteriaGroupBigQueryViewBuilder",
    ]
) -> TaskCriteriaBigQueryViewBuilder:
    if isinstance(
        sub_criteria,
        (
            StateSpecificTaskCriteriaBigQueryViewBuilder,
            StateAgnosticTaskCriteriaBigQueryViewBuilder,
        ),
    ):
        return sub_criteria
    if hasattr(sub_criteria, "as_criteria_view_builder"):
        return sub_criteria.as_criteria_view_builder

    raise TypeError(
        f"Inverted sub_criteria is not of a supported type: " f"[{type(sub_criteria)}]"
    )


class StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder(
    StateAgnosticTaskCriteriaBigQueryViewBuilder
):
    """Criteria view builder that represents an inversion of a state-agnostic
    sub-criteria (NOT boolean logic).
    """

    def __init__(
        self,
        *,
        sub_criteria: Union[
            StateAgnosticTaskCriteriaBigQueryViewBuilder,
            # TODO(#41711): Eventually remove this type once we have
            #  StateAgnosticTaskCriteriaGroupBigQueryViewBuilder which extends
            #  StateAgnosticTaskCriteriaBigQueryViewBuilder
            "TaskCriteriaGroupBigQueryViewBuilder",
        ],
    ) -> None:

        if (
            isinstance(sub_criteria, TaskCriteriaGroupBigQueryViewBuilder)
            and sub_criteria.state_code
        ):
            raise ValueError(
                f"Building StateAgnosticInvertedTaskCriteriaBigQueryViewBuilder with "
                f"state-specific criteria [{sub_criteria.criteria_name}]. Must provide "
                f"a state-agnostic criteria."
            )

        super().__init__(
            criteria_name=inverted_criteria_name(sub_criteria),
            description=inverted_criteria_description(sub_criteria),
            reasons_fields=sub_criteria.reasons_fields,
            meets_criteria_default=inverted_meets_criteria_default(sub_criteria),
            criteria_spans_query_template=inverted_criteria_query_template(
                sub_criteria
            ),
        )
        self.sub_criteria = sub_criteria


class StateSpecificInvertedTaskCriteriaBigQueryViewBuilder(
    StateSpecificTaskCriteriaBigQueryViewBuilder
):
    """Criteria view builder that represents an inversion of a state-specific
    sub-criteria (NOT boolean logic).
    """

    def __init__(
        self,
        *,
        sub_criteria: Union[
            StateSpecificTaskCriteriaBigQueryViewBuilder,
            # TODO(#41711): Eventually remove this type once we have
            #  StateAgnosticTaskCriteriaGroupBigQueryViewBuilder which extends
            #  StateAgnosticTaskCriteriaBigQueryViewBuilder
            "TaskCriteriaGroupBigQueryViewBuilder",
        ],
    ) -> None:

        if not sub_criteria.state_code:
            raise ValueError(
                f"Building StateSpecificInvertedTaskCriteriaBigQueryViewBuilder with "
                f"state-agnostic criteria [{sub_criteria.criteria_name}]. Must provide "
                f"a state-specific criteria."
            )

        super().__init__(
            criteria_name=inverted_criteria_name(sub_criteria),
            description=inverted_criteria_description(sub_criteria),
            reasons_fields=sub_criteria.reasons_fields,
            meets_criteria_default=inverted_meets_criteria_default(sub_criteria),
            criteria_spans_query_template=inverted_criteria_query_template(
                sub_criteria
            ),
            state_code=sub_criteria.state_code,
        )
        self.sub_criteria = sub_criteria
