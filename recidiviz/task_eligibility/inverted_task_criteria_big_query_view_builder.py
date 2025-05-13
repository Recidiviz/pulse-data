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
from functools import cached_property
from typing import TYPE_CHECKING, List, Optional, Union

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
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

if TYPE_CHECKING:
    # Import for mypy only to avoid circular imports
    from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
        TaskCriteriaGroupBigQueryViewBuilder,
    )


@attr.define
class InvertedTaskCriteriaBigQueryViewBuilder:
    """Class that represents an inversion of a sub-criteria (NOT boolean
    logic).
    """

    sub_criteria: Union[
        TaskCriteriaBigQueryViewBuilder,
        "TaskCriteriaGroupBigQueryViewBuilder",
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
    def reasons_fields(self) -> List[ReasonsField]:
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
    def _sub_criteria_as_view_builder(self) -> TaskCriteriaBigQueryViewBuilder:
        if isinstance(
            self.sub_criteria,
            (
                StateSpecificTaskCriteriaBigQueryViewBuilder,
                StateAgnosticTaskCriteriaBigQueryViewBuilder,
            ),
        ):
            return self.sub_criteria
        if hasattr(self.sub_criteria, "as_criteria_view_builder"):
            return self.sub_criteria.as_criteria_view_builder

        raise TypeError(
            f"Inverted sub_criteria is not of a supported type: "
            f"[{type(self.sub_criteria)}]"
        )
