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
"""Defines BigQueryViewBuilders that can be used to define single criteria span views.
These views are used as inputs to a task eligibility spans view.
"""
import re
from typing import Any, List, Optional, Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    convert_cols_to_json,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField


def get_template_with_reasons_as_json(
    query_template: str,
    reasons_fields: List[ReasonsField],
    state_code: Optional[str] = None,
) -> str:
    # If no reason fields are provided, default to NULL
    reasons_query_fragment = "TO_JSON(STRUCT())"
    # Package reason fields into a json, maintaining original typing of fields
    if reasons_fields:
        reasons_query_fragment = convert_cols_to_json(
            [field.name for field in reasons_fields]
        )
    if state_code:
        state_code_query_fragment = f"\nWHERE state_code = '{state_code}'"
    else:
        state_code_query_fragment = ""
    return f"""
WITH criteria_query_base AS (
{query_template.rstrip().rstrip(";")}
)
,
_pre_sessionized AS
(
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    {reasons_query_fragment} AS reason_v2,
FROM
    criteria_query_base
)
,
_aggregated AS
(
{aggregate_adjacent_spans(
    table_name="_pre_sessionized",
    index_columns=['person_id','state_code'],
    end_date_field_name="end_date",
    attribute=['meets_criteria','reason','reason_v2'],
    struct_attribute_subset=['reason','reason_v2']
)}
)
SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    meets_criteria,
    reason,
    reason_v2
FROM _aggregated{state_code_query_fragment}
"""


def _get_reason_field_by_name(
    criteria: "TaskCriteriaBigQueryViewBuilder", reason_name: str
) -> ReasonsField:
    for reason_field in criteria.reasons_fields:
        if reason_field.name == reason_name:
            return reason_field

    raise ValueError(
        f"Criteria {criteria.criteria_name} has no reason field named {reason_name}"
    )


class StateSpecificTaskCriteriaBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """A builder for a view that defines spans of time during which someone does (or
    does not) satisfy a single criteria. This should only be used for views that contain
    state-specific logic that could not be applied generally as a criteria in multiple
    states.
    """

    def __init__(
        self,
        state_code: StateCode,
        criteria_name: str,
        criteria_spans_query_template: str,
        description: str,
        reasons_fields: List[ReasonsField],
        meets_criteria_default: bool = False,
        # TODO(#14311): Add arguments to allow bounding the policy to specific dates
        #  and use those values in the span-collapsing logic in the
        #  SingleTaskEligibilitySpansBigQueryViewBuilder.
        **query_format_kwargs: str,
    ) -> None:
        if criteria_name.upper() != criteria_name:
            raise ValueError(f"Criteria name [{criteria_name}] must be upper case.")

        state_code_prefix = f"{state_code.value}_"
        if not criteria_name.startswith(state_code_prefix):
            raise ValueError(
                f"Found state-specific task criteria [{criteria_name}] whose name "
                f"does not start with [{state_code_prefix}]."
            )
        view_id = criteria_name.removeprefix(state_code_prefix).lower()
        super().__init__(
            dataset_id=f"task_eligibility_criteria_{state_code.value.lower()}",
            view_id=view_id,
            description=description,
            view_query_template=get_template_with_reasons_as_json(
                query_template=criteria_spans_query_template,
                reasons_fields=reasons_fields,
                state_code=state_code.value,
            ),
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
            materialized_table_schema=None,
            **query_format_kwargs,
        )
        self.state_code = state_code
        self.criteria_name = criteria_name
        self.meets_criteria_default = meets_criteria_default
        self.reasons_fields = reasons_fields

    def get_descendant_criteria(self) -> set["TaskCriteriaBigQueryViewBuilder"]:
        """Returns all the criteria that are descendants (sub-criteria) of this
        criterion, if this is a complex criterion.
        """
        return set()

    def get_reason_field_from_name(self, reason_name: str) -> ReasonsField:
        """Return the reason field object with the corresponding name"""
        return _get_reason_field_by_name(self, reason_name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, StateSpecificTaskCriteriaBigQueryViewBuilder):
            return False
        return (
            self.criteria_name == other.criteria_name
            and self.description == other.description
            and self.view_query_template == other.view_query_template
            and self.state_code == other.state_code
            and tuple(self.reasons_fields) == tuple(other.reasons_fields)
            and self.meets_criteria_default == other.meets_criteria_default
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.criteria_name,
                self.description,
                self.view_query_template,
                self.state_code,
                tuple(self.reasons_fields),
                self.meets_criteria_default,
            )
        )


class StateAgnosticTaskCriteriaBigQueryViewBuilder(SimpleBigQueryViewBuilder):
    """A builder for a view that defines spans of time during which someone does (or
    does not) satisfy a single criteria. This should only be used for views that contain
    NO state-specific logic and could be reused as a criteria in multiple states.
    """

    def __init__(
        self,
        criteria_name: str,
        criteria_spans_query_template: str,
        description: str,
        reasons_fields: List[ReasonsField],
        meets_criteria_default: bool = False,
        **query_format_kwargs: str,
    ) -> None:
        if criteria_name.upper() != criteria_name:
            raise ValueError(f"Criteria name [{criteria_name}] must be upper case.")

        if match := re.match(r"^(US_[A-Z]{2})_.*", criteria_name):
            state_code = match.group(1)
            raise ValueError(
                f"Found state-agnostic task criteria [{criteria_name}] whose name "
                f"starts with state_code [{state_code}]. This criteria should be "
                f"renamed to have a state-agnostic name."
            )

        super().__init__(
            dataset_id="task_eligibility_criteria_general",
            view_id=criteria_name.lower(),
            description=description,
            view_query_template=get_template_with_reasons_as_json(
                query_template=criteria_spans_query_template,
                reasons_fields=reasons_fields,
            ),
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=None,
            time_partitioning=None,
            materialized_table_schema=None,
            **query_format_kwargs,
        )
        self.criteria_name = criteria_name
        self.meets_criteria_default = meets_criteria_default
        self.reasons_fields = reasons_fields

    def get_descendant_criteria(
        self,
    ) -> set["StateAgnosticTaskCriteriaBigQueryViewBuilder"]:
        """Returns all the criteria that are descendants (sub-criteria) of this
        criterion, if this is a complex criterion.
        """
        return set()

    def get_reason_field_from_name(self, reason_name: str) -> ReasonsField:
        """Return the reason field object with the corresponding name"""
        return _get_reason_field_by_name(self, reason_name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, StateAgnosticTaskCriteriaBigQueryViewBuilder):
            return False
        return (
            self.criteria_name == other.criteria_name
            and self.description == other.description
            and self.view_query_template == other.view_query_template
            and tuple(self.reasons_fields) == tuple(other.reasons_fields)
            and self.meets_criteria_default == other.meets_criteria_default
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.criteria_name,
                self.description,
                self.view_query_template,
                tuple(self.reasons_fields),
                self.meets_criteria_default,
            )
        )


TaskCriteriaBigQueryViewBuilder = Union[
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
]
