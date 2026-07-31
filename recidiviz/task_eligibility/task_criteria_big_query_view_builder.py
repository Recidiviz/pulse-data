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
from typing import Any, List, Optional, Sequence, Union

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import (
    BigQueryViewColumn,
    Bool,
    Date,
    Integer,
    Json,
    SqlFieldMode,
    String,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    convert_cols_to_json,
)
from recidiviz.calculator.query.state.views.tasks.contact_type import ContactType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    TASK_ELIGIBILITY_CRITERIA_GENERAL,
    task_eligibility_criteria_state_specific_dataset,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField


def _build_reason_v2_description(reasons_fields: List[ReasonsField]) -> str:
    """Builds a description for the reason_v2 column from the list of
    ReasonsField objects defined by a criteria builder."""
    if not reasons_fields:
        return "Structured JSON reason blob (v2 format). No fields defined for this criteria."
    fields_desc = "; ".join(
        f"{f.name} ({f.type.name}): {f.description}" for f in reasons_fields
    )
    return f"Structured JSON reason blob (v2 format). Fields: {fields_desc}"


def span_with_reasons_schema(
    *,
    value_column: BigQueryViewColumn,
    reasons_fields: List[ReasonsField],
    span_label: str,
    state_code_mode: SqlFieldMode,
    person_id_mode: SqlFieldMode,
) -> Sequence[BigQueryViewColumn]:
    """Returns the shared schema for a "span with reasons" view: the base span
    keys, a caller-supplied |value_column|, and the reason / reason_v2 JSON
    blobs. |span_label| names the span in the column descriptions (e.g.
    "criteria span"). Shared by the criteria schema (value = meets_criteria) and
    the classification score component schema (value = component_score)."""
    return [
        String(
            name="state_code",
            mode=state_code_mode,
            description=f"The state code for this {span_label}",
        ),
        Integer(
            name="person_id",
            mode=person_id_mode,
            description=f"The person ID for this {span_label}",
        ),
        Date(
            name="start_date",
            mode="REQUIRED",
            description=f"Start date of the {span_label} (inclusive)",
        ),
        Date(
            name="end_date",
            mode="NULLABLE",
            description=f"End date of the {span_label} (exclusive). NULL means the span is ongoing.",
        ),
        value_column,
        Json(
            name="reason",
            mode="NULLABLE",
            description="Legacy JSON reason blob. Prefer reason_v2 for structured access.",
        ),
        Json(
            name="reason_v2",
            mode="REQUIRED",
            description=_build_reason_v2_description(reasons_fields),
        ),
    ]


def task_criteria_schema(
    reasons_fields: List[ReasonsField] | None = None,
) -> Sequence[BigQueryViewColumn]:
    """Returns the schema for a criteria view. When |reasons_fields| is
    provided, the reason_v2 column description enumerates the fields."""
    return span_with_reasons_schema(
        # Some criteria produce NULL meets_criteria values.
        value_column=Bool(
            name="meets_criteria",
            mode="NULLABLE",
            description="Whether the person meets the criteria during this span",
        ),
        reasons_fields=reasons_fields or [],
        span_label="criteria span",
        # TODO(#62322): make state_code, person_id and meets_criteria REQUIRED after there are no more nulls in
        # task_eligibility_criteria_general.at_least_2_months_since_negative_drug_test_streak_began
        state_code_mode="NULLABLE",
        person_id_mode="NULLABLE",
    )


def get_template_with_reasons_as_json(
    query_template: str,
    reasons_fields: List[ReasonsField],
    state_code: Optional[str] = None,
    # Name of the span-attribute column the base query emits. Defaults to the
    # criteria contract's boolean `meets_criteria`; the classification score
    # component builder passes an integer `component_score` instead.
    value_column_name: str = "meets_criteria",
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
    {value_column_name},
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
    attribute=[value_column_name,'reason','reason_v2'],
    struct_attribute_subset=['reason','reason_v2']
)}
)
SELECT 
    state_code,
    person_id,
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date AS DATE) AS end_date,
    {value_column_name},
    reason,
    reason_v2
FROM _aggregated{state_code_query_fragment}
"""


def _validated_contact_types(
    contact_types: Sequence[ContactType] | None,
) -> list[ContactType]:
    """Returns the contact types as a non-null list, raising if any element is not
    a ContactType member."""
    contact_types = list(contact_types or [])
    if any(not isinstance(ct, ContactType) for ct in contact_types):
        raise ValueError(
            f"All contact_types must be ContactType members; got [{contact_types}]."
        )
    return contact_types


def validate_and_derive_state_specific_view_id(
    *,
    state_code: StateCode,
    name: str,
    name_type_label: str,
    entity_description: str,
) -> str:
    """Validates that |name| is upper case and prefixed with |state_code|, then
    returns the derived (state-prefix-stripped, lower-cased) view id.

    |name_type_label| names the identifier in the upper-case error (e.g.
    "Criteria" → "Criteria name [...] must be upper case."); |entity_description|
    names the entity in the prefix error (e.g. "state-specific task criteria").
    Shared by the state-specific criteria builder and the classification score
    component builder.
    """
    if name.upper() != name:
        raise ValueError(f"{name_type_label} name [{name}] must be upper case.")
    state_code_prefix = f"{state_code.value}_"
    if not name.startswith(state_code_prefix):
        raise ValueError(
            f"Found {entity_description} [{name}] whose name "
            f"does not start with [{state_code_prefix}]."
        )
    return name.removeprefix(state_code_prefix).lower()


def get_reason_field_by_name(
    reasons_fields: Sequence[ReasonsField], reason_name: str, builder_label: str
) -> ReasonsField:
    """Returns the reason field with the given name, raising a |builder_label|-
    qualified error if none matches. Shared by the criteria and classification
    score component builders."""
    for reason_field in reasons_fields:
        if reason_field.name == reason_name:
            return reason_field

    raise ValueError(f"{builder_label} has no reason field named {reason_name}")


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
        # The contact types (from `<state>_contact_events_preprocessed`) that
        # satisfy / close out this criterion. Empty for criteria not tied to
        # specific contact types.
        contact_types: Sequence[ContactType] | None = None,
        # Whether only COMPLETED contacts close out this criterion. Defaults to True;
        # set to False for criteria whose triggers count non-completed contacts (e.g.
        # an attempted contact that nonetheless verified employment).
        only_include_completed_contacts: bool = True,
        # TODO(#14311): Add arguments to allow bounding the policy to specific dates
        #  and use those values in the span-collapsing logic in the
        #  SingleTaskEligibilitySpansBigQueryViewBuilder.
        **query_format_kwargs: str,
    ) -> None:
        view_id = validate_and_derive_state_specific_view_id(
            state_code=state_code,
            name=criteria_name,
            name_type_label="Criteria",
            entity_description="state-specific task criteria",
        )
        super().__init__(
            dataset_id=task_eligibility_criteria_state_specific_dataset(state_code),
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
            schema=task_criteria_schema(reasons_fields),
            **query_format_kwargs,
        )
        self.state_code = state_code
        self.criteria_name = criteria_name
        self.meets_criteria_default = meets_criteria_default
        self.reasons_fields = reasons_fields
        self.contact_types = _validated_contact_types(contact_types)
        self.only_include_completed_contacts = only_include_completed_contacts

    def get_descendant_criteria(self) -> set["TaskCriteriaBigQueryViewBuilder"]:
        """Returns all the criteria that are descendants (sub-criteria) of this
        criterion, if this is a complex criterion.
        """
        return set()

    def get_reason_field_from_name(self, reason_name: str) -> ReasonsField:
        """Return the reason field object with the corresponding name"""
        return get_reason_field_by_name(
            self.reasons_fields, reason_name, f"Criteria {self.criteria_name}"
        )

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
        # The contact types (from `<state>_contact_events_preprocessed`) that
        # satisfy / close out this criterion. Empty for criteria not tied to
        # specific contact types.
        contact_types: Sequence[ContactType] | None = None,
        # Whether only COMPLETED contacts close out this criterion. Defaults to True;
        # set to False for criteria whose triggers count non-completed contacts (e.g.
        # an attempted contact that nonetheless verified employment).
        only_include_completed_contacts: bool = True,
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
            dataset_id=TASK_ELIGIBILITY_CRITERIA_GENERAL,
            view_id=criteria_name.lower(),
            description=description,
            view_query_template=get_template_with_reasons_as_json(
                query_template=criteria_spans_query_template,
                reasons_fields=reasons_fields,
            ),
            should_materialize=True,
            materialized_address_override=None,
            projects_to_deploy=None,
            clustering_fields=["state_code"],
            time_partitioning=None,
            schema=task_criteria_schema(reasons_fields),
            **query_format_kwargs,
        )
        self.criteria_name = criteria_name
        self.meets_criteria_default = meets_criteria_default
        self.reasons_fields = reasons_fields
        self.contact_types = _validated_contact_types(contact_types)
        self.only_include_completed_contacts = only_include_completed_contacts

    def get_descendant_criteria(
        self,
    ) -> set["StateAgnosticTaskCriteriaBigQueryViewBuilder"]:
        """Returns all the criteria that are descendants (sub-criteria) of this
        criterion, if this is a complex criterion.
        """
        return set()

    def get_reason_field_from_name(self, reason_name: str) -> ReasonsField:
        """Return the reason field object with the corresponding name"""
        return get_reason_field_by_name(
            self.reasons_fields, reason_name, f"Criteria {self.criteria_name}"
        )

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
