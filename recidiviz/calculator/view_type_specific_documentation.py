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
"""
This module contains functions that provide additional documentation for specific types of views.
"""

import json
from typing import Union

from pytablewriter import MarkdownTableWriter

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.task_eligibility.criteria_condition import (
    LessThanCriteriaCondition,
    LessThanOrEqualCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)


def get_view_type_specific_documentation(
    view_builder: BigQueryViewBuilder,
) -> str | None:
    """Returns additional documentation for certain view builder types, if available.
    Otherwise, returns None.
    """
    if isinstance(view_builder, SingleTaskEligibilitySpansBigQueryViewBuilder):
        return _get_tes_spans_additional_documentation(view_builder)

    if isinstance(
        view_builder,
        (
            StateSpecificTaskCriteriaBigQueryViewBuilder,
            StateAgnosticTaskCriteriaBigQueryViewBuilder,
        ),
    ):
        return _get_task_criteria_additional_documentation(view_builder)
    return None


def _formatted_view_address_link(
    address: BigQueryAddress, link_name: str | None = None
) -> str:
    """Returns a formatted link to the view address of a file."""
    link_name = link_name or address.to_str()
    return f"[{link_name}](../{address.dataset_id}/{address.table_id}.md)"


def _get_task_criteria_additional_documentation(
    view_builder: TaskCriteriaBigQueryViewBuilder,
) -> str:
    """Returns additional documentation individual criteria."""

    reasons_table_writer = MarkdownTableWriter(
        headers=["Reason field", "Type", "Description"],
        value_matrix=[
            [reason.name, reason.type, reason.description]
            for reason in view_builder.reasons_fields
        ],
        # Margin values other than 0 have nondeterministic spacing. Do not change.
        margin=0,
    )

    reasons_table = reasons_table_writer.dumps()

    return f"""

Criteria name: {view_builder.criteria_name}</br>
Meets criteria default: {view_builder.meets_criteria_default}</br>

#### Reasons fields
{reasons_table}
"""


def _get_tes_spans_additional_documentation(
    view_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
) -> str:
    """Returns additional documentation for SingleTaskEligibilitySpansBigQueryViewBuilder."""

    candidate_population = view_builder.candidate_population_view_builder
    completion_event = view_builder.completion_event_builder

    return f"""
#### i. Candidate Population: {_formatted_view_address_link(candidate_population.address, link_name=candidate_population.population_name)}
{candidate_population.description}</br>

#### ii. Completion Event: {_formatted_view_address_link(completion_event.address, link_name=completion_event.task_title)}
{completion_event.description}</br>
Task type: {completion_event.task_type_name}</br>

#### iii. Eligibility criteria:
{_eligible_criteria_docs_for_tes_view(view_builder)}

#### iv. Almost eligible conditions:
{_almost_eligibile_conditions_docs_for_tes_view(view_builder)}
"""


def _format_reasons_as_pretty_json(reasons: list[ReasonsField]) -> str:
    json_str = json.dumps({r.name: r.type for r in reasons}, indent=2)
    return json_str.replace("\n", "</br>")


def _eligible_criteria_docs_for_tes_view(
    view_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
) -> str:
    table_writer = MarkdownTableWriter(
        headers=["Criteria", "Reasons structure", "Meets criteria default"],
        value_matrix=[
            [
                _formatted_view_address_link(
                    criteria_view.address, link_name=criteria_view.criteria_name
                ),
                _format_reasons_as_pretty_json(criteria_view.reasons_fields),
                criteria_view.meets_criteria_default,
            ]
            for criteria_view in view_builder.criteria_spans_view_builders
        ],
        # Margin values other than 0 have nondeterministic spacing. Do not change.
        margin=0,
    )

    return table_writer.dumps()


def _almost_eligibile_conditions_docs_for_tes_view(
    view_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
) -> str:
    """Returns additional documentation for almost eligible conditions in TES view."""
    if view_builder.almost_eligible_condition is None:
        return "No almost eligible condition defined."

    documentation = ""
    # There's only one almost eligible condition
    if isinstance(
        view_builder.almost_eligible_condition,
        (
            TimeDependentCriteriaCondition,
            NotEligibleCriteriaCondition,
            LessThanCriteriaCondition,
            LessThanOrEqualCriteriaCondition,
        ),
    ):
        ae_table_matrix = [
            [
                view_builder.almost_eligible_condition.criteria.criteria_name,
                view_builder.almost_eligible_condition.description,
            ]
        ]
        ae_table_writer = _write_table_for_almost_eligible_conditions(ae_table_matrix)
        documentation += ae_table_writer.dumps()
        return documentation
    # There are multiple almost eligible conditions
    if isinstance(
        view_builder.almost_eligible_condition, PickNCompositeCriteriaCondition
    ):
        documentation += (
            view_builder.almost_eligible_condition.description.split("\n")[0] + "\n"
        )

        # Create a table with one row for each almost eligible condition
        ae_table_matrix = []
        for condition in view_builder.almost_eligible_condition.sub_conditions_list:
            if isinstance(
                condition,
                (
                    TimeDependentCriteriaCondition,
                    NotEligibleCriteriaCondition,
                    LessThanCriteriaCondition,
                    LessThanOrEqualCriteriaCondition,
                ),
            ):
                ae_table_matrix.append(
                    [
                        condition.criteria.criteria_name,
                        condition.description,
                    ]
                )
            elif isinstance(
                view_builder.almost_eligible_condition, PickNCompositeCriteriaCondition
            ):
                description_with_br = condition.description.replace("\n", "<br>")
                ae_table_matrix.append(["Composite criteria", description_with_br])
        ae_table_writer = _write_table_for_almost_eligible_conditions(ae_table_matrix)
        documentation += ae_table_writer.dumps()
        return documentation
    raise ValueError(
        f"Unexpected almost eligible condition type: [{type(view_builder.almost_eligible_condition)}]"
    )


def _create_table_matrix_for_almost_eligible_conditions(
    almost_eligible_condition: Union[
        TimeDependentCriteriaCondition,
        NotEligibleCriteriaCondition,
        LessThanCriteriaCondition,
        LessThanOrEqualCriteriaCondition,
    ]
) -> list[list[str]]:
    """Creates a table matrix for almost eligible conditions."""
    return [
        [
            almost_eligible_condition.criteria.criteria_name,
            almost_eligible_condition.description,
        ]
    ]


def _write_table_for_almost_eligible_conditions(
    matrix: list[list[str]],
) -> MarkdownTableWriter:
    """Writes a table in Markdown format for almost eligible conditions."""
    return MarkdownTableWriter(
        headers=[
            "Criteria Name",
            "Condition",
        ],
        value_matrix=matrix,
        # Margin values other than 0 have nondeterministic spacing. Do not change.
        margin=0,
    )
