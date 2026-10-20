# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
# ============================================================================
"""Helper methods that build completion event view builders with similar logic
that can be parameterized.
"""
from recidiviz.calculator.query.state.views.classification.all_classification_assessment_events import (
    state_classification_assessment_events_query,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_CRITERIA_GENERAL
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.utils.string_formatting import fix_indent


def _is_within_intake_window_clause(intake_window_months: int) -> str:
    """Returns whether classification decision c falls within the first
    |intake_window_months| months of the state prison custody stay that
    has_initial_classification_in_state_prison_custody row i belongs to.

    The window includes the day the stay reaches |intake_window_months| months. That day
    is the exclusive end date of an intake eligibility span of that length, and TES
    marks a span TASK_COMPLETED only when a completion event falls on its end date. The
    non-intake path for someone screened that day would open and close on the same day,
    a zero-day span TES drops, so counting the decision as non-intake would leave
    neither task with a completion.
    """
    return (
        f"c.classification_decision_date <= DATE_ADD("
        f"CAST(JSON_VALUE(i.reason_v2, '$.most_recent_prison_custody_start_date') AS DATE), "
        f"INTERVAL {intake_window_months} MONTH)"
    )


def intake_classification_decision_completed_view_builder(
    *,
    state_code: StateCode,
    description: str,
    intake_window_months: int | None,
    additional_where_clause: str | None = None,
) -> StateSpecificTaskCompletionEventBigQueryViewBuilder:
    """
    Args:
        state_code (StateCode): State whose classification assessment events to read.
        description (str): Description of the completion event, passed through to the
            view builder.
        intake_window_months (int | None): If set, someone's first classification
            decision in a state prison custody stay only counts if it falls within
            this many months of the stay's start; a later one counts toward
            non_intake_classification_decision_completed_view_builder instead. If
            None, every first classification decision counts.
        additional_where_clause (str, optional): Extra condition AND'd onto the query,
            e.g. an assessment_instrument filter isolating a specific policy version.
    Returns:
        StateSpecificTaskCompletionEventBigQueryViewBuilder: Completion event firing on
            someone's first classification decision after starting or re-starting
            state-prison custody, within intake_window_months if set.
    """
    where_clauses = []
    if intake_window_months is not None:
        where_clauses.append(_is_within_intake_window_clause(intake_window_months))
    if additional_where_clause:
        where_clauses.append(additional_where_clause)
    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query_template = f"""
    SELECT
        c.state_code,
        c.person_id,
        c.classification_decision_date AS completion_event_date,
    FROM (
{fix_indent(state_classification_assessment_events_query(state_code=state_code), indent_level=8)}
    ) c
    INNER JOIN
        `{{project_id}}.{{task_eligibility_criteria_general_dataset}}.has_initial_classification_in_state_prison_custody_materialized` i
        ON c.person_id = i.person_id
        AND c.state_code = i.state_code
        AND c.classification_decision_date = i.start_date
    {where_clause}
"""
    return StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=state_code,
        completion_event_type=TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_2026_POLICY_COMPLETED,
        description=description,
        completion_event_query_template=query_template,
        task_eligibility_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    )


def non_intake_classification_decision_completed_view_builder(
    *,
    state_code: StateCode,
    description: str,
    intake_window_months: int | None,
    additional_where_clause: str | None = None,
) -> StateSpecificTaskCompletionEventBigQueryViewBuilder:
    """
    Args:
        state_code (StateCode): State whose classification assessment events to read.
        description (str): Description of the completion event, passed through to the
            view builder.
        intake_window_months (int | None): If set, someone's first classification
            decision in a state prison custody stay also counts if it falls after this
            many months of the stay, for states where someone with no classification
            by then is due for a non-intake review instead. If None, a first
            classification decision never counts.
        additional_where_clause (str, optional): Extra condition AND'd onto the query,
            e.g. an assessment_instrument filter isolating a specific policy version.
    Returns:
        StateSpecificTaskCompletionEventBigQueryViewBuilder: Completion event firing on
            every classification decision after someone's first one in a state prison
            custody stay (i.e. excludes intake), plus any first one made after
            intake_window_months if set.
    """
    is_non_intake_clause = "i.person_id IS NULL"
    if intake_window_months is not None:
        is_non_intake_clause = (
            f"({is_non_intake_clause} OR NOT "
            f"{_is_within_intake_window_clause(intake_window_months)})"
        )
    where_clauses = [is_non_intake_clause]
    if additional_where_clause:
        where_clauses.append(additional_where_clause)
    query_template = f"""
    SELECT
        c.state_code,
        c.person_id,
        c.classification_decision_date AS completion_event_date,
    FROM (
{fix_indent(state_classification_assessment_events_query(state_code=state_code), indent_level=8)}
    ) c
    LEFT JOIN
        `{{project_id}}.{{task_eligibility_criteria_general_dataset}}.has_initial_classification_in_state_prison_custody_materialized` i
        ON c.person_id = i.person_id
        AND c.state_code = i.state_code
        AND c.classification_decision_date = i.start_date
    WHERE {" AND ".join(where_clauses)}
"""
    return StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=state_code,
        completion_event_type=TaskCompletionEventType.INCARCERATION_ASSESSMENT_2026_POLICY_COMPLETED,
        description=description,
        completion_event_query_template=query_template,
        task_eligibility_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    )
