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
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_CRITERIA_GENERAL
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)


def intake_classification_decision_completed_view_builder(
    *,
    state_code: StateCode,
    description: str,
    additional_where_clause: str | None = None,
) -> StateSpecificTaskCompletionEventBigQueryViewBuilder:
    """
    Args:
        state_code (StateCode): State to filter `custody_classification_assessment_dates`
            spans to.
        description (str): Description of the completion event, passed through to the
            view builder.
        additional_where_clause (str, optional): Extra condition AND'd onto the query,
            e.g. an assessment_type filter isolating a specific policy version.
    Returns:
        StateSpecificTaskCompletionEventBigQueryViewBuilder: Completion event firing on
            someone's first classification decision after starting or re-starting
            state-prison custody.
    """
    where_clauses = [f"c.state_code = '{state_code.value}'"]
    if additional_where_clause:
        where_clauses.append(additional_where_clause)
    query_template = f"""
    SELECT
        c.state_code,
        c.person_id,
        c.classification_decision_date AS completion_event_date,
    FROM `{{project_id}}.{{analyst_views_dataset}}.custody_classification_assessment_dates_materialized` c
    INNER JOIN
        `{{project_id}}.{{task_eligibility_criteria_general_dataset}}.has_initial_classification_in_state_prison_custody_materialized` i
        ON c.person_id = i.person_id
        AND c.state_code = i.state_code
        AND c.classification_decision_date = i.start_date
    WHERE {" AND ".join(where_clauses)}
"""
    return StateSpecificTaskCompletionEventBigQueryViewBuilder(
        state_code=state_code,
        completion_event_type=TaskCompletionEventType.INCARCERATION_INTAKE_ASSESSMENT_2026_POLICY_COMPLETED,
        description=description,
        completion_event_query_template=query_template,
        analyst_views_dataset=ANALYST_VIEWS_DATASET,
        task_eligibility_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    )


def non_intake_classification_decision_completed_view_builder(
    *,
    state_code: StateCode,
    description: str,
    additional_where_clause: str | None = None,
) -> StateSpecificTaskCompletionEventBigQueryViewBuilder:
    """
    Args:
        state_code (StateCode): State to filter `custody_classification_assessment_dates`
            spans to.
        description (str): Description of the completion event, passed through to the
            view builder.
        additional_where_clause (str, optional): Extra condition AND'd onto the query,
            e.g. an assessment_type filter isolating a specific policy version.
    Returns:
        StateSpecificTaskCompletionEventBigQueryViewBuilder: Completion event firing on
            every classification decision after someone's first one (i.e. excludes
            intake).
    """
    where_clauses = [f"c.state_code = '{state_code.value}'", "i.person_id IS NULL"]
    if additional_where_clause:
        where_clauses.append(additional_where_clause)
    query_template = f"""
    SELECT
        c.state_code,
        c.person_id,
        c.classification_decision_date AS completion_event_date,
    FROM `{{project_id}}.{{analyst_views_dataset}}.custody_classification_assessment_dates_materialized` c
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
        analyst_views_dataset=ANALYST_VIEWS_DATASET,
        task_eligibility_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
    )
