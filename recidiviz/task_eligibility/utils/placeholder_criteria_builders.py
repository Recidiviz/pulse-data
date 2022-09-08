# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Helper methods that return criteria view builders with placeholder/stubbed data to
be used for populating a new criteria before the criteria query is developed.
"""
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def state_agnostic_placeholder_query_template(reason_query: str) -> str:
    """Returns a query template for state-agnostic eligibility criteria spans with the
    provided |reason_query| set and the whole population set to meets_criteria = True.
    """
    # TODO(#15105): simplify this query once missing spans can be set to default True
    return f"""
/*{{description}}*/
SELECT
    state_code,
    person_id,
    start_date,
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
    TRUE AS meets_criteria,
    {reason_query} AS reason,
FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized`
"""


def state_specific_placeholder_criteria_view_builder(
    criteria_name: str, description: str, reason_query: str, state_code: StateCode
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state specific criteria view builder to be used as a placeholder
    criteria during development.
    """
    query_template = state_agnostic_placeholder_query_template(reason_query)
    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=state_code,
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
    )


def state_agnostic_placeholder_criteria_view_builder(
    criteria_name: str, description: str, reason_query: str
) -> StateAgnosticTaskCriteriaBigQueryViewBuilder:
    """Returns a state agnostic criteria view builder to be used as a placeholder
    criteria during development.
    """
    query_template = state_agnostic_placeholder_query_template(reason_query)
    return StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        sessions_dataset=SESSIONS_DATASET,
    )
