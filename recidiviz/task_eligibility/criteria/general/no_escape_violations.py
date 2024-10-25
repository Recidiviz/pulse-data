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
"""Defines a criteria span view that shows spans of time during which someone has never
had a disciplinary violation for an escape.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NO_ESCAPE_VIOLATIONS"

_EXCLUDED_OUTCOME_TYPES = ["DISMISSED", "NOT_GUILTY"]
_excluded_outcome_types_bq_list = list_to_query_string(
    _EXCLUDED_OUTCOME_TYPES, quoted=True
)

_QUERY_TEMPLATE = f"""
    WITH omitted_violations AS (
        -- Identifies incidents with any outcome type that implies the violation 
        -- was dropped. (e.g., if someone has a violation with the outcome "DISMISSED", but 
        -- also "GOOD_TIME_LOSS", we omit that violation from the list of escape violations.)
        SELECT DISTINCT
            state_code,
            person_id,
            incarceration_incident_id,
        FROM `{{project_id}}.normalized_state.state_incarceration_incident_outcome` outcome
        WHERE outcome.outcome_type IN ({_excluded_outcome_types_bq_list})
    )
    , 
    escape_violations AS (
        SELECT DISTINCT
            incident.state_code,
            incident.person_id,
            FIRST_VALUE(incident.incident_date) OVER (PARTITION BY incident.state_code, incident.person_id ORDER BY incident.incident_date ASC) AS first_escape_violation_date,
        FROM `{{project_id}}.normalized_state.state_incarceration_incident` incident
        LEFT JOIN omitted_violations
        USING(state_code, person_id, incarceration_incident_id)
        WHERE 
            incident.incident_type = "ESCAPE" 
            -- Exclude omitted violations
            AND omitted_violations.incarceration_incident_id IS NULL
    )
    SELECT 
        state_code,
        person_id,
        -- Span starts with someone's first escape violation
        first_escape_violation_date AS start_date,
        -- Span never ends
        CAST(NULL AS DATE) AS end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(first_escape_violation_date AS first_escape_violation_date)) AS reason,
        first_escape_violation_date,
    FROM escape_violations
"""


VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="first_escape_violation_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of first escape violation",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
