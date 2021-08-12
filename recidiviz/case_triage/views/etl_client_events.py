# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Retrieves a history of supervision-related events for clients currently on caseloads.

To generate the view, run:
    python -m recidiviz.case_triage.views.etl_client_events
"""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.case_triage.client_event.types import ClientEventType
from recidiviz.case_triage.views.dataset_config import VIEWS_DATASET
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_EVENTS_DESCRIPTION = """
    Retrieves a history of supervision-related events for clients currently on caseloads.
"""

CLIENT_EVENTS_QUERY_TEMPLATE = f"""
with clients AS (
    SELECT
        etl_clients.state_code,
        person_id,
        person_external_id,
        etl_clients.projected_end_date
    FROM `{{project_id}}.{{case_triage_dataset}}.etl_clients_materialized` etl_clients
    JOIN `{{project_id}}.state.state_person_external_id` state_person_external_id
        ON state_person_external_id.state_code = etl_clients.state_code
           AND state_person_external_id.external_id = etl_clients.person_external_id
),
risk_assessments AS (
    SELECT state_code,
        person_id,
        person_external_id,
        '{ClientEventType.ASSESSMENT.value}' AS event_type,
        assessment_sessions.assessment_date AS event_date,
        TO_JSON_STRING(
            STRUCT (
                LAG(assessment_sessions.assessment_score)
                    OVER (
                        PARTITION BY state_code, person_id 
                        ORDER BY assessment_date
                    ) AS previous_assessment_score,
                assessment_sessions.assessment_score
            )
        ) AS event_metadata
    FROM clients
    JOIN `{{project_id}}.{{analyst_views_dataset}}.assessment_score_sessions_materialized` assessment_sessions 
        USING (state_code, person_id)
    
),
contacts AS (
    SELECT state_code,
        person_id,
        person_external_id,
        '{ClientEventType.CONTACT.value}' AS event_type,
        contact_date AS event_date,
        TO_JSON_STRING(
            STRUCT (
                state_supervision_contact.contact_type, 
                state_supervision_contact.location
            )
        ) AS event_metadata
    FROM clients
    JOIN `{{project_id}}.state.state_supervision_contact` state_supervision_contact
        USING (state_code, person_id)
    WHERE
        state_supervision_contact.status = "{StateSupervisionContactStatus.COMPLETED.value}"
),
events AS (
    SELECT * FROM risk_assessments
    UNION ALL 
    SELECT * FROM contacts
),
export_time AS (
  SELECT CURRENT_TIMESTAMP AS exported_at
)
 
SELECT {{columns}} FROM events
FULL OUTER JOIN export_time ON TRUE
"""

CLIENT_EVENTS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id="etl_client_events",
    view_query_template=CLIENT_EVENTS_QUERY_TEMPLATE,
    should_materialize=True,
    description=CLIENT_EVENTS_DESCRIPTION,
    columns=[
        "state_code",
        "person_external_id",
        "event_type",
        "event_date",
        "event_metadata",
        "exported_at",
    ],
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    case_triage_dataset=VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_EVENTS_VIEW_BUILDER.build_and_print()
