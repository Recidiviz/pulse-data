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
"""Creates the view builder and view for client (person) events concatenated in a common
format."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_EVENTS_VIEW_NAME = "person_events"

PERSON_EVENTS_VIEW_DESCRIPTION = (
    "View concatenating client (person) events in a common format"
)

PERSON_EVENTS_QUERY_TEMPLATE = """
-- compartment_level_0 starts
SELECT
    state_code,
    person_id,
    CONCAT(compartment_level_0, "_START") AS event,
    start_date AS event_date,
FROM
    `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized`
WHERE
    compartment_level_0 IN ("SUPERVISION", "INCARCERATION", "RELEASE")

UNION ALL

-- violation occurrence by type
SELECT
    state_code,
    person_id,
    "VIOLATION" AS event,
    COALESCE(violation_date, response_date) AS event_date,
    CASE
        WHEN violation_type IN ("ABSCONDED", "ESCAPED") THEN "ABSCONDED"
        WHEN violation_type IN ("FELONY", "LAW", "MISDEMEANOR", "MUNICIPAL") THEN 
            "LEGAL"
        WHEN violation_type IN ("TECHNICAL") THEN "TECHNICAL"
        ELSE "UNKNOWN" END AS attribute_1,
FROM
    `{project_id}.{dataflow_dataset}.most_recent_violation_with_response_metrics_materialized`

UNION ALL

/* most severe violation responses by type
Would we rather get all violation responses instead of only the most severe, ie use 
state_supervision_violation_response?
*/
SELECT
    state_code,
    person_id,
    "VIOLATION_RESPONSE_MOST_SEVERE" AS event,
    response_date AS event_date,
    CASE
        WHEN most_severe_response_decision IN ("COMMUNITY_SERVICE", "CONTINUANCE", 
            "DELAYED_ACTION", "EXTENSION", "NEW_CONDITIONS", "PRIVILEGES_REVOKED", 
            "REVOCATION", "SERVICE_TERMINATION", "SHOCK_INCARCERATION", 
            "SPECIALIZED_COURT", "SUSPENSION", "TREATMENT_IN_FIELD",
            "TREATMENT_IN_PRISON", "WARNING", "WARRANT_ISSUED") THEN 
            most_severe_response_decision
        ELSE "UNKNOWN" END AS attribute_1,
FROM
    `{project_id}.{dataflow_dataset}.most_recent_violation_with_response_metrics_materialized`

UNION ALL

-- LSIR assessed
SELECT
    state_code,
    person_id,
    "LSIR_ASSESSMENT" AS event,
    assessment_date AS event_date,
    CAST(assessment_score AS STRING) AS attribute_1,
FROM
    `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized`

"""

PERSON_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PERSON_EVENTS_VIEW_NAME,
    view_query_template=PERSON_EVENTS_QUERY_TEMPLATE,
    description=PERSON_EVENTS_VIEW_DESCRIPTION,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_EVENTS_VIEW_BUILDER.build_and_print()
