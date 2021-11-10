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
"""Creates the view builder and view for key discrete events concatenated in a common
format."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENTS_VIEW_NAME = "events"

EVENTS_VIEW_DESCRIPTION = "View concatenating key events in a common format"

EVENTS_QUERY_TEMPLATE = """
-- compartment_level_0 starts
SELECT
    state_code,
    CAST(person_id AS STRING) AS subject_id,
    "person_id" AS id_type,
    CONCAT(compartment_level_0, '_START') AS metric,
    start_date AS event_date,
FROM
    `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized`
WHERE
    compartment_level_0 IN ('SUPERVISION', 'INCARCERATION', 'RELEASE')

UNION ALL

-- violation occurrence by type
SELECT
    state_code,
    CAST(person_id AS STRING) AS subject_id,
    "person_id" AS id_type,
    CASE
        WHEN violation_type IN ("ABSCONDED", "ESCAPED") THEN "VIOLATION_ABSCONDED"
        WHEN violation_type IN ("FELONY", "LAW", "MISDEMEANOR", "MUNICIPAL") THEN 
            "VIOLATION_LEGAL"
        WHEN violation_type IN ("TECHNICAL") THEN "VIOLATION_TECHNICAL"
        ELSE "VIOLATION_UNKNOWN" END AS metric,
    COALESCE(violation_date, response_date) AS event_date,
FROM
    `{project_id}.{dataflow_dataset}.most_recent_violation_with_response_metrics_materialized`

UNION ALL

/* most severed violation responses by type
Would we rather get all violation responses instead of only the most severe, ie use 
state_supervision_violation_response?
*/
SELECT
    state_code,
    CAST(person_id AS STRING) AS subject_id,
    "person_id" AS id_type,
    CASE
        WHEN most_severe_response_decision IN ("COMMUNITY_SERVICE", "CONTINUANCE", 
            "DELAYED_ACTION", "EXTENSION", "NEW_CONDITIONS", "PRIVILEGES_REVOKED", 
            "REVOCATION", "SERVICE_TERMINATION", "SHOCK_INCARCERATION", 
            "SPECIALIZED_COURT", "SUSPENSION", "TREATMENT_IN_FIELD",
            "TREATMENT_IN_PRISON", "WARNING", "WARRANT_ISSUED") THEN 
            CONCAT("VIOLATION_RESPONSE_", most_severe_response_decision)
        WHEN most_severe_response_decision IN ("INTERNAL_UNKNOWN", "OTHER") THEN 
            "VIOLATION_RESPONSE_UNKNOWN"
        ELSE "VIOLATION_RESPONSE_UNKNOWN" END AS metric,
    response_date AS event_date,
FROM
    `{project_id}.{dataflow_dataset}.most_recent_violation_with_response_metrics_materialized`

UNION ALL

-- LSIR assessed
SELECT
    state_code,
    CAST(person_id AS STRING) AS subject_id,
    "person_id" AS id_type,
    "LSIR_ASSESSMENT" AS metric,
    assessment_date AS event_date,
FROM
    `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized`

"""

EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=EVENTS_VIEW_NAME,
    view_query_template=EVENTS_QUERY_TEMPLATE,
    description=EVENTS_VIEW_DESCRIPTION,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "subject_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENTS_VIEW_BUILDER.build_and_print()
