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
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
    SHARED_METRIC_VIEWS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_RAW_DATASET = "us_id_raw_data_up_to_date_views"

PERSON_EVENTS_VIEW_NAME = "person_events"

PERSON_EVENTS_VIEW_DESCRIPTION = (
    "View concatenating client (person) events in a common format"
)

PERSON_EVENTS_QUERY_TEMPLATE = """
-- compartment_level_1 (in-state only) starts
SELECT
    state_code,
    person_id,
    CONCAT(compartment_level_1, "_START") AS event,
    start_date AS event_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized`
WHERE
    compartment_level_1 IN ("SUPERVISION", "INCARCERATION", "RELEASE")

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
    CAST(NULL AS STRING) AS attribute_2,
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
    CAST(NULL AS STRING) AS attribute_2,
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
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized`

UNION ALL

-- Rewards and sanctions
SELECT DISTINCT
    "US_ID" AS state_code,
    person_id,
    UPPER(response_reward_sanction) AS event,
    action_date AS event_date,
    REPLACE(response_type, ' ', '_') AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM 
    `{project_id}.{analyst_dataset}.us_id_behavior_responses_materialized`

UNION ALL

-- Contacts
SELECT DISTINCT
    state_code,
    person_id,
    "CONTACT" AS event,
    contact_date AS event_date,
    contact_reason AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{state_base_dataset}.state_supervision_contact` 

UNION ALL 

-- Supervision level downgrades & upgrades
SELECT  
    state_code,
    person_id,
    "SUPERVISION_LEVEL_CHANGE",
    start_date AS event_date,
    CASE
        WHEN supervision_downgrade = 1 THEN 'DOWNGRADE'
        WHEN supervision_upgrade = 1 THEN 'UPGRADE' END AS attribute_1,
    supervision_level AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized`
WHERE
    supervision_downgrade + supervision_upgrade > 0

UNION ALL 

-- New supervision level assigned
SELECT  
    state_code,
    person_id,
    "SUPERVISION_LEVEL_CHANGE",
    start_date AS event_date,
    "NEW" AS attribute_1,
    supervision_level AS attribute_2,
FROM 
    `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized`
WHERE TRUE
QUALIFY 
    LAG(supervision_level) OVER (PARTITION BY person_id ORDER BY start_date) IS NULL 
    AND supervision_level IS NOT NULL

UNION ALL

-- GEO CIS starts
SELECT
    "US_ID" AS state_code,
    person_id,
    "PROGRAM_START" AS event,
    DATE(start_date) AS event_date,
    "GEO_CIS" AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM 
    `{project_id}.{us_id_raw_dataset}.geo_cis_participants_latest` c
LEFT JOIN 
    `{project_id}.{state_base_dataset}.state_person_external_id` p
ON 
    c.person_external_id = p.external_id
    AND p.state_code = "US_ID"

UNION ALL 

-- GEO CIS ends
SELECT
    "US_ID" AS state_code,
    person_id,
    "PROGRAM_END" AS event,
    DATE(end_date) AS event_date,
    "GEO_CIS" AS attribute_1,
    COALESCE(discharge_reason, "UNKNOWN") AS attribute_2,
FROM 
    `{project_id}.{us_id_raw_dataset}.geo_cis_participants_latest` c
LEFT JOIN 
    `{project_id}.{state_base_dataset}.state_person_external_id` p
ON 
    c.person_external_id = p.external_id
    AND p.state_code = "US_ID"
WHERE 
    end_date IS NOT NULL

UNION ALL

-- Supervision officer assigned (transition from NULL officer session to at least one 
-- non-NULL officer)
SELECT DISTINCT
    state_code,
    person_id,
    "OFFICER_ASSIGNED" AS event,
    start_date AS event_date,
    "NEW" AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized`
WHERE TRUE 
QUALIFY 
    LAG(supervising_officer_external_id) OVER (
        PARTITION BY person_id ORDER BY start_date) IS NULL 
    AND supervising_officer_external_id IS NOT NULL

UNION ALL

-- Supervision officer changed (transition between two supervision officer sessions 
-- with non-null officers)
SELECT DISTINCT
    state_code,
    person_id,
    "OFFICER_ASSIGNED" AS event,
    start_date AS event_date,
    "CHANGE" AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized`
WHERE TRUE 
QUALIFY 
    COALESCE(
        LAG(supervising_officer_external_id) 
            OVER (PARTITION BY person_id ORDER BY start_date) != 
            supervising_officer_external_id, 
        FALSE
    )

UNION ALL

-- New job starts (records all job starts, not just changes in (un)employment status)
SELECT
    e.state_code,
    person_id,
    "EMPLOYMENT_JOB_START" AS event,
    employment_start_date AS event_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM 
    `{project_id}.{sessions_dataset}.employment_periods_preprocessed_materialized` e
WHERE
    is_unemployed = FALSE

UNION ALL

-- Transitions to unemployment (ends of employment periods where is_employed is True)
-- This will capture both employment terminations while on supervision, as well as 
-- terminations due to someone's transition to incarceration.
SELECT
    state_code,
    person_id,
    "UNEMPLOYMENT_START" AS event,
    employment_status_end_date AS event_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized`
WHERE
    is_employed

UNION ALL 

-- Supervision downgrade recommendations surfaced to staff
SELECT
    state_code,
    person_id,
    "SUPERVISION_DOWNGRADE_SURFACED" AS event,
    surfaced_date AS event_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    surfaced_date IS NOT NULL

UNION ALL

-- Supervision downgrade recommendations corrected after being surfaced to staff
SELECT
    state_code,
    person_id,
    "SURFACED_SUPERVISION_DOWNGRADE_CORRECTED" AS event,
    end_date AS event_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    surfaced_date IS NOT NULL
    AND mismatch_corrected

UNION ALL 

-- Overdue discharge reports surfaced to staff
SELECT 
    state_code, 
    person_id,
    "OVERDUE_DISCHARGE_SURFACED" AS event,
    report_date AS event_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM 
    `{project_id}.{shared_metric_views_dataset}.overdue_discharge_outcomes`

UNION ALL 

-- Overdue discharge reports corrected (by releasing the person)
SELECT 
    state_code, 
    person_id,
    "SURFACED_OVERDUE_DISCHARGE_CORRECTED" AS event,
    discharge_date AS event_date,
    CAST(NULL AS STRING) AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM 
    `{project_id}.{shared_metric_views_dataset}.overdue_discharge_outcomes`
WHERE
    discharge_date IS NOT NULL
    AND discharge_outflow = "RELEASE"

UNION ALL

-- Positive drug tests within a supervision super session
SELECT
    d.state_code,
    d.person_id,
    'POSITIVE_DRUG_TEST' AS event,
    d.drug_screen_date AS event_date,
    d.sample_type AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM `{project_id}.{sessions_dataset}.drug_screens_preprocessed_materialized` d
INNER JOIN `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` ss
    ON d.person_id = ss.person_id
    AND d.drug_screen_date BETWEEN ss.start_date AND COALESCE(ss.end_date, '9999-01-01')
    AND d.is_positive_result

UNION ALL

-- Initial positive drug test within a supervision super session
SELECT
    d.state_code,
    d.person_id,
    'INITIAL_POSITIVE_DRUG_TEST' AS event,
    d.drug_screen_date AS event_date,
    d.sample_type AS attribute_1,
    CAST(NULL AS STRING) AS attribute_2,
FROM `{project_id}.{sessions_dataset}.drug_screens_preprocessed_materialized` d
INNER JOIN `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized` ss
    ON d.person_id = ss.person_id
    AND d.drug_screen_date BETWEEN ss.start_date AND COALESCE(ss.end_date, '9999-01-01')
    AND d.is_positive_result
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ss.person_id, ss.supervision_super_session_id
    ORDER BY event_date
) = 1

UNION ALL

-- Supervision super session starts
-- attribute_1: start_reason
-- attribute_2: inflow_from_level_1
SELECT
    state_code,
    person_id,
    'SUPERVISION_SUPER_SESSION_START' AS event,
    start_date AS event_date,
    start_reason AS attribute_1,
    inflow_from_level_1 AS attribute_2,
FROM `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized`

UNION ALL

-- Supervision super session ends
-- attribute_1: end_reason
-- attribute_2: outlow_to_level_1
SELECT
    state_code,
    person_id,
    'SUPERVISION_SUPER_SESSION_END' AS event,
    end_date AS event_date,
    end_reason AS attribute_1,
    outflow_to_level_1 AS attribute_2,
FROM `{project_id}.{sessions_dataset}.supervision_super_sessions_materialized`
WHERE end_date IS NOT NULL

UNION ALL

-- add experiment assignments as cohort start events
SELECT
    state_code,
    person_id,
    "ASSIGNED_VARIANT" AS event,
    variant_date AS event_date,
    experiment_id AS attribute_1,
    variant_id AS attribute_2,
FROM
`{project_id}.{experiments_dataset}.person_assignments_materialized`

"""

PERSON_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PERSON_EVENTS_VIEW_NAME,
    view_query_template=PERSON_EVENTS_QUERY_TEMPLATE,
    description=PERSON_EVENTS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    dataflow_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    shared_metric_views_dataset=SHARED_METRIC_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    us_id_raw_dataset=US_ID_RAW_DATASET,
    experiments_dataset=EXPERIMENTS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_EVENTS_VIEW_BUILDER.build_and_print()
