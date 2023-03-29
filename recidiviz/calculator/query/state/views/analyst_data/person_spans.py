# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Creates the view builder and view for client (person) spans concatenated in a common
format."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    REFERENCE_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_DATASET_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_SPANS_VIEW_NAME = "person_spans"

PERSON_SPANS_VIEW_DESCRIPTION = (
    "View concatenating client (person) spans in a common format. Note that end_dates "
    "are exclusive, i.e. the last full day of the span (if any) was the day prior to "
    "the end_date. Spans of the same type may be overlapping."
)

PERSON_SPANS_QUERY_TEMPLATE = """

-- compartment_sessions
SELECT
    state_code,
    person_id,
    "COMPARTMENT_SESSION" AS span,
    start_date,
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
    TO_JSON_STRING(STRUCT(
        compartment_level_1,
        compartment_level_2,
        case_type_start
    )) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.compartment_sessions_materialized`

UNION ALL

-- person_demographics
SELECT
    state_code,
    person_id,
    "PERSON_DEMOGRAPHICS" AS span,
    birthdate AS start_date,
    DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY) AS end_date,
    TO_JSON_STRING(STRUCT(
        birthdate,
        gender,
        prioritized_race_or_ethnicity
    )) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.person_demographics_materialized`

UNION ALL

-- assessment_score_sessions
SELECT
    state_code,
    person_id,
    "ASSESSMENT_SCORE_SESSION" AS span,
    assessment_date AS start_date,
    DATE_ADD(score_end_date, INTERVAL 1 DAY) AS end_date,
    TO_JSON_STRING(STRUCT(
        assessment_type,
        assessment_score,
        assessment_level
    )) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized`
WHERE
    assessment_date IS NOT NULL
    AND assessment_type IS NOT NULL
    AND assessment_score IS NOT NULL

UNION ALL

-- employed periods
SELECT
    state_code,
    person_id,
    "EMPLOYMENT_PERIOD" AS span,
    start_date,
    end_date,
    TO_JSON_STRING(STRUCT(
        employer_name
    )) AS span_attributes,
FROM
    `{project_id}.{normalized_state_dataset}.state_employment_period`
WHERE
    start_date IS NOT NULL
    AND employment_status != "UNEMPLOYED"

UNION ALL

-- employment_status_session
SELECT
    state_code,
    person_id,
    "EMPLOYMENT_STATUS_SESSION" AS span,
    employment_status_start_date AS start_date,
    DATE_ADD(employment_status_end_date, INTERVAL 1 DAY) AS end_date,
    TO_JSON_STRING(STRUCT(
        CAST(is_employed AS STRING) AS is_employed
    )) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized`
WHERE
    employment_status_start_date IS NOT NULL

UNION ALL

-- contacts completed
-- this creates spans of contact dates and the subsequent contact date to help us 
-- identify the most recent completed contact
SELECT
    state_code, 
    person_id,
    "COMPLETED_CONTACT_SESSION" AS span,
    contact_date AS start_date,
    LEAD(contact_date) OVER (
        PARTITION BY person_id
        ORDER BY contact_date
    ) AS end_date,
    CAST(NULL AS STRING) AS span_attributes,
FROM (
    SELECT DISTINCT
        state_code,
        person_id,
        contact_date
    FROM
        `{project_id}.{normalized_state_dataset}.state_supervision_contact`
    WHERE
        status = "COMPLETED"
)

UNION ALL

-- supervision levels
SELECT
    state_code,
    person_id,
    "SUPERVISION_LEVEL_SESSION" AS span,
    start_date,
    end_date_exclusive AS end_date,
    TO_JSON_STRING(STRUCT(
        IFNULL(supervision_level, "INTERNAL_UNKNOWN") AS supervision_level
    )) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_level_sessions_materialized`
    
UNION ALL

-- open supervision mismatch (downgrades only)
-- ends when mismatch corrected or supervision period ends
SELECT
    state_code,
    person_id,
    "SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE" AS span,
    start_date,
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
    TO_JSON_STRING(STRUCT(
        "SUPERVISION_DOWNGRADE" AS task_name,
        CAST(mismatch_corrected AS STRING) AS mismatch_corrected,
        recommended_supervision_downgrade_level
    )) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_downgrade_sessions_materialized`
WHERE
    recommended_supervision_downgrade_level IS NOT NULL

UNION ALL

SELECT
    state_code,
    person_id,
    "SUPERVISION_OFFICER_SESSION" AS span,
    start_date,
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
    TO_JSON_STRING(STRUCT(
        supervising_officer_external_id
    )) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized`

UNION ALL

-- all task eligibility spans
SELECT
    state_code,
    person_id,
    "TASK_ELIGIBILITY_SESSION" AS span,
    start_date,
    end_date,
    TO_JSON_STRING(STRUCT(
        task_name,
        completion_event_type AS task_type,
        is_eligible,
        ineligible_criteria
    )) AS span_attributes
FROM
    `{project_id}.{task_eligibility_dataset}.all_tasks_materialized`
INNER JOIN
    `{project_id}.{reference_views_dataset}.task_to_completion_event`
USING
    (task_name)

"""

PERSON_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PERSON_SPANS_VIEW_NAME,
    view_query_template=PERSON_SPANS_QUERY_TEMPLATE,
    description=PERSON_SPANS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    task_eligibility_dataset=TASK_ELIGIBILITY_DATASET_ID,
    should_materialize=True,
    clustering_fields=["state_code", "span"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_SPANS_VIEW_BUILDER.build_and_print()
