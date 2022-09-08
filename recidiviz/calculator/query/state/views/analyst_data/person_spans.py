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
"""Creates the view builder and view for client (person) spans concatenated in a common
format."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_SPANS_VIEW_NAME = "person_spans"

PERSON_SPANS_VIEW_DESCRIPTION = (
    "View concatenating client (person) spans in a common format"
)

PERSON_SPANS_QUERY_TEMPLATE = """
/*
{description}
*/

-- compartment_sessions
SELECT
    state_code,
    person_id,
    "COMPARTMENT_SESSION" AS span,
    start_date,
    end_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        compartment_level_1,
        compartment_level_2,
        case_type_start
    ))[OFFSET(0)]) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- person_demographics
SELECT
    state_code,
    person_id,
    "PERSON_DEMOGRAPHICS" AS span,
    MIN(start_date) AS start_date,
    CURRENT_DATE("US/Eastern") AS end_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        birthdate,
        pd.gender,
        pd.prioritized_race_or_ethnicity
    ))[OFFSET(0)]) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.compartment_sessions_materialized` cs
INNER JOIN
    `{project_id}.{sessions_dataset}.person_demographics_materialized` pd
USING
    (state_code, person_id)
GROUP BY 1, 2, 3

UNION ALL

-- assessment_score_sessions
SELECT
    state_code,
    person_id,
    "ASSESSMENT_SCORE_SESSION" AS span,
    assessment_date AS start_date,
    score_end_date AS end_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        assessment_type,
        assessment_score,
        assessment_level
    ))[OFFSET(0)]) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized`
WHERE
    assessment_date IS NOT NULL
    AND assessment_type IS NOT NULL
    AND assessment_score IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- employment_periods_preprocessed, employed periods only
SELECT
    state_code,
    person_id,
    "EMPLOYMENT_PERIOD" AS span,
    employment_start_date AS start_date,
    employment_end_date AS end_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        employer_name
    ))[OFFSET(0)]) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.employment_periods_preprocessed_materialized`
WHERE
    employment_start_date IS NOT NULL
    AND NOT is_unemployed
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- employment_status_session
SELECT
    state_code,
    person_id,
    "EMPLOYMENT_STATUS_SESSION" AS span,
    employment_status_start_date AS start_date,
    employment_status_end_date AS end_date,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(
        CAST(is_employed AS STRING) AS is_employed
    ))[OFFSET(0)]) AS span_attributes,
FROM
    `{project_id}.{sessions_dataset}.supervision_employment_status_sessions_materialized`
WHERE
    employment_status_start_date IS NOT NULL
GROUP BY 1, 2, 3, 4, 5

UNION ALL

-- contacts completed
-- this creates spans of contact dates and the subsequent contact date to help us 
-- identify the most recent completed contact
SELECT
    state_code, 
    person_id,
    "COMPLETED_CONTACT_SESSION" AS span,
    contact_date AS start_date,
    DATE_SUB(
        LEAD(contact_date) OVER (
            PARTITION BY person_id
            ORDER BY contact_date
        ), INTERVAL 1 DAY
    ) AS end_date,
    CAST(NULL AS STRING) AS span_attributes,
FROM (
    SELECT DISTINCT
        state_code,
        person_id,
        contact_date
    FROM
        `{project_id}.{state_base_dataset}.state_supervision_contact`
    WHERE
        status = "COMPLETED"
)
"""

PERSON_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=PERSON_SPANS_VIEW_NAME,
    view_query_template=PERSON_SPANS_QUERY_TEMPLATE,
    description=PERSON_SPANS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "span"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_SPANS_VIEW_BUILDER.build_and_print()
