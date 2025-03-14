# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Describes the spans of time when a client's latest Vantage (StrongR, either 1.0 or
2.0) assessment shows them not scoring High for any need level.

Note that if a client has not yet had any assessments, they will not meet this criteria.

Also note that this criteria is specific to StrongR assessment, which at the time of
this writing was only used in TN but could in the future be used in other states.
"""

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    STRONG_R2_ASSESSMENT_METADATA_KEYS,
    STRONG_R_ASSESSMENT_METADATA_KEYS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_ASSESSED_NOT_HIGH_ON_STRONG_R_DOMAINS"

_QUERY_TEMPLATE = f"""
    WITH strong_r_assessments_prioritized AS (
        /* Though `assessment_score_sessions_materialized` is already sessionized, there
        can be overlapping sessions there if multiple assessment types have been used to
        assess risk in a state. To work around this, we'll take the assessment data from
        that view but end up constructing criterion-specific spans later in this query,
        since we want to avoid having overlapping spans. */
        SELECT
            state_code,
            person_id,
            assessment_type,
            assessment_date,
            /* The `assessment_metadata` field is a string in
            `assessment_score_sessions`,
            so we convert it to a JSON here using PARSE_JSON. */
            PARSE_JSON(assessment_metadata) AS assessment_metadata,
        FROM `{{project_id}}.sessions.assessment_score_sessions_materialized`
        WHERE assessment_type IN ('STRONG_R', 'STRONG_R2')
        /* If someone happened to have both STRONG-R and STRONG-R 2.0 assessments on the
        same day, prioritize the 2.0 assessment, since it's intended to replace the
        original assessment. (This probably shouldn't happen, but this QUALIFY statement
        is here as a safeguard.) */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY state_code, person_id, assessment_date
            ORDER BY 
                CASE
                    WHEN assessment_type='STRONG_R2' THEN 1
                    ELSE 2
                    END
        ) = 1
    ),
    strong_r_spans AS (
        /* Create STRONG-R (1.0/2.0) spans. We create non-overlapping spans such that if
        a person has a previous STRONG-R assessment of either type, that span will end
        when a subsequent STRONG-R assessment is completed, regardless of the type of
        the subsequent assessment. */
        SELECT
            state_code,
            person_id,
            assessment_type,
            assessment_metadata,
            assessment_date,
            LEAD(assessment_date) OVER (
                PARTITION BY state_code, person_id
                ORDER BY assessment_date
            ) AS score_end_date_exclusive,
        FROM strong_r_assessments_prioritized
    ),
    unnested_needs AS (
        -- pull data for STRONG-R 1.0 spans
        SELECT
            state_code,
            person_id,
            assessment_date AS start_date,
            score_end_date_exclusive AS end_date,
            assessment_date,
            assessment_type,
            assessment_metadata,
            /* UNNEST returns one row per key in a column called "need". Then,
            `assessment_metadata[need]` extracts the JSON value associated with the key
            from the need column and JSON_VALUE formats this as a string. */
        NULLIF(JSON_VALUE(assessment_metadata[need]), '') AS need_level,
        FROM strong_r_spans,
        UNNEST({STRONG_R_ASSESSMENT_METADATA_KEYS}) AS need
        WHERE assessment_type='STRONG_R'
        UNION ALL
        -- pull data for STRONG-R 2.0 (which should be non-overlapping w/ 1.0 spans)
        SELECT
            state_code,
            person_id,
            assessment_date AS start_date,
            score_end_date_exclusive AS end_date,
            assessment_date,
            assessment_type,
            assessment_metadata,
        NULLIF(JSON_VALUE(assessment_metadata[need]), '') AS need_level,
        FROM strong_r_spans,
        UNNEST({STRONG_R2_ASSESSMENT_METADATA_KEYS}) AS need
        WHERE assessment_type='STRONG_R2'
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        assessment_date,
        assessment_type,
        /* The `assessment_metadata` field should be the same in every row within a
        group, so we use ANY_VALUE to pick one here, but it wouldn't matter which one we
        picked. */
        ANY_VALUE(assessment_metadata) AS assessment_metadata,
        TO_JSON(STRUCT(
            assessment_date,
            assessment_type,
            ANY_VALUE(assessment_metadata) AS assessment_metadata
        )) AS reason,
        LOGICAL_AND(COALESCE(need_level, "MISSING") != 'HIGH') AS meets_criteria,
    FROM unnested_needs
    GROUP BY 1,2,3,4,5,6
"""


VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
    state_code=StateCode.US_TN,
    reasons_fields=[
        ReasonsField(
            name="assessment_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest assessment",
        ),
        ReasonsField(
            name="assessment_type",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Type of latest assessment",
        ),
        ReasonsField(
            name="assessment_metadata",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Metadata containing all the needs and their levels for this assessment",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
