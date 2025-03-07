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
"""Describes the spans of time when a client's latest Vantage (StrongR) assessment shows them scoring not High
 on all need levels.

Note that if a client has not yet had any assessments, they will not meet this criteria

Also note that this criteria is specific to StrongR assessment, which at the time of this writing was only used in
TN but could in the future be used in other states
"""

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#38066): Add a unittest to make sure this list matches domains in ingest mappings

STRONG_R_ASSESSMENT_METADATA_KEYS = [
    "FRIENDS_NEED_LEVEL",
    "ATTITUDE_BEHAVIOR_NEED_LEVEL",
    "AGGRESSION_NEED_LEVEL",
    "MENTAL_HEALTH_NEED_LEVEL",
    "ALCOHOL_DRUG_NEED_LEVEL",
    "RESIDENT_NEED_LEVEL",
    "FAMILY_NEED_LEVEL",
    "EMPLOYMENT_NEED_LEVEL",
    "EDUCATION_NEED_LEVEL",
]

_CRITERIA_NAME = "US_TN_ASSESSED_NOT_HIGH_ON_STRONG_R_DOMAINS"

_QUERY_TEMPLATE = f"""
    WITH unnested_needs AS (
      SELECT
        state_code,
        person_id,
        assessment_date AS start_date,
        assessment_date,
        assessment_metadata,
        score_end_date_exclusive AS end_date,
        need,
        /* UNNEST returns one row per key in a column called "need"
         PARSE_JSON(assessment_metadata)[need] extracts the JSON value associated with the key
         from the need column, and JSON_VALUE formats this as a string
        */
        NULLIF(JSON_VALUE(PARSE_JSON(assessment_metadata)[need]),'') AS need_level,
      FROM 
        `{{project_id}}.sessions.assessment_score_sessions_materialized`,
      UNNEST({STRONG_R_ASSESSMENT_METADATA_KEYS}) AS need
      WHERE
        -- TODO(#38876): Handle upcoming StrongR 2.0 Case in TN preemptively
        assessment_type='STRONG_R'
    )
    SELECT
      state_code,
      person_id,
      start_date,
      end_date,
      assessment_date,
      assessment_metadata,
      TO_JSON(
        STRUCT(
            assessment_date,
            assessment_metadata
        )
       ) AS reason,
      LOGICAL_AND(COALESCE(need_level,"MISSING") != 'HIGH') AS meets_criteria
    FROM 
        unnested_needs
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
            description="Date of latest StrongR assessment",
        ),
        ReasonsField(
            name="assessment_metadata",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Metadata containing on all the needs and their levels for this assessment",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
