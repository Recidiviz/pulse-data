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
"""Describes spans of time when someone is eligible due to a risk release assessment of 
MINIMUM in AZ. This is an assessment done at intake right after people are released from 
prison to supervision."""

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_RISK_RELEASE_ASSESSMENT_LEVEL_IS_MINIMUM"

_QUERY_TEMPLATE = """
    SELECT 
        state_code,
        person_id,
        assessment_date AS start_date,
        score_end_date_exclusive AS end_date,
        JSON_VALUE(assessment_metadata, '$.LEVEL') = 'MINIMUM' AS meets_criteria,
        TO_JSON(STRUCT(
            assessment_score AS assessment_score,
            JSON_VALUE(assessment_metadata, '$.LEVEL') AS assessment_level)) AS reason,
        assessment_score,
        JSON_VALUE(assessment_metadata, '$.LEVEL') AS assessment_level,
    FROM `{project_id}.sessions.assessment_score_sessions_materialized`
    WHERE state_code= 'US_AZ'
        AND assessment_type = 'CCRRA'
        AND assessment_class = 'RISK'"""

_REASONS_FIELDS = [
    ReasonsField(
        name="assessment_score",
        type=bigquery.enums.StandardSqlTypeNames.STRING,
        description="Assessment score ranging from 0 to 60.",
    ),
    ReasonsField(
        name="assessment_level",
        type=bigquery.enums.StandardSqlTypeNames.STRING,
        description="Assessment level",
    ),
]

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=_REASONS_FIELDS,
        state_code=StateCode.US_AZ,
        meets_criteria_default=False,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
