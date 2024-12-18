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
# ============================================================================
"""Describes spans of time when someone is eligible due to a mental health score of 3 or below in AZ."""

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_AZ_MENTAL_HEALTH_SCORE_3_OR_BELOW"

_QUERY_TEMPLATE = """
    SELECT 
        state_code,
        person_id,
        assessment_date AS start_date,
        score_end_date_exclusive AS end_date,
        (assessment_score <= 3) AS meets_criteria,
        TO_JSON(STRUCT(assessment_score AS assessment_score, assessment_class AS assessment_class)) AS reason,
        assessment_score, 
        assessment_class,
    FROM `{project_id}.sessions.assessment_score_sessions_materialized`
    WHERE state_code = 'US_AZ'
    -- TODO(#35297): Update this query to not use assessment_type = 'INTERNAL_UNKNOWN'
        AND assessment_type = 'INTERNAL_UNKNOWN'
        AND assessment_class = 'MENTAL_HEALTH'
    """

_REASONS_FIELDS = [
    ReasonsField(
        name="assessment_score",
        type=bigquery.enums.StandardSqlTypeNames.STRING,
        description="A score that goes from 1-5, where 1 is the lowest and 5 is the highest mental health needs",
    ),
    ReasonsField(
        name="assessment_class",
        type=bigquery.enums.StandardSqlTypeNames.STRING,
        description="Assessment class: should be equal to 'MENTAL_HEALTH'",
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
