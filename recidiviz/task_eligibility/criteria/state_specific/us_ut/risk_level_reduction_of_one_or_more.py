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
"""Spans of time when someone in UT saw a 1 level or more reduction in risk from initial
supervision risk score.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_ut_query_fragments import (
    assessment_scores_with_first_score_ctes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_UT_RISK_LEVEL_REDUCTION_OF_ONE_OR_MORE"

_QUERY_TEMPLATE = f"""
# TODO(#37900) - Change after schema change request is approved
WITH {assessment_scores_with_first_score_ctes(assessment_types_list = ["INTERNAL_UNKNOWN"])}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    (IFNULL(first_assessment_level_raw_text_number - assessment_level_raw_text_number, 0)>=1) AS meets_criteria,
    TO_JSON(STRUCT(
        first_assessment_level_raw_text_number - assessment_level_raw_text_number AS assessment_level_change,
        assessment_level_raw_text AS assessment_level_raw_text,
        first_assessment_level_raw_text AS first_assessment_level_raw_text
        )) AS reason,
    first_assessment_level_raw_text_number - assessment_level_raw_text_number AS assessment_level_change,
    assessment_level_raw_text,
    first_assessment_level_raw_text,
FROM assessment_scores_with_first_score
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    reasons_fields=[
        ReasonsField(
            name="assessment_level_change",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Change in risk score level between first and current assessment",
        ),
        ReasonsField(
            name="assessment_level_raw_text",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Current risk level",
        ),
        ReasonsField(
            name="first_assessment_level_raw_text",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="First risk level in this supervision span",
        ),
    ],
    state_code=StateCode.US_UT,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
