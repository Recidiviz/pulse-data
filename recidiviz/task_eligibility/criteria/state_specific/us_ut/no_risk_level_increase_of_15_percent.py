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
"""
Spans of time where a person's risk level has not increased by 15% or more between their 
current assessment and the lowest assessment in the past year.
"""
from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_UT_NO_RISK_LEVEL_INCREASE_OF_15_PERCENT"

_ASSESSMENT_TYPE = "LS_RNR"
_QUERY_TEMPLATE = f"""WITH min_assessment_during_supervision AS (
        SELECT 
            sss.state_code,
            sss.person_id,
            sss.supervision_super_session_id,
            sss.start_date,
            sss.end_date,
            ass.assessment_type,
            MIN(ass.assessment_score) AS min_assessment_score
        FROM `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
        INNER JOIN `{{project_id}}.sessions.assessment_score_sessions_materialized` ass
            USING(person_id, state_code)
        WHERE ass.assessment_type = '{_ASSESSMENT_TYPE}'
            AND ass.assessment_class = 'RISK'
            AND ass.state_code= 'US_UT'
            AND ass.assessment_date BETWEEN DATE_SUB(IFNULL(sss.end_date, CURRENT_DATE('US/Eastern')), INTERVAL 1 YEAR) AND IFNULL(sss.end_date, '9999-12-31')
        GROUP BY 1,2,3,4,5,6
    ),

    assessment_scores_with_min_score AS (
    SELECT 
        ass.state_code,
        ass.person_id,
        mads.supervision_super_session_id,
        ass.assessment_date AS start_date,
        ass.score_end_date_exclusive AS end_date,
        ass.assessment_score,
        mads.min_assessment_score,
        SAFE_DIVIDE(mads.min_assessment_score - ass.assessment_score, mads.min_assessment_score)*100 AS assessment_score_percent_reduction,
    FROM min_assessment_during_supervision mads
    INNER JOIN `{{project_id}}.sessions.assessment_score_sessions_materialized` ass
        USING(person_id, state_code, assessment_type)
    WHERE ass.assessment_date BETWEEN mads.start_date AND IFNULL(mads.end_date, '9999-12-31')
        AND ass.assessment_type = 'LS_RNR'
    )

    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        (IFNULL(assessment_score_percent_reduction, 0)>=-15) AS meets_criteria,
        TO_JSON(STRUCT(
            assessment_score_percent_reduction AS assessment_score_percent_reduction,
            assessment_score AS assessment_score,
            min_assessment_score AS min_assessment_score
        )) AS reason,
        assessment_score_percent_reduction,
        assessment_score,
        min_assessment_score,
    FROM assessment_scores_with_min_score
    WHERE state_code = 'US_UT'
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        reasons_fields=[
            ReasonsField(
                name="assessment_score_percent_reduction",
                type=bigquery.enums.StandardSqlTypeNames.INT64,
                description="Percentage risk score reduction",
            ),
            ReasonsField(
                name="assessment_score",
                type=bigquery.enums.StandardSqlTypeNames.INT64,
                description="Current risk score",
            ),
            ReasonsField(
                name="min_assessment_score",
                type=bigquery.enums.StandardSqlTypeNames.INT64,
                description="Minimum risk score in the past year",
            ),
        ],
        state_code=StateCode.US_UT,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
