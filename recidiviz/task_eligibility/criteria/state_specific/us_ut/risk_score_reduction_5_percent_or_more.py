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
"""Spans of time when someone in UT saw a 5% or more reduction in risk from initial 
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

_CRITERIA_NAME = "US_UT_RISK_SCORE_REDUCTION_5_PERCENT_OR_MORE"

_QUERY_TEMPLATE = f"""
# TODO(#37900) - Change after schema change request is approved
WITH {assessment_scores_with_first_score_ctes(assessment_types_list = ["INTERNAL_UNKNOWN"])}

SELECT 
    state_code,
    person_id,
    start_date,
    end_date,
    (IFNULL(assessment_score_percent_reduction, 0)>=5) AS meets_criteria,
    TO_JSON(STRUCT(
        assessment_score_percent_reduction AS assessment_score_percent_reduction,
        assessment_score AS assessment_score,
        first_assessment_score AS first_assessment_score
        )) AS reason,
    assessment_score_percent_reduction,
    assessment_score,
    first_assessment_score,
FROM assessment_scores_with_first_score
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=__doc__,
    reasons_fields=[
        ReasonsField(
            name="assessment_score_percent_reduction",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Percent change in risk scores between first and current assessment",
        ),
        ReasonsField(
            name="assessment_score",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Current risk score",
        ),
        ReasonsField(
            name="first_assessment_score",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="First risk score in this supervision span",
        ),
    ],
    state_code=StateCode.US_UT,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    meets_criteria_default=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
