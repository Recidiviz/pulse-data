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
"""Describes the spans of time when a TN client does not have a prior record with an
offense that would make them ineligible for Compliant Reporting.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_PRIOR_RECORD_WITH_INELIGIBLE_CR_OFFENSE"

_QUERY_TEMPLATE = f"""
    WITH prior_record_cte AS 
    (
    SELECT 
        person_id,
        state_code,
        offense_date AS start_date,
        CAST(NULL AS DATE) AS end_date,
        offense_date,
        FALSE AS meets_criteria,
        description,
    FROM `{{project_id}}.{{analyst_data_dataset}}.us_tn_prior_record_preprocessed_materialized`
    WHERE is_violent_domestic OR is_sex_offense OR is_dui OR is_victim_under_18 OR is_violent OR is_homicide
    )
    ,
    /*
    If a person has more than 1 prior record with an ineligible offense, they will have overlapping sessions created in
    the above CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('prior_record_cte')}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(description ORDER BY COALESCE(offense_date,'9999-01-01')) AS ineligible_offenses,
            ARRAY_AGG(offense_date ORDER BY COALESCE(offense_date,'9999-01-01')) AS ineligible_offense_dates
            )) AS reason,
        ARRAY_AGG(description ORDER BY COALESCE(offense_date,'9999-01-01')) AS ineligible_offenses,
        ARRAY_AGG(offense_date ORDER BY COALESCE(offense_date,'9999-01-01')) AS ineligible_offense_dates,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        analyst_data_dataset=ANALYST_VIEWS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Disqualifying offenses",
            ),
            ReasonsField(
                name="ineligible_offense_dates",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Dates of disqualifying offenses",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
