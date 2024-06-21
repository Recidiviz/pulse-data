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
# ============================================================================
"""Describes the spans of time when a TN client is eligible with discretion to due past sentences for
ineligible offenses that are less than 10 years expired"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_INELIGIBLE_OFFENSES_EXPIRED"

_DESCRIPTION = """Describes the spans of time when a TN client is eligible with discretion to due past sentences for
ineligible offenses that are less than 10 years expired.
"""

_QUERY_TEMPLATE = f"""
    WITH cte AS 
    (
    SELECT 
        state_code,
        person_id,
        projected_completion_date_max AS start_date,
        --If the year of the date is greater than 9000, set it to NULL to avoid date overflow errors for dates within
        --10 years of the max date in bigquery ('9999-12-31')
        --TODO(#21472): Investigate placeholder expiration dates far in the future
        DATE_ADD(IF(projected_completion_date_max>='3000-01-01', NULL, projected_completion_date_max),
            INTERVAL 10 YEAR) AS end_date,
        projected_completion_date_max AS expiration_date,
        description,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
    WHERE state_code = 'US_TN'
        AND (is_violent_domestic OR is_sex_offense OR is_dui OR is_violent OR is_victim_under_18)
        AND completion_date IS NOT NULL
    )
    ,
    /*
    If a person has more than 1 sentence completed in the 10 year period, they will have overlapping sessions created in
    the above CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('cte')}

    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        FALSE AS meets_criteria,
        TO_JSON(STRUCT(
            ARRAY_AGG(description ORDER BY expiration_date) AS ineligible_offenses,
            ARRAY_AGG(expiration_date ORDER BY expiration_date) AS ineligible_sentences_expiration_dates
            )) AS reason,
        ARRAY_AGG(description ORDER BY expiration_date) AS ineligible_offenses,
        ARRAY_AGG(expiration_date ORDER BY expiration_date) AS ineligible_sentences_expiration_dates
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4,5

    
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="ineligible_sentences_expiration_dates",
                type=bigquery.enums.SqlTypeNames.RECORD,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
