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
"""Describes the spans of time when a TN client has not had a DUI offense in the past 5
years.
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
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    compliant_reporting_offense_type_condition,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_DUI_OFFENSE_IN_PAST_5_YEARS"

_QUERY_TEMPLATE = f"""
    WITH dui_offense_cte AS
    (
    SELECT
        state_code,
        person_id,
        offense_date AS start_date,
        DATE_ADD(offense_date, INTERVAL 5 YEAR) AS end_date,
        offense_date AS latest_dui_offense_date,
        FALSE as meets_criteria,
    FROM `{{project_id}}.sentence_sessions.sentences_and_charges_materialized`
    WHERE state_code = 'US_TN' 
        AND {compliant_reporting_offense_type_condition('is_dui')}
    
    UNION DISTINCT

    SELECT 
        state_code,
        person_id,
        offense_date AS start_date,
        DATE_ADD(offense_date, INTERVAL 5 YEAR) AS end_date,
        offense_date AS latest_dui_offense_date,
        FALSE AS meets_criteria,
    FROM `{{project_id}}.{{analyst_data_dataset}}.us_tn_prior_record_preprocessed_materialized`
    WHERE is_dui
    )
    ,
    /*
    If a person has more than 1 DUI offense in a 5 year period, they will have overlapping sessions created in the above
    CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('dui_offense_cte')}
    ,
    dedup_cte AS
    /*
    If a person has more than 1 DUI offense in a 5 year period, they will have duplicate sub-sessions for the period of
    time where there were more than 1 offense. For example, if a person has a offense on Jan 1 and March 1
    there would be duplicate sessions for the period starting March 1 because both DUI offenses are relevant at that time.
    We deduplicate below so that we surface the most-recent DUI offense that is relevant at each time. 
    */
    (
    SELECT
        *,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY latest_dui_offense_date DESC) = 1
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_dui_offense_date AS latest_dui_offense)) AS reason,
        latest_dui_offense_date,
    FROM dedup_cte
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
                name="latest_dui_offense_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of latest DUI offense",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
