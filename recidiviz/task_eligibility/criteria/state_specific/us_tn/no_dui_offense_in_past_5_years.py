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
"""Describes the spans of time when a TN client has not had a DUI offense for 12 months."""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_NO_DUI_OFFENSE_IN_PAST_5_YEARS"

_DESCRIPTION = """Describes the spans of time when a TN client has not had a DUI offense for 12 months.
"""

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
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
    WHERE state_code = 'US_TN' 
        AND is_dui
    
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
    If a person has more than 1 DUI offense in a 5 month period, they will have duplicate sub-sessions for the period of
    time where there were more than 1 offense. For example, if a person has a offense on Jan 1 and March 1
    there would be duplicate sessions for the period March 1 - Dec 31 because both DUI offenses are relevant at that time.
    We deduplicate below so that we surface the most-recent DUI offense that is relevant at each time. 
    */
    (
    SELECT
        *,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY latest_dui_offense_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is not eligible due to a DUI offense. A
    new session exists either when a person becomes eligible, or if a person has an additional DUI offense within a 
    5-year period which changes the "latest_dui_offense_date" value.
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                       attribute=['latest_dui_offense_date','meets_criteria'],
                       end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_dui_offense_date AS latest_dui_offense)) AS reason,
        latest_dui_offense_date,
    FROM sessionized_cte
    
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        analyst_data_dataset=ANALYST_VIEWS_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="latest_dui_offense_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
