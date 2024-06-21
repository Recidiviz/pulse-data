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
"""Describes the spans of time when a TN client has not had a murder conviction."""
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

_CRITERIA_NAME = "US_TN_NO_MURDER_CONVICTIONS"

_DESCRIPTION = """Describes the spans of time when a TN client has not had a murder conviction.
"""

_QUERY_TEMPLATE = f"""
    WITH homicide_offense_cte AS 
    (
    SELECT 
        person_id,
        state_code,
        offense_date AS start_date,
        CAST(NULL AS DATE) AS end_date,
        offense_date AS latest_homicide_offense_date,
        FALSE AS meets_criteria,
    FROM `{{project_id}}.{{analyst_data_dataset}}.us_tn_prior_record_preprocessed_materialized`
    WHERE is_homicide
    
    UNION DISTINCT
    
    SELECT 
        person_id,
        state_code,
        offense_date,
        CAST(NULL AS DATE) AS end_date,
        offense_date AS latest_homicide_offense_date,
        FALSE AS meets_criteria,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized`
    WHERE state_code = 'US_TN' 
      AND is_homicide
    )
    ,
    /*
    If a person has more than 1 homicide offense, they will have overlapping sessions created in the above
    CTE. Therefore we use `create_sub_sessions_with_attributes` to break these up
    */
    {create_sub_sessions_with_attributes('homicide_offense_cte')}
    ,
    dedup_cte AS
    /*
    If a person has more than 1 homicide offense, they will have duplicate sub-sessions. We deduplicate below so that we
    surface the most-recent homicide offense.
    */
    (
    SELECT
        *,
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date 
        ORDER BY latest_homicide_offense_date DESC) = 1
    )
    ,
    sessionized_cte AS 
    /*
    Sessionize so that we have continuous periods of time for which a person is not eligible due to a homicide offense. 
    */
    (
    {aggregate_adjacent_spans(table_name='dedup_cte',
                              attribute=['latest_homicide_offense_date', 'meets_criteria'],
                              end_date_field_name='end_date')}
    )
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(latest_homicide_offense_date AS latest_homicide_offense)) AS reason,
        latest_homicide_offense_date,
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
                name="latest_homicide_offense_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
