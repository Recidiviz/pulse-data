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
"""Describes the spans of time when a TN client has any sentence(s) that has/have
expired within the past 10 years for any offense(s) that would make them ineligible for
Compliant Reporting.
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
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

_CRITERIA_NAME = "US_TN_INELIGIBLE_OFFENSES_EXPIRED"

_QUERY_TEMPLATE = f"""
    -- TODO(#38294) Update this to use sentence serving periods
    WITH cte AS 
    (
    SELECT DISTINCT
        state_code,
        person_id,
        sentence_projected_full_term_release_date_max AS start_date,
        --If the year of the date is greater than 9000, set it to NULL to avoid date overflow errors for dates within
        --10 years of the max date in bigquery ('9999-12-31')
        --TODO(#21472): Investigate placeholder expiration dates far in the future
        DATE_ADD(IF(sentence_projected_full_term_release_date_max>='3000-01-01', NULL, sentence_projected_full_term_release_date_max),
            INTERVAL 10 YEAR) AS end_date,
        sentence_projected_full_term_release_date_max AS expiration_date,
        description,
    FROM `{{project_id}}.sentence_sessions.person_projected_date_sessions_materialized` span,
    UNNEST(sentence_array)
    JOIN `{{project_id}}.sentence_sessions.sentences_and_charges_materialized` sent
      USING(person_id, state_code, sentence_id)
    JOIN `{{project_id}}.sentence_sessions.sentence_serving_period_materialized` sp
        USING(person_id, state_code, sentence_id)
    WHERE state_code = 'US_TN'
        AND ({compliant_reporting_offense_type_condition(['is_violent_domestic','is_dui','is_victim_under_18'])}
        OR is_sex_offense OR is_violent)
        AND sp.end_date_exclusive IS NOT NULL
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
        description=__doc__,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="ineligible_offenses",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Disqualifying offenses",
            ),
            ReasonsField(
                name="ineligible_sentences_expiration_dates",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Expiration dates for sentences for disqualifying offenses",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
