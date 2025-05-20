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
"""Describes the spans of time when a TN client has passed the drug screen check for
Compliant Reporting.

Identifies when the following is true for a TN client:
- For people considered “low drug risk”:
    - One negative screen in the last 12 months
    - At least 6 months since most recent positive test
    - Latest test is negative
- For people considered “high drug risk”:
    - Two negative screens in the last 12 months
    - At least 12 months since most recent positive test
    - Latest test is negative
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.criteria.general.at_least_6_months_since_most_recent_positive_drug_test import (
    VIEW_BUILDER as at_least_6_months_since_most_recent_positive_drug_test_builder,
)
from recidiviz.task_eligibility.criteria.general.at_least_12_months_since_most_recent_positive_drug_test import (
    VIEW_BUILDER as at_least_12_months_since_most_recent_positive_drug_test_builder,
)
from recidiviz.task_eligibility.criteria.general.has_at_least_1_negative_drug_test_past_year import (
    VIEW_BUILDER as has_at_least_1_negative_drug_test_past_year_builder,
)
from recidiviz.task_eligibility.criteria.general.has_at_least_2_negative_drug_tests_past_year import (
    VIEW_BUILDER as has_at_least_2_negative_drug_tests_past_year_builder,
)
from recidiviz.task_eligibility.criteria.general.latest_drug_test_is_negative import (
    VIEW_BUILDER as latest_drug_test_is_negative_builder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    STRONGR_ASSESSMENT_METADATA_DICT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_PASSED_DRUG_SCREEN_CHECK"

ASSESSMENT_TYPE_CASE_WHEN = "CASE assessment_type\n"

for assessment_type, metadata_mapping in STRONGR_ASSESSMENT_METADATA_DICT[
    "ALC_DRUG_NEED_LEVEL"
].items():
    ASSESSMENT_TYPE_CASE_WHEN += (
        f"WHEN '{assessment_type}' "
        f"THEN NULLIF(REPLACE(JSON_EXTRACT(assessment_metadata, '$.{metadata_mapping}'), '\"', ''),'')\n"
    )

ASSESSMENT_TYPE_CASE_WHEN += "ELSE NULL\nEND AS alc_drug_need_level"


_QUERY_TEMPLATE = f"""
    WITH combine_views AS (
        SELECT 
            state_code,
            person_id,
            assessment_date AS start_date,
            LEAD(assessment_date) OVER(PARTITION BY person_id ORDER BY assessment_date ASC) AS end_date,
            COALESCE(alc_drug_need_level, 'MISSING') IN ('LOW') AS meets_criteria_low,
            COALESCE(alc_drug_need_level, 'MISSING') IN ('MOD','HIGH') AS meets_criteria_high,
            CAST(NULL AS BOOL) AS meets_criteria_low_1_negative_screen,
            CAST(NULL AS BOOL) AS meets_criteria_low_6_months_since_positive,
            CAST(NULL AS BOOL) AS meets_criteria_high_2_negative_screens,
            CAST(NULL AS BOOL) AS meets_criteria_high_12_months_since_positive,
            CAST(NULL AS BOOL) AS meets_criteria_latest_screen_negative,
            TO_JSON(STRUCT('LATEST_ALCOHOL_DRUG_NEED_LEVEL' AS criteria_name,
                    COALESCE(alc_drug_need_level, 'MISSING') AS reason)) 
                    AS reason,
            TO_JSON(STRUCT(COALESCE(alc_drug_need_level, 'MISSING') AS latest_alcohol_drug_need_level)) AS reason_v2,
        FROM
            (
            SELECT *,
            {ASSESSMENT_TYPE_CASE_WHEN},
            FROM
                `{{project_id}}.sessions.risk_assessment_score_sessions_materialized`
            WHERE
                state_code = 'US_TN'
            QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, assessment_date
                ORDER BY CASE WHEN alc_drug_need_level = 'HIGH' THEN 0
                    WHEN alc_drug_need_level = 'MOD' THEN 1
                    WHEN alc_drug_need_level = 'LOW' THEN 2
                    ELSE 3 END) = 1
            )
        
        UNION ALL
        
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS meets_criteria_low,
            NULL AS meets_criteria_high,
            meets_criteria AS meets_criteria_low_1_negative_screen,
            NULL AS meets_criteria_low_6_months_since_positive,
            NULL AS meets_criteria_high_2_negative_screens,
            NULL AS meets_criteria_high_12_months_since_positive,
            NULL AS meets_criteria_latest_screen_negative,
            TO_JSON(STRUCT('{has_at_least_1_negative_drug_test_past_year_builder.criteria_name}' AS criteria_name, reason AS reason))
            reason,
            reason_v2,
        FROM
            `{{project_id}}.{{criteria_dataset}}.{has_at_least_1_negative_drug_test_past_year_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"
        
        UNION ALL
        
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS meets_criteria_low,
            NULL AS meets_criteria_high,
            NULL AS meets_criteria_low_1_negative_screen,
            meets_criteria AS meets_criteria_low_6_months_since_positive,
            NULL AS meets_criteria_high_2_negative_screens,
            NULL AS meets_criteria_high_12_months_since_positive,
            NULL AS meets_criteria_latest_screen_negative,
            TO_JSON(STRUCT('{at_least_6_months_since_most_recent_positive_drug_test_builder.criteria_name}' AS criteria_name, reason AS reason))
            reason,
            reason_v2,
        FROM
            `{{project_id}}.{{criteria_dataset}}.{at_least_6_months_since_most_recent_positive_drug_test_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"
            
        UNION ALL
        
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS meets_criteria_low,
            NULL AS meets_criteria_high,
            NULL AS meets_criteria_low_1_negative_screen,
            NULL AS meets_criteria_low_6_months_since_positive,
            NULL AS meets_criteria_high_2_negative_screens,
            NULL AS meets_criteria_high_12_months_since_positive,
            meets_criteria AS meets_criteria_latest_negative,
            TO_JSON(STRUCT('{latest_drug_test_is_negative_builder.criteria_name}' AS criteria_name, reason AS reason))
            reason,
            reason_v2,
        FROM
            `{{project_id}}.{{criteria_dataset}}.{latest_drug_test_is_negative_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"
            
        UNION ALL
        
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS meets_criteria_low,
            NULL AS meets_criteria_high,
            NULL AS meets_criteria_low_1_negative_screen,
            NULL AS meets_criteria_low_6_months_since_positive,
            meets_criteria AS meets_criteria_high_2_negative_screens,
            NULL AS meets_criteria_high_12_months_since_positive,
            NULL AS meets_criteria_latest_screen_negative,
            TO_JSON(STRUCT('{has_at_least_2_negative_drug_tests_past_year_builder.criteria_name}' AS criteria_name, reason AS reason))
            reason,
            reason_v2,
        FROM
            `{{project_id}}.{{criteria_dataset}}.{has_at_least_2_negative_drug_tests_past_year_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"
        
        UNION ALL
        
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date,
            NULL AS meets_criteria_low,
            NULL AS meets_criteria_high,
            NULL AS meets_criteria_low_1_negative_screen,
            NULL AS meets_criteria_low_6_months_since_positive,
            NULL AS meets_criteria_high_2_negative_screens,
            meets_criteria AS meets_criteria_high_12_months_since_positive,
            NULL AS meets_criteria_latest_screen_negative,
            TO_JSON(STRUCT('{at_least_12_months_since_most_recent_positive_drug_test_builder.criteria_name}' AS criteria_name, reason AS reason))
            reason,
            reason_v2,
        FROM
            `{{project_id}}.{{criteria_dataset}}.{at_least_12_months_since_most_recent_positive_drug_test_builder.view_id}_materialized`
        WHERE
            state_code = "US_TN"
    ),
    {create_sub_sessions_with_attributes('combine_views')},
    grouped AS (
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            -- If someone is missing assessment info, we assume they are not mod/high risk for alcohol/drug
            COALESCE(LOGICAL_OR(meets_criteria_low),TRUE) AS meets_criteria_low,
            COALESCE(LOGICAL_OR(meets_criteria_high),FALSE) AS meets_criteria_high,
            -- Setting each of these to the `meets_criteria_default` since a person may not have a span at all and would
            -- get null for a criteria if that were true
            COALESCE(LOGICAL_OR(meets_criteria_low_1_negative_screen),FALSE) AS meets_criteria_low_1_negative_screen,
            COALESCE(LOGICAL_OR(meets_criteria_low_6_months_since_positive),TRUE) AS meets_criteria_low_6_months_since_positive,
            COALESCE(LOGICAL_OR(meets_criteria_high_2_negative_screens),FALSE) AS meets_criteria_high_2_negative_screens,
            COALESCE(LOGICAL_OR(meets_criteria_high_12_months_since_positive),TRUE) AS meets_criteria_high_12_months_since_positive,
            LOGICAL_OR(meets_criteria_latest_screen_negative) AS meets_criteria_latest_screen_negative,            
            TO_JSON(ARRAY_AGG(
                reason ORDER BY JSON_VALUE(reason, "$.criteria_name")
            )) AS reason,

            ANY_VALUE(JSON_EXTRACT(reason_v2, "$.latest_alcohol_drug_need_level")) AS alc_drug_need_level,
            ANY_VALUE(JSON_EXTRACT(reason_v2, "$.negative_drug_screen_history_array")) AS negative_drug_screen_history_array,
            ANY_VALUE(JSON_EXTRACT(reason_v2, "$.most_recent_positive_test_date")) AS most_recent_positive_test_date,
            ANY_VALUE(JSON_EXTRACT(reason_v2, "$.latest_drug_screen_result")) AS latest_drug_screen_result,
            ANY_VALUE(JSON_EXTRACT(reason_v2, "$.latest_drug_screen_date")) AS latest_drug_screen_date,
        FROM sub_sessions_with_attributes
        GROUP BY
            1,2,3,4
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        CASE
            WHEN meets_criteria_low
                AND meets_criteria_low_1_negative_screen
                AND meets_criteria_low_6_months_since_positive
                AND meets_criteria_latest_screen_negative THEN TRUE
            WHEN meets_criteria_high
                AND meets_criteria_high_2_negative_screens
                AND meets_criteria_high_12_months_since_positive
                AND meets_criteria_latest_screen_negative THEN TRUE
            ELSE FALSE
            END AS meets_criteria,
        reason,
        alc_drug_need_level,
        negative_drug_screen_history_array,
        most_recent_positive_test_date,
        latest_drug_screen_result,
        latest_drug_screen_date,
    FROM grouped
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_TN,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    criteria_dataset=has_at_least_1_negative_drug_test_past_year_builder.dataset_id,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="alc_drug_need_level",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Latest assessed need level for alcohol/drugs",
        ),
        ReasonsField(
            name="negative_drug_screen_history_array",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Array that contains dates and results of all negative drug screens within the relevant lookback period",
        ),
        ReasonsField(
            name="most_recent_positive_test_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest positive drug screen",
        ),
        ReasonsField(
            name="latest_drug_screen_result",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="Result of latest drug screen",
        ),
        ReasonsField(
            name="latest_drug_screen_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date of latest drug screen",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
