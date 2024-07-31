# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# =============================================================================
"""Defines a criteria span view that shows spans of time during which
someone with a history of drug/alcohol risk has a recent negative
drug analysis test and no positive result within 90 days.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NEGATIVE_DA_WITHIN_90_DAYS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone with a history of drug/alcohol risk has a recent negative
drug analysis test and no positive result within 90 days"""

_QUERY_TEMPLATE = f"""
    WITH check_spans AS (
    /* This CTE checks for ALCOHOL/DRUG offenses as a proxy for alcohol or drug risk and sets a start date for needing a
    drug analysis test as the earliest date imposed for a alcohol/drug offense */
    SELECT
        state_code,
        person_id,
    --find the earliest ALCOHOL_DRUG sentence date for each person 
        MIN(date_imposed) AS start_date,
    FROM `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` 
    --exclude very old sentences that have improper parsing of date_imposed from 19XX to 20XX
    WHERE date_imposed <= CURRENT_DATE('US/Pacific')
        AND (ncic_category_uniform IN ('Liquor', 'Dangerous Drugs')
        OR ncic_description_uniform IN ('Driving Under Influence Drugs', 'Driving Under Influence Liquor', 'Drugs - Adulterated'))
    GROUP BY state_code, person_id
    ),
    test_spans AS (
       /* This query creates 90 day spans based off of every ua test and sets needs_ua_check to FALSE*/
    SELECT
        cs.state_code,
        cs.person_id,
        ds.drug_screen_date AS start_date,
        DATE_ADD(ds.drug_screen_date, INTERVAL 90 DAY) AS end_date,
        IF(ds.drug_screen_result = "NEGATIVE", false, true) AS is_positive_result,
        ds.drug_screen_date,
        FALSE AS needs_ua_check,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_drug_screen` ds
    INNER JOIN check_spans cs
        ON cs.person_id = ds.person_id
        AND cs.state_code = ds.state_code
        --only include tests after someone has a history of drug/alcohol risk
        AND ds.drug_screen_date >= cs.start_date
      WHERE sample_type in ("URINE","SALIVA")
        AND ds.state_code IS NOT NULL 
        AND ds.person_id IS NOT NULL
    /* This query creates a span of time which someone has a drug/alcohol risk and sets needs_ua_check to TRUE*/
    UNION ALL 
    SELECT 
        state_code,
        person_id,
        start_date,
        "9999-12-31" AS end_date,
        NULL AS is_positive_result,
        NULL AS drug_screen_date,
        TRUE AS needs_ua_check,
        FROM check_spans
    ),
    {create_sub_sessions_with_attributes('test_spans')}
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        --have TRUE tests take precendence over FALSE tests (ie. if a result is TRUE, there will be a 90 day span
        --where is_positive is TRUE, regardless of subsequent testing
        --any true test will make meets_criteria False
        --spans with no drug tests will result in needs_ua_check as True and meets_criteria as False
        (NOT LOGICAL_AND(needs_ua_check) AND NOT LOGICAL_OR(is_positive_result)) AS meets_criteria,
        --include all ua results in the past 90 days
        TO_JSON(STRUCT(ARRAY_AGG(is_positive_result IGNORE NULLS ORDER BY drug_screen_date, is_positive_result) AS latest_ua_results,
                    ARRAY_AGG(drug_screen_date IGNORE NULLS ORDER BY drug_screen_date, is_positive_result) AS latest_ua_dates)) AS reason,
        ARRAY_AGG(is_positive_result IGNORE NULLS ORDER BY drug_screen_date, is_positive_result) AS latest_ua_results,
        ARRAY_AGG(drug_screen_date IGNORE NULLS ORDER BY drug_screen_date, is_positive_result) AS latest_ua_dates,
    FROM sub_sessions_with_attributes
    GROUP BY 1,2,3,4
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        meets_criteria_default=True,
        reasons_fields=[
            ReasonsField(
                name="latest_ua_results",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="latest_ua_dates",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
