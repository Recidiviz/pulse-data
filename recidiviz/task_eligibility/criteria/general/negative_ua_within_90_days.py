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
UA test (within 90 days)
"""
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_start_date_clause,
    revert_nonnull_end_date_clause,
    revert_nonnull_start_date_clause,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "NEGATIVE_UA_WITHIN_90_DAYS"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which
someone with a history of drug/alcohol risk has a recent negative
urine analysis test (within 90 days)"""

_QUERY_TEMPLATE = f"""
/*{{description}}*/
WITH active_supervision_population AS (
/* This CTE first identifies the entire supervision population 
to include individuals who may not have sentencing criteria and default
their eligibility for this task to True*/
  SELECT DISTINCT
    state_code,
    person_id,
FROM `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized`,
UNNEST(session_attributes) AS attr
WHERE attr.compartment_level_1 = "SUPERVISION"
    AND attr.compartment_level_2 NOT IN (
        "INTERNAL_UNKNOWN",
        "ABSCONSION",
        "BENCH_WARRANT"
    )
),
span_no_checking AS (
#TODO(#15105) simplify logic once default TRUE option is available
/* This CTE checks for ALCOHOL/DRUG offenses as a proxy for 
alcohol or drug risk and sets spans when a ua_check is not needed
 because an ALCOHOL/DRUG offense does not exist during that time*/
  SELECT
    pop.state_code,
    pop.person_id,
    FALSE AS needs_ua_check,
    --find the earliest sentence date for each person, 
    --if null, set to non null start date in the past
    {nonnull_start_date_clause('''MIN(date_imposed)''')} AS start_date,
    --find the earliest ALCOHOL_DRUG sentence date for each person 
    --if NULL set to date in future 
    {nonnull_end_date_clause('''MIN(IF(offense_type_short = "ALCOHOL_DRUG", date_imposed, NULL))''')} AS end_date,
  FROM active_supervision_population pop
  LEFT JOIN `{{project_id}}.{{sessions_dataset}}.sentences_preprocessed_materialized` sent
    ON pop.person_id = sent.person_id
    AND pop.state_code = sent.state_code 
    --exclude very old sentences that have improper parsing of date_imposed from 19XX to 20XX
    AND date_imposed <= CURRENT_DATE('US/Pacific')
  GROUP BY state_code, person_id
),
all_spans AS (
/* This CTE excludes zero day spans and creates spans for which there is an
ALCOHOL/DRUG offense and a ua_check is needed */
    SELECT * FROM span_no_checking
    --do not include false 0 day spans where the first sentence is an ALCOHOL_DRUG offense
    WHERE start_date != end_date
    UNION ALL
    SELECT
      state_code,
      person_id,
      TRUE AS needs_ua_check,
      end_date AS start_date, --date of first ALCOHOL_DRUG offense
      "9999-12-31" AS end_date --is now always true 
    FROM span_no_checking
     --if end date is NULL, then no ALCOHOL_DRUG offenses
    WHERE end_date != "9999-12-31"
),
drug_screen_spans AS(
/* This CTE combines spans where ua_checks are/are not needed with the actual
drug screen tests and results. Only tests within the last 90 days are included */
    SELECT sc.*,
        ds.drug_screen_date,
        ds.is_positive_result,
    FROM all_spans sc
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.drug_screens_preprocessed_materialized` ds
      ON sc.state_code = ds.state_code
      AND sc.person_id = ds.person_id
      AND ds.drug_screen_date BETWEEN sc.start_date AND sc.end_date 
      AND ds.sample_type = "URINE"
      --only include drug screens for those where a check is needed
      AND needs_ua_check
),
drug_screen_spans_next AS (
  /* This CTE adds the adj positive drug screen date to the previous CTE to create spans of eligibility
  based on 90 day windows around each drug screen date */
    SELECT 
        *, 
        LEAD(is_positive_result) 
            OVER(PARTITION BY person_id ORDER BY start_date, drug_screen_date) AS next_drug_screen_result,
        {nonnull_end_date_clause(
            '''LEAD(drug_screen_date) 
                    OVER(PARTITION BY person_id ORDER BY start_date, drug_screen_date) 
            ''')} AS next_drug_screen_date,
    FROM drug_screen_spans
),
final_spans AS (
    SELECT 
        --create true spans for all periods with no alcohol/drug risk 
        state_code,
        person_id,
        start_date,
        IF(end_date != "9999-12-31", DATE_SUB(end_date, INTERVAL 1 DAY), "9999-12-31") AS end_date,
        TRUE as meets_criteria,
        TO_JSON(STRUCT(needs_ua_check AS history_of_drug_alcohol, NULL AS ua_result, NULL AS ua_date)) AS reason
    FROM drug_screen_spans_next
    WHERE NOT needs_ua_check
    UNION ALL 
    SELECT
        --create true spans for 90 days around a negative ua
        --however, if a positive ua occurs within that window, end true span early
        state_code,
        person_id,
        drug_screen_date AS start_date,
        --negative ua test creates a 90 day true span
        --unless there is a adjacent drug screen date sooner 
        LEAST(DATE_ADD(drug_screen_date, INTERVAL 90 DAY),
                                            DATE_SUB(next_drug_screen_date, INTERVAL 1 DAY)) AS end_date,
        TRUE as meets_criteria,
        TO_JSON(
            STRUCT(
                needs_ua_check AS history_of_drug_alcohol, 
                is_positive_result AS ua_result, 
                drug_screen_date AS ua_date)) AS reason
    FROM drug_screen_spans_next
    WHERE needs_ua_check AND NOT is_positive_result
    UNION ALL 
    SELECT
        --create false spans for periods between negative ua expiration and next ua 
        state_code,
        person_id,
        DATE_ADD(drug_screen_date, INTERVAL 91 DAY) AS start_date,
        IF(next_drug_screen_date != "9999-12-31", DATE_SUB(next_drug_screen_date, INTERVAL 1 DAY), "9999-12-31") AS end_date,
        FALSE as meets_criteria,
        TO_JSON(
            STRUCT(
                needs_ua_check AS history_of_drug_alcohol, 
                is_positive_result AS ua_result, 
                drug_screen_date AS ua_date)) AS reason
        FROM drug_screen_spans_next
    WHERE needs_ua_check AND NOT is_positive_result
        AND DATE_DIFF(COALESCE(next_drug_screen_date, "9999-12-31"), drug_screen_date, DAY)> 90
    UNION ALL 
    SELECT
        --create false spans for positive ua from drug screen date to date of next ua 
        state_code,
        person_id,
        drug_screen_date AS start_date,
        IF(next_drug_screen_date != "9999-12-31", DATE_SUB(next_drug_screen_date, INTERVAL 1 DAY), "9999-12-31") AS end_date,
        FALSE as meets_criteria,
        TO_JSON(
            STRUCT(
                needs_ua_check AS history_of_drug_alcohol, 
                is_positive_result AS ua_result, 
                drug_screen_date AS ua_date)) AS reason
        FROM drug_screen_spans_next
    WHERE needs_ua_check AND is_positive_result
    UNION ALL
    SELECT
        --create false span from beginning of span with drug/alcohol risk
        --to first drug screen date
        state_code,
        person_id,
        start_date AS start_date,
        IF(drug_screen_date IS NOT NULL, DATE_SUB(drug_screen_date, INTERVAL 1 DAY), "9999-12-31") AS end_date,
        FALSE as meets_criteria,
        TO_JSON(
            STRUCT(
                needs_ua_check AS history_of_drug_alcohol, 
                is_positive_result AS ua_result, 
                drug_screen_date AS ua_date)) AS reason
        FROM drug_screen_spans_next
    WHERE needs_ua_check 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, start_date ORDER BY start_date, drug_screen_date) =1
    )
SELECT 
    state_code,
    person_id,
    {revert_nonnull_start_date_clause('start_date')} AS start_date,
    {revert_nonnull_end_date_clause('end_date')} AS end_date,
    meets_criteria,
    reason,
FROM final_spans
ORDER BY person_id, start_date
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
