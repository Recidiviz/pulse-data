# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Defines a criteria span view that shows spans of time during which someone is eligible for an in person review
from the ADD during their security classification review, as the number of expected reviews is greater than the
number of observed reviews."""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
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

_CRITERIA_NAME = "US_MI_EXPECTED_NUMBER_OF_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEWS_GREATER_THAN_OBSERVED"

_DESCRIPTION = """Defines a criteria span view that shows spans of time during which someone is eligible
for an ADD in person security classification review based on the number of observed reviews. 

A resident is eligible for an in person review from the ADD every year they are in solitary confinement, 
regardless of if they transfer facilities.

This view keeps a tally of how many ADD in person reviews a resident should have, and subtracts instances where we 
see an ADD in person review take place."""

_QUERY_TEMPLATE = f"""
   WITH review_dates AS (
       /* This CTE generates the dates for which an ADD in person scc review should occur. These should happen 
       at one year intervals from the start date of the solitary stay. */
        SELECT 
            h.state_code,
            h.person_id, 
            DATE_TRUNC(DATE_ADD(h.start_date, INTERVAL offset MONTH), WEEK(MONDAY)) AS change_date
        FROM `{{project_id}}.{{sessions_dataset}}.us_mi_housing_unit_type_collapsed_solitary_sessions_materialized` h,
            UNNEST(GENERATE_ARRAY(12, 1200, 12)) AS offset
        WHERE
        --calculate recurring scc reviews until the solitary session ends or for 100 years
          DATE_TRUNC(DATE_ADD(h.start_date, INTERVAL offset MONTH), WEEK(MONDAY)) <= {nonnull_end_date_exclusive_clause('end_date_exclusive')}
          --because the case note for tracking add reviews was introduced in 04/2024, we only start counting up
          --expected reviews at most one year prior to this date. Therefore, everyone in ad seg for > 1 year should 
          --be due for one (and not more) review
          AND DATE_ADD(start_date, INTERVAL offset MONTH) >= '2023-04-01' 
          AND h.housing_unit_type_collapsed_solitary = 'SOLITARY_CONFINEMENT'
          AND h.state_code = 'US_MI'
    ),
    population_change_dates AS (
    /* this CTE gathers all dates at which eligibility might change */ 
        --add 1 for each expected SCC review (this includes the start dates for solitary sessions that happen 
        --after the COMS migration)  
        SELECT
            state_code,
            person_id,
            change_date,
            1 AS expected_review,
            1 AS activity_type,
        FROM review_dates
        
        UNION ALL   
        
        --include a population change date for solitary sessions that happen before the COMS migration, 
        --so that even though we aren't accruing expected reviews before then, we can still aggregate within sessions. 
        SELECT 
            state_code,
            person_id,
            start_date AS change_date,
            0 AS expected_review,
            0 AS activity_type,
        FROM
            `{{project_id}}.{{sessions_dataset}}.us_mi_housing_unit_type_collapsed_solitary_sessions_materialized` h
        WHERE
            state_code = 'US_MI'
            AND housing_unit_type_collapsed_solitary = 'SOLITARY_CONFINEMENT'
            AND start_date <= '2023-04-01'
       
        UNION ALL
        
        -- add a change date for when the solitary session ends
        SELECT 
            state_code,
            person_id,
            h.end_date_exclusive AS change_date,
            0 AS expected_review,
            0 AS activity_type,
        FROM
            `{{project_id}}.{{sessions_dataset}}.us_mi_housing_unit_type_collapsed_solitary_sessions_materialized` h
        WHERE
            state_code = 'US_MI'
            AND housing_unit_type_collapsed_solitary = 'SOLITARY_CONFINEMENT'
        
        UNION ALL
        
        --add -1 for every time we observe a review 
        SELECT 
            state_code,
            person_id,
            completion_event_date AS change_date,
            0 AS expected_review,
            -1 AS activity_type,
        FROM
            `{{project_id}}.{{analyst_views_dataset}}.us_mi_add_in_person_security_classification_committee_review_materialized` s
        WHERE completion_event_date >= '2023-08-14'

    ),
    population_change_dates_agg AS (
        SELECT 
            state_code,
            person_id,
            change_date,
            SUM(expected_review) AS expected_review,
            SUM(activity_type) AS activity_type,
        FROM
            population_change_dates
        GROUP BY
            1,2,3
    ),
    time_spans AS (
        SELECT 
            p.state_code,
            p.person_id,
            p.change_date AS start_date,
            LEAD(change_date) OVER (PARTITION BY p.state_code,
                                               p.person_id
                                  ORDER BY change_date) AS end_date,
            p.expected_review, 
            p.activity_type,
        FROM
            population_change_dates_agg p
    ),
    time_spans_agg AS (
        SELECT 
            ts.state_code,
            ts.person_id,
            ts.start_date,
            ts.end_date,
            hu.session_id,
            hu.start_date AS housing_start_date,
            hu.end_date_exclusive AS housing_end_date,
            SUM(expected_review) OVER (PARTITION BY ts.state_code, 
                                                    ts.person_id,
                                                    hu.session_id
                                    ORDER BY ts.start_date
            ) AS expected_reviews,
            SUM(activity_type) OVER (PARTITION BY ts.state_code, 
                                                    ts.person_id,
                                                    hu.session_id
                                    ORDER BY ts.start_date
            ) AS reviews_due,
        FROM
            time_spans ts
        INNER JOIN
            `{{project_id}}.{{sessions_dataset}}.us_mi_housing_unit_type_collapsed_solitary_sessions_materialized` hu
            ON ts.person_id = hu.person_id
            AND ts.state_code = hu.state_code
            AND ts.start_date < {nonnull_end_date_clause('hu.end_date_exclusive')}
            AND hu.start_date < {nonnull_end_date_clause('ts.end_date')}
            AND hu.housing_unit_type_collapsed_solitary = 'SOLITARY_CONFINEMENT'
    )
    SELECT 
        t.state_code,
        t.person_id,
        t.start_date,
        t.end_date,
        t.reviews_due > 0 AS meets_criteria,
        TO_JSON(STRUCT(
                housing_start_date AS solitary_start_date,
                t.expected_reviews AS number_of_expected_reviews,
                t.expected_reviews-t.reviews_due AS number_of_reviews,
                p.change_date AS latest_add_in_person_scc_review_date
            )) AS reason,
        housing_start_date AS solitary_start_date,
        t.expected_reviews AS number_of_expected_reviews,
        t.expected_reviews-t.reviews_due AS number_of_reviews,
        p.change_date AS latest_add_in_person_scc_review_date,
    FROM time_spans_agg t
    LEFT JOIN population_change_dates  p
        ON p.state_code = t.state_code
        AND p.person_id = t.person_id 
        AND p.change_date BETWEEN housing_start_date AND {nonnull_end_date_exclusive_clause('housing_end_date')}
        AND p.change_date < {nonnull_end_date_exclusive_clause('t.end_date')}
        --only join SCC reviews
        AND p.activity_type = -1
    --pick latest SCC review within the relevant housing unit session
    QUALIFY ROW_NUMBER() OVER(PARTITION BY t.person_id, t.start_date ORDER BY p.change_date DESC)=1
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name=_CRITERIA_NAME,
    description=_DESCRIPTION,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    state_code=StateCode.US_MI,
    sessions_dataset=SESSIONS_DATASET,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="solitary_start_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date resident was placed in any type of solitary confinement",
        ),
        ReasonsField(
            name="number_of_expected_reviews",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Number of expected ADD in person reviews based on time spent in solitary",
        ),
        ReasonsField(
            name="number_of_reviews",
            type=bigquery.enums.StandardSqlTypeNames.INT64,
            description="Number of observed ADD in person reviews",
        ),
        ReasonsField(
            name="latest_add_in_person_scc_review_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Latest observed ADD in person review",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
