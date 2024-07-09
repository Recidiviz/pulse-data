#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View of cohorts with PROBATION, RIDER, and TERM sentences."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Only consider recent data, starting from this year
RECENT_DATA_START_YEAR = "2010"

SENTENCE_COHORT_VIEW_NAME = "sentence_cohort"

SENTENCE_COHORT_DESCRIPTION = """
This view combines cohorts of people who were sentenced to PROBATION, RIDER, or TERM.

This allows for recidivism and sentence disposition calculations for the purpose of PSI Case Insights.

The query combines 4 different sub-cohorts across 3 different cohorts:
    * PROBATION cohort:
        Those in the PROBATION compartment with inflow from LIBERTY or INVESTIGATION
    * RIDER cohort:
        * RIDER_TO_PROBATION sub-cohort:
            Those in the RIDER_TO_PROBATION cohort_sub_group, in TREATMENT_IN_PRISON compartment, with inflow from
            LIBERTY or INVESTIGATION and outflow to PROBATION
        * RIDER_TO_TERM sub-cohort:
            Those in the TREATMENT_IN_PRISON compartment, with inflow from LIBERTY or INVESTIGATION, outflow to GENERAL,
            and lead_outflow_to_level_1 is LIBERTY or SUPERVISION
    * GENERAL cohort:
        Those in the GENERAL compartment, with inflow from LIBERTY or INVESTIGATION, and outflow to SUPERVISION or
        LIBERTY
"""

SENTENCE_COHORT_QUERY_TEMPLATE = f"""
        WITH probation_cte AS
        (
        SELECT
          person_id,
          state_code,
          session_id,
          gender,
          start_date AS session_start_date,
          start_date AS cohort_start_date,
          'PROBATION' AS cohort_group,
          'PROBATION' AS cohort_sub_group,
          assessment_score_start AS assessment_score,
          assessment_score_bucket_start AS assessment_score_bucket,
        FROM `{{project_id}}.sessions.compartment_sessions_materialized`
        WHERE compartment_level_1 = 'SUPERVISION'
          AND compartment_level_2 = 'PROBATION'
          AND inflow_from_level_1 IN ('LIBERTY','INVESTIGATION')
        )
        ,
        rider_to_probation_cte AS
        (
        SELECT
          person_id,
          state_code,
          session_id,
          gender,
          start_date AS session_start_date,
          --end date of this session is the start of the probation period
          end_date_exclusive AS cohort_start_date,
          'RIDER' AS cohort_group,
          'RIDER_TO_PROBATION' AS cohort_sub_group,
          assessment_score_start AS assessment_score,
          assessment_score_bucket_start AS assessment_score_bucket,
        FROM `{{project_id}}.sessions.compartment_sessions_materialized`
        WHERE compartment_level_1 = 'INCARCERATION'
          AND compartment_level_2 = 'TREATMENT_IN_PRISON'
          AND inflow_from_level_1 IN ('LIBERTY','INVESTIGATION')
          AND outflow_to_level_1 = 'SUPERVISION'
          AND outflow_to_level_2 = 'PROBATION'
        )
        ,
        rider_to_term_cte AS
        (
        SELECT
          person_id,
          state_code,
          session_id,
          gender,
          start_date AS session_start_date,
          --the lead of the end date exclusive will be the start of the superivison or liberty period following general
          lead_end_date_exclusive AS cohort_start_date,
          'RIDER' AS cohort_group,
          'RIDER_TO_TERM' AS cohort_sub_group,
          assessment_score_start AS assessment_score,
          assessment_score_bucket_start AS assessment_score_bucket,
        FROM 
        (
            SELECT
                *,
                LEAD(compartment_level_1,2) OVER w AS lead_outflow_to_level_1,
                LEAD(compartment_level_2,2) OVER w AS lead_outflow_to_level_2,
                LEAD(end_date_exclusive,1) OVER w AS lead_end_date_exclusive,
            FROM `{{project_id}}.sessions.compartment_sessions_materialized`
            WINDOW w AS (PARTITION BY person_id ORDER BY session_id)
        )
        WHERE compartment_level_1 = 'INCARCERATION'
          AND compartment_level_2 = 'TREATMENT_IN_PRISON'
          AND inflow_from_level_1 IN ('LIBERTY','INVESTIGATION')
          AND outflow_to_level_2 = 'GENERAL'
          AND lead_outflow_to_level_1 IN ('SUPERVISION','LIBERTY')
        )
        ,
        general_cte AS
        (
        SELECT
          person_id,
          state_code,
          session_id,
          gender,
          start_date AS session_start_date,
          end_date_exclusive AS cohort_start_date,
          'TERM' AS cohort_group,
          'TERM' AS cohort_sub_group,
          assessment_score_start AS assessment_score,
          assessment_score_bucket_start AS assessment_score_bucket,
        FROM `{{project_id}}.sessions.compartment_sessions_materialized`
        WHERE compartment_level_1 = 'INCARCERATION'
          AND compartment_level_2 = 'GENERAL'
          AND inflow_from_level_1 IN ('LIBERTY','INVESTIGATION')
          AND outflow_to_level_1 IN ('SUPERVISION','LIBERTY')
        )
        ,
        cohort_start_cte AS
        (
        SELECT 
          *
        FROM probation_cte
        UNION ALL
        SELECT 
          *
        FROM rider_to_probation_cte
        UNION ALL
        SELECT 
          *
        FROM rider_to_term_cte
        UNION ALL
        SELECT 
          *
        FROM general_cte
        )
        SELECT
          cohort_start_cte.state_code,
          cohort_start_cte.person_id,
          cohort_start_cte.gender,
          cohort_start_cte.assessment_score,
          cohort_start_cte.cohort_group,
          cohort_start_cte.cohort_start_date,
          imposed_summary.most_severe_description,
          imposed_summary.any_is_drug_uniform,
          imposed_summary.any_is_violent_uniform,
          imposed_summary.any_is_sex_offense,

        FROM cohort_start_cte
        JOIN `{{project_id}}.sessions.compartment_sessions_closest_sentence_imposed_group` closest_imposed_group
          ON cohort_start_cte.person_id = closest_imposed_group.person_id
          AND cohort_start_cte.session_id = closest_imposed_group.session_id
        LEFT JOIN `{{project_id}}.sessions.sentence_imposed_group_summary_materialized` imposed_summary
          ON closest_imposed_group.person_id = imposed_summary.person_id
          AND closest_imposed_group.sentence_imposed_group_id = imposed_summary.sentence_imposed_group_id
        WHERE EXTRACT(YEAR FROM session_start_date) >= {RECENT_DATA_START_YEAR}
            # Must be at least 3 years in the past to properly calculate 36-month recidivism rates
            AND cohort_start_date <= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 3 YEAR)
            # For now, restricting to exact sentencing-to-session matching. We can probably relax this to <=7 or higher.
            AND DATE_DIFF(imposed_summary.date_imposed, session_start_date, DAY) = 0
        """


SENTENCE_COHORT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=SENTENCE_COHORT_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCE_COHORT_QUERY_TEMPLATE,
    description=SENTENCE_COHORT_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCE_COHORT_VIEW_BUILDER.build_and_print()
