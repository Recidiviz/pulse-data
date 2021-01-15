# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""View tracking monthly positive and backstop metrics, along with demographic attributes, at the district level
for use in impact measurement"""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET, ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.calculator.query.state.views.po_report.violation_reports_query import violation_reports_query

PO_REPORT_IMPACT_METRICS_VIEW_NAME = "po_report_impact_metrics"

PO_REPORT_IMPACT_METRICS_VIEW_DESCRIPTION = \
    "Reports district-level averages for a variety of metrics to help track impact of PO monthly report rollouts"

PO_REPORT_IMPACT_METRICS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH po_report_metrics_monthly AS
    /* Selects completions, discharges, supervision downgrades, and revocation counts by type from PO monthly report data */
      (
      SELECT state_code, year, month, district,
        COUNTIF(successful_completion_date IS NOT NULL) AS num_successful_completions,
        COUNTIF(earned_discharge_date IS NOT NULL) AS num_earned_discharges,
        COUNTIF(DATE_TRUNC(latest_supervision_downgrade_date, MONTH) = DATE(year, month, 1)) AS num_supervision_downgrades,
        COUNTIF(revocation_violation_type = 'TECHNICAL' OR revocation_report_date IS NOT NULL) AS num_technical_or_absconsion_revocations,
        COUNTIF(revocation_violation_type = 'NEW_CRIME') AS num_new_crime_revocations,
        COUNTIF(absconsion_report_date IS NOT NULL) AS num_absconsions
      FROM `{project_id}.{po_report_dataset}.report_data_by_person_by_month_materialized`
      JOIN `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
      USING (state_code, year, month, officer_external_id)
      GROUP BY state_code, year, month, district
      ),

    violation_reports_monthly AS
    /* Selects unique violation reports based on person id and violation report dates, and counts total reports per district and month */
      (
        SELECT state_code, year, month, district, COUNT(*) AS num_violation_reports
        FROM
          (
            SELECT DISTINCT state_code, person_id, year, month, response_date, officer_external_id
            FROM ({violation_reports_query}))
        JOIN `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
          USING (state_code, year, month, officer_external_id)
        GROUP BY state_code, year, month, district
      ),
    rider_boardhold_revocations_monthly AS
    /* Counts unique monthly rider revocations (where revocation type is TREATMENT_IN_PRISON) and board hold revocations
     (revocation occurring from PAROLE supervision period) based on person and date. */
      (
        SELECT state_code, year, month, district,
          SUM(is_rider_revocation) AS num_rider_revocations,
          SUM(is_parole_board_hold_revocation) AS num_parole_board_hold_revocations,
        FROM
          (
            SELECT DISTINCT state_code, year, month, supervising_district_external_id as district,
                person_id, revocation_admission_date,
                IF(revocation_type = 'TREATMENT_IN_PRISON', 1, 0) AS is_rider_revocation,
                IF(supervision_type = 'PAROLE', 1, 0) AS is_parole_board_hold_revocation
            FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_revocation_analysis_metrics_materialized`
          )
        GROUP BY state_code, year, month, district
      ),
    length_of_stay_metrics_monthly AS
    /* Pulls the average length of stay, and average ratio of length of stay to projected sentence length, for each supervision completion in a given month */
      (
        SELECT DISTINCT
          state_code, year, month, district,
          PERCENTILE_CONT(session_length_days, .5)
            OVER (PARTITION BY state_code, year, month, district) AS median_days_served,
          AVG(session_length_days)
            OVER (PARTITION BY state_code, year, month, district) AS avg_days_served,
            
          /* Get proportion of sentence length (time from start date to projected max completion date) served on supervision.
             Only include sentence lengths that are strictly positive. */
          PERCENTILE_CONT(session_length_days/IF(projected_sentence_length_days > 0, projected_sentence_length_days, NULL), .5)
            OVER (PARTITION BY state_code, year, month, district) AS median_prop_days_served,
          AVG(session_length_days/IF(projected_sentence_length_days > 0, projected_sentence_length_days, NULL))
            OVER (PARTITION BY state_code, year, month, district) AS avg_prop_days_served
        FROM
          (
            SELECT
                state_code,
                EXTRACT(YEAR FROM sub_sessions.end_date) AS year,
                EXTRACT(MONTH FROM sub_sessions.end_date) AS month,
                SPLIT(compartment_location, '|')[OFFSET(1)] AS district,
                sessions.session_length_days as session_length_days,
                DATE_DIFF(projected_completion_date_max, sessions.start_date, DAY) AS projected_sentence_length_days
            FROM `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized` sub_sessions
            JOIN `{project_id}.{analyst_dataset}.compartment_sentences_materialized` sentences
              USING (state_code, person_id, session_id)
            JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
              USING (state_code, person_id, session_id)
            WHERE sub_sessions.compartment_level_1 = 'SUPERVISION'
              AND sub_sessions.outflow_to_level_1 = 'RELEASE'
              AND sub_sessions.end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
          )
      ),

    pop_adj_metrics AS
    /* Normalizes all count metrics by total district-level caseload (count per 100 people on supervision).
       Combines all metrics and demographic attributes into a single view for each month in the last 2 years. */
      (
        SELECT state_code, year, month, district,
          DATE_DIFF(DATE(year, month, 1), DATE_TRUNC(CURRENT_DATE(), MONTH), MONTH) AS absolute_date,
          num_successful_completions*100/supervision_population AS num_successful_completions_pop,
          num_earned_discharges*100/supervision_population AS num_earned_discharges_pop,
          avg_days_served,
          avg_prop_days_served,
          median_days_served,
          median_prop_days_served,
          num_supervision_downgrades*100/supervision_population AS num_supervision_downgrades_pop,
          num_technical_or_absconsion_revocations*100/supervision_population AS num_technical_or_absconsion_revocations_pop,
          num_new_crime_revocations*100/supervision_population AS num_new_crime_revocations_pop,
          num_rider_revocations*100/supervision_population AS num_rider_revocations_pop,
          num_parole_board_hold_revocations*100/supervision_population AS num_parole_board_hold_revocations_pop,
          num_absconsions*100/supervision_population AS num_absconsions_pop,
          num_violation_reports*100/supervision_population AS num_violation_reports_pop,

          /* Demographic attributes about composition of supervision population */
          supervision_population,
          probation_population,
          parole_population,
          gender_male_population,
          race_white_population,
          age_under_30_population,
          supervision_level_minimum_population,
          supervision_level_medium_population,
          supervision_level_high_population,
          risk_score_over_30_population,
          risk_score_over_39_population,
          risk_score_under_24_population,
          rolling_average_months
        FROM po_report_metrics_monthly
        LEFT JOIN violation_reports_monthly
          USING (state_code, year, month, district)
        LEFT JOIN rider_boardhold_revocations_monthly
          USING (state_code, year, month, district)
        LEFT JOIN length_of_stay_metrics_monthly
          USING (state_code, year, month, district)
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_population_attributes_by_district_by_month_materialized`
          USING (state_code, year, month, district),
        {rolling_average_months_dimension}
        WHERE DATE(year, month, 1) >= DATE_SUB(CURRENT_DATE(), INTERVAL 25 MONTH)
        AND DATE(year, month, 1) < DATE_TRUNC(CURRENT_DATE(), MONTH)
      )

    /* Calculates rolling averages of all population adjusted metrics */
    SELECT m1.state_code, m1.year, m1.month, m1.district, m1.rolling_average_months,
      AVG(m2.num_successful_completions_pop) AS num_successful_completions_pop,
      AVG(m2.num_earned_discharges_pop) AS num_earned_discharges_pop,
      AVG(m2.avg_days_served) AS avg_days_served,
      AVG(m2.avg_prop_days_served) AS avg_prop_days_served,
      AVG(m2.median_days_served) AS median_days_served,
      AVG(m2.median_prop_days_served) AS median_prop_days_served,
      AVG(m2.num_supervision_downgrades_pop) AS num_supervision_downgrades_pop,
      AVG(m2.num_technical_or_absconsion_revocations_pop) AS num_technical_or_absconsion_revocations_pop,
      AVG(m2.num_new_crime_revocations_pop) AS num_new_crime_revocations_pop,
      AVG(m2.num_rider_revocations_pop) AS num_rider_revocations_pop,
      AVG(m2.num_parole_board_hold_revocations_pop) AS num_parole_board_hold_revocations_pop,
      AVG(m2.num_absconsions_pop) AS num_absconsions_pop,
      AVG(m2.num_violation_reports_pop) AS num_violation_reports_pop,
      AVG(m2.supervision_population) AS supervision_population,
      AVG(m2.probation_population) AS probation_population,
      AVG(m2.parole_population) AS parole_population,
      AVG(m2.gender_male_population) AS gender_male_population,
      AVG(m2.race_white_population) AS race_white_population,
      AVG(m2.age_under_30_population) AS age_under_30_population,
      AVG(m2.supervision_level_minimum_population) AS supervision_level_minimum_population,
      AVG(m2.supervision_level_medium_population) AS supervision_level_medium_population,
      AVG(m2.supervision_level_high_population) AS supervision_level_high_population,
      AVG(m2.risk_score_over_30_population) AS risk_score_over_30_population,
      AVG(m2.risk_score_over_39_population) AS risk_score_over_39_population,
      AVG(m2.risk_score_under_24_population) AS risk_score_under_24_population,
    FROM pop_adj_metrics m1
    /* Self join on the window of dates that fall within the rolling_average_months period, and calculate average */
    JOIN pop_adj_metrics m2
      USING (state_code, district, rolling_average_months)
    WHERE DATE(m1.year, m1.month, 1)
      BETWEEN DATE(m2.year, m2.month, 1)
        AND DATE_ADD(DATE(m2.year, m2.month, 1), INTERVAL m1.rolling_average_months - 1 MONTH)

    GROUP BY m1.state_code, m1.year, m1.month, m1.district, m1.rolling_average_months
    ORDER BY m1.state_code, m1.year, m1.month, m1.district, m1.rolling_average_months
    """

PO_REPORT_IMPACT_METRICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=PO_REPORT_IMPACT_METRICS_VIEW_NAME,
    view_query_template=PO_REPORT_IMPACT_METRICS_QUERY_TEMPLATE,
    description=PO_REPORT_IMPACT_METRICS_VIEW_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
    violation_reports_query=violation_reports_query(
        state_dataset=dataset_config.STATE_BASE_DATASET,
        reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET
    ),
    rolling_average_months_dimension=bq_utils.unnest_rolling_average_months()
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_REPORT_IMPACT_METRICS_VIEW_BUILDER.build_and_print()
