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
"""Data to populate the monthly PO report email.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.po_report.po_monthly_report_data
"""
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import PO_REPORT_DATASET
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PO_MONTHLY_REPORT_DATA_VIEW_NAME = "po_monthly_report_data"

PO_MONTHLY_REPORT_DATA_DESCRIPTION = """
 Monthly data regarding an officer's success in discharging people from supervision, recommending early discharge
 from supervision, and keeping cases in compliance with state standards.
 """

# TODO(#3514): handle officers with caseloads across multiple districts
# TODO(#5034): Make deterministic and reduce complexity to be covered by metric output comparison script
PO_MONTHLY_REPORT_DATA_QUERY_TEMPLATE = """
    /*{description}*/
    WITH report_data_per_officer AS (
      SELECT * FROM `{project_id}.{po_report_dataset}.report_data_by_officer_by_month_materialized`
    ),
    goals AS (
      SELECT 
        state_code, 
        officer_external_id,
        year,
        month,
        COALESCE(LEAST(3, ARRAY_LENGTH(assessments_out_of_date_clients)), 0) as overdue_assessments_goal,
        COALESCE(LEAST(9, ARRAY_LENGTH(facetoface_out_of_date_clients)), 0) as overdue_facetoface_goal,
      FROM report_data_per_officer
    ),
    last_month AS (
      SELECT
        * EXCEPT (year, month),
        # Project this year/month data onto the next month to calculate the MoM change
        EXTRACT(YEAR FROM DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)) AS year,
        EXTRACT(MONTH FROM DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)) AS month,
      FROM report_data_per_officer
    ),
    streaks AS (
      SELECT
        state_code,
        officer_external_id,
        year,
        month,
        # compute length of the active streaks for a given month, only if they are streaks of zero
        IF( 
          technical_revocations = 0,
          COUNT(1) OVER (PARTITION BY state_code, officer_external_id, technical_revocations_block ORDER BY year, month),
          0 
        ) AS technical_revocations_zero_streak,
        IF( 
          crime_revocations = 0,
          COUNT(1) OVER (PARTITION BY state_code, officer_external_id, crime_revocations_block ORDER BY year, month),
          0 
        ) AS crime_revocations_zero_streak,
        IF (
          absconsions = 0,
          COUNT(1) OVER (PARTITION BY state_code, officer_external_id, absconsions_block ORDER BY year, month),
          0 
        ) AS absconsions_zero_streak
      FROM (
        SELECT
          report_month.*,
          # divide history of the metric's value into numbered streaks
          COUNTIF(
            # start a new streak when these values don't match.
            # can be null when we don't have a value for the previous month, which also breaks a streak
            # (there may be gaps in historical data, or officer may not have had a caseload that month)
            IFNULL(report_month.technical_revocations != last_month.technical_revocations,
              TRUE)) OVER (officer_history) AS technical_revocations_block,
          COUNTIF( IFNULL(report_month.crime_revocations != last_month.crime_revocations,
              TRUE)) OVER (officer_history) AS crime_revocations_block,
          COUNTIF( IFNULL(report_month.absconsions != last_month.absconsions,
              TRUE)) OVER (officer_history) AS absconsions_block,
        FROM
          report_data_per_officer report_month
        LEFT JOIN last_month
          USING (state_code, year, month, officer_external_id)
        WINDOW
          officer_history AS (
            PARTITION BY state_code, officer_external_id
            ORDER BY year, month
          )
      )
    ),
    aggregates_by_state_and_district AS (
      SELECT
        state_code, year, month,
        district,
        AVG(pos_discharges) AS avg_pos_discharges,
        AVG(earned_discharges) AS avg_earned_discharges,
        AVG(supervision_downgrades) AS avg_supervision_downgrades,
        AVG(technical_revocations) AS avg_technical_revocations,
        AVG(crime_revocations) AS avg_crime_revocations,
        AVG(absconsions) AS avg_absconsions,
        SUM(pos_discharges) AS total_pos_discharges,
        SUM(earned_discharges) AS total_earned_discharges,
        SUM(supervision_downgrades) AS total_supervision_downgrades,
        MAX(pos_discharges) AS max_pos_discharges,
        MAX(earned_discharges) AS max_earned_discharges,
        MAX(supervision_downgrades) AS max_supervision_downgrades,
        MAX(technical_revocations_zero_streak) AS max_technical_revocations_zero_streak,
        MAX(crime_revocations_zero_streak) AS max_crime_revocations_zero_streak,
        MAX(absconsions_zero_streak) AS max_absconsions_zero_streak,
      FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
      LEFT JOIN report_data_per_officer
        USING (state_code, year, month, officer_external_id)
      LEFT JOIN streaks
        USING (state_code, year, month, officer_external_id),
      {district_dimension}
      GROUP BY state_code, year, month, district
    ),
    action_items_per_officer AS (
      SELECT
        state_code,
        officer_external_id,
        ARRAY_AGG(
          IF(
            recommended_level IS NOT NULL, 
            STRUCT(person_external_id, full_name, current_supervision_level, recommended_level),
            NULL) IGNORE NULLS) as mismatches,
      FROM `{project_id}.{po_report_dataset}.current_action_items_by_person_materialized` 
      GROUP BY state_code, officer_external_id
    ),
    agents AS (
      SELECT
        state_code,
        external_id AS officer_external_id,
        TRIM(SPLIT(given_names, ' ')[SAFE_OFFSET(0)]) AS officer_given_name
      FROM `{project_id}.{reference_views_dataset}.augmented_agent_info`
      GROUP BY state_code, officer_external_id, officer_given_name
    )
    SELECT
      state_code, officer_external_id, district,
      email_address,
      agents.officer_given_name,
      month as review_month,
      year as review_year,
      report_month.pos_discharges_clients,
      report_month.pos_discharges,
      IFNULL(last_month.pos_discharges, 0) AS pos_discharges_last_month,
      district_agg.avg_pos_discharges AS pos_discharges_district_average,
      district_agg.total_pos_discharges AS pos_discharges_district_total,
      district_agg.max_pos_discharges AS pos_discharges_district_max,
      state_agg.avg_pos_discharges AS pos_discharges_state_average,
      state_agg.total_pos_discharges AS pos_discharges_state_total,
      state_agg.max_pos_discharges AS pos_discharges_state_max,
      report_month.supervision_downgrades_clients,
      report_month.supervision_downgrades,
      IFNULL(last_month.supervision_downgrades, 0) AS supervision_downgrades_last_month,
      district_agg.avg_supervision_downgrades AS supervision_downgrades_district_average,
      district_agg.total_supervision_downgrades AS supervision_downgrades_district_total,
      district_agg.max_supervision_downgrades AS supervision_downgrades_district_max,
      state_agg.avg_supervision_downgrades AS supervision_downgrades_state_average,
      state_agg.total_supervision_downgrades AS supervision_downgrades_state_total,
      state_agg.max_supervision_downgrades AS supervision_downgrades_state_max,
      action_items.mismatches,
      report_month.earned_discharges_clients,
      report_month.earned_discharges,
      IFNULL(last_month.earned_discharges, 0) AS earned_discharges_last_month,
      district_agg.avg_earned_discharges AS earned_discharges_district_average,
      district_agg.total_earned_discharges AS earned_discharges_district_total,
      district_agg.max_earned_discharges AS earned_discharges_district_max,
      state_agg.avg_earned_discharges AS earned_discharges_state_average,
      state_agg.total_earned_discharges AS earned_discharges_state_total,
      state_agg.max_earned_discharges AS earned_discharges_state_max,
      report_month.revocations_clients,
      report_month.technical_revocations,
      IFNULL(last_month.technical_revocations, 0) AS technical_revocations_last_month,
      district_agg.avg_technical_revocations AS technical_revocations_district_average,
      state_agg.avg_technical_revocations AS technical_revocations_state_average,
      technical_revocations_zero_streak,
      district_agg.max_technical_revocations_zero_streak AS technical_revocations_zero_streak_district_max,
      state_agg.max_technical_revocations_zero_streak AS technical_revocations_zero_streak_state_max,
      report_month.crime_revocations,
      IFNULL(last_month.crime_revocations, 0) AS crime_revocations_last_month,
      district_agg.avg_crime_revocations AS crime_revocations_district_average,
      state_agg.avg_crime_revocations AS crime_revocations_state_average,
      crime_revocations_zero_streak,
      district_agg.max_crime_revocations_zero_streak AS crime_revocations_zero_streak_district_max,
      state_agg.max_crime_revocations_zero_streak AS crime_revocations_zero_streak_state_max,
      report_month.absconsions_clients,
      report_month.absconsions,
      IFNULL(last_month.absconsions, 0) AS absconsions_last_month,
      district_agg.avg_absconsions AS absconsions_district_average,
      state_agg.avg_absconsions AS absconsions_state_average,
      absconsions_zero_streak,
      district_agg.max_absconsions_zero_streak AS absconsions_zero_streak_district_max,
      state_agg.max_absconsions_zero_streak AS absconsions_zero_streak_state_max,
      report_month.assessments_out_of_date_clients,
      report_month.assessments_upcoming_clients,
      report_month.assessments,
      report_month.caseload_count,
      report_month.assessments_up_to_date,
      report_month.facetoface_out_of_date_clients,
      report_month.facetoface_upcoming_clients,
      report_month.facetoface,
      report_month.facetoface_frequencies_sufficient,
      overdue_assessments_goal,
      overdue_facetoface_goal,
      report_month.upcoming_release_date_clients,
      # TODO(#9106): refactor to move these calcs into Python 
      IF(report_month.caseload_count = 0,
        1,
        IEEE_DIVIDE(report_month.assessments_up_to_date, report_month.caseload_count)) * 100 AS assessments_percent,
      IF(report_month.caseload_count = 0,
        1,
        IEEE_DIVIDE(report_month.facetoface_frequencies_sufficient, report_month.caseload_count)) * 100 as facetoface_percent,
      IF(report_month.caseload_count = 0,
        1,
        IEEE_DIVIDE(report_month.assessments_up_to_date + overdue_assessments_goal, report_month.caseload_count)) * 100 as overdue_assessments_goal_percent,
      IF(report_month.caseload_count = 0,
        1,
        IEEE_DIVIDE(report_month.facetoface_frequencies_sufficient + overdue_facetoface_goal, report_month.caseload_count)) * 100 as overdue_facetoface_goal_percent
    FROM `{project_id}.{static_reference_dataset}.po_report_recipients`
    LEFT JOIN report_data_per_officer report_month
      USING (state_code, officer_external_id)
    LEFT JOIN action_items_per_officer action_items
      USING (state_code, officer_external_id)
    LEFT JOIN (
      SELECT * FROM aggregates_by_state_and_district
      WHERE district != 'ALL'
    ) district_agg
      USING (state_code, year, month, district)
    LEFT JOIN (
      SELECT * EXCEPT (district) FROM aggregates_by_state_and_district
      WHERE district = 'ALL'
    ) state_agg
      USING (state_code, year, month)
    LEFT JOIN last_month
      USING (state_code, year, month, officer_external_id)
    LEFT JOIN streaks
      USING (state_code, year, month, officer_external_id)
    LEFT JOIN agents
      USING (state_code, officer_external_id)
    LEFT JOIN goals
        USING (state_code, officer_external_id, year, month)
    -- Only include output for the month before the current month
    WHERE DATE(year, month, 1) = DATE_SUB(DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1), INTERVAL 1 MONTH)
    """

PO_MONTHLY_REPORT_DATA_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=PO_MONTHLY_REPORT_DATA_VIEW_NAME,
    should_materialize=True,
    view_query_template=PO_MONTHLY_REPORT_DATA_QUERY_TEMPLATE,
    dimensions=("state_code", "review_month", "officer_external_id", "district"),
    district_dimension=bq_utils.unnest_district(district_column="district"),
    description=PO_MONTHLY_REPORT_DATA_DESCRIPTION,
    po_report_dataset=PO_REPORT_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_MONTHLY_REPORT_DATA_VIEW_BUILDER.build_and_print()
