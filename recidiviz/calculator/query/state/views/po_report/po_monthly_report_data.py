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
"""Data to populate the monthly PO report email."""
# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bq_utils
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import PO_REPORT_DATASET
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
      SELECT
        state_code, year, month,
        officer_external_id,
        COUNT(DISTINCT IF(successful_completion_date IS NOT NULL, person_id, NULL)) AS pos_discharges,
        ARRAY_AGG(
          IF(successful_completion_date IS NOT NULL, STRUCT(person_external_id, full_name, successful_completion_date), NULL)
          IGNORE NULLS
        ) AS pos_discharges_clients,
        COUNT(DISTINCT IF(earned_discharge_date IS NOT NULL, person_id, NULL)) AS earned_discharges,
        ARRAY_AGG(
          IF(earned_discharge_date IS NOT NULL, STRUCT(person_external_id, full_name, earned_discharge_date), NULL)
          IGNORE NULLS
        ) AS earned_discharges_clients,
        ARRAY_AGG(
          IF(latest_supervision_downgrade_date IS NOT NULL, STRUCT(person_id, full_name, latest_supervision_downgrade_date, previous_supervision_level, supervision_level), NULL)
          IGNORE NULLS
        ) AS supervision_downgrade_clients,
        COUNT(DISTINCT IF(revocation_violation_type IN ('TECHNICAL'), person_id, NULL)) AS technical_revocations,
        COUNT(DISTINCT IF(revocation_violation_type IN ('NEW_CRIME'), person_id, NULL)) AS crime_revocations,
        ARRAY_AGG(
          IF(revocation_report_date IS NOT NULL,
          STRUCT(person_external_id, full_name, revocation_violation_type, revocation_report_date), NULL)
          IGNORE NULLS
        ) AS revocations_clients,
        COUNT(DISTINCT IF(absconsion_report_date IS NOT NULL, person_id, NULL)) AS absconsions,
        ARRAY_AGG(
          IF(absconsion_report_date IS NOT NULL, STRUCT(person_external_id, full_name, absconsion_report_date), NULL)
          IGNORE NULLS
        ) AS absconsions_clients,
        SUM(assessment_count) AS assessments,
        COUNT(DISTINCT IF(num_days_assessment_overdue = 0, person_id, NULL)) AS assessments_up_to_date,
        ARRAY_AGG(
          IF(num_days_assessment_overdue > 0, STRUCT(person_external_id, full_name), NULL)
          IGNORE NULLS
        ) AS assessments_out_of_date_clients,
        SUM(face_to_face_count) AS facetoface,
        COUNT(DISTINCT IF(face_to_face_frequency_sufficient, person_id, NULL)) AS facetoface_frequencies_sufficient,
        ARRAY_AGG(
          IF(face_to_face_frequency_sufficient IS FALSE, STRUCT(person_external_id, full_name), NULL)
          IGNORE NULLS
        ) AS facetoface_out_of_date_clients
      FROM `{project_id}.{po_report_dataset}.report_data_by_person_by_month_materialized`
      GROUP BY state_code, year, month, officer_external_id
    ),
    compliance_caseloads AS (
      SELECT
        state_code, year, month, officer_external_id,
        COUNT(DISTINCT IF(num_days_assessment_overdue IS NOT NULL, person_id, NULL)) AS assessment_compliance_caseload_count,
        COUNT(DISTINCT IF(face_to_face_frequency_sufficient IS NOT NULL, person_id, NULL)) AS facetoface_compliance_caseload_count
      FROM `{project_id}.{po_report_dataset}.supervision_compliance_by_person_by_month_materialized`
      GROUP BY state_code, year, month, officer_external_id
    ),
    averages_by_state_and_district AS (
      SELECT
        state_code, year, month,
        district,
        AVG(pos_discharges) AS avg_pos_discharges,
        AVG(earned_discharges) AS avg_earned_discharges,
        AVG(ARRAY_LENGTH(supervision_downgrade_clients)) AS avg_supervision_downgrades,
        AVG(technical_revocations) AS avg_technical_revocations,
        AVG(crime_revocations) AS avg_crime_revocations,
        AVG(absconsions) AS avg_absconsions
      FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized`
      LEFT JOIN report_data_per_officer
        USING (state_code, year, month, officer_external_id),
      {district_dimension}
      GROUP BY state_code, year, month, district
    ),
    agents AS (
      SELECT
        state_code,
        external_id AS officer_external_id,
        TRIM(SPLIT(given_names, ' ')[SAFE_OFFSET(0)]) AS officer_given_name
      FROM `{project_id}.{reference_views_dataset}.augmented_agent_info`
      GROUP BY state_code, external_id, officer_given_name
    )
    SELECT
      state_code, officer_external_id, district,
      email_address,
      agents.officer_given_name,
      month as review_month,
      report_month.pos_discharges_clients,
      report_month.pos_discharges,
      IFNULL(last_month.pos_discharges, 0) AS pos_discharges_last_month,
      district_avg.avg_pos_discharges AS pos_discharges_district_average,
      state_avg.avg_pos_discharges AS pos_discharges_state_average,
      report_month.supervision_downgrade_clients,
      IFNULL(last_month.supervision_downgrade_clients, []) AS supervision_downgrade_clients_last_month,
      district_avg.avg_supervision_downgrades AS supervision_downgrades_district_average,
      state_avg.avg_supervision_downgrades AS supervision_downgrades_state_average,
      report_month.earned_discharges_clients,
      report_month.earned_discharges,
      IFNULL(last_month.earned_discharges, 0) AS earned_discharges_last_month,
      district_avg.avg_earned_discharges AS earned_discharges_district_average,
      state_avg.avg_earned_discharges AS earned_discharges_state_average,
      report_month.revocations_clients,
      report_month.technical_revocations,
      IFNULL(last_month.technical_revocations, 0) AS technical_revocations_last_month,
      district_avg.avg_technical_revocations AS technical_revocations_district_average,
      state_avg.avg_technical_revocations AS technical_revocations_state_average,
      report_month.crime_revocations,
      IFNULL(last_month.crime_revocations, 0) AS crime_revocations_last_month,
      district_avg.avg_crime_revocations AS crime_revocations_district_average,
      state_avg.avg_crime_revocations AS crime_revocations_state_average,
      report_month.absconsions_clients,
      report_month.absconsions,
      IFNULL(last_month.absconsions, 0) AS absconsions_last_month,
      district_avg.avg_absconsions AS absconsions_district_average,
      state_avg.avg_absconsions AS absconsions_state_average,
      report_month.assessments_out_of_date_clients,
      report_month.assessments,
      IEEE_DIVIDE(report_month.assessments_up_to_date, assessment_compliance_caseload_count) * 100 AS assessment_percent,
      report_month.facetoface_out_of_date_clients,
      report_month.facetoface,
      IEEE_DIVIDE(report_month.facetoface_frequencies_sufficient, facetoface_compliance_caseload_count) * 100 as facetoface_percent
    FROM `{project_id}.{static_reference_dataset}.po_report_recipients`
    LEFT JOIN report_data_per_officer report_month
      USING (state_code, officer_external_id)
    LEFT JOIN (
      SELECT * FROM averages_by_state_and_district
      WHERE district != 'ALL'
    ) district_avg
      USING (state_code, year, month, district)
    LEFT JOIN (
      SELECT * EXCEPT (district) FROM averages_by_state_and_district
      WHERE district = 'ALL'
    ) state_avg
      USING (state_code, year, month)
    LEFT JOIN compliance_caseloads
      USING (state_code, year, month, officer_external_id)
    LEFT JOIN (
      SELECT
        * EXCEPT (year, month),
        -- Project this year/month data onto the next month to calculate the MoM change
        EXTRACT(YEAR FROM DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)) AS year,
        EXTRACT(MONTH FROM DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)) AS month,
      FROM report_data_per_officer
    ) last_month
      USING (state_code, year, month, officer_external_id)
    LEFT JOIN agents
      USING (state_code, officer_external_id)
    -- Only include output for the month before the current month
    /* TODO(#7437): TESTING ALERT */
    /* Remove the specific date that is inserted based on the lack of current data in PA. This needs to be removed prior to launch. */
    WHERE DATE(year, month, 1) = IF(state_code = 'US_PA', DATE_SUB(DATE(2020, 06, 1), INTERVAL 1 MONTH), DATE_SUB(DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1), INTERVAL 1 MONTH))
    ORDER BY review_month, email_address
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
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_MONTHLY_REPORT_DATA_VIEW_BUILDER.build_and_print()
