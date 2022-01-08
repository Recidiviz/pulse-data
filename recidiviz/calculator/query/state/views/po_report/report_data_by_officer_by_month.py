# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import PO_REPORT_DATASET
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_VIEW_NAME = "report_data_by_officer_by_month"

REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_DESCRIPTION = """
 Monthly data regarding an officer's success in discharging people from supervision, recommending early discharge
 from supervision, and keeping cases in compliance with state standards.
 """

# TODO(#5034): Make deterministic and reduce complexity to be covered by metric output comparison script
REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_QUERY_TEMPLATE = """
    /*{description}*/
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
      IF(latest_supervision_downgrade_date IS NOT NULL, STRUCT(person_external_id, full_name, latest_supervision_downgrade_date, previous_supervision_level, supervision_level), NULL)
      IGNORE NULLS
    ) AS supervision_downgrades_clients,
    COUNT(DISTINCT IF(latest_supervision_downgrade_date IS NOT NULL, person_id, NULL)) AS supervision_downgrades,
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
    COUNT(DISTINCT IF(next_recommended_assessment_date IS NULL OR next_recommended_assessment_date > LAST_DAY(DATE(year, month, 1), MONTH),
      person_id,
      NULL)) AS assessments_up_to_date,
    ARRAY_AGG(
      IF(next_recommended_assessment_date <= LAST_DAY(DATE(year, month, 1), MONTH), STRUCT(person_external_id, full_name), NULL)
      IGNORE NULLS
    ) AS assessments_out_of_date_clients,
    ARRAY_AGG(
      IF(
        # next date falls within the subsequent month
        (DATE_TRUNC(next_recommended_assessment_date, MONTH) = DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)),
        STRUCT(person_external_id, full_name, next_recommended_assessment_date AS recommended_date),
        NULL
      ) 
      IGNORE NULLS
    ) AS assessments_upcoming_clients,
    SUM(face_to_face_count) AS facetoface,
    COUNT(DISTINCT IF(next_recommended_face_to_face_date IS NULL OR next_recommended_face_to_face_date > LAST_DAY(DATE(year, month, 1), MONTH),
      person_id,
      NULL)) AS facetoface_frequencies_sufficient,
    ARRAY_AGG(
      IF(next_recommended_face_to_face_date <= LAST_DAY(DATE(year, month, 1), MONTH), STRUCT(person_external_id, full_name), NULL)
      IGNORE NULLS
    ) AS facetoface_out_of_date_clients,
    ARRAY_AGG(
      IF(
        # next date falls within the subsequent month
        (DATE_TRUNC(next_recommended_face_to_face_date, MONTH) = DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)),
        STRUCT(person_external_id, full_name, next_recommended_face_to_face_date AS recommended_date),
        NULL
      ) 
      IGNORE NULLS
    ) AS facetoface_upcoming_clients,
    COUNT(DISTINCT person_id) AS caseload_count,
    ARRAY_AGG(
      IF(
        # end date falls within the subsequent month
        (DATE_TRUNC(projected_end_date, MONTH) = DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)) 
        AND (successful_completion_next_month IS NULL)
        # this table is manually updated to enumerate clients whose release dates in our system
        # are known to deviate from internal US_ID data; we are excluding them manually while
        # data quality improvements are in progress
        AND person_external_id NOT IN (
            SELECT person_external_id from `{project_id}.{static_reference_dataset}.po_monthly_report_manual_exclusions` excl
            WHERE excl.state_code = state_code AND excl.exclusion_type = "upcoming_release_date"
        ), 
        STRUCT(person_external_id, full_name, projected_end_date), 
        NULL
      )
      IGNORE NULLS
      ORDER BY projected_end_date
    ) AS upcoming_release_date_clients,
  FROM (
    SELECT 
      *,
      # look ahead to the following month for successful completions
      # so we can filter them out of lists of action items
      LEAD(successful_completion_date) 
        OVER (PARTITION BY state_code, person_external_id 
        ORDER BY year, month) as successful_completion_next_month
    FROM `{project_id}.{po_report_dataset}.report_data_by_person_by_month_materialized`
  )
  GROUP BY state_code, year, month, officer_external_id
"""

REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_VIEW_NAME,
    should_materialize=True,
    view_query_template=REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_QUERY_TEMPLATE,
    dimensions=("state_code", "review_month", "officer_external_id", "district"),
    description=REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_DESCRIPTION,
    po_report_dataset=PO_REPORT_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REPORT_DATA_BY_OFFICER_BY_MONTH_DATA_VIEW_BUILDER.build_and_print()
