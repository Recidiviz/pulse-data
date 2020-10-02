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
# pylint: disable=trailing-whitespace,line-too-long

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import PO_REPORT_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PO_MONTHLY_REPORT_DATA_VIEW_NAME = 'po_monthly_report_data'

PO_MONTHLY_REPORT_DATA_DESCRIPTION = """
 Monthly data regarding an officer's success in discharging people from supervision, recommending early discharge
 from supervision, and keeping cases in compliance with state standards.
 """

# TODO(#3514): handle officers with caseloads across multiple districts
PO_MONTHLY_REPORT_DATA_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH report_data AS (
      SELECT
        state_code, year, month,
        officer_external_id, district,
        pos_discharges,
        pos_discharges_district_average,
        pos_discharges_state_average,
        earned_discharges,
        earned_discharges_district_average,
        earned_discharges_state_average,
        technical_revocations,
        absconsions,
        crime_revocations,
        assessments,
        assessment_percent,
        facetoface,
        facetoface_percent,
      FROM `{project_id}.{po_report_dataset}.supervision_discharges_by_officer_by_month_materialized`
      FULL OUTER JOIN `{project_id}.{po_report_dataset}.supervision_compliance_by_officer_by_month_materialized`
        USING (state_code, year, month, district, officer_external_id)
      FULL OUTER JOIN `{project_id}.{po_report_dataset}.supervision_absconsion_terminations_by_officer_by_month_materialized`
        USING (state_code, year, month, district, officer_external_id)
      FULL OUTER JOIN `{project_id}.{po_report_dataset}.revocations_by_officer_by_month_materialized`
        USING (state_code, year, month, district, officer_external_id)
      FULL OUTER JOIN `{project_id}.{po_report_dataset}.supervision_early_discharge_requests_by_officer_by_month_materialized`
        USING (state_code, year, month, district, officer_external_id)
    )
    SELECT
      state_code, officer_external_id, district,
      email_address,
      month as review_month,
      report_month.pos_discharges,
      report_month.pos_discharges_district_average,
      report_month.pos_discharges_state_average,
      report_month.earned_discharges,
      report_month.earned_discharges - IFNULL(last_month.earned_discharges, 0) as earned_discharges_change,
      report_month.earned_discharges_district_average,
      report_month.earned_discharges_state_average,
      report_month.technical_revocations,
      report_month.technical_revocations - IFNULL(last_month.technical_revocations, 0) as technical_revocations_change,
      report_month.absconsions,
      report_month.absconsions - IFNULL(last_month.absconsions, 0) as absconsions_change,
      report_month.crime_revocations,
      report_month.crime_revocations - IFNULL(last_month.crime_revocations, 0) as crime_revocations_change,
      report_month.assessments,
      report_month.assessment_percent,
      IFNULL(last_month.assessment_percent, 0) as assessment_percent_last_month,
      report_month.facetoface,
      report_month.facetoface_percent,
      IFNULL(last_month.facetoface_percent, 0) as facetoface_percent_last_month
    FROM `{project_id}.{po_report_dataset}.po_report_recipients`
    LEFT JOIN report_data report_month
      USING (state_code, officer_external_id, district)
    LEFT JOIN (
      SELECT
        * EXCEPT (year, month),
        -- Project this year/month data onto the next month to calculate the MoM change
        EXTRACT(YEAR FROM DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)) AS year,
        EXTRACT(MONTH FROM DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH)) AS month,
      FROM report_data
    ) last_month
      USING (state_code, year, month, officer_external_id, district)
    -- Only include output for the month before the current month
    WHERE DATE(year, month, 1) = DATE_SUB(DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1), INTERVAL 1 MONTH)
    ORDER BY review_month, email_address
    """

PO_MONTHLY_REPORT_DATA_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=PO_MONTHLY_REPORT_DATA_VIEW_NAME,
    should_materialize=True,
    view_query_template=PO_MONTHLY_REPORT_DATA_QUERY_TEMPLATE,
    dimensions=['state_code', 'review_month', 'officer_external_id', 'district'],
    description=PO_MONTHLY_REPORT_DATA_DESCRIPTION,
    po_report_dataset=PO_REPORT_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_MONTHLY_REPORT_DATA_VIEW_BUILDER.build_and_print()
