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
"""Metrics for display from the PO report

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.metrics_from_po_report
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.case_triage.views import dataset_config as case_triage_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

METRICS_FROM_PO_REPORT_VIEW_NAME = "metrics_from_po_report"

METRICS_FROM_PO_REPORT_DESCRIPTION = """Metrics for display from the PO report"""

METRICS_FROM_PO_REPORT_QUERY_TEMPLATE = """
WITH new_to_caseload AS (
    SELECT 
        state_code,
        supervising_officer_external_id as officer_external_id,
        COUNT(DISTINCT person_id) AS new_to_caseload_count
    FROM `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` supervision_officer_sessions_materialized
    WHERE 
        start_date BETWEEN DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 3 MONTH)
                AND DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH)
    GROUP BY 1, 2
)
SELECT
    po_monthly_report_data.state_code,
    po_monthly_report_data.officer_external_id,
    JSON_EXTRACT_SCALAR(etl_officers.full_name, '$.full_name') AS full_name,
    SUM(new_to_caseload.new_to_caseload_count) AS new_to_caseload_count, 
    SUM(po_monthly_report_data.pos_discharges) AS pos_discharges,
    SUM(po_monthly_report_data.absconsions) AS absconsions,
    SUM(po_monthly_report_data.technical_revocations) AS technical_revocations,
    SUM(po_monthly_report_data.crime_revocations) AS crime_revocations,
    SUM(po_monthly_report_data.earned_discharges) AS earned_discharges,
    SUM(po_monthly_report_data.facetoface) AS facetoface,
    SUM(po_monthly_report_data.assessments) AS assessments
 FROM `{project_id}.{po_report_dataset}.report_data_by_officer_by_month_materialized` po_monthly_report_data
 JOIN new_to_caseload USING (state_code, officer_external_id)
 JOIN `{project_id}.{case_triage_dataset}.etl_officers_materialized` etl_officers ON etl_officers.external_id = po_monthly_report_data.officer_external_id
 WHERE DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 3 MONTH)
 GROUP BY 1, 2, 3
"""

METRICS_FROM_PO_REPORT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=METRICS_FROM_PO_REPORT_VIEW_NAME,
    should_materialize=True,
    view_query_template=METRICS_FROM_PO_REPORT_QUERY_TEMPLATE,
    description=METRICS_FROM_PO_REPORT_DESCRIPTION,
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    case_triage_dataset=case_triage_dataset_config.CASE_TRIAGE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        METRICS_FROM_PO_REPORT_VIEW_BUILDER.build_and_print()
