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
"""Supervision case compliance to state standards by officer by month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_NAME = 'supervision_compliance_by_officer_by_month'

SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_DESCRIPTION = """
    Supervision case compliance to state standards by officer by month
 """

SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH frequencies AS ( 
    SELECT
        state_code,
        year,
        month,
        supervising_officer_external_id AS officer_external_id,
        SUM(assessment_count) AS assessments,
        COUNT(DISTINCT IF(assessment_up_to_date, person_id, NULL)) AS assessments_up_to_date,
        COUNT(DISTINCT IF(assessment_up_to_date IS NOT NULL, person_id, NULL)) AS assessment_compliance_caseload_count,
        SUM(face_to_face_count) AS facetoface,
        COUNT(DISTINCT IF(face_to_face_frequency_sufficient, person_id, NULL)) AS facetoface_frequencies_sufficient,
        COUNT(DISTINCT IF(face_to_face_frequency_sufficient IS NOT NULL, person_id, NULL)) 
            AS facetoface_compliance_caseload_count
      FROM `{project_id}.{metrics_dataset}.supervision_case_compliance_metrics`
      JOIN `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
        USING (state_code, job_id, year, month, metric_period_months, metric_type)
      WHERE methodology = 'PERSON'
        AND person_id IS NOT NULL
        AND metric_period_months = 0
        AND date_of_evaluation = DATE_SUB(DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH), INTERVAL 1 DAY)
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 3 YEAR))
      GROUP BY state_code, year, month, officer_external_id)
    SELECT
      *,
      IEEE_DIVIDE(assessments_up_to_date, assessment_compliance_caseload_count) * 100 AS assessment_percent,
      IEEE_DIVIDE(facetoface_frequencies_sufficient, facetoface_compliance_caseload_count) * 100 as facetoface_percent
      FROM `{project_id}.{po_report_dataset}.officer_supervision_district_association_materialized` 
      LEFT JOIN frequencies
        USING (state_code, month, officer_external_id, year)
    ORDER BY state_code, year, month, district, officer_external_id
    """

SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    po_report_dataset=dataset_config.PO_REPORT_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_BUILDER.build_and_print()
