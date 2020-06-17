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

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config

SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_NAME = 'supervision_discharges_by_officer_by_month'

SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_DESCRIPTION = """
    Supervision case compliance to state standards by officer by month
 """

# TODO(3364): Implement home visit and face-to-face calculation support
SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      *,
      IEEE_DIVIDE(assessments_up_to_date, caseload_count) AS assessments_percent,
      NULL AS home_visits,
      NULL AS home_visits_percent,
      NULL AS facetoface,
      NULL AS facetoface_percent
    FROM
      (SELECT
        state_code,
        year,
        month,
        SPLIT(supervising_district_external_id, '|')[OFFSET(0)] AS district,
        supervising_officer_external_id AS officer_external_id,
        COUNT(DISTINCT(person_id)) AS caseload_count,
        COUNT(DISTINCT IF(assessment_up_to_date, person_id, NULL)) AS assessments_up_to_date
      FROM `{project_id}.{metrics_dataset}.supervision_case_compliance_metrics` 
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND person_id IS NOT NULL
        AND metric_period_months = 0
        AND date_of_evaluation = DATE_SUB(DATE_ADD(DATE(year, month, 1), INTERVAL 1 MONTH), INTERVAL 1 DAY)
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 3 YEAR))
        AND job.metric_type = 'SUPERVISION_COMPLIANCE'
      GROUP BY state_code, year, month, district, officer_external_id)
    ORDER BY state_code, year, month, district, officer_external_id
    """

SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW = BigQueryView(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_NAME,
    view_query_template=SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
)

if __name__ == '__main__':
    print(SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW.view_id)
    print(SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW.view_query)
