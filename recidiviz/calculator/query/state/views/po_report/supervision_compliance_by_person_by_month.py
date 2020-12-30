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
"""Supervision case compliance to state standards by person by month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_VIEW_NAME = 'supervision_compliance_by_person_by_month'

SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_DESCRIPTION = """
    Supervision case compliance to state standards by person by month
 """

SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT 
        state_code, year, month, person_id, 
        supervising_officer_external_id AS officer_external_id,
        assessment_count,
        face_to_face_count,
        assessment_up_to_date,
        face_to_face_frequency_sufficient
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_case_compliance_metrics`
    WHERE methodology = 'PERSON'
        AND person_id IS NOT NULL
        AND supervising_officer_external_id IS NOT NULL
        AND metric_period_months = 0
        AND date_of_evaluation = LAST_DAY(DATE(year, month, 1), MONTH)
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 3 YEAR))
    """

SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_QUERY_TEMPLATE,
    description=SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_VIEW_BUILDER.build_and_print()
