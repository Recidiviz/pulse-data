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
"""Supervisees with overdue LSIR by PO by day."""
# pylint: disable=line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_NAME = 'overdue_lsir_by_po_by_day'

OVERDUE_LSIR_BY_PO_BY_DAY_DESCRIPTION = """
    Number of supervisees with overdue LSIR by PO by day
 """

OVERDUE_LSIR_BY_PO_BY_DAY_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
        compliance.state_code,
        date_of_supervision,
        supervising_officer_external_id,
        IFNULL(compliance.level_1_supervision_location_external_id, 'UNKNOWN') as district_id,
        locations.level_1_supervision_location_name as district_name,
        COUNT (DISTINCT(IF(num_days_assessment_overdue > 0, person_id, NULL))) as total_overdue,
        supervision_type,
        case_type
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_case_compliance_metrics_materialized` compliance
    LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names` locations
    ON compliance.state_code = locations.state_code
        AND compliance.level_1_supervision_location_external_id = locations.level_1_supervision_location_external_id
    # Note: because compliance metrics are calculated EOM for each month, this will currently only produce output for
    # each of the last days of the past 3 months.
    WHERE date_of_supervision > DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 90 DAY)
    GROUP BY
        state_code,
        date_of_supervision,
        supervision_type,
        case_type,
        supervising_officer_external_id,
        district_id,
        district_name
    ORDER BY date_of_supervision DESC, total_overdue DESC
    """

OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=OVERDUE_LSIR_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=OVERDUE_LSIR_BY_PO_BY_DAY_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
