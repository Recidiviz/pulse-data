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
"""Technical revocations count by PO by admission date."""
# pylint: disable=line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_VIEW_NAME = 'technical_revocations_count_by_po_by_admission_date'

TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_DESCRIPTION = """
    Technical revocations count by PO by admission date
 """

TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
        revocations.state_code,
        revocation_admission_date,
        IFNULL(supervising_officer_external_id, 'UNKNOWN') as supervising_officer,
        IFNULL(revocations.level_1_supervision_location_external_id, 'UNKNOWN') as district_id,
        locations.level_1_supervision_location_name as district_name,
        COUNT(DISTINCT(person_id)) as num_revocations,
        case_type,
        supervision_type,
        source_violation_type
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_revocation_metrics_materialized` revocations
    LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names` locations
    ON revocations.state_code = locations.state_code
        AND revocations.level_1_supervision_location_external_id = locations.level_1_supervision_location_external_id
    WHERE source_violation_type = "TECHNICAL"
    AND revocation_admission_date > DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 90 DAY)
    GROUP BY
        state_code,
        revocation_admission_date,
        case_type,
        supervision_type,
        source_violation_type,
        supervising_officer,
        district_id,
        district_name
    ORDER BY revocation_admission_date DESC, num_revocations DESC
    """

TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        TECHNICAL_REVOCATIONS_COUNT_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
