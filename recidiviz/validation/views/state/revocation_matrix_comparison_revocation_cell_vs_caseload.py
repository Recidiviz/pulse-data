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

"""A view which provides a comparison of total revocation counts in the initial, unfiltered view of the
Revocation Analysis Tool: the grid cells and the corresponding caseload chart across all filter combinations."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_NAME = \
    'revocation_matrix_comparison_revocation_cell_vs_caseload'

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_DESCRIPTION = """ 
Revocation matrix comparison of summed revocation counts between the grid cells and the month chart """

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH cell_counts AS (
      SELECT 
        state_code as region_code, metric_period_months, district, charge_category, supervision_type,
        SUM(total_revocations) as total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_cells`
      WHERE district != 'ALL'
        -- State-specific filtering used in the caseload view--
        AND (state_code = 'US_PA' OR (supervision_type != 'ALL' AND charge_category != 'ALL'))
        AND (state_code = 'US_MO' or (supervision_level != 'ALL'))
      GROUP BY state_code, metric_period_months, district, charge_category, supervision_type
    ),
    caseload_counts AS (
      SELECT 
        state_code as region_code, metric_period_months, district, charge_category, supervision_type,
        COUNT(DISTINCT state_id) as total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_filtered_caseload`
      -- The matrix doesn't have any of these rows --
      WHERE violation_type != 'NO_VIOLATION_TYPE' AND reported_violations != '0'
      GROUP BY state_code, metric_period_months, district, charge_category, supervision_type
    )
    SELECT 
      region_code, metric_period_months, district, charge_category, supervision_type, 
      IFNULL(c.total_revocations, 0) as cell_sum,
      IFNULL(cl.total_revocations, 0) as caseload_sum
    FROM cell_counts c 
    FULL OUTER JOIN caseload_counts cl
    USING (region_code, metric_period_months, district, charge_category, supervision_type)
    ORDER BY region_code, metric_period_months, district, charge_category, supervision_type
"""

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_NAME,
    view_query_template=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_QUERY_TEMPLATE,
    description=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_DESCRIPTION,
    view_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER.build_and_print()
