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

"""A view which provides a comparison of total revocation counts summed across all dimensional breakdowns
between two views in the Revocation Analysis Matrix tool: the grid cells and the month-over-month chart."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import view_config

from recidiviz.validation.validation_models import VALIDATION_VIEWS_DATASET

VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW_NAME = \
    'revocation_matrix_comparison_revocation_cell_vs_month'

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_DESCRIPTION = """ 
Revocation matrix comparison of summed revocation counts between the grid cells and the month chart """

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH cell_counts AS (
      SELECT state_code as region_code, SUM(total_revocations) as total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_cells`
      WHERE metric_period_months = 36
      GROUP BY state_code
    ),
    month_counts AS (
      SELECT state_code as region_code, SUM(total_revocations) as total_revocations
      FROM `{project_id}.{view_dataset}.revocations_matrix_by_month`
      WHERE DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH),
                                                      INTERVAL 36 - 1 MONTH)
      GROUP BY state_code
    )
    SELECT c.region_code, c.total_revocations as cell_sum, m.total_revocations as month_sum
    FROM cell_counts c JOIN month_counts m on c.region_code = m.region_code
"""

REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW = BigQueryView(
    dataset_id=VALIDATION_VIEWS_DATASET,
    view_id=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW_NAME,
    view_query_template=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_QUERY_TEMPLATE,
    description=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_DESCRIPTION,
    view_dataset=VIEWS_DATASET,
)

if __name__ == '__main__':
    print(REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW.view_id)
    print(REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW.view_query)
