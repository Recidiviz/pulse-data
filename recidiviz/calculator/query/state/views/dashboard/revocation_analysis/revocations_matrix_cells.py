# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Revocations Matrix Cells."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_MATRIX_CELLS_VIEW_NAME = 'revocations_matrix_cells'

REVOCATIONS_MATRIX_CELLS_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by metric period month.
 This counts all individuals admitted to prison for a revocation of probation or parole, broken down by number of 
 violations leading up to the revocation and the most severe violation. 
 """

REVOCATIONS_MATRIX_CELLS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
        state_code,
        violation_type, reported_violations,
        COUNT(DISTINCT person_id) AS total_revocations,
        supervision_type,
        charge_category,
        district,
        metric_period_months
    FROM `{project_id}.{reference_dataset}.revocations_matrix_by_person`
    WHERE reported_violations > 0
    GROUP BY state_code, violation_type, reported_violations, supervision_type, charge_category, district,
        metric_period_months
    ORDER BY state_code, district, metric_period_months, violation_type, reported_violations, supervision_type,
        charge_category
    """

REVOCATIONS_MATRIX_CELLS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_MATRIX_CELLS_VIEW_NAME,
    view_query_template=REVOCATIONS_MATRIX_CELLS_QUERY_TEMPLATE,
    description=REVOCATIONS_MATRIX_CELLS_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_MATRIX_CELLS_VIEW_BUILDER.build_and_print()
