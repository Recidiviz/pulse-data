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
"""Revocations Matrix Supervision Distribution by District."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW_NAME = \
    'revocations_matrix_supervision_distribution_by_district'

REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_DESCRIPTION = """
 Supervision matrix of violation response count and most severe violation by district and metric period month.
 This counts all individuals on supervision, broken down by number of violations during the last 12 months on
 supervision, the most severe violation, the district, and the metric period. 
 """

REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code,
      district,
      supervision_type,
      COUNT(DISTINCT person_id) AS total_population,
      charge_category,
      violation_type,
      reported_violations,
      metric_period_months
    FROM `{project_id}.{reference_dataset}.supervision_matrix_by_person`
    WHERE current_month
      AND district != 'ALL'
    GROUP BY state_code, district, supervision_type, charge_category, violation_type, reported_violations,
      metric_period_months
    ORDER BY state_code, metric_period_months, district, supervision_type, reported_violations, charge_category,
      violation_type
    """.format(
        description=REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET
        )

REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW.view_id)
    print(REVOCATIONS_MATRIX_SUPERVISION_DISTRIBUTION_BY_DISTRICT_VIEW.view_query)
