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
"""Revocations Matrix Distribution by District."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_VIEW_NAME = 'revocations_matrix_distribution_by_district'

REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by district and metric period month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation, the most severe violation, the supervision district, and the metric period.
 """

REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code,
      violation_type,
      reported_violations,
      IFNULL(population_count, 0) AS population_count,
      total_supervision_count,
      supervision_type,
      charge_category,
      district,
      metric_period_months
    FROM (
      SELECT
        state_code,
        violation_type,
        reported_violations,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        charge_category,
        district,
        metric_period_months
      FROM `{project_id}.{reference_dataset}.supervision_matrix_by_person`
      WHERE current_month
      GROUP BY state_code, violation_type, reported_violations, supervision_type, charge_category, district, metric_period_months
    ) pop
    LEFT JOIN (
      SELECT
        state_code,
        violation_type,
        reported_violations,
        COUNT(DISTINCT person_id) as population_count,
        supervision_type,
        charge_category,
        district,
        metric_period_months
      FROM `{project_id}.{reference_dataset}.revocations_matrix_by_person`
      WHERE current_month
      GROUP BY state_code, violation_type, reported_violations, supervision_type, charge_category, district, metric_period_months
    ) rev
    USING (state_code, violation_type, reported_violations, supervision_type, charge_category, district, 
      metric_period_months)
    ORDER BY state_code, district, supervision_type, metric_period_months, violation_type, reported_violations,
      charge_category
    """.format(
        description=REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
        )

REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_VIEW.view_id)
    print(REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_VIEW.view_query)
