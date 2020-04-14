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
"""Revocations Matrix by month."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

REVOCATIONS_MATRIX_BY_MONTH_VIEW_NAME = 'revocations_matrix_by_month'

REVOCATIONS_MATRIX_BY_MONTH_DESCRIPTION = """
 Revocations matrix of violation response count and most severe violation by month.
 This counts all individuals admitted to prison for a revocation of supervision, broken down by number of
 violations leading up to the revocation and the most severe violation.
 """

REVOCATIONS_MATRIX_BY_MONTH_QUERY = \
    """
    /*{description}*/
    SELECT
        state_code, year, month, violation_type, reported_violations,
        supervision_type, charge_category, district,
        COUNT(DISTINCT person_id) AS total_revocations
    FROM `{project_id}.{reference_dataset}.revocations_matrix_by_person`
    WHERE metric_period_months = 1
    GROUP BY state_code, year, month, violation_type, reported_violations, supervision_type, charge_category, district
    ORDER BY state_code, year, month, district, supervision_type, violation_type, reported_violations
    """.format(
        description=REVOCATIONS_MATRIX_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
        )

REVOCATIONS_MATRIX_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_MATRIX_BY_MONTH_VIEW_NAME,
    view_query=REVOCATIONS_MATRIX_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_MATRIX_BY_MONTH_VIEW.view_id)
    print(REVOCATIONS_MATRIX_BY_MONTH_VIEW.view_query)
