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
"""Revocations by supervision type by month."""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import view_config

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_NAME = \
    'revocations_by_supervision_type_by_month'

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_DESCRIPTION = \
    """ Revocations by supervision type by month """

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code, year, month,
      SUM(IF(supervision_type = 'PROBATION', revocation_count, 0)) AS probation_count,
      SUM(IF(supervision_type = 'PAROLE', revocation_count, 0)) AS parole_count,
      district
    FROM (
      SELECT
        state_code, year, month,
        COUNT(DISTINCT person_id) AS revocation_count,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_revocations`
      WHERE supervision_type in ('PAROLE', 'PROBATION')
      GROUP BY state_code, year, month, supervision_type, district
    ) rev
    GROUP BY state_code, year, month, district
    ORDER BY state_code, year, month, district
    """

REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW = BigQueryView(
    dataset_id=view_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW_NAME,
    view_query_template=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_QUERY_TEMPLATE,
    description=REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_DESCRIPTION,
    reference_dataset=view_config.REFERENCE_TABLES_DATASET,
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW.view_id)
    print(REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW.view_query)
