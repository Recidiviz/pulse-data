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
"""Revocations by metric month period."""
# pylint: disable=trailing-whitespace

from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

REVOCATIONS_BY_PERIOD_VIEW_NAME = 'revocations_by_period'

REVOCATIONS_BY_PERIOD_DESCRIPTION = """ Revocations by metric month period """

REVOCATIONS_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code,
      metric_period_months,
      IFNULL(revocation_count, 0) as revocation_count,
      total_supervision_count,
      supervision_type,
      district
    FROM (
      SELECT
        state_code, metric_period_months,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, metric_period_months, supervision_type, district
    ) pop
    LEFT JOIN (
      SELECT
        state_code, metric_period_months,
        COUNT(DISTINCT person_id) AS revocation_count,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_revocations`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, metric_period_months, supervision_type, district
    ) rev
    USING (state_code, metric_period_months, supervision_type, district)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    ORDER BY state_code, district, supervision_type, metric_period_months
    """.format(
        description=REVOCATIONS_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
        metric_period_dimension=bq_utils.unnest_metric_period_months(),
        metric_period_condition=bq_utils.metric_period_condition(),
    )

REVOCATIONS_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_PERIOD_VIEW_NAME,
    view_query=REVOCATIONS_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_PERIOD_VIEW.view_id)
    print(REVOCATIONS_BY_PERIOD_VIEW.view_query)
