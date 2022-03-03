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
"""Revocations by month."""

from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_BY_MONTH_VIEW_NAME = "revocations_by_month"

REVOCATIONS_BY_MONTH_DESCRIPTION = """ Revocations by month """

REVOCATIONS_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      state_code, year, month,
      IFNULL(revocation_count, 0) as revocation_count,
      total_supervision_count,
      supervision_type,
      district
    FROM (
      SELECT 
        state_code, year, month,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district
      FROM `{project_id}.{shared_metric_views_dataset}.event_based_supervision_populations_with_commitments_for_rate_denominators`
      GROUP BY state_code, year, month, supervision_type, district
    ) pop
    LEFT JOIN (
      SELECT 
        state_code, year, month,
        COUNT(DISTINCT person_id) AS revocation_count,
        supervision_type,
        district
      FROM `{project_id}.{shared_metric_views_dataset}.event_based_commitments_from_supervision_materialized`
      GROUP BY state_code, year, month, supervision_type, district
    ) rev
    USING (state_code, year, month, supervision_type, district)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    ORDER BY state_code, year, month, supervision_type, district
    """

REVOCATIONS_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_BY_MONTH_VIEW_NAME,
    view_query_template=REVOCATIONS_BY_MONTH_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", "supervision_type", "district"),
    description=REVOCATIONS_BY_MONTH_DESCRIPTION,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_BY_MONTH_VIEW_BUILDER.build_and_print()
