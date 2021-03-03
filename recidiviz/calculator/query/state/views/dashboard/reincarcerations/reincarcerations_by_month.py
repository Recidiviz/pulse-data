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
"""Reincarcerations by month."""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REINCARCERATIONS_BY_MONTH_VIEW_NAME = "reincarcerations_by_month"

REINCARCERATIONS_BY_MONTH_DESCRIPTION = """ Reincarcerations by month """

REINCARCERATIONS_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    WITH admissions AS (
      SELECT
        state_code, year, month,
        district,
        COUNT(person_id) as total_admissions
      FROM `{project_id}.{reference_views_dataset}.event_based_admissions`
      GROUP BY state_code, year, month, district
    ), reincarcerations AS (
      SELECT
        state_code, year, month,
        county_of_residence,
        person_id,
        ROW_NUMBER() OVER (PARTITION BY state_code, year, month, person_id
                            ORDER BY reincarceration_date, county_of_residence) as return_order
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_recidivism_count_metrics_materialized`
      WHERE year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 3 YEAR))
    ), person_based_reincarcerations AS (
      SELECT
        state_code, year, month,
        district,
        COUNT(person_id) AS returns
      FROM reincarcerations,
      {district_dimension}
      WHERE return_order = 1
      GROUP BY state_code, year, month, district
    )
    
    SELECT
      state_code, year, month, district,
      IFNULL(returns, 0) as returns,
      IFNULL(total_admissions, 0) as total_admissions
    FROM admissions
    LEFT JOIN person_based_reincarcerations
    USING (state_code, year, month, district)
    WHERE district IS NOT NULL
    ORDER BY state_code, year, month, district
    """

REINCARCERATIONS_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REINCARCERATIONS_BY_MONTH_VIEW_NAME,
    view_query_template=REINCARCERATIONS_BY_MONTH_QUERY_TEMPLATE,
    dimensions=["state_code", "year", "month", "district"],
    description=REINCARCERATIONS_BY_MONTH_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    district_dimension=bq_utils.unnest_district(district_column="county_of_residence"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATIONS_BY_MONTH_VIEW_BUILDER.build_and_print()
