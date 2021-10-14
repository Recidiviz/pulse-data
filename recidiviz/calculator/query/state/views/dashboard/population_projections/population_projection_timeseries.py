# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Admissions by metric period months"""
from datetime import date

from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#9103): Delete population_projection_time_series view when FE is no longer using it.

POPULATION_PROJECTION_TIME_SERIES_VIEW_NAME = "population_projection_timeseries"

POPULATION_PROJECTION_TIME_SERIES_DESCRIPTION = (
    "Projected incarcerated and supervised populations"
)

POPULATION_PROJECTION_TIME_SERIES_QUERY_TEMPLATE = """
    /*{description}*/
    WITH prepared_data AS (
      SELECT
        * EXCEPT (simulation_group),
        simulation_group as gender,
        ABS((year - {cur_year}) * 12 + (month - {cur_month})) as offset,
      FROM `{project_id}.{population_projection_dataset}.microsim_projection`
    )
    
    SELECT
      year,
      month,
      compartment,
      legal_status,
      gender,
      state_code,
      simulation_tag,
      total_population,
      total_population_min,
      total_population_max,
    FROM prepared_data
    WHERE offset <= 60
    ORDER BY year, month
"""

POPULATION_PROJECTION_TIME_SERIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=POPULATION_PROJECTION_TIME_SERIES_VIEW_NAME,
    view_query_template=POPULATION_PROJECTION_TIME_SERIES_QUERY_TEMPLATE,
    description=POPULATION_PROJECTION_TIME_SERIES_DESCRIPTION,
    dimensions=("state_code", "gender", "legal_status", "compartment"),
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    cur_year=str(date.today().year),
    cur_month=str(date.today().month),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_PROJECTION_TIME_SERIES_VIEW_BUILDER.build_and_print()
