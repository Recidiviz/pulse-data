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
from recidiviz.calculator.query.state.views.dashboard.population_projections.population_projection_time_series_query_template import (
    population_projection_query,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_VIEW_NAME = (
    "supervision_population_projection_time_series"
)

SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_DESCRIPTION = (
    "Projected incarcerated and supervised populations"
)

SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_QUERY_TEMPLATE = f"""
    {population_projection_query(compartment='SUPERVISION')}
"""

SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_DESCRIPTION,
    dimensions=("state_code", "gender", "legal_status"),
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    cur_year=str(date.today().year),
    cur_month=str(date.today().month),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_PROJECTION_TIME_SERIES_VIEW_BUILDER.build_and_print()
