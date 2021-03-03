# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""The run dates to use for the simulation validation"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SIMULATION_RUN_DATES_VIEW_NAME = "simulation_run_dates"

SIMULATION_RUN_DATES_VIEW_DESCRIPTION = (
    """"All of the run dates to use for validating the simulation"""
)

SIMULATION_RUN_DATES_QUERY_TEMPLATE = """
    SELECT *
    FROM
    UNNEST(GENERATE_DATE_ARRAY('2018-01-01', DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AS run_date
    """

SIMULATION_RUN_DATES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=SIMULATION_RUN_DATES_VIEW_NAME,
    view_query_template=SIMULATION_RUN_DATES_QUERY_TEMPLATE,
    description=SIMULATION_RUN_DATES_VIEW_DESCRIPTION,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SIMULATION_RUN_DATES_VIEW_BUILDER.build_and_print()
