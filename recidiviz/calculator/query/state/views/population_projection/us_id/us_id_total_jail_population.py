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
"""Historical jail population by month"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_TOTAL_JAIL_POPULATION_VIEW_NAME = "us_id_total_jail_population"

US_ID_TOTAL_JAIL_POPULATION_VIEW_DESCRIPTION = """"US_ID historical county jail population by compartment and run_date.
For US_ID, this represents the subset of the total county jail population that is not under IDOC custody."""

US_ID_TOTAL_JAIL_POPULATION_QUERY_TEMPLATE = """
    WITH incarceration_population AS (
      -- Fetch the total incarceration history for each run date and time step
      SELECT
        sessions.state_code,
        compartment,
        rd.run_date,
        time_step.run_date AS time_step,
        person_id,
      FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
      JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` rd
        ON sessions.start_date < rd.run_date
      JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` time_step
        ON time_step.run_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
      WHERE sessions.state_code = 'US_ID'
        AND compartment IN ('INCARCERATION - GENERAL',
          'INCARCERATION - PAROLE_BOARD_HOLD', 'INCARCERATION - TREATMENT_IN_PRISON')
    )
    -- Remove all "paid" sessions and count the remaining "unpaid" sessions
    SELECT
      pop.state_code,
      compartment,
      run_date,
      time_step,
      COUNT(*) AS total_population
    FROM incarceration_population pop
    LEFT JOIN `{project_id}.{population_projection_dataset}.us_id_monthly_paid_incarceration_population_materialized` paid_inc_pop
      ON pop.state_code = paid_inc_pop.state_code
      AND pop.person_id = paid_inc_pop.person_id
      AND pop.time_step = paid_inc_pop.report_month
    -- Only count the sessions that are not in the paid incarceration population table
    WHERE paid_inc_pop.person_id IS NULL
    GROUP BY state_code, compartment, run_date, time_step
    """

US_ID_TOTAL_JAIL_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=US_ID_TOTAL_JAIL_POPULATION_VIEW_NAME,
    view_query_template=US_ID_TOTAL_JAIL_POPULATION_QUERY_TEMPLATE,
    description=US_ID_TOTAL_JAIL_POPULATION_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_TOTAL_JAIL_POPULATION_VIEW_BUILDER.build_and_print()
