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
"""Microsimulation projection output"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MICROSIM_PROJECTION_VIEW_NAME = "microsim_projection"

MICROSIM_PROJECTION_VIEW_DESCRIPTION = (
    """"The projected population for the simulated policy and the baseline"""
)

MICROSIM_PROJECTION_QUERY_TEMPLATE = """
    /* {description} */
    WITH historical_dates AS (
      SELECT *
      FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH),
        INTERVAL 1 MONTH)) AS date
    ),
    historical_supervision_population_output AS (
      SELECT
          state_code,
          date,
          compartment_level_1 AS compartment,
          legal_status,
          simulation_group,
          COUNT(DISTINCT person_id) AS total_population
      FROM `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized`  sessions
      INNER JOIN historical_dates
        ON historical_dates.date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01'),
      UNNEST(['ALL', IF(compartment_level_2 = 'DUAL', 'PAROLE', compartment_level_2)]) AS legal_status,
      UNNEST(['ALL', gender]) AS simulation_group
      WHERE state_code IN ('US_ID', 'US_ND')
        AND gender IN ('FEMALE', 'MALE')
        AND ((compartment_level_1 = 'SUPERVISION' AND metric_source = 'SUPERVISION_POPULATION')
            OR (compartment_level_1 = 'INCARCERATION' AND state_code = 'US_ND')
        )
        AND compartment_level_2 NOT IN ('INTERNAL_UNKNOWN', 'INFORMAL_PROBATION')
      GROUP BY state_code, date, compartment, legal_status, simulation_group
    ),
    historical_incarceration_population_output AS (
      SELECT
        state_code,
        report_month AS date,
        compartment_level_1 AS compartment,
        legal_status,
        simulation_group,
        COUNT(DISTINCT person_id) AS total_population
      FROM `{project_id}.{population_projection_dataset}.us_id_monthly_paid_incarceration_population` inc_pop
      INNER JOIN historical_dates
        ON historical_dates.date = inc_pop.report_month,
      UNNEST(['ALL', compartment_level_2]) AS legal_status,
      UNNEST(['ALL', gender]) AS simulation_group
      WHERE state_code = 'US_ID'
        AND gender IN ('FEMALE', 'MALE')
        AND compartment_level_2 != 'OTHER'
      GROUP BY state_code, date, compartment, legal_status, simulation_group
    ),
    most_recent_results AS (
      SELECT
        simulation_tag, MAX(date_created) AS date_created
      FROM `{project_id}.{population_projection_output_dataset}.microsim_projection_raw`
      GROUP BY simulation_tag
    )
    SELECT
      simulation_tag AS state_code,
      "BASELINE" AS simulation_tag,
      date_created,
      simulation_date,
      EXTRACT(YEAR FROM simulation_date) AS year,
      EXTRACT(MONTH FROM simulation_date) AS month,
      -- Split the compartment and legal status into separate columns
      SPLIT(compartment, ' ')[OFFSET(0)] AS compartment,
      SPLIT(compartment, ' ')[OFFSET(2)] AS legal_status,
      simulation_group,
      total_population,
      total_population_min,
      total_population_max,
    FROM `{project_id}.{population_projection_output_dataset}.microsim_projection_raw`
    INNER JOIN most_recent_results
    USING (simulation_tag, date_created)
    WHERE compartment NOT LIKE 'RELEASE%'
      AND simulation_date > DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH)

    UNION ALL

    SELECT
        state_code,
        "HISTORICAL" AS simulation_tag,
        NULL AS date_created,
        date AS simulation_date,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        compartment,
        legal_status,
        simulation_group,
        total_population,
        total_population AS total_population_min,
        total_population AS total_population_max
    FROM historical_supervision_population_output

    UNION ALL

    SELECT
        state_code, 
        "HISTORICAL" AS simulation_tag,
        NULL AS date_created,
        date AS simulation_date,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        compartment,
        legal_status,
        simulation_group,
        total_population,
        total_population AS total_population_min,
        total_population AS total_population_max
    FROM historical_incarceration_population_output

    ORDER BY state_code, compartment, legal_status, simulation_group, simulation_date
    """

MICROSIM_PROJECTION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=MICROSIM_PROJECTION_VIEW_NAME,
    view_query_template=MICROSIM_PROJECTION_QUERY_TEMPLATE,
    description=MICROSIM_PROJECTION_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    population_projection_output_dataset=dataset_config.POPULATION_PROJECTION_OUTPUT_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        MICROSIM_PROJECTION_VIEW_BUILDER.build_and_print()
