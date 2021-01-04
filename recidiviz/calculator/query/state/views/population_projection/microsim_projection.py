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
# pylint: disable=line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MICROSIM_PROJECTION_VIEW_NAME = 'microsim_projection'

MICROSIM_PROJECTION_VIEW_DESCRIPTION = \
    """"The projected population for the simulated policy and the baseline"""

MICROSIM_PROJECTION_QUERY_TEMPLATE = \
    """
    /* {description} */
    WITH historical_dates AS (
      SELECT *
      FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH),
        INTERVAL 1 MONTH)) AS date
    ),
    historical_population_output AS (
      SELECT
          state_code,
          date,
          compartment_level_1 AS compartment,
          IF(compartment_level_1 = 'SUPERVISION' AND compartment_level_2 = 'DUAL', 'PAROLE', compartment_level_2) AS legal_status,
          gender,
          COUNT(DISTINCT person_id) AS total_population
      FROM `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized`  sessions
      INNER JOIN historical_dates
        ON historical_dates.date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
      WHERE state_code = 'US_ID'
        AND gender IN ('FEMALE', 'MALE')
        AND compartment_level_2 != 'OTHER'
        AND (
          compartment_level_1 != 'INCARCERATION'
          OR (
            compartment_location IN ('EAGLE PASS CORRECTIONAL FACILITY, TEXAS',
              'BONNEVILLE COUNTY SHERIFF DEPARTMENT',
              'JEFFERSON COUNTY SHERIFF DEPARTMENT',
              'KARNES COUNTY CORRECTIONAL CENTER, TEXAS')
            OR compartment_location NOT LIKE '%COUNTY%'
          )
        )
        AND (
          compartment_level_1 != 'SUPERVISION'
          OR metric_source = 'SUPERVISION_POPULATION'
        )
      GROUP BY state_code, date, compartment, legal_status, gender
    ),
    most_recent_results AS (
      SELECT
        simulation_tag, MAX(date_created) AS date_created
      FROM `{project_id}.{population_projection_dataset}.microsim_projection_raw`
      GROUP BY simulation_tag
    )
    SELECT
      simulation_tag,
      date_created,
      simulation_date,
      EXTRACT(YEAR FROM simulation_date) AS year,
      EXTRACT(MONTH FROM simulation_date) AS month,
      SPLIT(compartment, ' ')[OFFSET(0)] AS compartment,
      IF(SPLIT(compartment, ' ')[OFFSET(2)] = 'RE-INCARCERATION', 'GENERAL', SPLIT(compartment, ' ')[OFFSET(2)]) AS legal_status,
      simulation_group,
      SUM(total_population) AS total_population,
      SUM(total_population_min) AS total_population_min,
      SUM(total_population_max) AS total_population_max,
    FROM `{project_id}.{population_projection_dataset}.microsim_projection_raw`
    INNER JOIN most_recent_results
    USING (simulation_tag, date_created)
    WHERE compartment NOT LIKE 'RELEASE%'
      AND simulation_date > DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH), MONTH)
    GROUP BY simulation_tag, date_created, simulation_date, year, month, compartment, legal_status, simulation_group

    UNION ALL

    SELECT
        state_code AS simulation_tag,
        NULL AS date_created,
        date AS simulation_date,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        compartment,
        legal_status,
        gender AS simulation_group,
        total_population,
        total_population AS total_population_min,
        total_population AS total_population_max
    FROM historical_population_output

    ORDER BY compartment, legal_status, simulation_group, simulation_date
    """

MICROSIM_PROJECTION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=MICROSIM_PROJECTION_VIEW_NAME,
    view_query_template=MICROSIM_PROJECTION_QUERY_TEMPLATE,
    description=MICROSIM_PROJECTION_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        MICROSIM_PROJECTION_VIEW_BUILDER.build_and_print()
