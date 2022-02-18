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
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MICROSIM_PROJECTION_VIEW_NAME = "microsim_projection"

MICROSIM_PROJECTION_VIEW_DESCRIPTION = (
    """"The projected population for the simulated policy and the baseline"""
)

# TODO(#9488): include absconsion, bench warrant, and informal probation in the output
MICROSIM_PROJECTION_VIEW_INCLUDED_TYPES = [
    StateSupervisionPeriodSupervisionType.DUAL,
    StateSupervisionPeriodSupervisionType.PAROLE,
    StateSupervisionPeriodSupervisionType.PROBATION,
    StateSpecializedPurposeForIncarceration.GENERAL,
    StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
    StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
]

MICROSIM_PROJECTION_QUERY_TEMPLATE = """
    /* {description} */
    WITH historical_dates AS (
      -- Set the historical date array from Jan 2016 to the start of the current month
      SELECT *
      FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', DATE_TRUNC(CURRENT_DATE, MONTH),
        INTERVAL 1 MONTH)) AS date
    ),
    historical_supervision_population_output AS (
      SELECT
          sessions.state_code,
          date,
          compartment_level_1 AS compartment,
          legal_status,
          simulation_group,
          COUNT(DISTINCT sessions.person_id) AS total_population
      FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sessions
      INNER JOIN historical_dates
        ON historical_dates.date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01'),
      UNNEST(['ALL', IF(compartment_level_2 = 'DUAL', 'PAROLE', compartment_level_2)]) AS legal_status,
      UNNEST(['ALL', gender]) AS simulation_group
      WHERE sessions.state_code IN ('US_ID', 'US_ND')
        AND gender IN ('FEMALE', 'MALE')
        AND (compartment_level_1 = 'SUPERVISION'
            OR (compartment_level_1 = 'INCARCERATION' AND sessions.state_code = 'US_ND')
        )
        AND compartment_level_2 IN ('{included_types}')
      GROUP BY state_code, date, compartment, legal_status, simulation_group
    ),
    historical_incarceration_population_output AS (
      -- TODO(#8623): use `dataflow_sessions_materialized` directly once it has paid bed logic
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
        AND compartment_level_2 IN ('{included_types}')
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
      SPLIT(compartment, ' ')[SAFE_OFFSET(0)] AS compartment,
      SPLIT(compartment, ' ')[SAFE_OFFSET(2)] AS legal_status,
      simulation_group,
      total_population,
      total_population_min,
      total_population_max,
    FROM `{project_id}.{population_projection_output_dataset}.microsim_projection_raw`
    INNER JOIN most_recent_results
    USING (simulation_tag, date_created)
    WHERE compartment NOT LIKE 'RELEASE%'
      AND simulation_date > DATE_TRUNC(CURRENT_DATE, MONTH)

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
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    population_projection_output_dataset=dataset_config.POPULATION_PROJECTION_OUTPUT_DATASET,
    included_types="', '".join(
        [status.name for status in MICROSIM_PROJECTION_VIEW_INCLUDED_TYPES]
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        MICROSIM_PROJECTION_VIEW_BUILDER.build_and_print()
