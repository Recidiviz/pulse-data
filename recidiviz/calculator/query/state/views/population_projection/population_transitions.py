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
"""Historical total population by compartment, outflow compartment, and compartment duration (months)"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_TRANSITIONS_VIEW_NAME = "population_transitions"

POPULATION_TRANSITIONS_VIEW_DESCRIPTION = """"Historical total population by compartment, outflow compartment,
and compartment duration (months)"""

POPULATION_TRANSITIONS_QUERY_TEMPLATE = """
    WITH cohorts_per_run_date AS (
      SELECT
          CASE WHEN compartment = 'INCARCERATION - GENERAL' AND previously_incarcerated
            THEN 'INCARCERATION - RE-INCARCERATION'
            ELSE compartment
          END AS compartment,
          -- Count transitions to general incarceration as a re-incarceration if the individual was previously
          -- incarcerated or is transitioning from parole.
          CASE WHEN outflow_to = 'INCARCERATION - GENERAL'
                AND (previously_incarcerated OR compartment = 'SUPERVISION - PAROLE')
            THEN 'INCARCERATION - RE-INCARCERATION'
            ELSE outflow_to
          END AS outflow_to,
          DATE_TRUNC(start_date, MONTH) as start_month,
          start_date,
          end_date,
          run_dates.run_date,
          person_id,
          gender,
          state_code
      FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized` sessions
      JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_dates
        ON sessions.start_date < run_dates.run_date
      WHERE compartment NOT IN ('RELEASE - RELEASE', 'INTERNAL_UNKNOWN - INTERNAL_UNKNOWN')
          -- Only take data from the 10 years prior to the run date to match short-term behavior better
          AND DATE_DIFF(run_dates.run_date, sessions.start_date, year) <= 10
          -- Drop sessions that are on the cusp of the session-start boundary
          AND DATE_DIFF(CURRENT_DATE, sessions.start_date, YEAR) < 20
          -- Union the rider transitions at the end
          AND compartment NOT IN ('INCARCERATION - TREATMENT_IN_PRISON', 'INCARCERATION - PAROLE_BOARD_HOLD')
          -- Do not include outflows that actually look like dropped data
          AND outflow_to NOT LIKE '%INTERNAL_UNKNOWN%'
    ),
    cohort_sizes_cte AS (
      -- Collect total cohort size for the outflow fraction denominator
      SELECT
          compartment,
          start_month,
          run_date,
          gender,
          state_code,
          COUNT(*) as total_population
      FROM cohorts_per_run_date
      GROUP BY 1,2,3,4,5
      ORDER BY 1,2,3,4,5
    ),
    outflow_population_cte AS (
      -- Calculate the total population per cohort that outflows to each compartment at each duration
      SELECT
        compartment,
        gender,
        state_code,
        start_month,
        run_date,
        -- Prevent data leakage, do not count sessions that ended after the run date
        CASE WHEN (run_date < end_date) OR (end_date IS NULL) THEN NULL
          ELSE outflow_to
        END AS outflow_to,

        CASE WHEN (run_date < end_date) OR (end_date IS NULL) THEN NULL
          ELSE FLOOR(DATE_DIFF(end_date, start_date, DAY)/30)
        END AS compartment_duration,

        COUNT(*) AS outflow_population
      FROM cohorts_per_run_date sessions
      GROUP BY compartment, gender, state_code, start_month, run_date, outflow_to, compartment_duration
    ),
    cohort_counts AS (
      SELECT
        compartment,
        gender,
        state_code,
        run_date,
        COUNT(DISTINCT start_month) as cohort_count
      FROM outflow_population_cte
      WHERE start_month < run_date
      GROUP BY compartment, gender, state_code, run_date
    )
    SELECT
      compartment,
      gender,
      state_code,
      outflow_to,
      compartment_duration,
      run_date,
      -- cte constructed so this only averages over cohorts old enough to have had a chance to see that duration
      SUM(outflow_population/total_population) / cohort_counts.cohort_count as total_population
    FROM outflow_population_cte
    JOIN cohort_sizes_cte
      USING (compartment, gender, state_code, run_date, start_month)
    LEFT JOIN cohort_counts
      USING (compartment, gender, state_code, run_date)
    WHERE gender in ('MALE', 'FEMALE')
      -- wasn't sure how to handle this better, so for now 'null' are discarded and reinserted in the model
      AND outflow_to IS NOT NULL
      AND state_code IN ('US_ID', 'US_ND')
    GROUP BY compartment, gender, state_code, outflow_to, compartment_duration, run_date, cohort_counts.cohort_count

    UNION ALL

    SELECT
      compartment,
      gender,
      state_code,
      outflow_to,
      compartment_duration,
      run_date,
      total_population
    FROM `{project_id}.{population_projection_dataset}.us_id_non_bias_full_transitions_materialized`

    ORDER BY compartment, gender, state_code, outflow_to, compartment_duration, run_date
    """

POPULATION_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=POPULATION_TRANSITIONS_VIEW_NAME,
    view_query_template=POPULATION_TRANSITIONS_QUERY_TEMPLATE,
    description=POPULATION_TRANSITIONS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_TRANSITIONS_VIEW_BUILDER.build_and_print()
