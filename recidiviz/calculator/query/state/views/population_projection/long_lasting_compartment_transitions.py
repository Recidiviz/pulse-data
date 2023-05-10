# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""
Historical total population by compartment, outflow compartment, and compartment duration (months)
for compartments that can have short and relatively long durations, like `ABSCONSION`
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LONG_LASTING_COMPARTMENT_TRANSITIONS_VIEW_NAME = "long_lasting_compartment_transitions"

LONG_LASTING_COMPARTMENT_TRANSITIONS_VIEW_DESCRIPTION = """"Historical total population by compartment, outflow compartment,
and compartment duration (months) or compartments that can have short and relatively
long durations, like `ABSCONSION`"""

LONG_LASTING_COMPARTMENT_LIST = [
    "PENDING_CUSTODY - PENDING_CUSTODY",
    "SUSPENSION - SUSPENSION",
    "SUPERVISION - BENCH_WARRANT",
    "SUPERVISION - ABSCONSION",
    "SUPERVISION_OUT_OF_STATE - ABSCONSION",
    "SUPERVISION_OUT_OF_STATE - BENCH_WARRANT",
    "SUPERVISION_OUT_OF_STATE - INTERNAL_UNKNOWN",
    "ERRONEOUS_RELEASE - ERRONEOUS_RELEASE",
]

LONG_LASTING_COMPARTMENT_TRANSITIONS_QUERY_TEMPLATE = """
    WITH cohorts_per_run_date AS (
      SELECT
          compartment,
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
      WHERE (compartment IN ('{long_lasting_compartments}')
            -- Include the INTERNAL_UNKNOWN compartment for states where that is common
            OR (
                compartment = "INTERNAL_UNKNOWN - INTERNAL_UNKNOWN"
                AND state_code IN ("US_MO", "US_TN")
            )
          )
          AND COALESCE(end_date, CURRENT_DATE("US/Pacific")) <= DATE_ADD(start_date, INTERVAL 10 YEAR)
          -- Drop uncommon bench warrant transitions to liberty
          AND (
            compartment != "SUPERVISION - BENCH_WARRANT"
            OR outflow_to != "LIBERTY - LIBERTY_REPEAT_IN_SYSTEM"
          )
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
      AND outflow_to IS NOT NULL
    GROUP BY compartment, gender, state_code, outflow_to, compartment_duration, run_date, cohort_counts.cohort_count
    """

LONG_LASTING_COMPARTMENT_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=LONG_LASTING_COMPARTMENT_TRANSITIONS_VIEW_NAME,
    view_query_template=LONG_LASTING_COMPARTMENT_TRANSITIONS_QUERY_TEMPLATE,
    description=LONG_LASTING_COMPARTMENT_TRANSITIONS_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    long_lasting_compartments="', '".join(LONG_LASTING_COMPARTMENT_LIST),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LONG_LASTING_COMPARTMENT_TRANSITIONS_VIEW_BUILDER.build_and_print()
