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
"""Historical total population by month, compartment, outflow compartment, and model run date"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_OUTFLOWS_VIEW_NAME = "population_outflows"

POPULATION_OUTFLOWS_VIEW_DESCRIPTION = """"Historical total population by month, compartment, outflow compartment, and model run date"""

POPULATION_OUTFLOWS_QUERY_TEMPLATE = """
    WITH cte AS (
        SELECT
            run_date,
            state_code,
            DATE_TRUNC(start_date, MONTH) AS time_step,
            -- distinguish shell liberty compartment from full liberty compartment
            CASE
                WHEN inflow_from = 'LIBERTY - LIBERTY_REPEAT_IN_SYSTEM' THEN 'LIBERTY'
                -- Count session starts from `INTERNAL_UKNOWN` as admissions from liberty
                WHEN inflow_from = 'INTERNAL_UNKNOWN - INTERNAL_UNKNOWN' THEN 'LIBERTY'
                -- Count all admissions from "PRETRIAL" -> "PAROLE" in ND as coming from "LIBERTY" instead of "PRETRIAL"
                WHEN state_code = 'US_ND' AND compartment = 'SUPERVISION - PAROLE' AND inflow_from = 'PRETRIAL' THEN 'LIBERTY'
                ELSE inflow_from
            END AS compartment,
            CASE
                WHEN compartment = 'INCARCERATION - GENERAL' AND previously_incarcerated THEN 'INCARCERATION - RE-INCARCERATION'
                ELSE compartment
            END AS admission_to,
            gender as simulation_group,
            COUNT(1) as cohort_population,
        FROM `{project_id}.{population_projection_dataset}.population_projection_sessions_materialized`
        JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_date_array
            ON start_date < run_date
            -- Cap the historical outflows (admissions) data at 10 years before the run date
            AND DATE_DIFF(run_date, start_date, YEAR) < 10
        WHERE gender IN ('FEMALE', 'MALE')
        GROUP BY 1,2,3,4,5,6
    )
    SELECT
    *
    FROM cte
    -- TODO(#4755): This logic is pretty specific to US_ID/US_ND and should be handled by a config or a table
    -- if scaled to more states
    WHERE CASE
          WHEN compartment = 'PRETRIAL'
            THEN admission_to IN ('INCARCERATION - GENERAL', 'SUPERVISION - PROBATION',
              'INCARCERATION - TREATMENT_IN_PRISON', 'SUPERVISION_OUT_OF_STATE - PROBATION', 
              'SUPERVISION - INFORMAL_PROBATION', 'INVESTIGATION - INVESTIGATION')
          WHEN compartment = 'LIBERTY'
            THEN admission_to IN ('SUPERVISION - PROBATION', 'INCARCERATION - TREATMENT_IN_PRISON',
              'INCARCERATION - GENERAL', 'INCARCERATION - RE-INCARCERATION',
              'INVESTIGATION - INVESTIGATION')
          ELSE FALSE
        END
    """

POPULATION_OUTFLOWS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=POPULATION_OUTFLOWS_VIEW_NAME,
    view_query_template=POPULATION_OUTFLOWS_QUERY_TEMPLATE,
    description=POPULATION_OUTFLOWS_VIEW_DESCRIPTION,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_OUTFLOWS_VIEW_BUILDER.build_and_print()
