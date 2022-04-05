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
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_OUTFLOWS_VIEW_NAME = 'population_outflows'

POPULATION_OUTFLOWS_VIEW_DESCRIPTION = \
    """"Historical total population by month, compartment, outflow compartment, and model run date"""

POPULATION_OUTFLOWS_QUERY_TEMPLATE = \
    """
    WITH cte AS (
        SELECT
            run_date,
            state_code,
            DATE_TRUNC(start_date, MONTH) as time_step,
            COALESCE(inflow_from_level_1, 'PRETRIAL') as compartment,
            session_id,
            CONCAT(compartment_level_1, ' - ', COALESCE(compartment_level_2, '')) as outflow_to,
            gender,
            COUNT(1) as total_population,
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
        JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_date_array
            ON start_date < run_date
            -- Do not count start dates from the edge of the historical look back since those are not true admissions
            AND start_date >= '2000-12-01'
        WHERE state_code = 'US_ID'
            AND compartment_level_2 != 'OTHER'
        GROUP BY 1,2,3,4,5,6,7
    )
    SELECT
    *
    FROM cte
    WHERE gender IN ('FEMALE', 'MALE')
        and compartment = 'PRETRIAL' or compartment = 'RELEASE'
    """

POPULATION_OUTFLOWS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=POPULATION_OUTFLOWS_VIEW_NAME,
    view_query_template=POPULATION_OUTFLOWS_QUERY_TEMPLATE,
    description=POPULATION_OUTFLOWS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_OUTFLOWS_VIEW_BUILDER.build_and_print()
