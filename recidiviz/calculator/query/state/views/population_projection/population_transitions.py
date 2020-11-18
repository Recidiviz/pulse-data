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
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_TRANSITIONS_VIEW_NAME = 'population_transitions'

POPULATION_TRANSITIONS_VIEW_DESCRIPTION = \
    """"Historical total population by compartment, outflow compartment, and compartment duration (months)"""

POPULATION_TRANSITIONS_QUERY_TEMPLATE = \
    """
    WITH cte AS (
        SELECT
            sessions.person_id,
            sessions.state_code,
            sessions.session_id,
            CONCAT(sessions.compartment_level_1, ' - ', sessions.compartment_level_2) as compartment,
            sessions.gender,
            CASE
                WHEN sessions.outflow_to_level_1 = 'RELEASE' THEN 'RELEASE - FULL'
                ELSE CONCAT(sessions.outflow_to_level_1, ' - ', sessions.outflow_to_level_2)
            END as outflow_to,
            run_date,
            CEILING(sessions.session_length_days / 30) as compartment_duration
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
        JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates` run_date_array
            ON run_date BETWEEN start_date AND coalesce(end_date, '9999-01-01')
        WHERE (sessions.compartment_level_1 = 'INCARCERATION' or sessions.compartment_level_1 = 'SUPERVISION')
            AND sessions.outflow_to_level_2 != 'OTHER'
            AND sessions.compartment_level_2 != 'OTHER'
        ORDER BY person_id, session_id
    )
    SELECT
        run_date,
        state_code,
        compartment,
        compartment_duration,
        outflow_to,
        gender,
        count(1) as total_population
    FROM cte
    WHERE compartment_duration is not null
        AND state_code = 'US_ID'
        AND gender IN ('FEMALE', 'MALE')
        AND run_date >= '2017-01-01'
    GROUP BY 1,2,3,4,5,6 ORDER BY 1,2,3,4,5,6
    """

POPULATION_TRANSITIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=POPULATION_TRANSITIONS_VIEW_NAME,
    view_query_template=POPULATION_TRANSITIONS_QUERY_TEMPLATE,
    description=POPULATION_TRANSITIONS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_TRANSITIONS_VIEW_BUILDER.build_and_print()
