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
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

TOTAL_JAIL_POPULATION_VIEW_NAME = 'total_jail_population'

TOTAL_JAIL_POPULATION_VIEW_DESCRIPTION = \
    """"Historical population by compartment and month"""

TOTAL_JAIL_POPULATION_QUERY_TEMPLATE = \
    """
    WITH previously_incarcerated_cte AS 
    (
    -- Create a flag to indicate if the person was incarcerated prior to this session
    -- Only count a session as "previously incarcerated" if there was a release/supervision session post-incarceration
    SELECT
      session.state_code, 
      session.person_id, 
      session.session_id,
      LOGICAL_OR(prev_rel_session.session_id IS NOT NULL) AS previously_incarcerated
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` session
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` prev_inc_session
      ON session.state_code = prev_inc_session.state_code
      AND session.person_id = prev_inc_session.person_id
      AND prev_inc_session.start_date < session.start_date
      AND prev_inc_session.compartment_level_1 = 'INCARCERATION'
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` prev_rel_session
      ON session.state_code = prev_rel_session.state_code
      AND session.person_id = prev_rel_session.person_id
      AND prev_rel_session.start_date BETWEEN prev_inc_session.end_date AND session.start_date
      AND prev_rel_session.compartment_level_1 IN ('RELEASE', 'SUPERVISION')
    GROUP BY state_code, person_id, session_id
    )
    SELECT  
      state_code,
      CASE
        WHEN compartment_level_1 = 'INCARCERATION' AND compartment_level_2 = 'GENERAL' and previously_incarcerated
            THEN 'INCARCERATION - RE-INCARCERATION'
        ELSE CONCAT(compartment_level_1, ' - ', COALESCE(compartment_level_2, ''))
      END AS compartment,
      run_date,
      time_step,
      COUNT(1) as total_population
    FROM `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized`
    JOIN previously_incarcerated_cte
      USING (state_code, person_id, session_id)
    JOIN `{project_id}.{population_projection_dataset}.simulation_run_dates`
      ON start_date < run_date,
    UNNEST(GENERATE_DATE_ARRAY('2000-01-01', DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)) AS time_step
    WHERE state_code = 'US_ID'
      AND compartment_level_1 = 'INCARCERATION'
      AND compartment_location LIKE "%COUNTY%"
      AND compartment_location NOT IN ('EAGLE PASS CORRECTIONAL FACILITY, TEXAS', 
                'BONNEVILLE COUNTY SHERIFF DEPARTMENT', 
                'JEFFERSON COUNTY SHERIFF DEPARTMENT', 
                'KARNES COUNTY CORRECTIONAL CENTER, TEXAS')
      AND time_step BETWEEN start_date AND coalesce(end_date, '9999-01-01')
    GROUP BY 1,2,3,4
    ORDER BY 1,2,3,4
    """

TOTAL_JAIL_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=TOTAL_JAIL_POPULATION_VIEW_NAME,
    view_query_template=TOTAL_JAIL_POPULATION_QUERY_TEMPLATE,
    description=TOTAL_JAIL_POPULATION_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    population_projection_dataset=dataset_config.POPULATION_PROJECTION_DATASET,
    should_materialize=False
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        TOTAL_JAIL_POPULATION_VIEW_BUILDER.build_and_print()
