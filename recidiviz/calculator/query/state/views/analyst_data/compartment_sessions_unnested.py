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
"""Sessionized view of each individual merged onto an array of dates at daily intervals,
 used to calculate person-based metrics such as population"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSIONS_UNNESTED_VIEW_NAME = "compartment_sessions_unnested"

COMPARTMENT_SESSIONS_UNNESTED_VIEW_DESCRIPTION = """
Sessionized view of each individual merged onto an array of dates at daily intervals, 
used to calculate person-based metrics such as population
"""

COMPARTMENT_SESSIONS_UNNESTED_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT *,
        DATE_DIFF(population_date,super_session_incarceration_priority_start,day) as time_served_in_super_session_incarceration_priority,
        DATE_DIFF(population_date,supervision_super_session_start,day) as time_served_in_supervision_super_session,
        DATE_DIFF(population_date,incarceration_super_session_start,day) as time_served_in_incarceration_super_session,
        DATE_DIFF(population_date,session_start,day) as time_served_in_compartment,
        DATE_DIFF(population_date,system_session_start,day) as time_served_in_system        
    FROM (
        SELECT
            sessions.state_code,
            sessions.person_id,
            sessions.start_date as session_start,
            supervision_super_session.start_date as supervision_super_session_start,
            incarceration_super_session.start_date as incarceration_super_session_start,
            system_sessions.start_date as system_session_start,
            /* The only compartments that should have both of these start dates are incarceration compartments 'PAROLE_BOARD_HOLD',
            'TEMPORARY_CUSTODY', and 'SHOCK_INCARCERATION'. In this start date, we're prioritizing time spent in that given compartment
            across in/out of state incarceration when calculating time served, rather than looking at the start of an individual's
            supervision status */
            COALESCE(incarceration_super_session.start_date,supervision_super_session.start_date) as super_session_incarceration_priority_start,
            sessions.end_date,
            sessions.session_id,
            sessions.dataflow_session_id_start,
            sessions.dataflow_session_id_end,
            sessions.compartment_level_1,
            sessions.compartment_level_2,
            sessions.session_length_days as session_length_days,
            population_date,
        FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
        LEFT JOIN `{project_id}.{analyst_dataset}.supervision_super_sessions_materialized` supervision_super_session
            ON sessions.person_id = supervision_super_session.person_id
            AND sessions.state_code = supervision_super_session.state_code
            AND sessions.session_id BETWEEN supervision_super_session.session_id_start AND supervision_super_session.session_id_end
        LEFT JOIN `{project_id}.{analyst_dataset}.incarceration_super_sessions_materialized` incarceration_super_session
            ON sessions.person_id = incarceration_super_session.person_id
            AND sessions.state_code = incarceration_super_session.state_code
            AND sessions.session_id BETWEEN incarceration_super_session.session_id_start AND incarceration_super_session.session_id_end
        LEFT JOIN `{project_id}.{analyst_dataset}.system_sessions_materialized` system_sessions
            ON sessions.person_id = system_sessions.person_id
            AND sessions.session_id BETWEEN system_sessions.session_id_start AND system_sessions.session_id_end,
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 9 YEAR), CURRENT_DATE, INTERVAL 1 DAY)) AS population_date
        WHERE population_date BETWEEN sessions.start_date AND COALESCE(sessions.end_date, '9999-01-01')
    )
    """

COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SESSIONS_UNNESTED_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_UNNESTED_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_UNNESTED_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER.build_and_print()
