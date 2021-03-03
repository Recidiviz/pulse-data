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

COMPARTMENT_SUB_SESSIONS_UNNESTED_VIEW_NAME = "compartment_sub_sessions_unnested"

COMPARTMENT_SUB_SESSIONS_UNNESTED_VIEW_DESCRIPTION = """Sessionized view of each individual merged onto an array of dates at daily intervals, used to calculate person-based metrics such as population"""

COMPARTMENT_SUB_SESSIONS_UNNESTED_QUERY_TEMPLATE = """
    /*{description}*/

    /* Joins sub_sessions onto sessions view to get start and end dates associated with a given session, in order
    to calculate length of stay, then merges onto a date array for last 6 years to get daily count */
    SELECT
        sub_sessions.state_code,
        sub_sessions.person_id,
        sub_sessions.start_date,
        sub_sessions.end_date,
        sub_sessions.compartment_location,
        sub_sessions.session_id,
        sub_sessions.sub_session_id,
        sub_sessions.metric_source,
        sub_sessions.compartment_level_1,
        sub_sessions.compartment_level_2,
        sub_sessions.start_reason,
        sub_sessions.start_sub_reason,
        sub_sessions.end_reason,
        sub_sessions.gender,
        sub_sessions.age_start,
        sub_sessions.age_end,
        sub_sessions.prioritized_race_or_ethnicity,
        sub_sessions.inflow_from_level_1,
        sub_sessions.inflow_from_level_2,
        sub_sessions.session_length_days as sub_session_length_days,
        sub_sessions.assessment_score_start,
        sub_sessions.assessment_score_end,
        sub_sessions.correctional_level_start,
        sub_sessions.correctional_level_end,
        sub_sessions.supervising_officer_external_id_start,
        sub_sessions.supervising_officer_external_id_end,
        sub_sessions.case_type_start,
        sub_sessions.case_type_end,
        sessions.start_date as session_start,
        sessions.end_date as session_end,
        population_date,
        DATE_DIFF(population_date,sessions.start_date,day) as time_served_in_compartment
    FROM `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized` sub_sessions
    JOIN `{project_id}.{analyst_dataset}.compartment_sessions_materialized` sessions
        USING(person_id, session_id, state_code),
    UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 6 YEAR), CURRENT_DATE, INTERVAL 1 DAY)) AS population_date
    WHERE population_date BETWEEN sub_sessions.start_date AND COALESCE(sub_sessions.end_date, '9999-01-01')
    """

COMPARTMENT_SUB_SESSIONS_UNNESTED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SUB_SESSIONS_UNNESTED_VIEW_NAME,
    view_query_template=COMPARTMENT_SUB_SESSIONS_UNNESTED_QUERY_TEMPLATE,
    description=COMPARTMENT_SUB_SESSIONS_UNNESTED_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SUB_SESSIONS_UNNESTED_VIEW_BUILDER.build_and_print()
