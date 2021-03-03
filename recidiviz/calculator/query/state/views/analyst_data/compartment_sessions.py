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
"""Sessionized view of each individual. Session defined as continuous stay within a compartment"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMPARTMENT_SESSIONS_VIEW_NAME = "compartment_sessions"

COMPARTMENT_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of each individual. Session defined as continuous stay within a compartment"""

COMPARTMENT_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH sessions AS 
    /*
    Takes the sub-sessions view and aggregates across compartment_location to create a sessions view defined as a 
    continuous stay in a compartment, regardless of incarceration facility or supervision district.
    */
    (
    SELECT
        person_id,
        session_id,
        state_code,
        compartment_level_1,
        compartment_level_2,
        last_day_of_data,
        MIN(start_date) AS start_date,
        --this is done to ensure we take a null end date if present instead of the max
        CASE WHEN MAX(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) = 0 THEN MAX(end_date) END AS end_date
    FROM `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized` sub_sessions
    GROUP BY 1,2,3,4,5,6
    ORDER BY 1,2,3,4,5,6
    )
    /*
    Take the initial aggregate and join back to the first sub-session and last sub-session within a session to get 
    information associated with the start and end of the session. This view pulls admission reason and inflow compartment 
    from the first sub-session, and the release reason and outflow compartment from the outflow compartment. 
    */
    SELECT 
        sessions.person_id,
        sessions.session_id,
        sessions.start_date,
        sessions.end_date,
        sessions.state_code,
        sessions.compartment_level_1,
        sessions.compartment_level_2,
        first.start_reason,
        first.start_sub_reason,
        last.end_reason,
        last.release_date,
        first.compartment_location AS compartment_location_start,
        last.compartment_location AS compartment_location_end,
        first.inflow_from_level_1,
        first.inflow_from_level_2,
        last.outflow_to_level_1,
        last.outflow_to_level_2,
        DATE_DIFF(COALESCE(sessions.end_date, sessions.last_day_of_data), sessions.start_date, DAY) AS session_length_days,
        sessions.last_day_of_data,
        first.gender,
        first.prioritized_race_or_ethnicity,
        first.age_start,
        last.age_end,
        first.assessment_score_start,
        last.assessment_score_end,
        CASE WHEN first.age_start <=24 THEN '<25'
            WHEN first.age_start <=29 THEN '25-29'
            WHEN first.age_start <=34 THEN '30-34'
            WHEN first.age_start <=39 THEN '35-39'
            WHEN first.age_start >=40 THEN '40+' END as age_bucket_start,
        CASE WHEN last.age_end <=24 THEN '<25'
            WHEN last.age_end <=29 THEN '25-29'
            WHEN last.age_end <=34 THEN '30-34'
            WHEN last.age_end <=39 THEN '35-39'
            WHEN last.age_end >=40 THEN '40+' END as age_bucket_end,
        CASE WHEN first.assessment_score_start<=23 THEN '0-23'
            WHEN first.assessment_score_start<=29 THEN '24-29'
            WHEN first.assessment_score_start<=38 THEN '30-38'
            WHEN first.assessment_score_start>=39 THEN '39+' END as assessment_score_bucket_start,
        CASE WHEN last.assessment_score_end<=23 THEN '0-23'
            WHEN last.assessment_score_end<=29 THEN '24-29'
            WHEN last.assessment_score_end<=38 THEN '30-38'
            WHEN last.assessment_score_end>=39 THEN '39+' END as assessment_score_bucket_end,
        first.correctional_level_start,
        last.correctional_level_end,
        first.supervising_officer_external_id_start,
        last.supervising_officer_external_id_end,
        first.case_type_start,
        last.case_type_end
    FROM sessions
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized` first
        ON first.person_id = sessions.person_id
        AND first.session_id = sessions.session_id
        AND first.first_sub_session_in_session = 1
    LEFT JOIN `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized` last
        ON last.person_id = sessions.person_id
        AND last.session_id = sessions.session_id
        AND last.last_sub_session_in_session = 1
    ORDER BY 1,2
    """

COMPARTMENT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=COMPARTMENT_SESSIONS_VIEW_NAME,
    view_query_template=COMPARTMENT_SESSIONS_QUERY_TEMPLATE,
    description=COMPARTMENT_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPARTMENT_SESSIONS_VIEW_BUILDER.build_and_print()
