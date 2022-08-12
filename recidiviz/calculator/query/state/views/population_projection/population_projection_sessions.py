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
"""A sessions view specifically altered for the population projection simulation"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_PROJECTION_SESSIONS_VIEW_NAME = "population_projection_sessions"

POPULATION_PROJECTION_SESSIONS_VIEW_DESCRIPTION = (
    """"Compartment sessions view altered for the population projection simulation"""
)

POPULATION_PROJECTION_SESSIONS_QUERY_TEMPLATE = """
    WITH previously_incarcerated_cte AS (
      -- Create a flag to indicate if the person was incarcerated prior to this session
      -- Only count a session as "previously incarcerated" if there was a release/supervision session post-incarceration
      SELECT
        session.state_code, session.person_id, session.session_id,
        LOGICAL_OR(prev_rel_session.session_id IS NOT NULL) AS previously_incarcerated
      FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` session
      LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` prev_inc_session
        ON session.state_code = prev_inc_session.state_code
        AND session.person_id = prev_inc_session.person_id
        AND prev_inc_session.start_date < session.start_date
        AND prev_inc_session.compartment_level_1 = 'INCARCERATION'
      LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` prev_rel_session
        ON session.state_code = prev_rel_session.state_code
        AND session.person_id = prev_rel_session.person_id
        AND prev_rel_session.start_date BETWEEN prev_inc_session.end_date AND session.start_date
        AND prev_rel_session.compartment_level_1 IN ('LIBERTY', 'SUPERVISION')
      GROUP BY state_code, person_id, session_id
    ), all_sessions AS (
    SELECT
      person_id,
      session_id,
      state_code,
      previously_incarcerated,
      -- Reclassify some compartment values
      CASE WHEN compartment_level_1 LIKE 'SUPERVISION%' THEN
           CASE WHEN compartment_level_2 = 'DUAL' THEN CONCAT(compartment_level_1, ' - PAROLE')
                WHEN compartment_level_2 = 'INTERNAL_UNKNOWN' THEN CONCAT(compartment_level_1, ' - PROBATION')
                -- TODO(#9554): Reclassify out of state BW to in-state
                WHEN compartment_level_2 = 'BENCH_WARRANT' THEN 'SUPERVISION - BENCH_WARRANT'
                ELSE CONCAT(compartment_level_1, ' - ', COALESCE(compartment_level_2, ''))
           END
           WHEN compartment_level_1 = 'INCARCERATION_OUT_OF_STATE' THEN 'INCARCERATION_OUT_OF_STATE'
        ELSE CONCAT(compartment_level_1, ' - ', COALESCE(compartment_level_2, ''))
      END AS compartment,
      start_date,
      start_reason,
      start_sub_reason,
      end_date,
      end_reason,
      gender,
      age_bucket_start AS age_bucket,
      prioritized_race_or_ethnicity,
      -- Reclassify some inflow values
      CASE WHEN inflow_from_level_1 LIKE 'SUPERVISION%' THEN
           CASE WHEN inflow_from_level_2 = 'DUAL' THEN CONCAT(inflow_from_level_1, ' - PAROLE')
                WHEN inflow_from_level_2 = 'INTERNAL_UNKNOWN' THEN CONCAT(inflow_from_level_1, ' - PROBATION')
                -- TODO(#9554): Reclassify out of state BW to in-state
                WHEN inflow_from_level_2 = 'BENCH_WARRANT' THEN 'SUPERVISION - BENCH_WARRANT'
                ELSE CONCAT(inflow_from_level_1, ' - ', COALESCE(inflow_from_level_2, ''))
           END
        WHEN inflow_from_level_2 = 'LIBERTY_FIRST_TIME_IN_SYSTEM' THEN 'PRETRIAL'
        WHEN inflow_from_level_1 = 'INCARCERATION_OUT_OF_STATE' THEN 'INCARCERATION_OUT_OF_STATE'
        ELSE CONCAT(inflow_from_level_1, ' - ', COALESCE(inflow_from_level_2, ''))
      END AS inflow_from,
      -- Reclassify some outflow values
      CASE WHEN outflow_to_level_1 LIKE 'SUPERVISION%' THEN
           CASE WHEN outflow_to_level_2 = 'DUAL' THEN CONCAT(outflow_to_level_1, ' - PAROLE')
                WHEN outflow_to_level_2 = 'INTERNAL_UNKNOWN' THEN CONCAT(outflow_to_level_1, ' - PROBATION')
                -- TODO(#9554): Reclassify out of state BW to in-state
                WHEN outflow_to_level_2 = 'BENCH_WARRANT' THEN 'SUPERVISION - BENCH_WARRANT'
                ELSE CONCAT(outflow_to_level_1, ' - ', COALESCE(outflow_to_level_2, ''))
           END
           WHEN outflow_to_level_1 = 'INCARCERATION_OUT_OF_STATE' THEN 'INCARCERATION_OUT_OF_STATE'
           -- TODO(#7385): handle incarceration to 0 day supervision period as release to liberty
           WHEN outflow_to_level_1 = 'PENDING_SUPERVISION' THEN 'LIBERTY - LIBERTY_REPEAT_IN_SYSTEM'
        ELSE CONCAT(outflow_to_level_1, ' - ', COALESCE(outflow_to_level_2, ''))
      END AS outflow_to,
      session_length_days,
      last_day_of_data
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    INNER JOIN previously_incarcerated_cte
      USING (state_code, person_id, session_id)
    ),
    dedup_sessions AS (
        -- Collapse adjacent sessions with the same compartment to merge some supervision sessions
        -- DUAL -> PAROLE and INTERNAL_UNKNOWN -> PROBATION
        SELECT
            * EXCEPT(session_length_days),
            session_id = MIN(session_id) OVER(PARTITION BY person_id, temp_session_id) AS first_sub_session_in_session,
            session_id = MAX(session_id) OVER(PARTITION BY person_id, temp_session_id) AS last_sub_session_in_session,
            SUM(session_length_days) OVER(PARTITION BY person_id, temp_session_id ORDER BY session_id) AS session_length_days,
        FROM (
            SELECT *,
                -- Increment the temp_session_id whenever the inflow & compartment aren't the same value
                SUM(IF(compartment != COALESCE(inflow_from,''), 1, 0))
                    OVER(PARTITION BY person_id ORDER BY session_id) AS temp_session_id
            FROM all_sessions
        )
    )
    SELECT
        first.* EXCEPT(end_date, end_reason, outflow_to, session_length_days, temp_session_id,
            first_sub_session_in_session, last_sub_session_in_session),
        last.end_date,
        last.end_reason,
        last.outflow_to,
        last.session_length_days
    FROM dedup_sessions first
    LEFT JOIN dedup_sessions last
        ON first.state_code = last.state_code
        AND first.person_id = last.person_id
        AND first.temp_session_id = last.temp_session_id
        AND last.last_sub_session_in_session
    WHERE first.first_sub_session_in_session
        -- Drop 0 day sessions
        AND first.start_date != COALESCE(last.end_date, last.last_day_of_data)
    """

POPULATION_PROJECTION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=POPULATION_PROJECTION_SESSIONS_VIEW_NAME,
    view_query_template=POPULATION_PROJECTION_SESSIONS_QUERY_TEMPLATE,
    description=POPULATION_PROJECTION_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_PROJECTION_SESSIONS_VIEW_BUILDER.build_and_print()
