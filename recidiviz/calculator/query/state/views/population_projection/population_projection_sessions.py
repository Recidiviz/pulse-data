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
# pylint: disable=trailing-whitespace
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
      person_id,
      session_id,
      state_code,
      previously_incarcerated,
      -- Count DUAL supervision as PAROLE
      CASE WHEN compartment_level_1 = 'SUPERVISION' AND compartment_level_2 = 'DUAL' THEN 'SUPERVISION - PAROLE'
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
      -- Count DUAL supervision as PAROLE
      CASE WHEN inflow_from_level_1 = 'SUPERVISION' AND inflow_from_level_2 = 'DUAL' THEN 'SUPERVISION - PAROLE'
        WHEN inflow_from_level_1 IS NULL THEN 'PRETRIAL'
        ELSE CONCAT(inflow_from_level_1, ' - ', COALESCE(inflow_from_level_2, ''))
      END AS inflow_from,
      -- Count DUAL supervision as PAROLE and CONDITIONAL_RELEASE as outflow to PAROLE
      CASE WHEN outflow_to_level_1 = 'SUPERVISION' AND outflow_to_level_2 = 'DUAL' THEN 'SUPERVISION - PAROLE'
        WHEN end_reason = 'CONDITIONAL_RELEASE' AND outflow_to_level_1 = 'RELEASE' THEN 'SUPERVISION - PAROLE'
        ELSE CONCAT(outflow_to_level_1, ' - ', COALESCE(outflow_to_level_2, ''))
      END AS outflow_to,
      session_length_days,
      last_day_of_data
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized`
    INNER JOIN previously_incarcerated_cte
      USING (state_code, person_id, session_id)
    WHERE compartment_level_2 != 'OTHER'
    """

POPULATION_PROJECTION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.POPULATION_PROJECTION_DATASET,
    view_id=POPULATION_PROJECTION_SESSIONS_VIEW_NAME,
    view_query_template=POPULATION_PROJECTION_SESSIONS_QUERY_TEMPLATE,
    description=POPULATION_PROJECTION_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_PROJECTION_SESSIONS_VIEW_BUILDER.build_and_print()
