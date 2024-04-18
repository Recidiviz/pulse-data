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
"""Sessionized view of each individual that is incarcerated. Session defined as continuous period of time on custody level, using
Recidiviz schema mappings"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CUSTODY_LEVEL_SESSIONS_VIEW_NAME = "custody_level_sessions"

CUSTODY_LEVEL_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of periods of continuous stay in incarceration on a given custody level, using
Recidiviz schema mappings"""

CUSTODY_LEVEL_SESSIONS_QUERY_TEMPLATE = f"""
    WITH sub_sessions_cte AS
    (
    SELECT 
        state_code,
        person_id,
        correctional_level AS custody_level,
        start_date,
        end_date_exclusive,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` css
    WHERE compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
        -- TODO(#5178): Once custody level ingest issues in TN are resolved, this can be removed
        AND state_code != "US_TN"
    
    UNION ALL
    
    -- TODO(#5178): Once custody level ingest issues in TN are resolved, this can be removed
    SELECT
        state_code,
        person_id,
        custody_level,
        start_date,
        end_date_exclusive
    FROM `{{project_id}}.analyst_data.us_tn_classification_raw_materialized`    
    )
    ,
    sessionized_cte AS
    (
    {aggregate_adjacent_spans(table_name='sub_sessions_cte',
                       attribute='custody_level',
                       session_id_output_name='custody_level_session_id',
                       end_date_field_name='end_date_exclusive')}
    )
    ,
    dedup_priority AS (
        SELECT session.*, cl.custody_level_priority, cl.is_discretionary_level
        FROM
            sessionized_cte session
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.custody_level_dedup_priority` cl
            USING(custody_level)
    )
    SELECT
        session.* EXCEPT(custody_level_priority),
        CASE WHEN session.custody_level_priority < session_lag.custody_level_priority
            AND session.is_discretionary_level AND session_lag.is_discretionary_level
            THEN 1 ELSE 0 END as custody_upgrade,
        CASE WHEN session.custody_level_priority > session_lag.custody_level_priority
            AND session.is_discretionary_level AND session_lag.is_discretionary_level
            THEN 1 ELSE 0 END as custody_downgrade,
        CASE WHEN session.is_discretionary_level AND session_lag.is_discretionary_level
            THEN ABS(session.custody_level_priority - session_lag.custody_level_priority)
            END AS custody_level_num_change,
        session_lag.custody_level AS previous_custody_level,
    FROM dedup_priority session
    LEFT JOIN dedup_priority session_lag
        ON session.state_code = session_lag.state_code
        AND session.person_id = session_lag.person_id
        AND session.start_date = session_lag.end_date_exclusive
    """

CUSTODY_LEVEL_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=CUSTODY_LEVEL_SESSIONS_VIEW_NAME,
    view_query_template=CUSTODY_LEVEL_SESSIONS_QUERY_TEMPLATE,
    description=CUSTODY_LEVEL_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CUSTODY_LEVEL_SESSIONS_VIEW_BUILDER.build_and_print()
