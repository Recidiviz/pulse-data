# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Sessionized view of each individual on supervision. Session defined as continuous period of time on supervision level"""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LEVEL_SESSIONS_VIEW_NAME = "supervision_level_sessions"

SUPERVISION_LEVEL_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of non-verlapping periods of continuous stay on supervision on a given supervision level, along with level transitions (upgrades/downgrades)"""

SUPERVISION_LEVEL_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision_level_sessions AS
    (
    SELECT state_code, person_id, supervision_level, supervision_level_session_id,
        correctional_level_priority, is_discretionary_level,
        MIN(start_date) start_date,
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
    FROM (
        SELECT *, 
            SUM(CASE WHEN COALESCE(level_changed, 0) = 1 THEN 1 ELSE 0 END) 
                OVER(PARTITION BY person_id ORDER BY start_date) AS supervision_level_session_id
        FROM (
            SELECT DISTINCT session.state_code, session.person_id, session.correctional_level as supervision_level, 
                level_priority.correctional_level_priority, level_priority.is_discretionary_level,
                session.start_date, session.end_date,
                IF(COALESCE(session_lag.correctional_level, 'UNKNOWN') = COALESCE(session.correctional_level, 'UNKNOWN'), 0, 1) AS level_changed,
            FROM `{project_id}.{analyst_dataset}.dataflow_sessions_materialized` session
            LEFT JOIN `{project_id}.{analyst_dataset}.supervision_level_dedup_priority` as level_priority
                USING(correctional_level)
            LEFT JOIN `{project_id}.{analyst_dataset}.dataflow_sessions_materialized` session_lag
                ON session.state_code = session_lag.state_code
                AND session.person_id = session_lag.person_id
                -- Self join with previous sub session within the same session to get previous supervision level
                AND session.dataflow_session_id = session_lag.dataflow_session_id + 1
                AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
                AND session_lag.compartment_level_1 = 'SUPERVISION'
            WHERE session.compartment_level_1 IN ('SUPERVISION', 'SUPERVISION_OUT_OF_STATE')
            )
        )
GROUP BY state_code, person_id, supervision_level, supervision_level_session_id, correctional_level_priority, is_discretionary_level
)
SELECT session.state_code, session.person_id, session.supervision_level_session_id, session.supervision_level,
    session.start_date, session.end_date,
    session_lag.supervision_level as previous_supervision_level,
    CASE WHEN session.correctional_level_priority < session_lag.correctional_level_priority 
        AND session.is_discretionary_level AND session_lag.is_discretionary_level
        THEN 1 ELSE 0 END as supervision_upgrade,
    CASE WHEN session.correctional_level_priority > session_lag.correctional_level_priority 
        AND session.is_discretionary_level AND session_lag.is_discretionary_level
        THEN 1 ELSE 0 END as supervision_downgrade, 
FROM supervision_level_sessions session
LEFT JOIN supervision_level_sessions session_lag
    ON session.state_code = session_lag.state_code
        AND session.person_id = session_lag.person_id
        AND session.supervision_level_session_id = session_lag.supervision_level_session_id + 1
        AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
    """

SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=SUPERVISION_LEVEL_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_LEVEL_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_LEVEL_SESSIONS_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER.build_and_print()
