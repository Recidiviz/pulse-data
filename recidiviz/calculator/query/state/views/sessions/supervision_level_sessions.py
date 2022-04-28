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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import non_active_supervision_levels
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LEVEL_SESSIONS_VIEW_NAME = "supervision_level_sessions"

SUPERVISION_LEVEL_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of non-overlapping periods of continuous stay on supervision on a given supervision level, along with level transitions (upgrades/downgrades)"""

SUPERVISION_LEVEL_SESSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    WITH sub_sessions_attributes_unnested AS
    (
    SELECT DISTINCT
        state_code,
        person_id,
        session_attributes.correctional_level AS supervision_level,
        start_date,
        end_date,
        dataflow_session_id,
    FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    WHERE session_attributes.compartment_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE')
    )
    ,
    deduped_cte AS
    (
    SELECT
        *
    FROM sub_sessions_attributes_unnested ss
    LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_dedup_priority` p
        ON ss.supervision_level  = p.correctional_level
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, dataflow_session_id
        ORDER BY COALESCE(correctional_level_priority, 999)) = 1
    )
    ,
    sessionized_cte AS
    (
    SELECT
        state_code,
        person_id,
        supervision_level,
        supervision_level_session_id_unordered,
        supervision_group_id,
        correctional_level_priority,
        is_discretionary_level,
        MIN(start_date) start_date,
        CASE WHEN LOGICAL_AND(end_date IS NOT NULL) THEN MAX(end_date) END AS end_date,
        MIN(dataflow_session_id) AS dataflow_session_id_start,
        MAX(dataflow_session_id) AS dataflow_session_id_end
        FROM
            (
            SELECT
                *,
                SUM(CASE WHEN COALESCE(level_changed, 1) = 1 THEN 1 ELSE 0 END)
                    OVER(PARTITION BY person_id, state_code ORDER BY supervision_level, start_date) AS supervision_level_session_id_unordered,
                SUM(CASE WHEN COALESCE(date_gap, TRUE) THEN 1 ELSE 0 END) OVER(PARTITION BY person_id, state_code ORDER BY start_date) AS supervision_group_id
            FROM
                (
                SELECT
                    session.state_code,
                    session.person_id,
                    session.supervision_level,
                    session.correctional_level_priority,
                    session.is_discretionary_level,
                    session.start_date,
                    session.end_date,
                    session.dataflow_session_id,
                    MIN(IF(session_lag.supervision_level = session.supervision_level, 0, 1)) AS level_changed,
                    MAX(session_lag.start_date) IS NULL AS date_gap
                FROM deduped_cte session
                LEFT JOIN deduped_cte as session_lag
                    ON session.state_code = session_lag.state_code
                    AND session.person_id = session_lag.person_id
                    AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
                GROUP BY 1,2,3,4,5,6,7,8
                )
            )
    GROUP BY 1,2,3,4,5,6,7
    )
    ,
    sessionized_cte_ordered AS
    (
    SELECT
        *  EXCEPT(supervision_level_session_id_unordered),
        ROW_NUMBER() OVER(PARTITION BY person_id, state_code ORDER BY start_date, COALESCE(end_date,'9999-01-01')) AS supervision_level_session_id
    FROM sessionized_cte
    ORDER BY supervision_level_session_id
    )
    SELECT
        session.state_code,
        session.person_id,
        session.supervision_level_session_id,
        session.dataflow_session_id_start,
        session.dataflow_session_id_end,
        session.supervision_level,
        session.start_date,
        session.end_date,
        session_lag.supervision_level AS previous_supervision_level,
        LAST_VALUE(
            CASE WHEN session.supervision_level NOT IN {non_active_supervision_levels} THEN session.supervision_level END IGNORE NULLS)
            OVER(
                PARTITION BY session.state_code, session.person_id, session.supervision_group_id
                ORDER BY session.start_date
            ) AS most_recent_active_supervision_level,
        CASE WHEN session.correctional_level_priority < session_lag.correctional_level_priority
            AND session.is_discretionary_level AND session_lag.is_discretionary_level
            THEN 1 ELSE 0 END as supervision_upgrade,
        CASE WHEN session.correctional_level_priority > session_lag.correctional_level_priority
            AND session.is_discretionary_level AND session_lag.is_discretionary_level
            THEN 1 ELSE 0 END as supervision_downgrade,
    FROM sessionized_cte_ordered session
    LEFT JOIN sessionized_cte_ordered session_lag
        ON session.state_code = session_lag.state_code
            AND session.person_id = session_lag.person_id
            AND session.start_date = DATE_ADD(session_lag.end_date, INTERVAL 1 DAY)
    """

SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_LEVEL_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_LEVEL_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_LEVEL_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
    non_active_supervision_levels=non_active_supervision_levels(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER.build_and_print()
