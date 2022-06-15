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
"""Deduped supervision sessions for Pathways views.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.pathways_deduped_supervision_sessions
"""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    pathways_state_specific_supervision_level,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_NAME = (
    "pathways_deduped_supervision_sessions"
)

PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_DESCRIPTION = (
    """Deduped supervision sessions for Pathways views."""
)

PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cte AS (
        /*
        Use equivalent logic from compartment_sessions to deduplicate individuals who have more than
        one supervision location on a given day.
        */
        SELECT 
            s.state_code,
            s.dataflow_session_id,
            s.person_id,
            s.end_date,
            EXTRACT(YEAR FROM date_of_supervision) AS year,
            EXTRACT(MONTH FROM date_of_supervision) AS month,
            IFNULL(name_map.location_name,session_attributes.supervision_office) AS district,
            CASE
                WHEN s.state_code="US_ND" THEN NULL
                ELSE {state_specific_supervision_level}
            END AS supervision_level,
        FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized` s,
        UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH), 
            CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)) as date_of_supervision,
        UNNEST (session_attributes) session_attributes
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_supervision_location_name_map` name_map
            ON s.state_code = name_map.state_code
            AND session_attributes.supervision_office = name_map.location_id
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_2_dedup_priority` cl2_dedup
            ON "SUPERVISION" = cl2_dedup.compartment_level_1
            AND session_attributes.compartment_level_2=cl2_dedup.compartment_level_2
        LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_dedup_priority` sl_dedup
            ON session_attributes.correctional_level=sl_dedup.correctional_level
        WHERE session_attributes.compartment_level_1 = 'SUPERVISION' 
            AND date_of_supervision BETWEEN s.start_date AND COALESCE(s.end_date, CURRENT_DATE('US/Eastern'))
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, date_of_supervision
            ORDER BY COALESCE(cl2_dedup.priority, 999),
            COALESCE(sl_dedup.correctional_level_priority, 999),
            NULLIF(session_attributes.supervising_officer_external_id, 'EXTERNAL_UNKNOWN') NULLS LAST,
            NULLIF(session_attributes.case_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
            NULLIF(session_attributes.judicial_district_code, 'EXTERNAL_UNKNOWN') NULLS LAST
        ) = 1
    )
    SELECT
        cte.* EXCEPT(dataflow_session_id),
        s.prioritized_race_or_ethnicity as race
    FROM cte
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` s
        ON cte.state_code = s.state_code
        AND cte.person_id = s.person_id
        AND cte.dataflow_session_id <= s.dataflow_session_id_end
        AND cte.dataflow_session_id >= s.dataflow_session_id_start
    """

PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_NAME,
    view_query_template=PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_QUERY_TEMPLATE,
    # year must come before month to export correctly
    dimensions=("state_code", "year", "month", "district", "supervision_level", "race"),
    description=PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_DESCRIPTION,
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    state_specific_supervision_level=pathways_state_specific_supervision_level(
        "s.state_code",
        "session_attributes.correctional_level",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PATHWAYS_DEDUPED_SUPERVISION_SESSIONS_VIEW_BUILDER.build_and_print()
