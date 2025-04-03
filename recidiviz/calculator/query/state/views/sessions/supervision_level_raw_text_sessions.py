# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Sessionized view of non-overlapping periods of continuous stay on supervision on a
given supervision level and case type, using a state's internal mappings."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_VIEW_NAME = "supervision_level_raw_text_sessions"

SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_QUERY_TEMPLATE = f"""
    WITH sub_sessions_attributes_unnested AS
    (
    SELECT DISTINCT
        state_code,
        person_id,
        session_attributes.compartment_level_1,
        session_attributes.compartment_level_2,
        session_attributes.correctional_level AS supervision_level,
        session_attributes.correctional_level_raw_text AS supervision_level_raw_text,
        session_attributes.case_type AS case_type,
        start_date,
        end_date_exclusive,
        dataflow_session_id,
    FROM `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    WHERE session_attributes.compartment_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE')
    )
    ,
    deduped_cte AS
    (
    SELECT
        ss.*
    FROM sub_sessions_attributes_unnested ss
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_level_dedup_priority` p
        ON ss.supervision_level  = p.correctional_level
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_2_dedup_priority` cp
        ON ss.compartment_level_1  = cp.compartment_level_1
        AND ss.compartment_level_2  = cp.compartment_level_2
    --Deduplicate first by the supervision level priority, then by the compartment level 2 value in cases where those
    --are identical across multiple forms of supervision.
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, dataflow_session_id
        ORDER BY COALESCE(correctional_level_priority, 999),
                    COALESCE(priority, 999),
                    supervision_level_raw_text,
                    case_type
                 ) = 1
    )
    ,
    sessionized_cte AS
    (
    {aggregate_adjacent_spans(table_name='deduped_cte',
                       attribute=['supervision_level','supervision_level_raw_text','case_type'],
                       session_id_output_name='supervision_level_session_id',
                       end_date_field_name='end_date_exclusive')}
    )
    SELECT
        *,
        DATE_SUB(end_date_exclusive, INTERVAL 1 DAY) AS end_date,
        LAG(supervision_level) OVER w AS previous_supervision_level,
        LAG(supervision_level_raw_text) OVER w AS previous_supervision_level_raw_text,
    FROM sessionized_cte
    WINDOW w AS (PARTITION BY person_id, state_code, date_gap_id ORDER BY start_date)
"""

SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_QUERY_TEMPLATE,
    description=__doc__,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER.build_and_print()
