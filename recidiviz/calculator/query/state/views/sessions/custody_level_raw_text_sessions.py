# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Sessionized view of non-overlapping periods of continuous stay on incarceration on a given custody level,
using a state's internal mappings"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_NAME = "custody_level_raw_text_sessions"

CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of non-overlapping periods of continuous stay on
incarceration on a given custody level, using a state's internal mappings"""

CUSTODY_LEVEL_RAW_TEXT_SESSIONS_QUERY_TEMPLATE = f"""
    WITH sub_sessions_cte AS
    (
    SELECT 
        state_code,
        person_id,
        correctional_level AS custody_level,
        correctional_level_raw_text AS custody_level_raw_text,
        start_date,
        end_date_exclusive,
    FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
    WHERE compartment_level_1 IN ('INCARCERATION','INCARCERATION_OUT_OF_STATE')
        -- TODO(#5178): Once custody level ingest issues in TN are resolved, this can be removed
        AND state_code != "US_TN"
    
    UNION ALL
    
    -- TODO(#5178): Once custody level ingest issues in TN are resolved, this can be removed
    SELECT
        state_code,
        person_id,
        custody_level,
        custody_level_raw_text,
        start_date,
        end_date_exclusive
    FROM `{{project_id}}.analyst_data.us_tn_classification_raw_materialized`    
    
    )
    SELECT *
    FROM ({aggregate_adjacent_spans(table_name='sub_sessions_cte',
                       attribute=['custody_level','custody_level_raw_text'],
                       session_id_output_name='custody_level_raw_text_session_id',
                       end_date_field_name='end_date_exclusive')})
    """

CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_NAME,
    view_query_template=CUSTODY_LEVEL_RAW_TEXT_SESSIONS_QUERY_TEMPLATE,
    description=CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER.build_and_print()
