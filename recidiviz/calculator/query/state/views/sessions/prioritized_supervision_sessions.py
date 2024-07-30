# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Sessionized view of non-overlapping periods of continuous stay within a supervision type within a supervision legal authority"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRIORITIZED_SUPERVISION_SESSIONS_VIEW_NAME = "prioritized_supervision_sessions"

PRIORITIZED_SUPERVISION_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of non-overlapping periods of continuous stay within a supervision type within a supervision legal authority"""

PRIORITIZED_SUPERVISION_SESSIONS_QUERY_TEMPLATE = f"""
    -- TODO(#30001): Use legal_authority once hydrated
    -- TODO(#30002): Deprecate supervision_super_sessions in favor of this view
    -- TODO(#30815): Consider refactoring this view to not dedup to a single supervision type
    WITH sub_sessions_cte AS
    (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        open_supervision_cl1 AS compartment_level_1,
        open_supervision_cl2 AS compartment_level_2,
    FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
    WHERE open_supervision_cl1 IS NOT NULL
    )
    SELECT
        person_id,
        state_code,
        prioritized_supervision_session_id,
        start_date,
        end_date_exclusive,
        compartment_level_1,
        compartment_level_2,
    FROM ({aggregate_adjacent_spans(table_name='sub_sessions_cte',
                                    attribute=['compartment_level_1','compartment_level_2'],
                                    session_id_output_name='prioritized_supervision_session_id',
                                    end_date_field_name='end_date_exclusive')})
    """

PRIORITIZED_SUPERVISION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=PRIORITIZED_SUPERVISION_SESSIONS_VIEW_NAME,
    view_query_template=PRIORITIZED_SUPERVISION_SESSIONS_QUERY_TEMPLATE,
    description=PRIORITIZED_SUPERVISION_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRIORITIZED_SUPERVISION_SESSIONS_VIEW_BUILDER.build_and_print()
