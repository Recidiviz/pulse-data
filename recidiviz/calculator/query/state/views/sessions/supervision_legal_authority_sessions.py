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
"""Sessionized view of non-overlapping periods of continuous stay with a supervision legal authority"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_LEGAL_AUTHORITY_SESSIONS_VIEW_NAME = "supervision_legal_authority_sessions"

SUPERVISION_LEGAL_AUTHORITY_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of non-overlapping periods of continuous stay with a supervision legal authority"""

SUPERVISION_LEGAL_AUTHORITY_SESSIONS_QUERY_TEMPLATE = f"""
    -- TODO(#30001): Use legal_authority once hydrated
    -- TODO(#30002): Deprecate supervision_super_sessions in favor of this view
    WITH sub_sessions_cte AS
    (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
    FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
    WHERE is_on_supervision
    )
    SELECT
        person_id,
        state_code,
        supervision_legal_authority_session_id,
        start_date,
        end_date_exclusive,
    FROM ({aggregate_adjacent_spans(table_name='sub_sessions_cte',
                                    session_id_output_name='supervision_legal_authority_session_id',
                                    end_date_field_name='end_date_exclusive')})
    """

SUPERVISION_LEGAL_AUTHORITY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_LEGAL_AUTHORITY_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_LEGAL_AUTHORITY_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_LEGAL_AUTHORITY_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LEGAL_AUTHORITY_SESSIONS_VIEW_BUILDER.build_and_print()
