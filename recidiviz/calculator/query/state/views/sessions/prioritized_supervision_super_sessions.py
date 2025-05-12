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
"""Sessionized view of continuous time on supervision, including any time spent simultaneously
supervised and incarcerated."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRIORITIZED_SUPERVISION_SUPER_SESSIONS_VIEW_NAME = (
    "prioritized_supervision_super_sessions"
)

PRIORITIZED_SUPERVISION_SUPER_SESSIONS_QUERY_TEMPLATE = f"""
WITH prioritized_supervision_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
    FROM `{{project_id}}.sessions.prioritized_supervision_sessions_materialized`
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive,
FROM ({aggregate_adjacent_spans(
    table_name="prioritized_supervision_sessions",
    index_columns=["state_code", "person_id"],
    end_date_field_name='end_date_exclusive')}
)
"""

PRIORITIZED_SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=PRIORITIZED_SUPERVISION_SUPER_SESSIONS_VIEW_NAME,
    view_query_template=PRIORITIZED_SUPERVISION_SUPER_SESSIONS_QUERY_TEMPLATE,
    description=__doc__,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRIORITIZED_SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER.build_and_print()
