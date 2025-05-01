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
"""View of continuous spans at a given client count for the specified assignment sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.staff_caseload_count_spans import (
    build_caseload_count_spans_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officer_caseload_count_spans"

_VIEW_DESCRIPTION = (
    "Spans defined per officer based on periods of a continuous caseload size"
)

_QUERY_TEMPLATE = f"""
WITH all_caseload_spans AS (
    {build_caseload_count_spans_query_template(
        assignment_sessions_sql_source='''
            SELECT person_id, state_code, start_date, end_date_exclusive, supervising_officer_external_id AS officer_id
            FROM `{project_id}.sessions.supervision_officer_sessions_materialized`
            WHERE supervising_officer_external_id IS NOT NULL
        ''',
        index_cols=["state_code", "officer_id"],
        start_date_col_name="start_date",
        end_date_exclusive_col_name="end_date_exclusive",
    )}
)
, active_caseload_spans AS (
    -- It's a bit weird to go from sessions -> agg metrics -> sessions, but the assignment sessions
    -- view is the best one we have for allowing overlapping officers and also taking into account
    -- periods of active supervision, so we do it anyway.
    -- TODO(#41771): Refactor this to use dataflow_sessions
    {build_caseload_count_spans_query_template(
        assignment_sessions_sql_source='''
            SELECT person_id, state_code, assignment_date, end_date_exclusive, officer_id
            FROM `{project_id}.aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized`
            WHERE officer_id IS NOT NULL
        ''',
        index_cols=["state_code", "officer_id"],
        start_date_col_name="assignment_date",
        end_date_exclusive_col_name="end_date_exclusive",
    )}
)
, intersection_spans AS (
    {create_intersection_spans(
        table_1_name="all_caseload_spans", 
        table_2_name="active_caseload_spans", 
        index_columns=["state_code", "officer_id"],
        use_left_join=True,
        table_1_columns=["caseload_count AS total_caseload_count"],
        table_2_columns=["caseload_count AS active_caseload_count"],
        table_1_end_date_field_name="end_date",
        table_2_end_date_field_name="end_date",
    )}
)
SELECT * FROM intersection_spans
"""

SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER.build_and_print()
