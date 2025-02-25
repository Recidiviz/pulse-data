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
"""Sessionized view of `task_eligibility.all_tasks` that collapses spans based on the `task_name` and `is_eligible`
fields for each client"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ALL_TASK_ELIGIBILITY_SPANS_VIEW_NAME = "all_task_eligibility_spans"

ALL_TASK_ELIGIBILITY_SPANS_VIEW_DESCRIPTION = """Sessionized view of `task_eligibility.all_tasks` that collapses spans
based on the `task_name` and `is_eligible` fields for each client"""

ALL_TASK_ELIGIBILITY_SPANS_QUERY_TEMPLATE = f"""
    WITH all_task_cte AS
    (
    SELECT
        state_code,
        person_id,
        task_name,
        completion_event_type AS task_type,
        start_date,
        end_date,
        is_eligible,
        is_almost_eligible,
    FROM
        `{{project_id}}.task_eligibility.all_tasks_materialized`
    INNER JOIN
        `{{project_id}}.reference_views.task_to_completion_event`
    USING (state_code, task_name)
    )
    ,
    sessionized_cte AS
    (
    {aggregate_adjacent_spans(table_name='all_task_cte',
                       attribute=['task_name', 'task_type', 'is_eligible', 'is_almost_eligible'],
                       session_id_output_name='task_eligibility_span_id',
                       end_date_field_name='end_date')}
    )
    SELECT
        *,
    FROM sessionized_cte
"""

ALL_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=ALL_TASK_ELIGIBILITY_SPANS_VIEW_NAME,
    view_query_template=ALL_TASK_ELIGIBILITY_SPANS_QUERY_TEMPLATE,
    description=ALL_TASK_ELIGIBILITY_SPANS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_TASK_ELIGIBILITY_SPANS_VIEW_BUILDER.build_and_print()
