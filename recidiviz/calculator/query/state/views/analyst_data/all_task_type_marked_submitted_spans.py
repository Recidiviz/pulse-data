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
"""Aggregates marked_submitted and unsubmitted events to the task type level"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_NAME = "all_task_type_marked_submitted_spans"

ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_DESCRIPTION = """
View of submitted and marked-unsubmitted events as spans,
sessionized and aggregated to the task type level.
"""


def sessionize_submitted_start_and_end_events(
    start_event_dataset: str,
    end_event_dataset: str,
    start_date_column: str = "timestamp",
    end_date_column: str = "timestamp",
) -> str:
    """
    Return a query fragment that selects all start dates and only the end date
    immediately after each start date, or NULL if there is no such end date
    """
    later_end_date_or_null = f"""
    IF(
        u.{end_date_column} > s.{start_date_column},
        DATETIME(u.{end_date_column}),
        NULL
    )
"""

    return f"""
    SELECT 
        person_id,
        state_code,
        opportunity_type,
        DATETIME(s.{start_date_column}) AS start_date,
        {revert_nonnull_end_date_clause(f"MIN({nonnull_end_date_clause(later_end_date_or_null)})")} AS end_date,
    FROM 
        `{start_event_dataset}` s
    LEFT JOIN 
        `{end_event_dataset}` u
    USING (person_id, opportunity_type, state_code)
    GROUP BY person_id, state_code, opportunity_type, start_date
"""


ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_QUERY_TEMPLATE = f"""
WITH all_submitted_spans AS (
{
    sessionize_submitted_start_and_end_events(start_event_dataset="{project_id}.workflows_views.clients_opportunity_marked_submitted",
                                              end_event_dataset="{project_id}.workflows_views.clients_opportunity_unsubmitted")
}
),
submitted_spans_with_task_types AS (
SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        completion_event_type AS task_type,
    FROM
        all_submitted_spans
    INNER JOIN 
        `{{project_id}}.reference_views.workflows_opportunity_configs_materialized` 
    USING (opportunity_type, state_code)
),
{create_sub_sessions_with_attributes("submitted_spans_with_task_types", index_columns=["person_id", "task_type"])}
,
sub_sessions_deduped AS (
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date,
    FROM sub_sessions_with_attributes
    -- Filter zero span days
    WHERE start_date != {nonnull_end_date_clause("end_date")}
    GROUP BY 1, 2, 3, 4, 5
)
{aggregate_adjacent_spans(
    table_name="sub_sessions_deduped",
    index_columns=["state_code", "person_id", "task_type"],
    session_id_output_name="marked_submitted_span_id",
)}
"""

ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_NAME,
    view_query_template=ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_QUERY_TEMPLATE,
    description=ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_BUILDER.build_and_print()
