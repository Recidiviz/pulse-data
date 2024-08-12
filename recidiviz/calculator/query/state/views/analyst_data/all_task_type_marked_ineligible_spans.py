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
"""Sessionized view of `clients_snooze_spans` aggregated to the task type level"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_NAME = (
    "all_task_type_marked_ineligible_spans"
)

ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_DESCRIPTION = """
Sessionized view of `clients_snooze_spans` aggregated to the task type level.
Person is marked ineligible if at least one snooze span exists during the time period
for a given task type.
"""

ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_QUERY_TEMPLATE = f"""
WITH all_snooze_spans AS (
    SELECT
        state_code,
        person_id,
        completion_event_type AS task_type,
        start_date,
        end_date_actual AS end_date_exclusive,
        TRUE AS marked_ineligible,
        denial_reasons,
    FROM
        `{{project_id}}.workflows_views.clients_snooze_spans_materialized`
    INNER JOIN
        `{{project_id}}.reference_views.workflows_opportunity_configs_materialized`
    USING (state_code, opportunity_type)
)
,
{create_sub_sessions_with_attributes("all_snooze_spans", index_columns=["state_code", "person_id", "task_type"], end_date_field_name="end_date_exclusive")}
,
sub_sessions_deduped AS (
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date_exclusive,
        TO_JSON_STRING(ARRAY_AGG(DISTINCT denial_reason IGNORE NULLS ORDER BY denial_reason)) AS denial_reasons,
    FROM sub_sessions_with_attributes
    -- Flatten the array by creating multiple rows for each array and then doing a group by to have them in a single row
    -- We cannot use DISTINCT directly to an array, we are using this approach instead
    CROSS JOIN UNNEST(denial_reasons) AS denial_reason
    -- Filter zero span days
    WHERE start_date != {nonnull_end_date_clause("end_date_exclusive")}
    GROUP BY
        state_code,
        person_id,
        task_type,
        start_date,
        end_date_exclusive
)
{aggregate_adjacent_spans(
    table_name="sub_sessions_deduped",
    index_columns=["state_code", "person_id", "task_type"],
    attribute=["denial_reasons"],
    session_id_output_name="marked_ineligible_span_id",
    end_date_field_name='end_date_exclusive'
)}
"""

ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_NAME,
    view_query_template=ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_QUERY_TEMPLATE,
    description=ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_BUILDER.build_and_print()
