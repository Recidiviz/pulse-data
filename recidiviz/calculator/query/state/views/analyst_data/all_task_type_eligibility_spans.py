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
"""Sessionized view of `all_task_eligibility_spans` that collapses spans based on completion event type
and eligibility status"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_NAME = "all_task_type_eligibility_spans"

ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_DESCRIPTION = """Sessionized view of `all_task_eligibility_spans` that collapses
spans based on completion event type and eligibility status"""

ALL_TASK_TYPE_ELIGIBILITY_SPANS_QUERY_TEMPLATE = f"""
WITH all_task_spans AS (SELECT * FROM `{{project_id}}.task_eligibility.all_tasks__collapsed_materialized`)
,
{create_sub_sessions_with_attributes("all_task_spans", index_columns=["state_code", "person_id", "task_type"])}
,
sub_sessions_deduped AS (
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date,
        # If person is eligible for at least one task, label as eligible
        LOGICAL_OR(is_eligible) AS is_eligible,
        # If person is not eligible for any tasks and is almost eligible for at least one task, label as almost eligible
        NOT LOGICAL_OR(is_eligible) AND LOGICAL_OR(is_almost_eligible) AS is_almost_eligible,
    FROM sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4, 5
)
,
sessionized_cte AS
(
{aggregate_adjacent_spans(
    table_name='sub_sessions_deduped',
    attribute=['task_type','is_eligible', 'is_almost_eligible'],
    session_id_output_name='task_type_eligibility_span_id',
    end_date_field_name='end_date'
)}
)
SELECT
    *,
    # Flags when someone became newly eligible or almost eligible on this date after a period of ineligibility
    # This indicates that someone would have been newly resurfaced on the Workflows app,
    # resetting other usage-related statuses from the previous stint of eligibility.
    (is_eligible OR is_almost_eligible)
    AND NOT LAG(is_eligible OR is_almost_eligible) OVER (PARTITION BY person_id, task_type ORDER BY start_date) 
    AS eligibility_reset,
FROM sessionized_cte
"""

ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_NAME,
    view_query_template=ALL_TASK_TYPE_ELIGIBILITY_SPANS_QUERY_TEMPLATE,
    description=ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_BUILDER.build_and_print()
