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
"""A view revealing when Workflows and TES completion event types do not match"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

COMPLETION_EVENT_TYPE_MISMATCH_VIEW_NAME = "completion_event_type_mismatch"

COMPLETION_EVENT_TYPE_MISMATCH_DESCRIPTION = """
Identifies configured Workflows opportunities that do not match TES
or do not match to the launched Workflows sheet using the completion
event type as the linking column.
"""

COMPLETION_EVENT_TYPE_MISMATCH_QUERY_TEMPLATE = """
WITH flags_per_event_type AS (
    SELECT
        state_code,
        state_code AS region_code,
        completion_event_type,
        workflows.state_code IS NOT NULL AS in_workflows,
        tes.completion_event_type IS NOT NULL AS in_tes,
        launches.completion_event_type IS NOT NULL AS in_launches,
    FROM
        `{project_id}.reference_views.workflows_opportunity_configs_materialized` workflows
    FULL OUTER JOIN
        `{project_id}.static_reference_tables.workflows_launch_metadata_materialized` launches
    USING
        (state_code, completion_event_type)
    -- Left join TES to Workflows since we are not concerned with experimental
    -- TES spans that are not configured in Workflows (ie. geriatric parole)
    LEFT JOIN
        `{project_id}.reference_views.task_to_completion_event` tes
    USING
        (state_code, completion_event_type)
)
SELECT * FROM flags_per_event_type
WHERE
    -- Flag any cases where a completion event is not
    -- mapped to a Workflow opportunity or TES config
    NOT in_workflows OR NOT in_tes
"""

COMPLETION_EVENT_TYPE_MISMATCH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=COMPLETION_EVENT_TYPE_MISMATCH_VIEW_NAME,
    view_query_template=COMPLETION_EVENT_TYPE_MISMATCH_QUERY_TEMPLATE,
    description=COMPLETION_EVENT_TYPE_MISMATCH_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMPLETION_EVENT_TYPE_MISMATCH_VIEW_BUILDER.build_and_print()
