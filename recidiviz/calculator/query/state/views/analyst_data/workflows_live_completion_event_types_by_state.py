#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View containing all task type <> state pairs associated with live opportunities in our states"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_live_completion_event_types_by_state"

_VIEW_DESCRIPTION = """
View containing all task type <> state pairs associated with live opportunities in our states,
with an indicator for opportunities that have been fully launched."""

_QUERY_TEMPLATE = """
-- Identify all workflows completion event types associated with an opportunity that has
-- had at least one usage event in a state, which indicates that the tool is live for
-- at least one user.
WITH all_live_workflows AS (
    SELECT
        a.state_code,
        b.completion_event_type,
        DATE(MIN(event_ts)) AS first_access_date,
    FROM
        `{project_id}.analyst_data.workflows_officer_events_materialized` a
    INNER JOIN
        `{project_id}.reference_views.workflows_opportunity_configs_materialized` b
    USING
        (opportunity_type)
    WHERE
        event IN ("WORKFLOWS_USER_ACTION", "WORKFLOWS_USER_CLIENT_STATUS_UPDATE", "WORKFLOWS_USER_PAGE")
    GROUP BY 1, 2
)
-- Join with workflows_launch_metadata to get an indicator of which task types
-- have been fully launched in a given state.
SELECT
    a.*,
    b.is_fully_launched,
FROM
    all_live_workflows a
LEFT JOIN
    `{project_id}.static_reference_tables.workflows_launch_metadata_materialized` b
USING
    (state_code, completion_event_type)
"""

WORKFLOWS_LIVE_COMPLETION_EVENT_TYPES_BY_STATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    clustering_fields=["state_code"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_LIVE_COMPLETION_EVENT_TYPES_BY_STATE_VIEW_BUILDER.build_and_print()
