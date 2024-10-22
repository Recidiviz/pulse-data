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
"""
This view is the source for the Workflows Usage dashboard in Looker.
It includes relevant front-end events from the Workflows product.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_officer_events import (
    WORKFLOWS_OFFICER_EVENT_QUERY_CONFIGS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

WORKFLOWS_USAGE_VIEW_NAME = "workflows_usage"

WORKFLOWS_USAGE_DESCRIPTION = """
This view is the source for the Workflows Usage dashboard in Looker.
It includes relevant front-end events from the Workflows product.
"""

WORKFLOWS_OFFICER_EVENT_NAME_SET = {
    config.officer_event_name.value for config in WORKFLOWS_OFFICER_EVENT_QUERY_CONFIGS
}


WORKFLOWS_USAGE_QUERY_TEMPLATE = """
SELECT
    state_code,
    officer_id AS officer_external_id,
    user_full_name_clean AS officer_full_name_clean,
    email,
    opportunity_type,
    DATE(event_ts) AS event_date,
    event,
    event_type,
    person_external_id,
    new_status,
    event_ts,
    MIN(DATE(event_ts)) OVER (PARTITION BY state_code, opportunity_type, email) AS officer_first_workflow_event_date,
    MAX(DATE(event_ts)) OVER () AS max_event_date,
    ROW_NUMBER() OVER (PARTITION BY state_code, email, opportunity_type, event_type, person_external_id ORDER BY event_ts DESC) = 1 AS latest_within_event_type
FROM `{project_id}.{analyst_views_dataset}.workflows_officer_events_materialized`
LEFT JOIN `{project_id}.{workflows_views_dataset}.reidentified_dashboard_users_materialized` 
    USING (state_code, email)
"""

WORKFLOWS_USAGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=WORKFLOWS_USAGE_VIEW_NAME,
    view_query_template=WORKFLOWS_USAGE_QUERY_TEMPLATE,
    description=WORKFLOWS_USAGE_DESCRIPTION,
    should_materialize=True,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_USAGE_VIEW_BUILDER.build_and_print()
