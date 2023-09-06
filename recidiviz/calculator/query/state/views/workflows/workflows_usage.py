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
from recidiviz.calculator.query.bq_utils import list_to_query_string
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


WORKFLOWS_USAGE_QUERY_TEMPLATE = f"""
WITH workflows_events AS (
    SELECT
        * EXCEPT (event_date, event_attributes, officer_id),
        officer_id AS officer_external_id,
        event_date as event_ts,
        DATE(event_date) AS event_date,
        JSON_EXTRACT_SCALAR(event_attributes, '$.opportunity_type') AS opportunity_type,
        JSON_EXTRACT_SCALAR(event_attributes, '$.person_external_id') AS person_external_id,
        JSON_EXTRACT_SCALAR(event_attributes, '$.event_type') AS event_type,
        JSON_EXTRACT_SCALAR(event_attributes, '$.new_status') AS new_status
    FROM `{{project_id}}.{{analyst_views_dataset}}.officer_events_materialized`
    WHERE event IN ({list_to_query_string(list(WORKFLOWS_OFFICER_EVENT_NAME_SET), quoted=True)})
)
SELECT
    state_code,
    officer_external_id,
    opportunity_type,
    event_date,
    event,
    event_type,
    person_external_id,
    new_status,
    event_ts,
    MIN(event_date) OVER (PARTITION BY state_code, opportunity_type, officer_external_id) AS officer_first_workflow_event_date,
    MAX(event_date) OVER () AS max_event_date,
    ROW_NUMBER() OVER (PARTITION BY state_code, officer_external_id, workflows_events.opportunity_type, event_type, person_external_id ORDER BY event_ts DESC) = 1 AS latest_within_event_type
FROM workflows_events
"""

WORKFLOWS_USAGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=WORKFLOWS_USAGE_VIEW_NAME,
    view_query_template=WORKFLOWS_USAGE_QUERY_TEMPLATE,
    description=WORKFLOWS_USAGE_DESCRIPTION,
    should_materialize=True,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_USAGE_VIEW_BUILDER.build_and_print()
