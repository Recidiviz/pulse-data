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
"""Creates the view for all Workflows-specific events at the person-level"""
from typing import Dict

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.models.event_selector import (
    EventSelector,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#28210): Refactor this view to use a view more upstream of `officer_events`

# A dictionary that maps each usage status to a set of Event conditions
USAGE_EVENTS_DICT: Dict[str, EventSelector] = {
    "MARKED_INELIGIBLE": EventSelector(
        event_type=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
        event_conditions_dict={
            "event_type": ["CLIENT_REFERRAL_STATUS_UPDATED"],
            "new_status": ["DENIED"],
        },
    ),
    "SURFACED": EventSelector(
        event_type=EventType.WORKFLOWS_USER_ACTION,
        event_conditions_dict={
            "event_type": ["CLIENT_SURFACED"],
        },
    ),
    "VIEWED": EventSelector(
        event_type=EventType.WORKFLOWS_USER_PAGE,
        event_conditions_dict={
            "event_type": ["OPPORTUNITY_PREVIEWED"],
        },
    ),
    "FORM_VIEWED": EventSelector(
        event_type=EventType.WORKFLOWS_USER_PAGE,
        event_conditions_dict={
            "event_type": ["FORM_VIEWED"],
        },
    ),
    "FORM_STARTED": EventSelector(
        event_type=EventType.WORKFLOWS_USER_ACTION,
        event_conditions_dict={
            "event_type": ["FORM_FIRST_EDITED", "FORM_EDITED"],
        },
    ),
    "FORM_DOWNLOADED": EventSelector(
        event_type=EventType.WORKFLOWS_USER_ACTION,
        event_conditions_dict={
            "event_type": ["FORM_DOWNLOADED", "FORM_COPIED"],
        },
    ),
    "FORM_SUBMITTED": EventSelector(
        event_type=EventType.WORKFLOWS_USER_ACTION,
        event_conditions_dict={
            "event_type": ["FORM_SUBMITTED"],
        },
    ),
}


def get_usage_status_case_statement(
    usage_event_selectors: Dict[str, EventSelector]
) -> str:
    return "\n".join(
        [
            f"""
        WHEN {v.generate_event_conditions_query_fragment(filter_by_event_type=True)}
        THEN "{k}"
    """
            for k, v in usage_event_selectors.items()
        ]
    )


WORKFLOWS_PERSON_EVENTS_VIEW_NAME = "workflows_person_events"

WORKFLOWS_PERSON_EVENTS_VIEW_DESCRIPTION = (
    "View of all Workflows-specific usage events that are associated with a person id"
)

WORKFLOWS_PERSON_EVENTS_QUERY_TEMPLATE = f"""
WITH usage_events AS (
    SELECT
        a.state_code,
        b.person_id,
        c.completion_event_type AS task_type,
        a.event_date AS start_date,
        CASE
            {get_usage_status_case_statement(USAGE_EVENTS_DICT)}
        END AS usage_event_type,
        event_attributes
    FROM
        `{{project_id}}.analyst_data.workflows_user_events_materialized` a
    INNER JOIN
        `{{project_id}}.normalized_state.state_person_external_id` b
    ON
        JSON_EXTRACT_SCALAR(a.event_attributes, "$.person_external_id") = b.external_id
        AND a.state_code = b.state_code
    INNER JOIN
        `{{project_id}}.reference_views.workflows_opportunity_configs_materialized` c
    ON
        JSON_EXTRACT_SCALAR(a.event_attributes, "$.opportunity_type") = c.opportunity_type
)
-- Only include events that have been configured
SELECT * FROM usage_events WHERE usage_event_type IS NOT NULL
"""


WORKFLOWS_PERSON_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=WORKFLOWS_PERSON_EVENTS_VIEW_NAME,
    description=WORKFLOWS_PERSON_EVENTS_VIEW_DESCRIPTION,
    view_query_template=WORKFLOWS_PERSON_EVENTS_QUERY_TEMPLATE,
    should_materialize=True,
    clustering_fields=["state_code", "person_id", "task_type"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_PERSON_EVENTS_VIEW_BUILDER.build_and_print()
