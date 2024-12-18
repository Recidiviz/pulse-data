#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
"""Creates the view for all Workflows-specific officer events"""
from typing import List, Optional

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    PULSE_DASHBOARD_SEGMENT_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

WORKFLOWS_OFFICER_EVENTS_VIEW_NAME = "workflows_officer_events"

WORKFLOWS_OFFICER_EVENTS_DESCRIPTION = """
Workflows-specific officer events from the front-end, used to power the Workflows Usage Metrics dashboard
"""


@attr.s
class WorkflowsOfficerEventQueryConfig:
    # The name of the source table for this officer event
    table_name: str = attr.ib()

    # The general officer event name
    officer_event_name: EventType = attr.ib()

    # The workflows-specific event type
    workflows_event_type: str = attr.ib()

    # True if the event has a person_external_id specific to the event, False otherwise.
    has_person_external_id: bool = attr.ib()

    # True if the event has a opportunity_type specific to the event, False otherwise.
    has_opportunity_type: bool = attr.ib()

    # True if the event has a status specific to the event, False otherwise.
    has_status: bool = attr.ib()

    # True if the context_page is needed from the event, which is then used to infer opportunity_type
    # The raw context_page_path value is formatted like "/workflows/compliantReporting/<person_id>",
    # so the expression "SPLIT(SUBSTR(context_page_path, 12), '/')[ORDINAL(1)]" for context_page
    # gets the substring starting after the second "/", i.e. "compliantReporting"
    should_get_context_page: bool = attr.ib()

    # Additional columns to dedup the events by, should be ordered!
    additional_dedup_cols: Optional[List[str]] = attr.ib(default=None)


WORKFLOWS_OFFICER_EVENT_QUERY_CONFIGS = [
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_profile_viewed",
        officer_event_name=EventType.WORKFLOWS_USER_PAGE,
        workflows_event_type="PROFILE_VIEWED",
        has_person_external_id=True,
        has_opportunity_type=False,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_opportunity_previewed",
        officer_event_name=EventType.WORKFLOWS_USER_PAGE,
        workflows_event_type="OPPORTUNITY_PREVIEWED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_form_viewed",
        officer_event_name=EventType.WORKFLOWS_USER_PAGE,
        workflows_event_type="FORM_VIEWED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_form_copied",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="FORM_COPIED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_form_printed",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="FORM_PRINTED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_form_first_edited",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="FORM_FIRST_EDITED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_status_updated",
        officer_event_name=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
        workflows_event_type="CLIENT_REFERRAL_STATUS_UPDATED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=True,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_form_submitted",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="FORM_SUBMITTED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_surfaced",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="CLIENT_SURFACED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="frontend_caseload_search",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="SEARCH_BAR_USED",
        has_person_external_id=False,
        has_opportunity_type=False,
        has_status=False,
        should_get_context_page=True,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_form_edited",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="FORM_EDITED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_referral_form_downloaded",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="FORM_DOWNLOADED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_milestones_congratulations_sent",
        officer_event_name=EventType.WORKFLOWS_USER_ACTION,
        workflows_event_type="MILESTONES_CONGRATULATIONS_SENT",
        has_person_external_id=True,
        has_opportunity_type=False,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="pages",
        officer_event_name=EventType.WORKFLOWS_USER_PAGE,
        workflows_event_type="WORKFLOWS_PAGE_VIEWED",
        has_person_external_id=False,
        has_opportunity_type=False,
        has_status=False,
        should_get_context_page=True,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_opportunity_marked_submitted",
        officer_event_name=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
        workflows_event_type="OPPORTUNITY_MARKED_SUBMITTED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
    WorkflowsOfficerEventQueryConfig(
        table_name="clients_opportunity_unsubmitted",
        officer_event_name=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
        workflows_event_type="OPPORTUNITY_UNSUBMITTED",
        has_person_external_id=True,
        has_opportunity_type=True,
        has_status=False,
        should_get_context_page=False,
    ),
]


def workflows_officer_event_template(config: WorkflowsOfficerEventQueryConfig) -> str:
    """
    Helper for querying events logged from the Workflows front-end.
    """
    if not config.has_opportunity_type and not config.has_person_external_id:
        # If there is no opportunity and person associated with this event,
        # the event is not from a workflows_views.clients_ table, so get the raw event
        # and join on reidentified_dashboard_users to get user_external_id.
        table_source = f"""`{{project_id}}.{{workflows_segment_dataset}}.{config.table_name}`
INNER JOIN `{{project_id}}.{{workflows_views_dataset}}.reidentified_dashboard_users_materialized` 
    USING (user_id)
WHERE context_page_path LIKE '%workflows%' AND context_page_url LIKE '%://dashboard.recidiviz.org/%'"""
    else:
        table_source = (
            f"""`{{project_id}}.{{workflows_views_dataset}}.{config.table_name}`"""
        )

    cols_to_dedup_by = ["email"]
    # The order of the steps to construct cols_to_dedup_by matters, so that events are deduped
    # by email (officer), person_external_id (if applicable), opportunity_type (if applicable), then event date
    if config.has_person_external_id:
        cols_to_dedup_by.append("person_external_id")
    if config.has_opportunity_type:
        cols_to_dedup_by.append("opportunity_type")
    if config.additional_dedup_cols:
        cols_to_dedup_by.extend(config.additional_dedup_cols)

    cols_to_dedup_by.append("DATE(event_ts)")

    template = f"""
SELECT
    state_code,
    user_external_id AS officer_id,
    LOWER(email) AS email,
    "{config.officer_event_name.value}" AS event,
    DATETIME(timestamp, "US/Eastern") AS event_ts,
    "{config.workflows_event_type}" AS event_type, 
    { "" if config.has_person_external_id else "CAST(NULL AS STRING) AS "}person_external_id,
    { "" if config.has_opportunity_type else "CAST(NULL AS STRING) AS "}opportunity_type,
    { "status" if config.has_status else "CAST(NULL AS STRING)"} AS new_status,
    { "SPLIT(SUBSTR(context_page_path, 12), '/')[ORDINAL(1)]" 
        if config.should_get_context_page 
        else "CAST(NULL AS STRING)"
    } AS context_page
FROM {table_source}
QUALIFY ROW_NUMBER() OVER (PARTITION BY {list_to_query_string(cols_to_dedup_by)} ORDER BY timestamp DESC) = 1
    """
    return template


def get_all_workflows_officer_events() -> str:
    full_query = "\nUNION ALL\n".join(
        [
            workflows_officer_event_template(config)
            for config in WORKFLOWS_OFFICER_EVENT_QUERY_CONFIGS
        ]
    )
    return full_query


def get_case_when_path_then_opportunity_type_str() -> str:
    conditionals = "\n        ".join(
        [
            f"""WHEN state_code='{config.state_code.value}' AND context_page='{config.opportunity_type_path_str}' THEN '{config.opportunity_type}'"""
            for config in WORKFLOWS_OPPORTUNITY_CONFIGS
        ]
    )

    return f"""
    CASE  -- infer opportunity type from context_page if the path includes an opportunity-related substring
        WHEN opportunity_type is NOT null THEN opportunity_type
        {conditionals}
        -- Check US_ID state code for events that occurred pre-ATLAS migration
        WHEN state_code='US_ID' AND context_page IN ('pastFTRD', 'LSU', 'earnedDischarge') THEN context_page 
        WHEN state_code='US_ID' AND context_page='supervisionLevelMismatch' THEN 'usIdSupervisionLevelDowngrade' 
        ELSE null
    END AS opportunity_type,
    """


WORKFLOWS_OFFICER_EVENTS_QUERY_TEMPLATE = f"""
WITH events AS (
    {get_all_workflows_officer_events()}
), profile_viewed_events AS (
    -- The frontend event for PROFILE_VIEWED does not have a specific opportunity type, so
    -- join on person_record and apply this event to all opportunities the person is eligible for
    SELECT
        state_code, 
        events.officer_id,
        event,
        event_ts, 
        event_type,
        person_external_id,
        opportunity_type,
        new_status,
        context_page,
        email,
    FROM events
    INNER JOIN `{{project_id}}.{{workflows_views_dataset}}.person_record_materialized` USING (state_code, person_external_id), UNNEST(all_eligible_opportunities) AS opportunity_type
    WHERE event_type = "PROFILE_VIEWED"
)

SELECT 
    state_code,
    officer_id,
    event,
    event_ts, 
    event_type,
    person_external_id,
    {get_case_when_path_then_opportunity_type_str()}
    new_status,
    context_page,
    email,
FROM events
WHERE event_type != "PROFILE_VIEWED"

UNION ALL

SELECT 
    state_code, 
    officer_id,
    event,
    event_ts, 
    event_type,
    person_external_id,
    opportunity_type,
    new_status,
    context_page, 
    email,
FROM profile_viewed_events
"""


WORKFLOWS_OFFICER_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=WORKFLOWS_OFFICER_EVENTS_VIEW_NAME,
    view_query_template=WORKFLOWS_OFFICER_EVENTS_QUERY_TEMPLATE,
    description=WORKFLOWS_OFFICER_EVENTS_DESCRIPTION,
    should_materialize=True,
    clustering_fields=["state_code", "email"],
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
    workflows_segment_dataset=PULSE_DASHBOARD_SEGMENT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_OFFICER_EVENTS_VIEW_BUILDER.build_and_print()
