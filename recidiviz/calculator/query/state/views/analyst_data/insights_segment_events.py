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
"""Creates the view for all Insights-specific segment events"""
from typing import List, Optional

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INSIGHTS_SEGMENT_EVENTS_VIEW_NAME = "insights_segment_events"

INSIGHTS_SEGMENT_EVENTS_DESCRIPTION = """
INSIGHTS-specific events from the front-end, used to power the Insights Impact Metrics dashboard
"""


@attr.s
class InsightsSegmentEventQueryConfig:
    # The name of the source table for this event
    table_name: str = attr.ib()

    # The Insights-specific event type
    insights_event_type: str = attr.ib()

    # Additional columns to dedup the events by, should be ordered!
    additional_dedup_cols: Optional[List[str]] = attr.ib(default=None)


INSIGHTS_SEGMENT_EVENT_QUERY_CONFIGS = [
    InsightsSegmentEventQueryConfig(
        table_name="frontend_outliers_staff_page_viewed",
        insights_event_type="VIEWED_STAFF_PAGE",
    ),
    InsightsSegmentEventQueryConfig(
        table_name="frontend_outliers_staff_metric_viewed",
        insights_event_type="VIEWED_STAFF_METRIC",
    ),
    InsightsSegmentEventQueryConfig(
        table_name="frontend_outliers_page_viewed_30_seconds",
        insights_event_type="VIEWED_PAGE_30_SECONDS",
    ),
    InsightsSegmentEventQueryConfig(
        table_name="frontend_outliers_client_page_viewed",
        insights_event_type="VIEWED_CLIENT_PAGE",
    ),
    InsightsSegmentEventQueryConfig(
        table_name="frontend_outliers_action_strategy_surfaced",
        insights_event_type="SURFACED_ACTION_STRATEGY",
    ),
    InsightsSegmentEventQueryConfig(
        table_name="frontend_outliers_action_strategy_popup_viewed",
        insights_event_type="VIEWED_ACTION_STRATEGY_POPUP",
    ),
    InsightsSegmentEventQueryConfig(
        table_name="frontend_outliers_action_strategy_popup_viewed_10_seconds",
        insights_event_type="VIEWED_ACTION_STRATEGY_POPUP_10_SECONDS",
    ),
]


def insights_segment_event_template(config: InsightsSegmentEventQueryConfig) -> str:
    """
    Helper for querying events logged from the Insights front-end.
    """
    table_source = f"""`{{project_id}}.pulse_dashboard_segment_metrics.{config.table_name}`
INNER JOIN `{{project_id}}.workflows_views.reidentified_dashboard_users_materialized` 
    USING (user_id)
WHERE context_page_path LIKE '%insights%' AND context_page_url LIKE '%://dashboard.recidiviz.org/%'"""

    cols_to_dedup_by = ["email"]
    # The order of the steps to construct cols_to_dedup_by matters, so that events are deduped
    # by email (officer), person_external_id (if applicable), opportunity_type (if applicable), then event date
    if config.additional_dedup_cols:
        cols_to_dedup_by.extend(config.additional_dedup_cols)

    cols_to_dedup_by.append("DATE(event_ts)")

    template = f"""
SELECT
    state_code,
    user_external_id AS user_id,
    LOWER(email) AS email,
    "{config.insights_event_type}" AS event,
    DATETIME(timestamp, "US/Eastern") AS event_ts,
FROM {table_source}
QUALIFY ROW_NUMBER() OVER (PARTITION BY {list_to_query_string(cols_to_dedup_by)} ORDER BY timestamp DESC) = 1
    """
    return template


def get_all_insights_segment_events() -> str:
    full_query = "\nUNION ALL\n".join(
        [
            insights_segment_event_template(config)
            for config in INSIGHTS_SEGMENT_EVENT_QUERY_CONFIGS
        ]
    )
    return full_query


INSIGHTS_SEGMENT_EVENTS_QUERY_TEMPLATE = get_all_insights_segment_events()


INSIGHTS_SEGMENT_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=INSIGHTS_SEGMENT_EVENTS_VIEW_NAME,
    view_query_template=INSIGHTS_SEGMENT_EVENTS_QUERY_TEMPLATE,
    description=INSIGHTS_SEGMENT_EVENTS_DESCRIPTION,
    should_materialize=True,
    clustering_fields=["state_code", "email"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_SEGMENT_EVENTS_VIEW_BUILDER.build_and_print()
