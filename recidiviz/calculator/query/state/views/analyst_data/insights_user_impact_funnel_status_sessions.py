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
"""
Sessionized view representing spans of time where a supervisor has surfaceable
officer outlier officers, along with tool usage.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.insights_segment_events import (
    INSIGHTS_SEGMENT_EVENT_QUERY_CONFIGS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "insights_user_impact_funnel_status_sessions"

_VIEW_DESCRIPTION = """
Sessionized view representing spans of time where a supervisor has surfaceable
officer outlier officers, along with tool usage.
"""

INSIGHTS_USAGE_EVENTS: list[str] = [
    event_config.insights_event_type
    for event_config in INSIGHTS_SEGMENT_EVENT_QUERY_CONFIGS
]

# TODO(#35954): Replace this with a generalized funnel status sessions function
def generate_insights_person_impact_funnel_status_sessions(
    usage_event_names: list[str],
) -> str:
    """Returns a query template that combines information about outlier status
    with the inputted set of usage statuses to create a unified funnel view
    of product usage."""
    # Query fragment that checks for the existence of a usage status for a given span.
    # Used to dedup sub sessions and generate boolean flags.
    usage_status_dedup_query_fragment = ",\n".join(
        [
            f"""COUNTIF(usage_event_type = "{k}") > 0 AS {k.lower()}"""
            for k in usage_event_names
        ]
    )

    all_attributes = [
        "has_outlier_officers",
        "num_outlier_officers",
    ] + [k.lower() for k in usage_event_names]

    query_template = f"""
WITH surfaceable_outlier_sessions AS (
    SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        num_outlier_officers > 0 AS has_outlier_officers,
        num_outlier_officers,
    FROM
        `{{project_id}}.analyst_data.insights_supervisor_outlier_status_archive_sessions_materialized`
)
,
usage_sessions AS (
    SELECT
        state_code,
        email AS email_address,
        event AS usage_event_type,
        event_ts AS start_date,
        -- Close out usage span at the end of the month
        DATE_ADD(DATE_TRUNC(event_ts, MONTH), INTERVAL 1 MONTH) AS end_date_exclusive,
    FROM
        `{{project_id}}.analyst_data.insights_segment_events_materialized`
)
,
all_sessions AS (
    SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        has_outlier_officers,
        num_outlier_officers,
        CAST(NULL AS STRING) AS usage_event_type,
    FROM
        surfaceable_outlier_sessions

    UNION ALL

    
    SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        FALSE AS has_outlier_officers,
        CAST(NULL AS INT64) AS num_outlier_officers,
        usage_event_type,
    FROM
        usage_sessions
)
,
{create_sub_sessions_with_attributes("all_sessions", index_columns=["state_code", "email_address"], end_date_field_name="end_date_exclusive")}
,
sub_sessions_dedup AS (
    SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        LOGICAL_OR(has_outlier_officers) AS has_outlier_officers,
        MAX(num_outlier_officers) AS num_outlier_officers,
        {usage_status_dedup_query_fragment},
    FROM
        sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4
)
{aggregate_adjacent_spans(
    "sub_sessions_dedup", 
    index_columns=["state_code", "email_address"],
    attribute=all_attributes,
    end_date_field_name="end_date_exclusive"
)}
"""
    return query_template


INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_VIEW_DESCRIPTION,
    view_query_template=generate_insights_person_impact_funnel_status_sessions(
        INSIGHTS_USAGE_EVENTS
    ),
    clustering_fields=["state_code", "email_address"],
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.build_and_print()
