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
"""Sessionized view of eligibility and Workflows tool engagement at the person-level"""

from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
    list_to_query_string,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    USAGE_EVENTS_DICT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_NAME = (
    "workflows_person_impact_funnel_status_sessions"
)

WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_DESCRIPTION = (
    "Sessionized view of eligibility and "
    "Workflows tool engagement at the person-level"
)

# Get the subset of usage events that occur as discrete events rather than spans
# This exclude MARKED_INELIGIBLE/snooze span starts, since those can be handled
# directly via client snooze spans.
DISCRETE_USAGE_EVENTS_DICT = {
    event: selector
    for event, selector in USAGE_EVENTS_DICT.items()
    if event != "MARKED_INELIGIBLE"
}

# TODO(#35954): Replace this with a generalized funnel status sessions function
def generate_workflows_person_impact_funnel_status_sessions(
    usage_event_names: List[str],
) -> str:
    """Returns query template for a view containing boolean flags describing eligibility and tool adoption information
    for all Workflows task types. Takes as input a dictionary that maps workflows usage statuses to EventSelectors
    for the conditions applied to each Event object."""

    # Query fragment that checks for the existence of a usage status for a given span.
    # Used to dedup sub sessions and generate boolean flags.
    usage_status_dedup_query_fragment = ",\n".join(
        [
            f"""COUNTIF(usage_event_type = "{k}") > 0 AS {k.lower()}"""
            for k in usage_event_names
        ]
    )

    all_attributes = [
        "is_justice_involved",
        "is_eligible",
        "is_almost_eligible",
        "is_surfaceable",
        "task_completed",
        "denial_reasons",
    ] + [k.lower() for k in usage_event_names]

    query_template = f"""
# Get spans of eligibility status
WITH distinct_task_types AS (
    SELECT DISTINCT
        state_code,
        task_type,
    FROM
        `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized`
)
,
# Get spans of justice involvement among states, and unnest across all task types live in a given state
system_sessions AS (
    SELECT * 
    FROM
        `{{project_id}}.sessions.system_sessions_materialized`
    INNER JOIN
        distinct_task_types
    USING
        (state_code)
)
,
# Construct task completion spans that begin on the task completion date and end on the nearest
# subsequent start of either an eligible or almost eligible span, or the start of a new system session.
task_completion_sessions AS (
    SELECT
        a.state_code,
        a.person_id,
        a.completion_event_type AS task_type,
        a.completion_event_date AS start_date,
        {revert_nonnull_end_date_clause(
        f'''
            LEAST(
                MIN({nonnull_end_date_clause("b.start_date")}), 
                MIN({nonnull_end_date_clause("c.start_date")})
            )'''
        )} AS end_date
    FROM
        `{{project_id}}.task_eligibility.all_completion_events_materialized` a
    LEFT JOIN
        `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized` b
    ON
        a.person_id = b.person_id
        AND b.start_date > a.completion_event_date
        AND a.completion_event_type = b.task_type
        AND b.eligibility_reset
        -- Exclude future eligibility span start dates
        AND b.start_date <= CURRENT_DATE("US/Eastern")
    LEFT JOIN
        system_sessions c
    ON
        a.person_id = c.person_id
        AND c.start_date > a.completion_event_date
        AND a.completion_event_type = c.task_type
    WHERE a.completion_event_date <= CURRENT_DATE("US/Eastern")
    GROUP BY 1, 2, 3, 4
)
,
# Get spans over which someone qualified for a certain usage status
# First, identify the set of possible events that could end a usage status span
usage_status_end_dates AS (
    # End all usage status spans when someone's eligibility status becomes newly eligible
    SELECT
        person_id,
        task_type,
        start_date AS end_date,
        usage_event_type,
    FROM
        `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized`,
        UNNEST([{list_to_query_string(list(DISCRETE_USAGE_EVENTS_DICT), quoted=True)}]) AS usage_event_type
    WHERE
        eligibility_reset
        -- Exclude future eligibility spans
        AND start_date <= CURRENT_DATE("US/Eastern")

    UNION ALL

    # End all usage status spans when someone begins a new session in the criminal justice system
    SELECT
        person_id,
        task_type,
        start_date AS end_date,
        usage_event_type,
    FROM
        system_sessions,
    UNNEST([{list_to_query_string(list(DISCRETE_USAGE_EVENTS_DICT), quoted=True)}]) AS usage_event_type
)
,
# Join each person-level usage event to the closest subsequent end date to construct usage sessions
usage_status_sessions AS (
    SELECT
        a.*,
        b.end_date,
    FROM
        `{{project_id}}.analyst_data.workflows_person_events_materialized` a
    LEFT JOIN
        usage_status_end_dates b
    ON
        a.person_id = b.person_id
        AND a.task_type = b.task_type
        AND a.usage_event_type = b.usage_event_type
        AND b.end_date > a.start_date
    WHERE
        a.usage_event_type IN ({list_to_query_string(list(DISCRETE_USAGE_EVENTS_DICT), quoted=True)})
    # Use the first end date following the event as the end date of the session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, task_type, usage_event_type, start_date 
        ORDER BY COALESCE(end_date, "9999-01-01")
    ) = 1
)
,
marked_ineligible_sessions AS (
    SELECT
        a.* EXCEPT (end_date_exclusive),
        # Takes the earliest end date between the snooze span end and the usage reset dates
        # This ensures that we close out snooze spans when a client moves out of the justice 
        # system or becomes newly eligible or almost eligible for an opportunity
        {revert_nonnull_end_date_clause(f"LEAST({nonnull_end_date_clause('a.end_date_exclusive')}, {nonnull_end_date_clause('b.end_date')})")} AS end_date_exclusive,
    FROM
        `{{project_id}}.analyst_data.all_task_type_marked_ineligible_spans_materialized` a
    LEFT JOIN
        usage_status_end_dates b
    ON
        a.person_id = b.person_id
        AND a.task_type = b.task_type
        AND {nonnull_end_date_clause("b.end_date")} > a.start_date
    # Use the first end date following the event as the end date of the session
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, task_type, start_date 
        ORDER BY {nonnull_end_date_clause("b.end_date")}
    ) = 1
)
,
surfaceable_sessions AS (
    SELECT
        state_code,
        person_id,
        b.completion_event_type AS task_type,
        start_date,
        end_date_exclusive,
    FROM
        `{{project_id}}.analyst_data.workflows_record_archive_surfaceable_person_sessions_materialized` a
    INNER JOIN
        `{{project_id}}.reference_views.workflows_opportunity_configs_materialized` b
    USING
        (state_code, opportunity_type)
)
,
all_sessions AS (
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date_exclusive AS end_date,
        TRUE AS is_justice_involved,
        NULL AS is_eligible,
        NULL AS is_almost_eligible,
        NULL AS is_surfaceable,
        CAST(NULL AS STRING) AS usage_event_type,
        NULL AS task_completed,
        NULL AS denial_reasons,
    FROM system_sessions
    UNION ALL
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        -- Convert any future end dates to NULL
        CASE WHEN end_date > CURRENT_DATE("US/Eastern") THEN NULL ELSE end_date END AS end_date,
        NULL AS is_justice_involved,
        is_eligible,
        is_almost_eligible,
        NULL AS is_surfaceable,
        CAST(NULL AS STRING) AS usage_event_type,
        NULL AS task_completed,
        NULL AS denial_reasons,
    FROM `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized` eligibility_sessions
    WHERE start_date <= CURRENT_DATE("US/Eastern")
    UNION ALL
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date,
        NULL AS is_justice_involved,
        NULL AS is_eligible,
        NULL AS is_almost_eligible,
        NULL AS is_surfaceable,
        usage_event_type,
        NULL AS task_completed,
        NULL AS denial_reasons,
    FROM usage_status_sessions
    UNION ALL
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        CASE WHEN end_date_exclusive > CURRENT_DATE("US/Eastern") THEN NULL ELSE end_date_exclusive END AS end_date,
        NULL AS is_justice_involved,
        NULL AS is_eligible,
        NULL AS is_almost_eligible,
        NULL AS is_surfaceable,
        "MARKED_INELIGIBLE" AS usage_event_type,
        NULL AS task_completed,
        denial_reasons,
    FROM marked_ineligible_sessions
    UNION ALL
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date,
        NULL AS is_justice_involved,
        NULL AS is_eligible,
        NULL AS is_almost_eligible,
        NULL AS is_surfaceable,
        CAST(NULL AS STRING) AS usage_event_type,
        TRUE AS task_completed,
        NULL AS denial_reasons,
    FROM task_completion_sessions
    UNION ALL
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date_exclusive AS end_date,
        NULL AS is_justice_involved,
        NULL AS is_eligible,
        NULL AS is_almost_eligible,
        TRUE AS is_surfaceable,
        CAST(NULL AS STRING) AS usage_event_type,
        NULL AS task_completed,
        NULL AS denial_reasons,
    FROM surfaceable_sessions
)
,
{create_sub_sessions_with_attributes("all_sessions", index_columns=["state_code", "person_id", "task_type"])}
,
# Deduplicate across all sub sessions to generate a single span of time with boolean flags for each status.
# The `usage_status_dedup_query_fragment` converts overlapping spans with string `usage_event_type`
# into a single span with a series of boolean flags, one flag per status type.

# For example, if there are three overlapping spans for the same start/end date, one with each of the following 
# usage statuses: ["MARKED_INELIGIBLE", "SURFACED", "VIEWED"], then the output would be a single span containing the 
# following usage flags: marked_ineligible = True, surfaced = True, viewed = True, and all other usage flags marked 
# False.

sub_sessions_dedup AS (
    SELECT
        state_code,
        person_id,
        task_type,
        start_date,
        end_date,
        LOGICAL_OR(COALESCE(is_justice_involved, FALSE)) AS is_justice_involved,
        LOGICAL_OR(COALESCE(is_eligible, FALSE)) AS is_eligible,
        LOGICAL_OR(COALESCE(is_almost_eligible, FALSE)) AS is_almost_eligible,
        LOGICAL_OR(COALESCE(is_surfaceable, FALSE)) AS is_surfaceable,
        {usage_status_dedup_query_fragment},
        LOGICAL_OR(COALESCE(task_completed, FALSE)) AS task_completed,
        CASE WHEN COUNTIF(usage_event_type = "MARKED_INELIGIBLE") > 0 THEN MAX(denial_reasons) END AS denial_reasons,
    FROM
        sub_sessions_with_attributes
    GROUP BY 1, 2, 3, 4, 5
)
{aggregate_adjacent_spans(
        "sub_sessions_dedup", 
        index_columns=["state_code", "person_id", "task_type"],
        attribute=all_attributes
    )}
"""
    return query_template


WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_NAME,
    view_query_template=generate_workflows_person_impact_funnel_status_sessions(
        list(USAGE_EVENTS_DICT.keys())
    ),
    description=WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.build_and_print()
