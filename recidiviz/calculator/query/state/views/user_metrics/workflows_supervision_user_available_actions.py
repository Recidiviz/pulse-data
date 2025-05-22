# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Surface all the Workflows users with an actionable caseload currently visible in the tool:
- At least 1 client eligible & not marked ineligible/submitted for a fully launched opportunity
"""
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    DEDUPED_TASK_COMPLETION_EVENT_VB,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.types import WorkflowsSystemType

WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_VIEW_NAME = (
    "workflows_supervision_user_available_actions"
)

tasks = [
    b.task_type_name
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]

tasks = [
    b.task_type_name
    for b in DEDUPED_TASK_COMPLETION_EVENT_VB
    if b.completion_event_type.system_type == WorkflowsSystemType.SUPERVISION
]


def generate_task_sum_columns(metric_column_prefixes: list[str]) -> str:
    return ",\n        ".join(
        f"({ ' + '.join(f'IFNULL({prefix}_{task.lower()}, 0)' for prefix in metric_column_prefixes) }) AS combined_{task.lower()}"
        for task in tasks
    )


def generate_unpivoted_subquery(
    metric_column_prefixes: list[str], final_metric: str
) -> str:
    sum_columns = generate_task_sum_columns(metric_column_prefixes)
    unpivot_mappings = ",\n                ".join(
        f"combined_{task.lower()} AS '{task}'" for task in tasks
    )
    return f"""
    SELECT *
    FROM (
        SELECT
            state_code,
            workflows_user_email_address,
            {sum_columns}
        FROM selected_users
    )
    UNPIVOT (
        {final_metric} FOR task_completion_event IN (
            {unpivot_mappings}
        )
    )
    """


def generate_aggregated_cte_snippet(
    client_type: str, metric_column_prefixes: list[str]
) -> str:
    return f"""
    {client_type}_clients AS (
        SELECT
            workflows_user_email_address,
            {client_type}_tasks.task_completion_event as opportunity_name,
            CAST({client_type}_tasks.num_{client_type}_clients AS INT64) AS num_{client_type}_clients
        FROM (
            {generate_unpivoted_subquery(metric_column_prefixes, f'num_{client_type}_clients')}
        ) AS {client_type}_tasks
    ),
    aggregated_{client_type}_clients AS (
        SELECT
            workflows_user_email_address,
            ARRAY_AGG(STRUCT(opportunity_name, num_{client_type}_clients)
                ORDER BY opportunity_name
            ) AS {client_type}_clients_by_opportunity,
        FROM
            {client_type}_clients
        GROUP BY
            workflows_user_email_address
    )
    """


WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_QUERY_TEMPLATE = f"""
WITH selected_users AS (
    SELECT
        users.workflows_user_email_address,
        IFNULL(users.location_name, users.location_id) AS location_name,
        (
            IFNULL(metrics.workflows_distinct_people_eligible_and_actionable, 0)
            + IFNULL(metrics.workflows_distinct_people_almost_eligible_and_actionable, 0)
        ) AS total_opportunities,
        IFNULL(metrics.workflows_distinct_people_eligible_and_actionable, 0) AS eligible_opportunities,
        IFNULL(metrics.workflows_distinct_people_almost_eligible_and_actionable, 0) AS almost_eligible_opportunities,
        metrics.*
    FROM
        `{{project_id}}.user_metrics.workflows__supervision_officer_aggregated_metrics_materialized` metrics
    INNER JOIN
        `{{project_id}}.analyst_data.workflows_provisioned_user_registration_sessions_materialized` users
    ON
        users.state_code = metrics.state_code
        AND users.staff_external_id = metrics.officer_id
        AND users.end_date_exclusive IS NULL
    WHERE
        # Pull the current day metrics representing the latest data
        metrics.period = "CURRENT_DAY"

        # Only include users with available Workflows opportunities
        AND (
            IFNULL(metrics.workflows_distinct_people_eligible_and_actionable, 0)
            + IFNULL(metrics.workflows_distinct_people_almost_eligible_and_actionable, 0)
        ) > 0

        # Do not include users that have access to Insights and Workflows
        # since the Insights users are included in a different view
        AND IFNULL(metrics.distinct_provisioned_insights_users, 0) != 1
),
{
    generate_aggregated_cte_snippet(
        client_type='total', 
        metric_column_prefixes=[
            'workflows_distinct_people_eligible_and_actionable', 
            'workflows_distinct_people_almost_eligible_and_actionable'
        ]
    )
},
{
    generate_aggregated_cte_snippet(
        client_type='urgent', 
        metric_column_prefixes=['avg_daily_population_task_eligible_and_unviewed_30_days']
    )
}
SELECT
    su.state_code,
    su.officer_id,
    su.officer_name,
    su.workflows_user_email_address,
    su.location_name,
    su.total_opportunities,
    su.eligible_opportunities,
    su.almost_eligible_opportunities,
    tc.total_clients_by_opportunity,
    uc.urgent_clients_by_opportunity
FROM
    selected_users su
INNER JOIN
    aggregated_total_clients tc
USING(workflows_user_email_address)
INNER JOIN
    aggregated_urgent_clients uc
USING(workflows_user_email_address)
"""

WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.USER_METRICS_DATASET_ID,
    view_id=WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_VIEW_NAME,
    view_query_template=WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_QUERY_TEMPLATE,
    description=__doc__,
    clustering_fields=["state_code"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_PRODUCTION):
        WORKFLOWS_SUPERVISION_USER_AVAILABLE_ACTIONS_VIEW_BUILDER.build_and_print()
