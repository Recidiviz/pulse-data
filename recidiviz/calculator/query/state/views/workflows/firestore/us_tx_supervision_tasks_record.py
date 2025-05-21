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
"""Query for supervision tasks that are either overdue or upcoming within 30 days for Texas"""
from typing import List, NotRequired, TypedDict

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TX_SUPERVISION_TASKS_RECORD_VIEW_NAME = "us_tx_supervision_tasks_record"

US_TX_SUPERVISION_TASKS_RECORD_DESCRIPTION = """
    View containing tasks that are either overdue or due within the next 30 days for each client on supervision in
    US_TX.
"""

"""This defines the views that will be used to generate tasks in TX as well as any task-specific information
for the query, such as the task type and where the due date is stored.
"""


class TaskConfig(TypedDict):
    type: str
    table: str
    due_date_field: str
    end_of_span_date_field: NotRequired[str]
    meets_criteria_means_action_required: NotRequired[bool]


TASK_CONFIGS: List[TaskConfig] = [
    {
        "type": "usTxAssessment",
        "table": "needs_risk_assessment_materialized",
        "due_date_field": "due_assessment_date",
        "end_of_span_date_field": "DATE(JSON_EXTRACT_SCALAR(reason_v2, '$.due_assessment_date'))",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxFieldContactScheduled",
        "table": "needs_scheduled_field_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxFieldContactUnscheduled",
        "table": "needs_unscheduled_field_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxHomeContactScheduled",
        "table": "needs_scheduled_home_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxHomeContactUnscheduled",
        "table": "needs_unscheduled_home_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxElectronicContactScheduled",
        "table": "needs_scheduled_electronic_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxHomeContactEdgeCase",
        "table": "meets_edge_case_home_contact_standards_materialized",
        "due_date_field": "contact_due_date",
    },
    {
        "type": "usTxCollateralContactScheduled",
        "table": "needs_scheduled_collateral_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxOfficeContactScheduled",
        "table": "needs_scheduled_office_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxTypeAgnosticContact",
        "table": "needs_type_agnostic_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxTypeAgnosticContact",
        "table": "needs_type_agnostic_contact_standard_policy_secondary_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
    {
        "type": "usTxInCustodyContact",
        "table": "meets_weekly_in_custody_contact_standards_materialized",
        "due_date_field": "contact_due_date",
    },
    {
        "type": "usTxElectronicOrOfficeContact",
        "table": "needs_scheduled_electronic_or_office_contact_materialized",
        "due_date_field": "contact_due_date",
        "meets_criteria_means_action_required": True,
    },
]


def generate_query_from_config(config: TaskConfig) -> str:
    due_task_clause = f"""
        COALESCE({config.get("end_of_span_date_field", "end_date")}, '9999-09-09') <= LAST_DAY(DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH))
    """

    if not config.get("meets_criteria_means_action_required", False):
        due_task_clause += """
            AND NOT meets_criteria
        """
    else:
        due_task_clause += """
            AND meets_criteria
        """

    return f"""
        SELECT
            person_id,
            state_code,
            '{config['type']}' AS type,
            TO_JSON(STRUCT(
                '{config['type']}' AS type,
                JSON_EXTRACT(reason_v2, '$.{config['due_date_field']}') AS dueDate,
                reason_v2 AS details
            )) AS task,
        FROM `{{project_id}}.task_eligibility_criteria_us_tx.{config['table']}`
        WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND COALESCE(end_date, '9999-09-09')
            AND state_code = "US_TX"
            AND {due_task_clause}
    """


US_TX_SUPERVISION_TASKS_RECORD_QUERY_TEMPLATE = f"""
    WITH tasks AS (
        { ' UNION ALL '.join(generate_query_from_config(config) for config in TASK_CONFIGS) }
    ), grouped_tasks AS (
        SELECT
            person_id,
            state_code,
            ARRAY_AGG(task ORDER BY type) as tasks,
        FROM tasks
        GROUP BY person_id, state_code
    )

    SELECT
        person_external_id,
        officer_id,
        state_code,
        tasks,
    FROM grouped_tasks
    LEFT JOIN `{{project_id}}.{{workflows_views}}.person_id_to_external_id_materialized`
    USING (state_code, person_id)
    INNER JOIN `{{project_id}}.{{workflows_views}}.client_record_materialized`
    USING (state_code, person_external_id)
    """

US_TX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TX_SUPERVISION_TASKS_RECORD_VIEW_NAME,
    view_query_template=US_TX_SUPERVISION_TASKS_RECORD_QUERY_TEMPLATE,
    description=US_TX_SUPERVISION_TASKS_RECORD_DESCRIPTION,
    workflows_views=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER.build_and_print()
