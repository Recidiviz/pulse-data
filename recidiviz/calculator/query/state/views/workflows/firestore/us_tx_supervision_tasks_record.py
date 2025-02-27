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

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TX_SUPERVISION_TASKS_RECORD_VIEW_NAME = "us_tx_supervision_tasks_record"

US_TX_SUPERVISION_TASKS_RECORD_DESCRIPTION = """
    View containing tasks that are either overdue or due within the next 30 days for each client on supervision in
    US_TX.
"""

"""This defined a mapping between the raw criteria to the task type. Only criteria in this dictionary will be
exported as a task."""
CRITERIA_TO_TASK_TYPE = {
    "US_TX_MEETS_RISK_ASSESSMENT_STANDARDS": "usTxAssessment",
    "US_TX_MEETS_SCHEDULED_FIELD_CONTACT_STANDARDS": "usTxFieldContact",
    "US_TX_MEETS_UNSCHEDULED_FIELD_CONTACT_STANDARDS": "usTxFieldContact",
    "US_TX_MEETS_SCHEDULED_HOME_CONTACT_STANDARDS": "usTxHomeContact",
    "US_TX_MEETS_UNSCHEDULED_HOME_CONTACT_STANDARDS": "usTxHomeContact",
    "US_TX_MEETS_SCHEDULED_ELECTRONIC_CONTACT_STANDARDS": "usTxElectronicContact",
}

CRITERIA_TO_TYPE_QUERY_FRAGMENT = f"""
    CASE criteria_name
        {' '.join( f"WHEN '{criteria}' THEN '{type}'" for (criteria, type) in CRITERIA_TO_TASK_TYPE.items() )}
    END
"""

# TODO(#38852): Store task due date in common field
US_TX_SUPERVISION_TASKS_RECORD_QUERY_TEMPLATE = f"""
    WITH tasks AS (
        SELECT
            person_id,
            state_code,
            JSON_OBJECT('type', {CRITERIA_TO_TYPE_QUERY_FRAGMENT}, 'dueDate', JSON_EXTRACT(reason_v2, '$.contact_due_date'), 'details', reason_v2) AS task,
        FROM `{{project_id}}.task_eligibility_criteria_us_tx.all_state_specific_criteria_materialized`
        WHERE CURRENT_DATE('US/Eastern') BETWEEN start_date AND IFNULL(end_date, '9999-09-09')
            AND state_code = "US_TX"
            AND NOT meets_criteria
            AND criteria_name IN UNNEST({list(CRITERIA_TO_TASK_TYPE.keys())})
    ), grouped_tasks AS (
        SELECT
            person_id,
            state_code,
            ARRAY_AGG(task) as tasks,
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
