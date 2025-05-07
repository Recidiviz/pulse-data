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

"""Query for supervision tasks that are either overdue or due within the next 30 days for North Dakota (US_ND)."""

from typing import List

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_SUPERVISION_TASKS_RECORD_VIEW_NAME = "us_nd_supervision_tasks_record"
US_ND_SUPERVISION_TASKS_RECORD_DESCRIPTION = """
    View containing tasks that are either overdue or due within the next 30 days
    for each client on supervision in US_ND.
"""

# ————————————————————————————————————————————————————————————————
# Configurable constants
# ————————————————————————————————————————————————————————————————
METRICS_TABLE = (
    "`{project_id}.{dataflow_metrics_materialized}."
    "most_recent_supervision_case_compliance_metrics_materialized`"
)


@attr.s
class UsNdSupervisionTaskQueryConfig:
    """Config for one task type: which due_date column to use and how to structure details."""

    due_date_column: str = attr.ib()
    task_details_struct: str = attr.ib()


# Only add to this list to support a new task type
SUPERVISION_TASK_CONFIGS: List[UsNdSupervisionTaskQueryConfig] = [
    UsNdSupervisionTaskQueryConfig(
        due_date_column="next_recommended_face_to_face_date",
        task_details_struct="""STRUCT(
            'contact' AS type,
            MAX(next_recommended_face_to_face_date) AS due_date,
               STRUCT() AS details
        )""",
    ),
    UsNdSupervisionTaskQueryConfig(
        due_date_column="next_recommended_assessment_date",
        task_details_struct="""STRUCT(
            'assessment' AS type,
            MAX(next_recommended_assessment_date) AS due_date,
               STRUCT() AS details
        )""",
    ),
]


def get_case_compliance_task_ctes() -> str:

    return "\n        UNION ALL\n".join(
        f"""
        SELECT
          client.person_external_id,
          m.state_code,
          MAX(client.officer_id) AS officer_id,
          IF(
            MAX(m.{config.due_date_column}) <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 30 DAY),
            TO_JSON({config.task_details_struct}),
            NULL
          ) AS task
        FROM {METRICS_TABLE} AS m
        INNER JOIN `{{project_id}}.{{workflows_views}}.client_record_materialized` AS client
          USING(person_id)
        WHERE m.state_code = 'US_ND'
          AND m.{config.due_date_column} IS NOT NULL
          AND date_of_evaluation = (
                SELECT MAX(date_of_evaluation)
                FROM {METRICS_TABLE}
            )
        GROUP BY
          client.person_external_id,
          m.state_code,
          client.officer_id
        """.strip()
        for config in SUPERVISION_TASK_CONFIGS
    )


US_ND_SUPERVISION_TASKS_RECORD_QUERY_TEMPLATE = f"""
    WITH all_supervision_tasks AS (
        {get_case_compliance_task_ctes()}
    ),
    combined_tasks AS (
        -- Aggregate non-null task JSONs into an array, ordered by type
        SELECT
            person_external_id,
            state_code,
            officer_id,
            ARRAY_AGG(task IGNORE NULLS ORDER BY JSON_VALUE(task, "$.type")) AS tasks,
            [] AS needs
        FROM all_supervision_tasks
        WHERE task IS NOT NULL
        GROUP BY person_external_id, state_code, officer_id
    )
    SELECT * FROM combined_tasks
"""

US_ND_SUPERVISION_TASKS_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ND_SUPERVISION_TASKS_RECORD_VIEW_NAME,
    view_query_template=US_ND_SUPERVISION_TASKS_RECORD_QUERY_TEMPLATE,
    description=US_ND_SUPERVISION_TASKS_RECORD_DESCRIPTION,
    dataflow_metrics_materialized=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    workflows_views=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_SUPERVISION_TASKS_RECORD_VIEW_BUILDER.build_and_print()
