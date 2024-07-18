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
"""Query for supervision tasks that are either overdue or upcoming within 30 days for Idaho"""
from typing import Optional

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_SUPERVISION_TASKS_RECORD_VIEW_NAME = "us_ix_supervision_tasks_record"

US_IX_SUPERVISION_TASKS_RECORD_DESCRIPTION = """
    View containing tasks that are either overdue or due within the next 30 days for each client on supervision in 
    US_IX.
"""


@attr.s
class UsIxSupervisionTaskQueryConfig:
    due_date_column: str = attr.ib()
    task_details_struct: str = attr.ib()
    task_details_joins: Optional[str] = attr.ib(default="")


CLIENT_RECORD_JOIN = """
    INNER JOIN `{project_id}.{workflows_views}.client_record_materialized` client
    USING(person_external_id, state_code)
"""

ASSESSMENT_SCORE_JOIN = """
    LEFT JOIN `{project_id}.{sessions}.assessment_score_sessions_materialized` ss 
    USING(person_id)
"""

SUPERVISION_TASK_CONFIGS = [
    UsIxSupervisionTaskQueryConfig(
        due_date_column="next_recommended_home_visit_date",
        task_details_struct="""STRUCT(
              'homeVisit' as type,
              MAX(next_recommended_home_visit_date) AS due_date,
              STRUCT(
                MAX(most_recent_home_visit_date) AS last_home_visit,
                MAX(client.supervision_level) AS supervision_level, 
                MAX(client.address) AS current_address,
                MAX(case_type) AS case_type
	          ) AS details
            )
        """,
    ),
    UsIxSupervisionTaskQueryConfig(
        due_date_column="next_recommended_face_to_face_date",
        task_details_struct="""STRUCT(
              'contact' as type,
              MAX(next_recommended_face_to_face_date) AS due_date,
              STRUCT(
                MAX(client.supervision_level) AS supervision_level,
                MAX(most_recent_face_to_face_date) AS last_contacted,
                MAX(case_type) AS case_type
              ) AS details
            )
        """,
    ),
    UsIxSupervisionTaskQueryConfig(
        due_date_column="next_recommended_assessment_date",
        task_details_struct="""STRUCT(
              'assessment' as type,
              MAX(next_recommended_assessment_date) AS due_date,
              STRUCT(
                MAX(ss.assessment_level_raw_text) AS risk_level, 
                MAX(ss.assessment_date) AS last_assessed_on,
                MAX(case_type) AS case_type
              ) AS details
            )
        """,
        task_details_joins="\n".join([ASSESSMENT_SCORE_JOIN]),
    ),
    UsIxSupervisionTaskQueryConfig(
        due_date_column="next_recommended_employment_verification_date",
        task_details_struct="""STRUCT(
              'employment' as type,
              MAX(next_recommended_employment_verification_date) AS due_date,
              STRUCT(
                MAX(client.supervision_level) AS supervision_level,
                MAX(most_recent_employment_verification_date) AS last_contacted,
                MAX(case_type) AS case_type
              ) AS details
            )
        """,
    ),
]


def get_case_compliance_task_ctes() -> str:
    """This CTE returns a union of each supervision task and each state_code defined in the
    SupervisionTaskQueryConfig list."""
    cte_body = "\n        UNION ALL\n".join(
        [
            f"""
            SELECT
                cc.person_external_id,
                cc.state_code,
                MAX(client.officer_id) AS officer_id,
                IF(
                    MAX({config.due_date_column}) <= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH),
                    TO_JSON({config.task_details_struct}),
                    NULL
                ) AS task
            FROM case_compliance_with_next_recommended_recalculated cc

            INNER JOIN `{{project_id}}.{{workflows_views}}.client_record_materialized` client
            USING(person_external_id, state_code)
    
            {config.task_details_joins}
            
            WHERE cc.state_code = 'US_IX'
            AND date_of_evaluation = (
                SELECT MAX(date_of_evaluation)
                FROM case_compliance_with_next_recommended_recalculated
            )
            AND {config.due_date_column} IS NOT NULL
            GROUP BY 1,2
            """
            for config in SUPERVISION_TASK_CONFIGS
        ]
    )
    return cte_body


def get_case_compliance_need_ctes() -> str:
    return """
        SELECT
            person_external_id,
            state_code,
            officer_id,
            TO_JSON(STRUCT(
                'employment' AS type
            )) as need,
        FROM `{project_id}.{workflows_views}.client_record_materialized`
        WHERE array_length(current_employers) = 0
        AND state_code = 'US_IX'
    """


US_IX_SUPERVISION_TASKS_RECORD_QUERY_TEMPLATE = f"""
    WITH 
    income_verification AS (
        SELECT 
            person_id,
            MAX(DATE(JSON_EXTRACT_SCALAR(reason, "$.income_verified_date"))) as most_recent_income_verification_date,
        FROM `{{project_id}}.task_eligibility_criteria_us_ix.income_verified_within_3_months_materialized`
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            USING(person_id)
        INNER JOIN `{{project_id}}.{{workflows_views}}.client_record_materialized` client
            ON pei.external_id = client.person_external_id AND pei.id_type = 'US_IX_DOC' and pei.state_code = client.state_code
        WHERE start_date >= supervision_start_date
        group by 1
    ),
    unable_to_work AS (
        SELECT person_id
        FROM `{{project_id}}.normalized_state.state_employment_period`
        WHERE state_code = 'US_IX'
          AND employment_status = 'UNABLE_TO_WORK'
          AND end_date IS NULL
    ),
    case_compliances_with_income_verification AS (
    select cc.* EXCEPT(most_recent_employment_verification_date, next_recommended_employment_verification_date),
        CASE 
            WHEN most_recent_income_verification_date IS NOT NULL 
                AND most_recent_employment_verification_date IS NOT NULL
            THEN
                IF(most_recent_income_verification_date > most_recent_employment_verification_date,
                most_recent_income_verification_date,
                most_recent_employment_verification_date)
            ELSE COALESCE(most_recent_income_verification_date, most_recent_employment_verification_date)
            END AS most_recent_employment_verification_date,
        next_recommended_employment_verification_date AS orig_next_recommended_employment_verification_date,
        (u.person_id IS NOT NULL) as unable_to_work
    FROM `{{project_id}}.{{dataflow_metrics_materialized}}.most_recent_supervision_case_compliance_metrics_materialized` cc
    LEFT JOIN income_verification USING(person_id)
    LEFT JOIN unable_to_work u ON cc.person_id = u.person_id
    WHERE state_code = 'US_IX'
    ),
    case_compliance_with_next_recommended_recalculated AS (
        SELECT * EXCEPT(unable_to_work),
            CASE 
            -- When this person has an open employment period with status = 'UNABLE_TO_WORK', we do not recommend an employment verification
            WHEN unable_to_work = True
                THEN NULL
            -- If no further employment verification was originally recommended, then leave as is because that means that standards were fulfilled regardless of any new info from income verifications
            WHEN orig_next_recommended_employment_verification_date IS NULL
                THEN orig_next_recommended_employment_verification_date
            -- Else if it's a GENERAL case and we do now see a most_recent_employment_verification_date, then no further employment verification is needed
            WHEN case_type = 'GENERAL' AND most_recent_employment_verification_date is NOT NULL
                THEN NULL
            -- if it's a sex offense case with level = HIGH and we do see a most recent employment verification date, then the next employment verification date is 30 days after the most recent employment verification date 
            WHEN case_type = 'SEX_OFFENSE' 
                AND supervision_level = 'HIGH'
                AND most_recent_employment_verification_date IS NOT NULL
                THEN DATE_ADD(most_recent_employment_verification_date, INTERVAL 30 DAY)
            -- if it's a sex offense case with level = MINIMUM or MEDIUM and we do see a most recent employment verification date, then the next employment verification date is 60 days after the most recent employment verification date 
            WHEN case_type = 'SEX_OFFENSE' 
                AND supervision_level in ('MINIMUM', 'MEDIUM')
                AND most_recent_employment_verification_date IS NOT NULL
                THEN DATE_ADD(most_recent_employment_verification_date, INTERVAL 60 DAY)
            ELSE orig_next_recommended_employment_verification_date
        END AS next_recommended_employment_verification_date
        FROM case_compliances_with_income_verification
    ),
    
    all_supervision_tasks AS ({get_case_compliance_task_ctes()}),

    combined_tasks AS (
        SELECT
            person_external_id,
            state_code,
            officer_id,
            ARRAY_AGG(task IGNORE NULLS) AS tasks,
            [] AS needs,
        FROM all_supervision_tasks
        WHERE task IS NOT NULL
        GROUP BY 1,2,3
    )
    select * from combined_tasks
    """

US_IX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_IX_SUPERVISION_TASKS_RECORD_VIEW_NAME,
    view_query_template=US_IX_SUPERVISION_TASKS_RECORD_QUERY_TEMPLATE,
    description=US_IX_SUPERVISION_TASKS_RECORD_DESCRIPTION,
    dataflow_metrics_materialized=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    workflows_views=dataset_config.WORKFLOWS_VIEWS_DATASET,
    sessions=dataset_config.SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER.build_and_print()
