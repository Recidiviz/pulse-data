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
"""
Helper SQL queries for California
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_CRITERIA_GENERAL
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)


def employment_milestone_builder(
    criteria_name: str, description: str, from_x_months: int, to_x_months: int
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criteria view builder indicating the spans of time when
    a person has been employed for a certain number of months. It also returns every employer
    name that was active from the start of the employment period until the desired milestone.

    Args:
        criteria_name (str): The name of the criteria
        description (str): The description of the criteria
        from_x_months (int): The number of months to start counting employment
        to_x_months (int): The number of months to end counting employment
    """

    criteria_query = f"""WITH has_employment AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
    FROM `{{project_id}}.{{tes_criteria_general_dataset}}.has_employment_materialized`
    WHERE state_code = 'US_CA'
),

has_employment_agg_adj_spans AS (
    # We need adjacent spans so we start the milestone count only when a person goes from 
    #   not having employment to having employment.
    {aggregate_adjacent_spans(table_name='has_employment')}
),

has_employment_unnested AS (
    SELECT 
        state_code,
        person_id,
        start_date,
        end_date,
        status_employer_start_date,
    FROM `{{project_id}}.{{tes_criteria_general_dataset}}.has_employment_materialized`,
    UNNEST(JSON_VALUE_ARRAY(reason.status_employer_start_date)) AS status_employer_start_date
    WHERE state_code = 'US_CA'
),

critical_date_spans AS (
    SELECT 
        adj.state_code,
        adj.person_id,
        DATE_ADD(adj.start_date, INTERVAL {from_x_months} MONTH) AS start_datetime,
        adj.end_date AS end_datetime,
        DATE_ADD(adj.start_date, INTERVAL {to_x_months} MONTH) AS critical_date,
        ARRAY_AGG(DISTINCT un.status_employer_start_date) AS status_employer_start_date,
    FROM has_employment_agg_adj_spans adj
    LEFT JOIN has_employment_unnested un
    ON adj.state_code = un.state_code
        AND adj.person_id = un.person_id
        AND adj.start_date < {nonnull_end_date_clause('un.end_date')}
        # Instead of using adj.end_date, we use the critical date as the end date.
        #   This is because we want to grab all employer names that were active
        #   from the start date to the critical date.
        AND un.start_date < DATE_ADD(adj.start_date, INTERVAL {to_x_months} MONTH)
    GROUP BY 1,2,3,4,5

),
{critical_date_has_passed_spans_cte(attributes=['status_employer_start_date'])}

SELECT 
    cd.state_code,
    cd.person_id,
    cd.start_date,
    cd.end_date,
    NOT cd.critical_date_has_passed AS meets_criteria,
    TO_JSON(STRUCT(status_employer_start_date AS status_employer_start_date)) AS reason,
    status_employer_start_date,
FROM critical_date_has_passed_spans cd
WHERE cd.start_date < {nonnull_end_date_clause('cd.end_date')}"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_CA,
        criteria_spans_query_template=criteria_query,
        tes_criteria_general_dataset=TASK_ELIGIBILITY_CRITERIA_GENERAL,
        reasons_fields=[
            ReasonsField(
                name="status_employer_start_date",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="List of status, employer and start date for each employer active during the employment period.",
            )
        ],
    )
