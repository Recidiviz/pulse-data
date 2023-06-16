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
# ============================================================================
"""Describes the spans of time during which someone in MO
is overdue for a Restrictive Housing hearing.
"""
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_OVERDUE_FOR_HEARING"

_DESCRIPTION = """Describes the spans of time during which someone in MO
is overdue for a Restrictive Housing hearing.
"""

_QUERY_TEMPLATE = f"""
    WITH critical_date_spans AS (
        SELECT
            state_code,
            person_id,
            hearing_date AS start_datetime,
            -- Someone is overdue if their next hearing date is past their next review date (or never occurs), and they 
            -- are no longer overdue once they've had their next hearing.
            COALESCE(LEAD(hearing_date) OVER hearing_window, '9999-12-31') AS end_datetime,
            next_review_date AS critical_date,
        FROM `{{project_id}}.{{analyst_views_dataset}}.us_mo_classification_hearings_preprocessed_materialized`
        WINDOW hearing_window AS (
            PARTITION BY state_code, person_id
            ORDER BY hearing_date ASC
        )
    )
    ,
    -- Add 1-day lag so that someone is overdue after, but not on, the "next review date"
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_days=-1)}
    SELECT 
        state_code,
        person_id,
        start_date,
        -- Set current span's end date to null
        IF(end_date > CURRENT_DATE('US/Pacific'), NULL, end_date) AS end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date as next_review_date
        )) AS reason
    FROM critical_date_has_passed_spans
    -- Exclude spans that start in the future
    WHERE start_date <= CURRENT_DATE('US/Pacific')
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_MO,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        meets_criteria_default=False,
        analyst_views_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
