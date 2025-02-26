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
"""Describes the spans of time when a TN client has been on an eligible supervision level for a sufficient amount of
time"""
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_ON_ELIGIBLE_LEVEL_FOR_SUFFICIENT_TIME"

_DESCRIPTION = """Describes the spans of time when a TN client has been on an eligible supervision level for a
sufficient amount of time"""

# TODO(#20870) - Deprecate this in favor of better long term solution to excluding these levels
# TODO(#22511) potentially refactor to build off of a general criteria view builder
EXCLUDED_MEDIUM_RAW_TEXT = ["6P1", "6P2", "6P3", "3D3"]

_QUERY_TEMPLATE = f"""
    WITH supervision_level_cte AS
    (
    /*
    Get all periods of time for which a person is on either medium or minimum supervision levels
    */
    SELECT
        person_id,
        state_code,
        start_date,
        end_date_exclusive,
        supervision_level,
    FROM `{{project_id}}.{{sessions_dataset}}.supervision_level_raw_text_sessions_materialized`
    WHERE state_code = 'US_TN'
        AND supervision_level IN ('MINIMUM','MEDIUM')
        AND supervision_level_raw_text NOT IN ('{{exclude_medium}}')
    )
    ,
    /*
    Sessionize so that we spans of continuous time on minimum and continuous time on medium (not always the case due to
    this coming from `supervision_level_raw_text_sessions`).
    */
    sessions_cte AS 
    (
    {aggregate_adjacent_spans(table_name='supervision_level_cte',
                       attribute='supervision_level',
                       end_date_field_name='end_date_exclusive',
                        )}
    )
    ,
    /*
    For each period of time on minimum or medium, calculate the critical date at which point a person would be eligible.
    This is the least of two values: (1) the individual span start date + the relevant number of months (12 if minimum, 
    18 if medium, (2) the "super-span" start date + 18 months where a super-span represents the continuous time on
    either of these levels (this is where the "date_gap_id" field is utilized). This latter date captures situations
    where a person transitions from MEDIUM --> MINIMUM and the MEDIUM start date + 18 months comes before the MINIMUM
    start date + 12 months.
    */
    critical_date_spans AS
    (
    SELECT
        person_id,
        state_code,
        start_date AS start_datetime,
        end_date_exclusive AS end_datetime,
        supervision_level,
        LEAST
            (
            CASE WHEN supervision_level = 'MINIMUM' THEN DATE_ADD(start_date, INTERVAL 12 MONTH)
                WHEN supervision_level = 'MEDIUM' THEN DATE_ADD(start_date, INTERVAL 18 MONTH) END,
            
            DATE_ADD(MIN(start_date) OVER(PARTITION BY person_id, state_code, date_gap_id 
                ORDER BY session_id), INTERVAL 18 MONTH)
            ) AS critical_date
        FROM sessions_cte
    )
    ,
    /*
    Use the critical date has passed function with the "attributes" parameter so that we keep the "supervision_level"
    that a person is on at that point for the reasons json.
    */
    {critical_date_has_passed_spans_cte(attributes=['supervision_level'])}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            supervision_level AS eligible_level,
            critical_date AS eligible_date
        )) AS reason
    FROM critical_date_has_passed_spans
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_TN,
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        meets_criteria_default=False,
        sessions_dataset=SESSIONS_DATASET,
        exclude_medium="', '".join(EXCLUDED_MEDIUM_RAW_TEXT),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
