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
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.task_eligibility.utils.us_tn_query_fragments import (
    EXCLUDED_MEDIUM_RAW_TEXT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_ON_ELIGIBLE_LEVEL_FOR_SUFFICIENT_TIME"

_DESCRIPTION = """Describes the spans of time when a TN client has been on an eligible supervision level for a
sufficient amount of time"""


# TODO(#22511) potentially refactor to build off of a general criteria view builder
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
    start date + 12 months. Using this logic we also calculate the relevant date, the date from which to start calculating
    the time spent on the eligible level.
    */
    critical_date_spans AS
    (
    SELECT
        person_id,
        state_code,
        start_date AS start_datetime,
        end_date_exclusive AS end_datetime,
        supervision_level,
        LEAST(critical_date_session_start, critical_date_super_session_start) AS critical_date,
        IF(critical_date_session_start<critical_date_super_session_start, start_date, super_session_start_date) AS relevant_date,
    FROM 
        (
        SELECT
            *,
            CASE WHEN supervision_level = 'MINIMUM' THEN DATE_ADD(start_date, INTERVAL 12 MONTH)
                WHEN supervision_level = 'MEDIUM' THEN DATE_ADD(start_date, INTERVAL 18 MONTH) END AS critical_date_session_start,
            DATE_ADD(super_session_start_date, INTERVAL 18 MONTH) AS critical_date_super_session_start,
        FROM
            (
            SELECT 
                *,
                MIN(start_date) OVER(PARTITION BY person_id, state_code, date_gap_id ORDER BY session_id) AS super_session_start_date,
            FROM sessions_cte
            )
        )
    )
    ,
    /*
    Use the critical date has passed function with the "attributes" parameter so that we keep the "supervision_level"
    that a person is on at that point for the reasons json.
    */
    {critical_date_has_passed_spans_cte(attributes=['supervision_level', 'relevant_date'])}
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            supervision_level AS eligible_level,
            critical_date AS eligible_date,
            relevant_date AS start_date_on_eligible_level
        )) AS reason,
        supervision_level AS eligible_level,
        critical_date AS eligible_date,
        relevant_date AS start_date_on_eligible_level,
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
        reasons_fields=[
            ReasonsField(
                name="eligible_level",
                type=bigquery.enums.SqlTypeNames.STRING,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="eligible_date",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
            ReasonsField(
                name="start_date_on_eligible_level",
                type=bigquery.enums.SqlTypeNames.DATE,
                description="#TODO(#29059): Add reasons field description",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
