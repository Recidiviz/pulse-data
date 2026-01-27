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
"""
Helper SQL queries for Nebraska
"""
from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def supervision_oras_overrides_completion_event_query_template(
    overridden_to_level: str,
) -> str:
    """Returns a query identifying supervision level overrides to a certain level.

    Args:
        overridden_to_level (str): The supervision level to which someone is overridden
    """
    return f"""
        SELECT
            state_code,
            person_id,
            CAST(SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', JSON_EXTRACT_SCALAR(assessment_metadata, '$.DATE_OF_OVERRIDE')) AS DATE) AS completion_event_date,
        FROM `{{project_id}}.normalized_state.state_assessment`
        WHERE
            state_code = 'US_NE'
            -- Gather only ORAS assessments
            AND assessment_type = 'ORAS_COMMUNITY_SUPERVISION_SCREENING'
            -- Filter out "overrides" that aren't really overrides
            AND JSON_EXTRACT_SCALAR(assessment_metadata, '$.SUPERVISION_LEVEL_OVERRIDE') != assessment_level
            -- Identify overrides to the specified level ("{overridden_to_level}"")
            AND JSON_EXTRACT_SCALAR(assessment_metadata, '$.SUPERVISION_LEVEL_OVERRIDE') = '{overridden_to_level}'
    """


def mr_reports_within_time_interval(
    *,
    criteria_name: str,
    description: str,
    date_interval: int,
    date_part: str,
    mr_type: str,
    max_number_of_mrs_exclusive: int,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Builds a filter for MR reports within a specified time interval"""
    query_template = f"""
        WITH filtered_mrs AS (
            SELECT
                state_code,
                incident.person_id,
                incident.incarceration_incident_id,
                -- incident -> outcome is 1 -> many so we agg here in case there are multiple reports
                MAX(report_date) as report_date
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` incident
            LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome` outcome
            USING (state_code, incarceration_incident_id)
            WHERE 
                incident.state_code = 'US_NE'
                AND JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.UDCorIDC') = '{mr_type}'
                AND outcome.report_date IS NOT NULL
                AND incident.incident_type NOT IN ('DISMISSED', 'NOT_GUILTY')
            GROUP BY 1,2,3
        ),
        filtered_mr_spans AS (
            SELECT
                state_code,
                person_id,
                incarceration_incident_id,
                report_date as start_date,
                DATE_ADD(report_date, INTERVAL {date_interval} {date_part}) as end_date,
                DATE_ADD(report_date, INTERVAL {date_interval} {date_part}) AS eligible_date,
                report_date
            FROM filtered_mrs
        ),
        -- Create sub-sessions to handle overlapping 6-month periods
        {create_sub_sessions_with_attributes(
            table_name="filtered_mr_spans",
            end_date_field_name="end_date",
        )},
        -- Count reports per person per time period
        report_counts AS (
            SELECT
                state_code,
                person_id,
                start_date,
                end_date,
                COUNT(*) AS report_count,
                ARRAY_AGG(report_date ORDER BY report_date DESC) AS report_dates,
                MAX(eligible_date) AS latest_eligible_date
            FROM sub_sessions_with_attributes
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            state_code,
            person_id,
            start_date,
            end_date,
            report_count < {max_number_of_mrs_exclusive} AS meets_criteria,
            report_dates,
            latest_eligible_date,
            TO_JSON(STRUCT(
                report_dates AS report_dates,
                latest_eligible_date AS latest_eligible_date
            )) AS reason,
        FROM report_counts"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        criteria_spans_query_template=query_template,
        reasons_fields=[
            ReasonsField(
                name="report_dates",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="Dates of relevant MRs",
            ),
            ReasonsField(
                name="latest_eligible_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date when the individual is eligible",
            ),
        ],
        state_code=StateCode.US_NE,
        meets_criteria_default=True,
        normalized_state_dataset=normalized_state_dataset_for_state_code(
            StateCode.US_NE
        ),
    )
