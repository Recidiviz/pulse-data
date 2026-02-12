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


def good_time_restoration_denial_fragment() -> str:
    """Shared query fragment for joining good time restoration request reviews (from the
    GTR_Approval table) to the requests themselves (from the GoodTimeRestoration table).
    """

    return """
    -- all reviews that were denials
    denials AS (
    SELECT
        app.pkApprovalId,
        app.approvalLevel AS reviewer,
        DATE(app.dateReviewed) AS dateReviewed,
    FROM `{project_id}.us_ne_raw_data_up_to_date_views.GTR_Approval_latest` app
    LEFT JOIN `{project_id}.us_ne_raw_data_up_to_date_views.CodeValue_latest` cv
    ON cv.codeType = 'goodTimeRestoration_recommendForRestoration' AND app.recommendForRestorationCode = cv.codeId
    WHERE cv.codeValue = 'Denied'
    ),
    -- associates all good time restoration requests with denials
    requests_with_denials AS (
    SELECT
        req.pkgoodTimeRestorationId,
        req.inmateNumber,
        ARRAY(
        SELECT AS STRUCT
            denials.reviewer,
            denials.dateReviewed
        FROM UNNEST([d1, d2, d3, d4, d5]) AS denials
        WHERE denials.pkApprovalId IS NOT NULL
        ORDER BY denials.dateReviewed DESC
        ) AS denials,
    FROM `{project_id}.us_ne_raw_data_up_to_date_views.GoodTimeRestoration_latest` req
    LEFT JOIN denials AS d1 ON d1.pkApprovalId = req.fkUCCApprovalId
    LEFT JOIN denials AS d2 ON d2.pkApprovalId = req.fkICCApprovalId
    LEFT JOIN denials AS d3 ON d3.pkApprovalId = req.fkWardenApprovalId
    LEFT JOIN denials AS d4 ON d4.pkApprovalId = req.fkDDApprovalId
    LEFT JOIN denials AS d5 ON d5.pkApprovalId = req.fkRecordsId
    )"""


def latest_good_time_restoration_or_denial_date_fragment() -> str:
    """Query fragment that pulls the latest good time restoration date or restoration
    request denial date.

    n.b. this value is used on the frontend to RESET the pending or snooze status if that
    was created before this date.
    """

    return f"""
    WITH {good_time_restoration_denial_fragment()},
    most_recent_denial AS (
        SELECT 
            pei.person_id,
            MAX(denials[SAFE_OFFSET(0)].dateReviewed) as most_recent_denial
        FROM requests_with_denials req
        LEFT JOIN `{{project_id}}.us_ne_normalized_state.state_person_external_id` pei
        ON 
            pei.external_id = req.inmateNumber 
            AND pei.id_type = 'US_NE_ID_NBR'
        WHERE ARRAY_LENGTH(denials) > 0
        GROUP BY 1
    ),
    most_recent_restoration AS (
        SELECT
            person_id,
            MAX(credit_date) as most_recent_restoration,
        FROM `{{project_id}}.analyst_data.us_ne_earned_credit_activity_preprocessed_materialized`
        WHERE 
            state_code = 'US_NE'
            AND credit_function = 'REINSTATE'
            AND credit_date IS NOT NULL
        GROUP BY 1
    )
    SELECT 
        person_id,
        GREATEST(COALESCE(most_recent_denial, most_recent_restoration), COALESCE(most_recent_restoration, most_recent_denial)) as latest_good_time_restoration_or_denial_date
    FROM most_recent_denial d
    FULL OUTER JOIN most_recent_restoration r
    USING(person_id)
    """
