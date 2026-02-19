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

from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
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
    mr_type_filter: str | None,
    severity_filter: str | None,
    max_number_of_mrs_exclusive: int,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """For each MR, creates a span of length |date_interval| |date_part| starting at the
    report date. A person fails this criterion at any point where [max_number_of_mrs_exclusive]
    or more MR spans overlap. If an MR span overlaps with a period where the person is
    out of custody (e.g., incarcerated out of state, federal custody, or absconded), the
    span is reset to start after that out of custody period ends.

      Args:
        criteria_name(str): The name of the criteria query
        description(str): Description of the criteria query
        date_interval(str): The length of time after each MR report during which it remains
            counted. Combined with date_part (e.g., 12 MONTH, 90 DAY).
        date_part(str): The BigQuery date part unit for date_interval. Supports standard
            BigQuery date parts: "DAY", "WEEK", "MONTH", "QUARTER", "YEAR".
        mr_type_filter(str | None): If provided, filters to only MRs where the UDCorIDC field in
            incident_metadata equals this value. If None, all MR types are included.
        severity_filter(str | None): If provided, filters to only MRs with this incident_severity
            value. If None, all severity levels are included.
        max_number_of_mrs_exclusive(int): The exclusive upper bound on the number of active
            MR spans for the criterion to be met. For example, a value of 2 means
            meets_criteria is True when fewer than 2 MRs are active (i.e., 0 or 1).
            Must be a positive integer.
    """

    if max_number_of_mrs_exclusive <= 0:
        raise ValueError(
            "Must specify positive integer for max_number_of_mrs_exclusive"
        )

    filter_statements = ""
    if mr_type_filter:
        filter_statements += f"AND JSON_EXTRACT_SCALAR(incident.incident_metadata, '$.UDCorIDC') = '{mr_type_filter}'\n"
    if severity_filter:
        filter_statements += f"AND incident.incident_severity = '{severity_filter}'\n"

    query_template = f"""
        WITH filtered_mrs AS (
            SELECT
                state_code,
                incident.person_id,
                SPLIT(incident.external_id, '-')[OFFSET(3)] AS report_number,
                -- incident -> outcome is 1 -> many and incident -> report is 1 so we agg 
                -- here in case there are multiple reports
                MAX(report_date) as report_date
            FROM `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident` incident
            LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_incarceration_incident_outcome` outcome
            USING (state_code, incarceration_incident_id)
            WHERE 
                incident.state_code = 'US_NE'
                AND outcome.report_date IS NOT NULL
                AND incident.incident_type NOT IN ('DISMISSED', 'NOT_GUILTY')
                {filter_statements}
            GROUP BY 1,2,3
        )
        ,
        filtered_mr_spans AS (
            SELECT
                state_code,
                person_id,
                report_number,
                report_date as start_date,
                DATE_ADD(report_date, INTERVAL {date_interval} {date_part}) as end_date,
                report_date
            FROM filtered_mrs
        )
        ,
        -- create indicators for when they are incarcerated but not in NCDS custody
        -- where their behavior cannot be accounted for
        super_sessions_with_ooc_indicator AS (
            SELECT 
                state_code,
                person_id,
                start_date,
                end_date_exclusive,
                (
                    compartment_level_1 = "INCARCERATION"
                    AND (
                        custodial_authority IN ('OTHER_STATE', 'FEDERAL')
                        OR compartment_level_2 IN ('ABSCONSION')
                    )
                ) AS is_ooc 
            FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
            WHERE state_code = "US_NE"
        )
        ,
        -- create supersessions of out of custody
        ooc_super_sessions AS (
            {aggregate_adjacent_spans(
                table_name='super_sessions_with_ooc_indicator',
                attribute='is_ooc',
                end_date_field_name='end_date_exclusive'
            )}
        )
        ,
        -- identify mr spans that overlap w/ an absconsion or being in another jurisdiction 
        filtered_mr_spans_overlapping_w_ooc AS (
            SELECT 
                mr.state_code,
                mr.person_id,
                mr.report_number,
                mr.report_date,
                mr.start_date,
                mr.end_date,
                ooc.person_id IS NOT NULL as has_overlapping_ooc,
                ooc.start_date as incar_super_start_date,
                ooc.end_date_exclusive as incar_super_end_date_exclusive,
                ooc.session_id,
                ooc.date_gap_id
            FROM filtered_mr_spans mr   
            -- join on ooc super sessions
            LEFT JOIN (
                SELECT * FROM ooc_super_sessions WHERE is_ooc
            ) ooc
            ON  
                mr.state_code = ooc.state_code
                AND mr.person_id = ooc.person_id
                -- filter to overlapping spans
                AND mr.start_date < {nonnull_end_date_exclusive_clause('ooc.end_date_exclusive')}
                AND mr.end_date > ooc.start_date
            -- if there is more than 1 ooc super session in our window, take the most
            -- recent
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY     
                    mr.state_code,
                    mr.person_id,
                    mr.report_number
                ORDER BY ooc.start_date DESC
            ) = 1
        )
        ,
        -- adjust mr spans to reset upon reentry into qualifying incarceration super session
        adjusted_mr_starts_for_ooc AS (
            SELECT 
                mr.state_code,
                mr.person_id,
                mr.report_number,
                mr.report_date,
                mr.start_date,
                -- if we can't find a future date, then this is null which means they are still ooc and will kick 
                -- back in when they are back in custody
                MIN(super.start_date) as clock_reset_date,
                DATE_ADD(MIN(super.start_date), INTERVAL {date_interval} {date_part}) as end_date,
            FROM filtered_mr_spans_overlapping_w_ooc mr
            LEFT JOIN ooc_super_sessions super
            ON 
                mr.state_code = super.state_code
                AND mr.person_id = super.person_id
                -- get all future sessions
                AND super.session_id > mr.session_id
            WHERE 
                -- filter to 
                -- 1. cases we identified in previous CTE as overlapping ooc
                mr.has_overlapping_ooc IS TRUE
                -- 2. future super sessions that are back in custody
                AND super.is_ooc IS FALSE
                -- 3. future super sessions that are either active or lasted the duration 
                --    of the intended MR span
                AND 
                -- the future super session lasts as long as this sessions (always returns true for active super sessions)
                DATE_ADD(super.start_date, INTERVAL {date_interval} {date_part}) <= {nonnull_end_date_exclusive_clause("super.end_date_exclusive")}
            GROUP BY 1,2,3,4,5
        )
        ,
        all_mr_spans AS (
            SELECT 
                state_code,
                person_id,
                report_number,
                report_date,
                clock_reset_date,
                start_date,
                end_date,
                end_date as eligible_date
            FROM adjusted_mr_starts_for_ooc
            
            UNION ALL 

            SELECT 
                state_code,
                person_id,
                report_number,
                report_date,
                NULL AS clock_reset_date,
                start_date,
                end_date, 
                end_date as eligible_date
            FROM filtered_mr_spans_overlapping_w_ooc
            WHERE not has_overlapping_ooc
        )
        ,
        -- Create sub-sessions to handle overlapping periods
        {create_sub_sessions_with_attributes(table_name="all_mr_spans")},
        -- Count reports per person per time period
        report_counts AS (
            SELECT
                state_code,
                person_id,
                start_date,
                end_date,
                COUNT(*) AS report_count,
                ARRAY_AGG(report_date ORDER BY report_date DESC) AS report_dates,
                ARRAY_AGG(clock_reset_date ORDER BY report_date DESC) AS clock_reset_dates,
                -- we want the latest eligibility date of the MR that makes this span 
                -- ineligible -- if there are fewer MRs than |max_number_of_mrs_exclusive|
                -- then we this will be NULL.
                ARRAY_AGG(eligible_date ORDER BY eligible_date DESC)[SAFE_OFFSET({max_number_of_mrs_exclusive - 1})] AS latest_eligible_date
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
            clock_reset_dates,
            TO_JSON(STRUCT(
                report_dates AS report_dates,
                latest_eligible_date AS latest_eligible_date,
                clock_reset_dates AS clock_reset_dates
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
            ReasonsField(
                name="clock_reset_dates",
                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                description="If the JII was out of custody this MR's initial ineligibility period, the date when the clock reset for each applicable MR",
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
    ),
    most_recent_restoration_or_denial AS (
        SELECT 
            person_id,
            GREATEST(COALESCE(most_recent_denial, most_recent_restoration), COALESCE(most_recent_restoration, most_recent_denial)) as latest_good_time_restoration_or_denial_date
        FROM most_recent_denial d
        FULL OUTER JOIN most_recent_restoration r
        USING(person_id)
    )
    SELECT 
        person_id,
        latest_good_time_restoration_or_denial_date,
        DATE_ADD(DATE_TRUNC(latest_good_time_restoration_or_denial_date, MONTH), INTERVAL 1 MONTH) AS next_month_after_latest_good_time_restoration_or_denial_date
    FROM most_recent_restoration_or_denial
    """
