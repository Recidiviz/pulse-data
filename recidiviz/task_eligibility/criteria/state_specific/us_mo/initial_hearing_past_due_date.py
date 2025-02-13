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
"""Spans when someone does not have a future initial hearing scheduled or due.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_MO_INITIAL_HEARING_PAST_DUE_DATE"

_QUERY_TEMPLATE = f"""
    -- 1. Identify all ITSC entries with a hearing_scheduled_date (date of the entry) and 
    -- next_review_date (date of the scheduled initial hearing).
    WITH itsc_entries AS (
        SELECT
            pei.state_code,
            pei.person_id,
            IF (
                itsc.JV_BA = "0", 
                NULL,
                SAFE.PARSE_DATE("%Y%m%d", itsc.JV_BA)
            ) AS hearing_scheduled_date,
            IF (
                itsc.JV_AY = "0", 
                NULL,
                SAFE.PARSE_DATE("%Y%m%d", itsc.JV_AY)
            ) AS next_review_date,
        -- ITSC: screen used to schedule initial meaningful hearing in Restrictive Housing
        FROM `{{project_id}}.us_mo_raw_data_up_to_date_views.LBAKRDTA_TAK295_latest` itsc
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
        ON
            itsc.JV_DOC = pei.external_id
            AND pei.state_code = 'US_MO'
    )
    ,
    itsc_entries_deduped AS (
        SELECT DISTINCT
            state_code,
            person_id,
            hearing_scheduled_date,
            FIRST_VALUE(next_review_date) OVER hearing_window AS next_review_date,
        FROM itsc_entries
        WHERE next_review_date IS NOT NULL
        -- Prioritize ITSC entries with scheduled review dates, with preference for the soonest (and non-null) date
        WINDOW hearing_window AS (
            PARTITION BY state_code, person_id, hearing_scheduled_date
            ORDER BY {nonnull_end_date_clause('next_review_date')} ASC
        )
    )
    ,
    -- 2. Assign ITSC entries to RH assignments, based on the earliest possible assignment 
    -- that could correspond to the entry
    itsc_entries_with_earliest_rh_assignment AS (
        SELECT DISTINCT
            itsc.state_code,
            itsc.person_id,
            itsc.hearing_scheduled_date,
            itsc.next_review_date,
            -- Identify the RH assignment that corresponds to this ITSC entry
            FIRST_VALUE(c.confinement_type_session_id) OVER confinement_window AS confinement_type_session_id,
        FROM itsc_entries_deduped itsc
        LEFT JOIN `{{project_id}}.sessions.us_mo_confinement_type_sessions_materialized` c
        ON
            c.person_id = itsc.person_id
            AND c.state_code = itsc.state_code
            -- Review date is after the start of the RH assignment
            AND c.start_date <= itsc.next_review_date
            -- Review was scheduled before the end of the RH assignment
            -- Exclude zero-day ITSC spans (ITSC entry is on last day of assignment) with
            -- strict inequality
            AND {nonnull_end_date_clause('c.end_date')} > itsc.hearing_scheduled_date
            -- TODO(#21788): Use ingested enums once MO housing_unit_type is ingested
            AND c.confinement_type IN ("SOLITARY_CONFINEMENT")
        -- Choose the earliest RH assignment that could correspond with the ITSC entry
        WINDOW confinement_window AS (
            PARTITION BY c.state_code, c.person_id, itsc.hearing_scheduled_date
            ORDER BY c.start_date ASC
        )
    )
    ,
    -- 3. Identify all RH assignments, with a corresponding ITSC entry, if applicable
    rh_assignments AS (
        SELECT DISTINCT
            c.state_code,
            c.person_id,
            c.start_date,
            c.end_date,
            FIRST_VALUE(scheduled.hearing_scheduled_date) OVER itsc_window AS scheduled_date,
            FIRST_VALUE(scheduled.next_review_date) OVER itsc_window AS scheduled_next_review_date,
        FROM `{{project_id}}.sessions.us_mo_confinement_type_sessions_materialized` c
        -- Join with ITSC entries pertitent to this RH assignment
        LEFT JOIN itsc_entries_with_earliest_rh_assignment scheduled
        ON
            c.person_id = scheduled.person_id
            AND c.state_code = scheduled.state_code
            AND c.confinement_type_session_id = scheduled.confinement_type_session_id
        WHERE 
            -- TODO(#21788): Use ingested enums once MO housing_unit_type is ingested
            confinement_type IN ("SOLITARY_CONFINEMENT")
        -- Pick the earliest ITSC entry that could correspond with the RH assignment
        WINDOW itsc_window AS (
            PARTITION BY c.state_code, c.person_id, c.start_date
            ORDER BY scheduled.hearing_scheduled_date ASC
        )
    )
    ,
    -- Spans of time when someone is in RH and has a scheduled initial hearing
    scheduled_date_spans AS (
        SELECT
            state_code,
            person_id,
            -- Clip the start of the span to the beginning of the RH assignment
            GREATEST(start_date, scheduled_date) AS start_date,
            end_date,
            scheduled_next_review_date AS next_review_date,
            FALSE AS due_date_inferred,
        FROM rh_assignments
        WHERE 
            -- Exclude assignments with no scheduled review date (no ITSC entry)
            scheduled_date IS NOT NULL
    )
    ,
    -- Conversion table between business days and calendar days
    business_days AS (
        SELECT 1 AS day_of_week, 7 AS business_days, 9 AS calendar_days UNION ALL
        SELECT 2 AS day_of_week, 7 AS business_days, 9 AS calendar_days UNION ALL
        SELECT 3 AS day_of_week, 7 AS business_days, 9 AS calendar_days UNION ALL
        SELECT 4 AS day_of_week, 7 AS business_days, 9 AS calendar_days UNION ALL
        SELECT 5 AS day_of_week, 7 AS business_days, 11 AS calendar_days UNION ALL
        SELECT 6 AS day_of_week, 7 AS business_days, 11 AS calendar_days UNION ALL
        SELECT 7 AS day_of_week, 7 AS business_days, 10 AS calendar_days
    )
    ,
    -- 4. Infer initial hearing dates for those (parts of) RH assignments that don't correspond to an ITSC entry. 
    inferred_date_spans AS (
        SELECT
            state_code,
            person_id,
            start_date,
            -- Clip the end of the span to the end of the RH assignment
            LEAST({nonnull_end_date_clause('end_date')}, {nonnull_end_date_clause('scheduled_date')}) AS end_date,
            DATE_ADD(start_date, INTERVAL business_days.calendar_days DAY) AS next_review_date,
            TRUE AS due_date_inferred,
        FROM rh_assignments
        LEFT JOIN business_days
        ON
            EXTRACT(DAYOFWEEK from DATE(rh_assignments.start_date)) = business_days.day_of_week
        -- Exclude RH assignments with scheduled review dates that cover the entire period (ITSC entry
        -- covers the whole RH assignment).
        WHERE
            scheduled_date IS NULL 
            OR scheduled_date > start_date
    )
    ,
    -- 5. Combine scheduled and inferred due date spans
    hearing_spans AS (
        SELECT * FROM scheduled_date_spans
        UNION ALL
        SELECT * FROM inferred_date_spans
    )
    ,
    -- 6. Identify spans when someone is overdue for their initial hearing (next_review_date has passed)
    critical_date_spans AS (
        SELECT
            h.state_code,
            h.person_id,
            h.start_date AS start_datetime,
            /* Someone is overdue if their next hearing date is past their next review date (or never occurs), and they 
            are no longer overdue once they've had their next hearing or are released from Restrictive Housing. */
            end_date AS end_datetime,
            h.next_review_date AS critical_date,
            due_date_inferred,
        FROM hearing_spans h
    )
    ,
    -- Add 1-day lag so that someone is overdue after, but not on, the next_review_date
    {critical_date_has_passed_spans_cte(
        meets_criteria_leading_window_time=-1, 
        attributes=["due_date_inferred"],
        date_part="DAY"
    )}
    SELECT 
        state_code,
        person_id,
        start_date,
        -- Set current span's end date to null
        IF(end_date > CURRENT_DATE('US/Pacific'), NULL, end_date) AS end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date AS next_review_date,
            due_date_inferred AS due_date_inferred
        )) AS reason,
        critical_date AS next_review_date,
        due_date_inferred AS due_date_inferred,
    FROM critical_date_has_passed_spans
    WHERE 
        -- Exclude spans that start in the future
        start_date <= CURRENT_DATE('US/Pacific')

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=__doc__,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="next_review_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Next Restrictive Housing review date (hearing) for the person.",
        ),
        ReasonsField(
            name="due_date_inferred",
            type=bigquery.enums.StandardSqlTypeNames.BOOL,
            description="Whether the due date is inferred based on the start date of the RH assignment, or not (it is scheduled).",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
