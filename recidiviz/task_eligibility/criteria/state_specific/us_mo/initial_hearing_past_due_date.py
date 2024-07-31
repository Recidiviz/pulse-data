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

_DESCRIPTION = """Spans when someone does not have a future initial hearing scheduled or due.
"""

_QUERY_TEMPLATE = f"""
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
    tasc_hearing_due_date_spans_scheduled AS (
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
    -- Someone has been in solitary confinement less than 7 business days, and they haven't had a hearing yet.
    tasc_hearing_due_date_spans_inferred AS (
        SELECT
            c.state_code,
            c.person_id,
            /* To identify when a hearing is "due", treat the beginning of a Restrictive 
            Housing assignment as the date the initial hearing was scheduled */
            c.start_date as hearing_scheduled_date,
            EXTRACT(DAYOFWEEK from DATE(c.start_date)) AS hearing_scheduled_date_day_of_week,
        FROM `{{project_id}}.sessions.us_mo_confinement_type_sessions_materialized` c
        /* To filter out people who already have initial hearings scheduled, we look for
        (1) An ITSC entry for hearing_scheduled_date on or before the RH assignment because
        this date is supposed to reflect when someone was placed on TA status and 
        (2) If the ITSC screen has a "next review date" after RH assignment start */
        LEFT JOIN tasc_hearing_due_date_spans_scheduled current_hearing_due_span
        ON
            c.person_id = current_hearing_due_span.person_id
            AND c.state_code = current_hearing_due_span.state_code
            AND current_hearing_due_span.hearing_scheduled_date <= c.start_date
            AND current_hearing_due_span.next_review_date >= c.start_date
        WHERE 
            -- TODO(#21788): Use ingested enums once MO housing_unit_type is ingested
            confinement_type IN ("SOLITARY_CONFINEMENT")
            -- Filter out people with hearing dates assigned in ITSC
            AND current_hearing_due_span.hearing_scheduled_date IS NULL
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
    -- Infer a due date 7 business days after RH assignment
    tasc_hearing_due_date_spans_inferred_with_due_date AS (
        SELECT
            t.state_code,
            t.person_id,
            t.hearing_scheduled_date,
            DATE_ADD(t.hearing_scheduled_date, INTERVAL business_days.calendar_days DAY) AS next_review_date,
        FROM tasc_hearing_due_date_spans_inferred t
        LEFT JOIN business_days
        ON
            t.hearing_scheduled_date_day_of_week = business_days.day_of_week
    )
    ,
    hearings_and_tasc_spans AS (
        SELECT 
            *, 
            FALSE AS due_date_inferred,
        FROM tasc_hearing_due_date_spans_scheduled 
        
        UNION ALL 
        
        SELECT 
            *,
            TRUE AS due_date_inferred,
        FROM tasc_hearing_due_date_spans_inferred_with_due_date
    )
    ,
    hearing_spans AS (
        SELECT
            state_code,
            person_id,
            hearing_scheduled_date,
            LEAD(hearing_scheduled_date) OVER hearing_window AS next_hearing_scheduled_date,
            next_review_date,
            due_date_inferred,
        FROM hearings_and_tasc_spans
        WINDOW hearing_window AS (
            PARTITION BY state_code, person_id
            ORDER BY hearing_scheduled_date ASC
        )
    )
    ,
    critical_date_spans AS (
    
        SELECT
            h.state_code,
            h.person_id,
            h.hearing_scheduled_date AS start_datetime,
            /* Someone is overdue if their next hearing date is past their next review date (or never occurs), and they 
            are no longer overdue once they've had their next hearing or are released from Restrictive Housing. */
            LEAST({nonnull_end_date_clause('h.next_hearing_scheduled_date')}, {nonnull_end_date_clause("c.end_date")}) AS end_datetime,
            h.next_review_date AS critical_date,
            due_date_inferred,
        FROM hearing_spans h
        LEFT JOIN `{{project_id}}.sessions.us_mo_confinement_type_sessions_materialized` c
        ON
            h.person_id = c.person_id
            AND h.hearing_scheduled_date >= c.start_date
            AND h.hearing_scheduled_date < c.end_date
            -- TODO(#23550): Use ingested enums once MO housing_unit_type is ingested
            AND c.confinement_type IN ("SOLITARY_CONFINEMENT")
    )
    ,
    -- Add 1-day lag so that someone is overdue after, but not on, the "next review date"
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
    description=_DESCRIPTION,
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="next_review_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Next Restrictive Housing review date (hearing) for the person.",
        ),
        ReasonsField(
            name="due_date_inferred",
            type=bigquery.enums.SqlTypeNames.BOOL,
            description="Whether the due date is inferred based on the start date of the RH assignment, or not (it is scheduled).",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
