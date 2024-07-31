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
TODO(#26722): Deprecate once new opportunities are live.
"""
from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
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

_CRITERIA_NAME = "US_MO_OVERDUE_FOR_HEARING"

_DESCRIPTION = """Describes the spans of time during which someone in MO
is overdue for a Restrictive Housing hearing.
"""

US_MO_CONFINEMENT_START_AFTER_HEARING_DAYS = 7

_QUERY_TEMPLATE = f"""
    WITH hearings AS (
        SELECT
            state_code,
            person_id,
            hearing_date,
            next_review_date,
        FROM `{{project_id}}.{{analyst_views_dataset}}.us_mo_classification_hearings_preprocessed_materialized`
    )
    ,
    -- Someone has been in solitary confinement less than 7 days, and they haven't had a hearing yet.
    tasc_hearing_spans AS (
        SELECT
            c.state_code,
            c.person_id,
            /* For the purposes of identifying when a hearing is "due", treat the 
            beginning of a Restrictive Housing assignment as a hearing */
            c.start_date as hearing_date,
            /* Someone is eligible for an initial hearing when they have been assigned
            to solitary confinement but haven't yet had a hearing. They are overdue
            once 7 days have passed. */
            DATE_ADD(c.start_date, INTERVAL 7 DAY) as next_review_date,
        FROM `{{project_id}}.{{sessions_dataset}}.us_mo_confinement_type_sessions_materialized` c
        /* Find any hearing *before* (or on the day of) the initial Restrictive Housing 
        assignment, if that hearing assigned a next review date coming *after* the assignment */
        LEFT JOIN hearings current_hearing_due_span
        ON
            c.person_id = current_hearing_due_span.person_id
            AND c.state_code = current_hearing_due_span.state_code
            AND (
                /* A due date for an initial hearing is not inferred if either a hearing
                is already scheduled upon assignment, or a hearing occurs the day of assignment
                (regardless of whether the next hearing is scheduled at that hearing). */
                (
                    current_hearing_due_span.hearing_date < c.start_date
                    AND
                    current_hearing_due_span.next_review_date >= c.start_date
                )
                OR current_hearing_due_span.hearing_date = c.start_date
            )
        WHERE 
            -- TODO(#21788): Use ingested enums once MO housing_unit_type is ingested
            confinement_type IN ("SOLITARY_CONFINEMENT")
            /* Filter out the beginning of Restrictive Housing assignments in cases
            where there has already been a hearing scheduled (this may happen if
            someone's hearing occurs before they're transferred to Restrictive
            Housing), or if a hearing occurred on the day of assignment. */
            AND current_hearing_due_span.hearing_date IS NULL
    )
    ,
    hearings_and_tasc_spans AS (
        SELECT 
            *, 
            "scheduled" AS due_date_type,
        FROM hearings 
        
        UNION ALL 
        
        SELECT 
            *,
            "inferred_from_tasc_assignment" AS due_date_type,
        FROM tasc_hearing_spans
    )
    ,
    hearing_spans AS (
        SELECT
            state_code,
            person_id,
            hearing_date,
            {nonnull_end_date_clause(column_name="LEAD(hearing_date) OVER hearing_window")} AS next_hearing_date,
            next_review_date,
            due_date_type,
        FROM hearings_and_tasc_spans
        WINDOW hearing_window AS (
            PARTITION BY state_code, person_id
            ORDER BY hearing_date ASC
        )
    )
    ,
    critical_date_spans AS (
    
        SELECT
            h.state_code,
            h.person_id,
            h.hearing_date AS start_datetime,
            /* Someone is overdue if their next hearing date is past their next review date (or never occurs), and they 
            are no longer overdue once they've had their next hearing or are released from Restrictive Housing. */
            LEAST(h.next_hearing_date, {nonnull_end_date_clause("c.end_date")}) AS end_datetime,
            h.next_review_date AS critical_date,
            due_date_type,
        FROM hearing_spans h
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.us_mo_confinement_type_sessions_materialized` c
        ON
            h.person_id = c.person_id
            AND h.hearing_date >= c.start_date
            AND h.hearing_date < c.end_date
            -- TODO(#23550): Use ingested enums once MO housing_unit_type is ingested
            AND c.confinement_type IN ("SOLITARY_CONFINEMENT")
    )
    ,
    -- Add 1-day lag so that someone is overdue after, but not on, the "next review date"
    {critical_date_has_passed_spans_cte(meets_criteria_leading_window_time=-1, attributes=["due_date_type"])}
    SELECT 
        state_code,
        person_id,
        start_date,
        -- Set current span's end date to null
        IF(end_date > CURRENT_DATE('US/Pacific'), NULL, end_date) AS end_date,
        critical_date_has_passed AS meets_criteria,
        TO_JSON(STRUCT(
            critical_date as next_review_date,
            due_date_type as due_date_type
        )) AS reason,
        critical_date as next_review_date,
        due_date_type as due_date_type,
    FROM critical_date_has_passed_spans
    -- Exclude spans that start in the future
    WHERE start_date <= CURRENT_DATE('US/Pacific')
"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_MO,
    criteria_name=_CRITERIA_NAME,
    criteria_spans_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    meets_criteria_default=False,
    analyst_views_dataset=ANALYST_VIEWS_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    reasons_fields=[
        ReasonsField(
            name="next_review_date",
            type=bigquery.enums.SqlTypeNames.DATE,
            description="Next Restrictive Housing review date (hearing) for the person.",
        ),
        ReasonsField(
            name="due_date_type",
            type=bigquery.enums.SqlTypeNames.STRING,
            description="Whether the due date is inferred based on the start date of the RH assignment, or not (it is scheduled).",
        ),
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
