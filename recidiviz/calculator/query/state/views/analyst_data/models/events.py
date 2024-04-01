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
"""Configures event query builder objects at the person-level."""

from typing import List

from recidiviz.calculator.query.sessions_query_fragments import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.views.analyst_data.models.event_query_builder import (
    EventQueryBuilder,
)
from recidiviz.calculator.query.state.views.analyst_data.models.event_type import (
    EventType,
)
from recidiviz.calculator.query.state.views.sessions.absconsion_bench_warrant_sessions import (
    ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER,
)


def get_task_eligible_event_query_builder(
    event_type: EventType, days_overdue: int
) -> EventQueryBuilder:
    """
    Returns an EventQueryBuilder with the specified EventType that represents the date when a
    task-eligible client has been overdue for an opportunity by greater than `days_overdue` days.
    If `days_overdue` is set to 0, returns an event on the date that eligibility began.
    """
    description_add_on = f" past {days_overdue} days ago" if days_overdue > 0 else ""
    date_condition_query_str = (
        f"""
        AND LEAST(
                IFNULL(a.end_date, "9999-01-01"),
                "9999-01-01"
            ) > DATE_ADD(a.start_date, INTERVAL {days_overdue} DAY)
    """
        if days_overdue > 0
        else ""
    )
    return EventQueryBuilder(
        event_type=event_type,
        description=f"Opportunity eligibility starts{description_add_on}",
        sql_source=f"""
    SELECT
        a.state_code,
        a.person_id,
        DATE_ADD(a.start_date, INTERVAL {days_overdue} DAY) AS overdue_date,
        a.task_name,
        b.completion_event_type AS task_type,
        COALESCE(c.surfaced, FALSE) AS after_tool_action,
        -- Flag if person was previously almost eligible before becoming fully eligible
        COALESCE(
            LAG(a.is_almost_eligible) OVER (PARTITION BY a.person_id, a.task_name ORDER BY a.start_date),
            FALSE
        ) AS after_almost_eligible,
    FROM
        `{{project_id}}.analyst_data.all_task_eligibility_spans_materialized` a
    INNER JOIN
        `{{project_id}}.reference_views.task_to_completion_event` b
    USING
        (task_name)
    -- Get information about impact funnel status.
    -- We convert the eligibility start date to a timestamp having the last time (23:59:59) on that date,
    -- to account for any usage events that may have occurred on the same date.
    LEFT JOIN
        `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` c
    ON
        a.person_id = c.person_id
        AND b.completion_event_type = c.task_type
        AND DATETIME_SUB(DATETIME(DATE_ADD(a.start_date, INTERVAL 1 DAY)), INTERVAL 1 SECOND)
            BETWEEN c.start_date AND {nonnull_end_date_exclusive_clause("c.end_date")}
    WHERE
        -- remove any spans that start after the current millennium, e.g.
        -- eligibility following a life sentence
        a.start_date < "3000-01-01"
        AND a.is_eligible{date_condition_query_str}
    """,
        attribute_cols=[
            "task_name",
            "task_type",
            "after_tool_action",
            "after_almost_eligible",
        ],
        event_date_col="overdue_date",
    )


EVENTS: List[EventQueryBuilder] = [
    EventQueryBuilder(
        event_type=EventType.ABSCONSION_BENCH_WARRANT,
        description="Transition to absconsion or bench warrant status",
        sql_source=ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER.table_for_query,
        attribute_cols=[],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.COMPARTMENT_LEVEL_2_START,
        description="Transitions to a new incarceration or supervision compartment level 2",
        sql_source="""SELECT *
FROM
    `{project_id}.sessions.compartment_sessions_materialized`
WHERE
    compartment_level_1 IN ("INCARCERATION", "SUPERVISION")
""",
        attribute_cols=[
            "compartment_level_1",
            "compartment_level_2",
            "inflow_from_level_1",
            "inflow_from_level_2",
            "start_reason",
            "start_sub_reason",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.CUSTODY_LEVEL_CHANGE,
        description="Custody level changes",
        sql_source="""SELECT *,
    IF(custody_downgrade > 0, "DOWNGRADE", "UPGRADE") AS change_type,
    custody_level AS new_custody_level,
FROM
    `{project_id}.sessions.custody_level_sessions_materialized`
WHERE
    custody_downgrade > 0 OR custody_upgrade > 0
""",
        attribute_cols=[
            "change_type",
            "previous_custody_level",
            "new_custody_level",
            "custody_level_num_change",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.DRUG_SCREEN,
        description="Drug screen with a non-null result",
        sql_source=f"""
SELECT
    d.state_code,
    d.person_id,
    drug_screen_date,
    is_positive_result,
    substance_detected,
    d.is_positive_result
        AND ROW_NUMBER() OVER (
            PARTITION BY
                sss.person_id, sss.supervision_super_session_id, is_positive_result
            ORDER BY drug_screen_date
        ) = 1 AS is_initial_within_supervision_super_session,
FROM
    `{{project_id}}.sessions.drug_screens_preprocessed_materialized` d
INNER JOIN
    `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
ON
    d.person_id = sss.person_id
    AND d.drug_screen_date BETWEEN sss.start_date AND {nonnull_end_date_exclusive_clause("sss.end_date_exclusive")}
WHERE
    is_positive_result IS NOT NULL
""",
        attribute_cols=[
            "is_positive_result",
            "substance_detected",
            "is_initial_within_supervision_super_session",
        ],
        event_date_col="drug_screen_date",
    ),
    EventQueryBuilder(
        event_type=EventType.EARLY_DISCHARGE_REQUEST,
        description="Valid early discharge requests, deduplicated to one per day regardless of decision",
        sql_source="""SELECT *
FROM
    `{project_id}.normalized_state.state_early_discharge`
WHERE
    decision_status != "INVALID"
    AND request_date IS NOT NULL""",
        attribute_cols=[],
        event_date_col="request_date",
    ),
    EventQueryBuilder(
        event_type=EventType.EARLY_DISCHARGE_REQUEST_DECISION,
        description="Valid early discharge request decisions, with one event per person-day-decision_type",
        sql_source="""SELECT *
FROM
    `{project_id}.normalized_state.state_early_discharge`
WHERE
    decision_status != "INVALID"
    AND decision_date IS NOT NULL""",
        attribute_cols=["decision"],
        event_date_col="decision_date",
    ),
    EventQueryBuilder(
        event_type=EventType.EMPLOYMENT_PERIOD_START,
        description="Employment period starts, keeping at most one employer per period start",
        sql_source="""SELECT
    *
FROM
    `{project_id}.normalized_state.state_employment_period`
WHERE
    start_date IS NOT NULL
    AND employment_status != "UNEMPLOYED"
QUALIFY
    -- max one employer per day
    ROW_NUMBER() OVER (
        PARTITION BY person_id, start_date ORDER BY employer_name ASC
    ) = 1""",
        attribute_cols=["employer_name"],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.EMPLOYMENT_STATUS_CHANGE,
        description="Employment status changes",
        sql_source="""SELECT *
FROM `{project_id}.sessions.supervision_employment_status_sessions_materialized`
QUALIFY
    # only keep transitions where the person gained or lost employment
    # edge case: treat jobs at supervision start as employment gains
    # but no job at supervision start is not an employment loss
    is_employed != IFNULL(LAG(is_employed) OVER (
        PARTITION BY person_id ORDER BY employment_status_start_date
    ), FALSE)""",
        attribute_cols=["is_employed"],
        event_date_col="employment_status_start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.INCARCERATION_RELEASE,
        description="Releases from incarceration to supervision or liberty",
        sql_source=f"""WITH incarceration_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        compartment_level_2,
        inflow_from_level_1,
        inflow_from_level_2,
        outflow_to_level_1,
        outflow_to_level_2,
        -- getting proportion of sentence days
        DATE_DIFF(end_date_exclusive, start_date, DAY) AS days_served,
    FROM
        `{{project_id}}.sessions.compartment_sessions_materialized`
    WHERE
        compartment_level_1 = "INCARCERATION"
        AND outflow_to_level_1 IN ("LIBERTY", "SUPERVISION")
)
,
sentence_deadline_spans AS (
    SELECT
        sentence_span_start.state_code,
        sentence_span_start.person_id,
        sentence_span_start.start_date,
        sentence_span_start.end_date_exclusive,
        MAX(task_deadlines.projected_incarceration_release_date) AS projected_incarceration_release_snapshot_date,
        MAX(task_deadlines.parole_eligibility_date) AS parole_eligibility_snapshot_date,
    FROM
        `{{project_id}}.sessions.sentence_spans_materialized` sentence_span_start,
        UNNEST(sentence_deadline_id_array) AS sentence_deadline_id
    LEFT JOIN
        `{{project_id}}.sessions.sentence_deadline_spans_materialized` task_deadlines
    USING
        (person_id, state_code, sentence_deadline_id)
    GROUP BY 1, 2, 3, 4
)
SELECT
    sessions.state_code,
    sessions.person_id,
    sessions.start_date,
    sessions.end_date_exclusive,

    -- Projected release date as of the start of incarceration session
    sentence_span_start.parole_eligibility_snapshot_date AS original_parole_eligibility_date,
    sentence_span_start.projected_incarceration_release_snapshot_date AS original_projected_release_date,

    -- Projected release date as of the end of incarceration session
    sentence_span_end.parole_eligibility_snapshot_date AS updated_parole_eligibility_date,
    sentence_span_end.projected_incarceration_release_snapshot_date AS updated_projected_release_date,

    sessions.compartment_level_2,
    sessions.inflow_from_level_1,
    sessions.inflow_from_level_2,
    sessions.outflow_to_level_1,
    sessions.outflow_to_level_2,

    --- checks if parole was delayed by a month
    CAST(DATE_ADD(sentence_span_end.parole_eligibility_snapshot_date, INTERVAL 1 MONTH) <=
        sessions.end_date_exclusive AS STRING) AS parole_release_1_month_flag,

    -- calculates proportion of days served relative to sentence snapshot
    sessions.days_served,
    IF(DATE_DIFF(
        sentence_span_start.projected_incarceration_release_snapshot_date,
        sessions.start_date, DAY
    ) < 0, NULL, DATE_DIFF(
        sentence_span_start.projected_incarceration_release_snapshot_date,
        sessions.start_date, DAY
    )) AS days_sentenced,
    SAFE_DIVIDE(
        sessions.days_served,
        DATE_DIFF(
            sentence_span_start.projected_incarceration_release_snapshot_date,
            sessions.start_date, DAY
        )
    ) AS prop_sentence_served,
FROM
    incarceration_sessions sessions
LEFT JOIN
    sentence_deadline_spans sentence_span_start
ON
    sessions.person_id = sentence_span_start.person_id
    AND sessions.start_date BETWEEN sentence_span_start.start_date AND {nonnull_end_date_exclusive_clause("sentence_span_start.end_date_exclusive")}
LEFT JOIN
    sentence_deadline_spans sentence_span_end
ON
    sessions.person_id = sentence_span_end.person_id
    AND sessions.end_date_exclusive BETWEEN sentence_span_end.start_date AND {nonnull_end_date_exclusive_clause("sentence_span_end.end_date_exclusive")}

-- Joining with incarceration_projected_completion_date_spans_materialized table
LEFT JOIN
    `{{project_id}}.task_eligibility_criteria_general.incarceration_past_full_term_completion_date_materialized` projected_completion_dates
ON
    sessions.person_id = projected_completion_dates.person_id
    AND DATE_SUB(sessions.end_date_exclusive, INTERVAL 1 DAY) BETWEEN
        projected_completion_dates.start_date AND {nonnull_end_date_exclusive_clause("projected_completion_dates.end_date")}

""",
        attribute_cols=[
            "start_date",
            "original_parole_eligibility_date",
            "original_projected_release_date",
            "updated_parole_eligibility_date",
            "updated_projected_release_date",
            "compartment_level_2",
            "inflow_from_level_1",
            "inflow_from_level_2",
            "outflow_to_level_1",
            "outflow_to_level_2",
            "parole_release_1_month_flag",
            "days_sentenced",
            "days_served",
            "prop_sentence_served",
        ],
        event_date_col="end_date_exclusive",
    ),
    EventQueryBuilder(
        event_type=EventType.INCARCERATION_START,
        description="Transitions to incarceration",
        sql_source=f"""SELECT
    a.state_code,
    a.person_id,
    a.start_date,
    a.inflow_from_level_1,
    a.inflow_from_level_2,
    a.start_reason,
    -- Returning to incarceration for the weekend is not at an officer's discretion, but usually pre-determined by a
    -- judge, parole board, etc
    CASE WHEN a.start_reason IN ('WEEKEND_CONFINEMENT') THEN FALSE 
         ELSE TRUE 
         END AS is_discretionary,
    -- Get the first non-null violation type among incarceration starts occurring during the super session
    COALESCE(d.most_severe_violation_type, "INTERNAL_UNKNOWN") AS most_severe_violation_type,
    viol.violation_date AS most_severe_violation_date,
    COALESCE(JSON_EXTRACT_SCALAR(viol.violation_metadata, '$.ViolationType'), '') = 'INFERRED' as violation_is_inferred,
    COUNT(DISTINCT c.referral_date)
        OVER (PARTITION BY a.person_id, a.start_date) AS prior_treatment_referrals_1y,
    css_active.compartment_level_2 AS latest_active_supervision_type,
    css_active.correctional_level_end AS latest_active_supervision_level,
    asmt.assessment_level AS latest_assessment_level,
FROM
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` a
-- Get treatment referrals within 1 year of incarceration
LEFT JOIN
    `{{project_id}}.normalized_state.state_program_assignment` c
ON
    a.person_id = c.person_id
    AND DATE_SUB(a.start_date, INTERVAL 365 DAY) <= c.referral_date
-- Get all incarceration commitments during the super session
LEFT JOIN
    `{{project_id}}.dataflow_metrics_materialized.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized` d
ON
    a.person_id = d.person_id
    AND d.admission_date BETWEEN a.start_date AND {nonnull_end_date_clause("a.end_date")}
LEFT JOIN
    `{{project_id}}.normalized_state.state_supervision_violation` viol
ON
    d.person_id = viol.person_id
    AND d.most_severe_violation_id = viol.supervision_violation_id
LEFT JOIN
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` s
ON s.person_id = a.person_id
  --join to the previous super session preceding the incarceration start if it's a supervision super session  
    AND a.compartment_level_1_super_session_id = s.compartment_level_1_super_session_id + 1 
    AND s.compartment_level_1 = "SUPERVISION"
-- This join with compartment sessions brings active compartments from the previous SUPERVISION super session
LEFT JOIN 
    `{{project_id}}.sessions.compartment_sessions_materialized` css_active
ON css_active.person_id = s.person_id
    AND css_active.session_id BETWEEN s.session_id_start AND s.session_id_end
    --only keep active compartment level 2 values
    AND css_active.compartment_level_2 IN ("PAROLE", "PROBATION", "INFORMAL_PROBATION", "DUAL", "COMMUNITY_CONFINEMENT") 
    --the incarceration start should be after the active compartment starts 
    AND a.start_date >= css_active.start_date
LEFT JOIN
    `{{project_id}}.sessions.system_sessions_materialized` ss
ON
    a.person_id = ss.person_id
    AND css_active.session_id BETWEEN ss.session_id_start AND ss.session_id_end
LEFT JOIN 
    `{{project_id}}.sessions.assessment_score_sessions_materialized` asmt
ON
    asmt.person_id = a.person_id
    -- Pull assessment session during incarceration start
    AND a.start_date BETWEEN asmt.assessment_date AND {nonnull_end_date_exclusive_clause("asmt.score_end_date")}    
    -- Ensure assessment is within the same system session
    AND asmt.assessment_date BETWEEN ss.start_date AND {nonnull_end_date_exclusive_clause("ss.end_date_exclusive")}
    -- Filter out incarceration-only assessments
    AND NOT asmt.is_incarceration_only_assessment_type
WHERE
    a.compartment_level_1 = "INCARCERATION"
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.person_id, a.start_date
    -- Prioritize the first known violation type and the latest active supervision type 
    ORDER BY
        IF(COALESCE(d.most_severe_violation_type, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN", 0, 1),
        d.admission_date,
        css_active.start_date DESC,
        asmt.assessment_date DESC 
) = 1""",
        attribute_cols=[
            "inflow_from_level_1",
            "inflow_from_level_2",
            "start_reason",
            "most_severe_violation_date",
            "most_severe_violation_type",
            "violation_is_inferred",
            "prior_treatment_referrals_1y",
            "is_discretionary",
            "latest_active_supervision_type",
            "latest_active_supervision_level",
            "latest_assessment_level",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.INCARCERATION_START_TEMPORARY,
        description="Transitions to temporary incarceration",
        sql_source="""SELECT *, COALESCE(start_sub_reason, "INTERNAL_UNKNOWN") AS most_severe_violation_type,
FROM
    `{project_id}.sessions.compartment_sessions_materialized`
WHERE
    compartment_level_1 = "INCARCERATION"
    AND compartment_level_2 IN (
        "PAROLE_BOARD_HOLD", "PENDING_CUSTODY", "TEMPORARY_CUSTODY", "SUSPENSION",
        "SHOCK_INCARCERATION"
    )
    -- Exclude transitions between temporary incarceration periods
    AND (
        inflow_from_level_1 != "INCARCERATION"
        OR inflow_from_level_2 NOT IN (
            "PAROLE_BOARD_HOLD", "PENDING_CUSTODY", "TEMPORARY_CUSTODY", "SUSPENSION",
            "SHOCK_INCARCERATION"
        )
    )""",
        attribute_cols=[
            "compartment_level_2",
            "inflow_from_level_1",
            "inflow_from_level_2",
            "start_reason",
            "most_severe_violation_type",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.LIBERTY_START,
        description="Transitions to liberty",
        sql_source="""SELECT *
FROM `{project_id}.sessions.compartment_level_1_super_sessions_materialized`
WHERE compartment_level_1 = "LIBERTY"
""",
        attribute_cols=[
            "inflow_from_level_1",
            "inflow_from_level_2",
            "start_reason",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.PAROLE_HEARING,
        description="Parole board hearings",
        sql_source="""SELECT *
FROM `{project_id}.sessions.parole_board_hearing_sessions_materialized`
""",
        attribute_cols=["decision", "days_since_incarceration_start"],
        event_date_col="hearing_date",
    ),
    EventQueryBuilder(
        event_type=EventType.PENDING_CUSTODY_START,
        description="Transitions to pending custody status",
        sql_source="""SELECT *
FROM `{project_id}.sessions.compartment_level_1_super_sessions_materialized`
WHERE compartment_level_1 = "PENDING_CUSTODY"
""",
        attribute_cols=[
            "inflow_from_level_1",
            "inflow_from_level_2",
            "start_reason",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.RISK_SCORE_ASSESSMENT,
        description="Risk assessments",
        sql_source=f"""SELECT *,
    IFNULL(assessment_score_change, NULL) > 0 AS assessment_score_increase,
    IFNULL(assessment_score_change, NULL) < 0 AS assessment_score_decrease,
FROM (
    SELECT
        a.state_code,
        a.person_id,
        assessment_type,
        assessment_date,
        assessment_score,
        # assessment score change within the same SSS
        assessment_score - LAG(assessment_score) OVER (PARTITION BY
            a.state_code, a.person_id, assessment_type, sss.start_date
            ORDER BY assessment_date
        ) AS assessment_score_change,
    FROM
        `{{project_id}}.sessions.assessment_score_sessions_materialized` a
    LEFT JOIN
        `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
    ON
        a.state_code = sss.state_code
        AND a.person_id = sss.person_id
        AND a.assessment_date BETWEEN sss.start_date AND {nonnull_end_date_exclusive_clause("sss.end_date_exclusive")}
    WHERE
        assessment_score IS NOT NULL
        AND assessment_type IS NOT NULL
)
""",
        attribute_cols=[
            "assessment_type",
            "assessment_score",
            "assessment_score_change",
            "assessment_score_increase",
            "assessment_score_decrease",
        ],
        event_date_col="assessment_date",
    ),
    EventQueryBuilder(
        event_type=EventType.SENTENCES_IMPOSED,
        description="Sentences imposed",
        sql_source="""SELECT * FROM `{project_id}.sessions.sentence_imposed_group_summary_materialized`
""",
        attribute_cols=[
            "max_sentence_imposed_group_length_days",
            "projected_completion_date_max",
            "projected_completion_date_min",
            "any_is_drug_uniform",
            "any_is_violent_uniform",
            "most_severe_classification_type",
            "most_severe_classification_subtype",
            "most_severe_description",
        ],
        event_date_col="date_imposed",
    ),
    EventQueryBuilder(
        event_type=EventType.SUPERVISING_OFFICER_CHANGE,
        description="Change in supervision officer, including initial assignment to officer; "
        "can happen multiple times per person-day",
        sql_source="""SELECT
    * EXCEPT(supervising_officer_external_id),
    supervising_officer_external_id AS supervising_officer_external_id_new,
    LAG(supervising_officer_external_id) OVER (
        PARTITION BY person_id ORDER BY start_date, supervising_officer_external_id
    ) AS supervising_officer_external_id_previous,
FROM
    `{project_id}.sessions.supervision_officer_sessions_materialized`
QUALIFY
    -- ORDER BY includes officer_id to make ordering deterministic, in the
    -- (rare) case multiple officers start on same day
    COALESCE(
        LAG(supervising_officer_external_id) OVER (PARTITION BY person_id
        ORDER BY start_date, supervising_officer_external_id), "UNKNOWN"
    ) != COALESCE(supervising_officer_external_id, "UNKNOWN")

""",
        attribute_cols=[
            "supervising_officer_external_id_new",
            "supervising_officer_external_id_previous",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.SUPERVISION_CONTACT,
        description="Supervision contacts, keeping one contact per person-day-event_attributes",
        sql_source="""SELECT *
FROM
    `{project_id}.normalized_state.state_supervision_contact`
""",
        attribute_cols=["contact_type", "location", "status"],
        event_date_col="contact_date",
    ),
    EventQueryBuilder(
        event_type=EventType.SUPERVISION_LEVEL_CHANGE,
        description="Supervision level changes",
        sql_source="""SELECT *,
    IF(supervision_downgrade > 0, "DOWNGRADE", "UPGRADE") AS change_type,
    supervision_level AS new_supervision_level,
FROM
    `{project_id}.sessions.supervision_level_sessions_materialized`
WHERE
    supervision_downgrade > 0 OR supervision_upgrade > 0
""",
        attribute_cols=[
            "change_type",
            "previous_supervision_level",
            "new_supervision_level",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.SUPERVISION_RELEASE,
        description="Releases from supervision to liberty",
        sql_source=f"""WITH supervision_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        compartment_level_2,
        inflow_from_level_1,
        inflow_from_level_2,
        outflow_to_level_1,
        -- getting proportion of sentence days
        DATE_DIFF(end_date_exclusive, start_date, DAY) AS days_served,
    FROM
        `{{project_id}}.sessions.compartment_sessions_materialized`
    WHERE
        compartment_level_1 = "SUPERVISION"
        AND outflow_to_level_1 = "LIBERTY"
)
,
sentence_deadline_spans AS (
    SELECT
        sentence_span_start.state_code,
        sentence_span_start.person_id,
        sentence_span_start.start_date,
        sentence_span_start.end_date_exclusive,
        MAX(task_deadlines.projected_supervision_release_date) AS projected_supervision_release_snapshot_date,
    FROM
        `{{project_id}}.sessions.sentence_spans_materialized` sentence_span_start,
        UNNEST(sentence_deadline_id_array) AS sentence_deadline_id
    LEFT JOIN
        `{{project_id}}.sessions.sentence_deadline_spans_materialized` task_deadlines
    USING
        (person_id, state_code, sentence_deadline_id)
    GROUP BY 1, 2, 3, 4
)

SELECT
    sessions.state_code,
    sessions.person_id,
    sessions.start_date,
    sessions.end_date_exclusive,
    -- Projected release date as of the start of supervision session
    sentence_span_start.projected_supervision_release_snapshot_date AS original_projected_release_date,
    -- Projected release date as of the end of supervision session
    sentence_span_end.projected_supervision_release_snapshot_date AS updated_projected_release_date,
    sessions.compartment_level_2,
    sessions.inflow_from_level_1,
    sessions.inflow_from_level_2,
    sessions.outflow_to_level_1,

    -- calculate proportion of days served relative to sentence snapshot
    sessions.days_served,
    IF(DATE_DIFF(
        sentence_span_start.projected_supervision_release_snapshot_date,
        sessions.start_date, DAY
    ) < 0, NULL, DATE_DIFF(
        sentence_span_start.projected_supervision_release_snapshot_date,
        sessions.start_date, DAY
    )) AS days_sentenced,
    SAFE_DIVIDE(
        sessions.days_served,
        DATE_DIFF(
            sentence_span_start.projected_supervision_release_snapshot_date,
            sessions.start_date, DAY
        )
    ) AS prop_sentence_served,
FROM
    supervision_sessions sessions
LEFT JOIN
    sentence_deadline_spans sentence_span_start
ON
    sessions.person_id = sentence_span_start.person_id
    AND sessions.start_date BETWEEN sentence_span_start.start_date AND {nonnull_end_date_exclusive_clause("sentence_span_start.end_date_exclusive")}
LEFT JOIN
    sentence_deadline_spans sentence_span_end
ON
    sessions.person_id = sentence_span_end.person_id
    AND sessions.end_date_exclusive BETWEEN sentence_span_end.start_date AND {nonnull_end_date_exclusive_clause("sentence_span_end.end_date_exclusive")}
""",
        attribute_cols=[
            "start_date",
            "original_projected_release_date",
            "updated_projected_release_date",
            "compartment_level_2",
            "inflow_from_level_1",
            "inflow_from_level_2",
            "outflow_to_level_1",
            "days_sentenced",
            "days_served",
            "prop_sentence_served",
        ],
        event_date_col="end_date_exclusive",
    ),
    EventQueryBuilder(
        event_type=EventType.SUPERVISION_START,
        description="Transitions to supervision",
        sql_source="""SELECT *
FROM `{project_id}.sessions.compartment_level_1_super_sessions_materialized`
WHERE compartment_level_1 = "SUPERVISION"
""",
        attribute_cols=[
            "outflow_to_level_1",
            "inflow_from_level_1",
            "inflow_from_level_2",
            "start_reason",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON,
        description="Supervision terminations with an end reason indicating subsequent incarceration, but"
        "without an observed compartment outflow to incarceration",
        sql_source=f"""SELECT
    a.state_code,
    a.person_id,
    a.end_date_exclusive,
    a.compartment_level_1,
    a.compartment_level_2,
    a.end_reason,
    a.correctional_level_end AS previous_supervision_level,
    TRUE AS is_discretionary,
    -- Get the first non-null violation type among supervision terminations occurring during the super session
    COALESCE(c.most_severe_violation_type, "INTERNAL_UNKNOWN") AS most_severe_violation_type,
    COALESCE(JSON_EXTRACT_SCALAR(viol.violation_metadata, '$.ViolationType'), '') = 'INFERRED' as violation_is_inferred,
    viol.violation_date AS most_severe_violation_date,
    c.termination_date AS most_severe_violation_type_termination_date,
    COUNT(DISTINCT d.referral_date)
        OVER (PARTITION BY a.person_id, a.end_date_exclusive) AS prior_treatment_referrals_1y,
    css.compartment_level_2 AS latest_active_supervision_type,
    css.correctional_level_end AS latest_active_supervision_level,
    asmt.assessment_level AS latest_assessment_level,
FROM
    `{{project_id}}.sessions.compartment_sessions_materialized` a
LEFT JOIN
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` b
ON
    a.person_id = b.person_id
    -- If person outflows to another supervision compartment, this join will consider all terminations
    -- during the remainder of that supervision compartment super session. Otherwise,
    -- it takes the next compartment level 1 super session.
    AND a.end_date_exclusive BETWEEN b.start_date AND {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
LEFT JOIN
    `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` s
ON
    a.person_id = s.person_id
    -- get the compartment_level_1_super_session that includes the supervision compartment that ends unsuccessfully
    AND a.end_date BETWEEN s.start_date AND {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
LEFT JOIN 
    `{{project_id}}.sessions.compartment_sessions_materialized` css
ON css.person_id = s.person_id
    AND css.session_id BETWEEN s.session_id_start AND s.session_id_end
    --only join active compartment level 2 values 
    AND css.compartment_level_2 IN ("PAROLE", "PROBATION", "INFORMAL_PROBATION", "DUAL", "COMMUNITY_CONFINEMENT")
    --the unsuccessful termination should be after the active compartment starts 
    AND a.end_date >= css.start_date
LEFT JOIN
    `{{project_id}}.dataflow_metrics_materialized.most_recent_supervision_termination_metrics_materialized` c
ON
    a.person_id = c.person_id
    AND c.termination_date BETWEEN a.end_date_exclusive AND {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
LEFT JOIN
    `{{project_id}}.normalized_state.state_supervision_violation` viol
ON
    c.person_id = viol.person_id
    AND c.most_severe_violation_id = viol.supervision_violation_id
-- Referrals within one year of supervision termination
LEFT JOIN
    `{{project_id}}.normalized_state.state_program_assignment` d
ON
    a.person_id = d.person_id
    AND DATE_SUB(a.end_date_exclusive, INTERVAL 365 DAY) <= d.referral_date
LEFT JOIN
    `{{project_id}}.sessions.system_sessions_materialized` ss
ON
    a.person_id = ss.person_id
    AND a.session_id BETWEEN ss.session_id_start AND ss.session_id_end
LEFT JOIN 
    `{{project_id}}.sessions.assessment_score_sessions_materialized` asmt
ON
    asmt.person_id = a.person_id
    -- Pull assessment session during supervision termination
    AND a.end_date_exclusive BETWEEN asmt.assessment_date AND {nonnull_end_date_exclusive_clause("asmt.score_end_date")}
    -- Ensure assessment is within the same system session
    AND asmt.assessment_date BETWEEN ss.start_date AND {nonnull_end_date_exclusive_clause("ss.end_date_exclusive")}
    -- Filter out incarceration-only assessments
    AND NOT asmt.is_incarceration_only_assessment_type
WHERE
    a.compartment_level_1 = "SUPERVISION"
    AND a.end_reason IN ("ADMITTED_TO_INCARCERATION", "REVOCATION")
    AND a.outflow_to_level_1 != "INCARCERATION"
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.person_id, a.end_date_exclusive
    -- Prioritize the first known violation type and the latest active supervision type 
    ORDER BY 
        IF(COALESCE(c.most_severe_violation_type, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN", 0, 1), 
        c.termination_date,
        css.start_date DESC,
        asmt.assessment_date DESC
) = 1
""",
        attribute_cols=[
            "compartment_level_1",
            "compartment_level_2",
            "end_reason",
            "violation_is_inferred",
            "most_severe_violation_date",
            "most_severe_violation_type",
            "most_severe_violation_type_termination_date",
            "prior_treatment_referrals_1y",
            "is_discretionary",
            "latest_active_supervision_type",
            "latest_active_supervision_level",
            "latest_assessment_level",
        ],
        event_date_col="end_date_exclusive",
    ),
    EventQueryBuilder(
        event_type=EventType.TASK_COMPLETED,
        description="Task completion events for all Workflows opportunities",
        sql_source=f"""
SELECT
    e.state_code,
    e.person_id,
    e.completion_event_date,
    e.completion_event_type AS task_type,
    t.is_eligible,
    IF(t.is_eligible, DATE_DIFF(e.completion_event_date, t.start_date, DAY), NULL) AS days_eligible,
    f.is_almost_eligible,
    -- Flag if someone has experienced at least one tool action before task completion
    COALESCE(f.surfaced, FALSE) AS after_tool_action,
FROM
    `{{project_id}}.task_eligibility.all_completion_events_materialized` e
-- Get information about continuous spans of eligibility
LEFT JOIN
    `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized` t
ON
    e.person_id = t.person_id
    AND e.completion_event_type = t.task_type
    AND e.completion_event_date BETWEEN t.start_date AND {nonnull_end_date_clause("t.end_date")}
-- Get information about impact funnel status.
-- We convert the completion event date to a timestamp having the last time (23:59:59) on that date,
-- to account for any usage events that may have occurred on the same date.
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` f
ON
    e.person_id = f.person_id
    AND e.completion_event_type = f.task_type
    AND DATETIME_SUB(DATETIME(DATE_ADD(e.completion_event_date, INTERVAL 1 DAY)), INTERVAL 1 SECOND)
        BETWEEN f.start_date AND {nonnull_end_date_clause("f.end_date")}
-- If completion event falls exactly on the border between two sessions, take the eligibility
-- attributes associated with the first span (i.e., the span that ended, rather than the span that started).
-- Uses RANK so that if there are multiple completion events for the same task type on the same day,
-- (e.g., multiple assessments), we don't drop these.
QUALIFY
    RANK() OVER (
        PARTITION BY e.person_id, e.completion_event_date, e.completion_event_type
        ORDER BY t.start_date, f.start_date
    ) = 1
""",
        attribute_cols=[
            "task_type",
            "is_eligible",
            "days_eligible",
            "is_almost_eligible",
            "after_tool_action",
        ],
        event_date_col="completion_event_date",
    ),
    get_task_eligible_event_query_builder(
        EventType.TASK_ELIGIBILITY_START, days_overdue=0
    ),
    get_task_eligible_event_query_builder(
        EventType.TASK_ELIGIBLE_7_DAYS, days_overdue=7
    ),
    get_task_eligible_event_query_builder(
        EventType.TASK_ELIGIBLE_30_DAYS, days_overdue=30
    ),
    EventQueryBuilder(
        event_type=EventType.TREATMENT_REFERRAL,
        description="Treatment referrals, keeping at most one per person per program-staff-status per day",
        sql_source="""SELECT *
FROM
    `{project_id}.normalized_state.state_program_assignment`
""",
        attribute_cols=[
            "program_id",
            "referring_staff_id",
            "referral_metadata",
            "participation_status",
        ],
        event_date_col="referral_date",
    ),
    EventQueryBuilder(
        event_type=EventType.TREATMENT_START,
        description="Treatment starts, keeping at most one per person per program-staff-status per day",
        sql_source="""SELECT *
FROM
    `{project_id}.normalized_state.state_program_assignment`
""",
        attribute_cols=[
            "program_id",
            "referring_staff_id",
            "referral_metadata",
            "participation_status",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.VARIANT_ASSIGNMENT,
        description="Assignment to a tracked experiment, with one variant assignment per person-day-experiment-variant",
        sql_source="""SELECT *
FROM
    `{project_id}.experiments.person_assignments_materialized`
""",
        attribute_cols=["experiment_id", "variant_id"],
        event_date_col="variant_date",
    ),
    EventQueryBuilder(
        event_type=EventType.INCARCERATION_INCIDENT,
        description="Incarceration incidents",
        sql_source="""
        SELECT
            state_code,
            person_id,
            incident_date,
            incident_class,
            injury_level,
            disposition,
            incident_type,
            incident_type_raw_text
        FROM
            `{project_id}.analyst_data.incarceration_incidents_preprocessed_materialized`
        """,
        attribute_cols=[
            "incident_class",
            "injury_level",
            "disposition",
            "incident_type",
            "incident_type_raw_text",
        ],
        event_date_col="incident_date",
    ),
    EventQueryBuilder(
        event_type=EventType.VIOLATION,
        description="Violations",
        sql_source=f"""SELECT
    v.state_code,
    v.person_id,
    IFNULL(violation_date, response_date) AS event_date,
    COALESCE(violation_type, "INTERNAL_UNKNOWN") AS violation_type,
    violation_type_subtype,
    violation_type_subtype_raw_text,
    most_severe_response_decision,
    is_most_severe_violation_type_of_all_violations AS is_most_severe_violation_type,
    -- Indicates that event_date is hydrated with response_date because violation_date is NULL
    violation_date IS NULL AS is_inferred_violation_date,
    -- Deduplicates to the earliest response date associated with the violation date
    #TODO(#27010) remove once support for custom units of analysis are implemented 
    v.gender,
    v.prioritized_race_or_ethnicity,
    MIN(response_date) AS response_date,
    -- This should already be unique since we're pulling from sub-sessions
    ANY_VALUE(css.correctional_level) AS supervision_level,
FROM
    `{{project_id}}.dataflow_metrics_materialized.most_recent_violation_with_response_metrics_materialized` v
LEFT JOIN
    `{{project_id}}.sessions.compartment_sub_sessions_materialized` css
ON v.person_id = css.person_id
AND IFNULL(violation_date, response_date) BETWEEN css.start_date AND {nonnull_end_date_exclusive_clause("css.end_date_exclusive")}
WHERE
    IFNULL(violation_date, response_date) IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11

""",
        attribute_cols=[
            "violation_type",
            "violation_type_subtype",
            "violation_type_subtype_raw_text",
            "most_severe_response_decision",
            "is_most_severe_violation_type",
            "response_date",
            "is_inferred_violation_date",
            "gender",
            "prioritized_race_or_ethnicity",
            "supervision_level",
        ],
        event_date_col="event_date",
    ),
    EventQueryBuilder(
        event_type=EventType.VIOLATION_RESPONSE,
        description="Violation responses, deduped per person-day",
        sql_source="""SELECT *
FROM
    `{project_id}.sessions.violation_responses_materialized`
WHERE
    most_severe_response_decision IS NOT NULL
""",
        attribute_cols=[
            "most_serious_violation_type",
            "most_serious_violation_sub_type",
            "most_severe_response_decision",
            "most_severe_sanction_level",
        ],
        event_date_col="response_date",
    ),
    EventQueryBuilder(
        event_type=EventType.WORKFLOWS_PERSON_USAGE_EVENT,
        description="Workflows usage event tracked at the client level",
        sql_source=f"""
SELECT
    a.*,
    -- Flag if this is the first tool action by checking if someone went from un-surfaced to surfaced at this datetime,
    COALESCE(
        b.surfaced AND NOT LAG(surfaced) OVER (
            PARTITION BY b.person_id, b.task_type
            ORDER BY b.start_date
        )
    , FALSE) AS is_first_tool_action,
    DATE_DIFF(a.start_date, c.start_date, DAY) days_eligible,
FROM
    `{{project_id}}.analyst_data.workflows_person_events_materialized` a
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` b
USING
    (person_id, task_type, start_date)
LEFT JOIN
    `{{project_id}}.analyst_data.all_task_type_eligibility_spans_materialized` c
ON
    a.person_id = c.person_id
    AND a.task_type = c.task_type
    AND c.is_eligible
    -- Cast to a date to account for date comparison with timestamps
    AND DATE(a.start_date) BETWEEN c.start_date AND {nonnull_end_date_exclusive_clause("c.end_date")}
""",
        attribute_cols=[
            "task_type",
            "usage_event_type",
            "is_first_tool_action",
            "days_eligible",
        ],
        event_date_col="start_date",
    ),
    EventQueryBuilder(
        event_type=EventType.WORKFLOWS_USER_ACTION,
        description="Event where the officer took a specific Workflows action, e.g., FORM_COPIED",
        sql_source=f"""
SELECT * 
FROM `{{project_id}}.analyst_data.workflows_officer_events_materialized`
WHERE event = "{EventType.WORKFLOWS_USER_ACTION.value}" """,
        attribute_cols=[
            "event_type",
            "opportunity_type",
            "person_external_id",
            "new_status",
        ],
        event_date_col="event_ts",
    ),
    EventQueryBuilder(
        event_type=EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE,
        description="Event where the officer updated a person's status on Workflows tool",
        sql_source=f"""
SELECT * 
FROM `{{project_id}}.analyst_data.workflows_officer_events_materialized`
WHERE event = "{EventType.WORKFLOWS_USER_CLIENT_STATUS_UPDATE.value}" """,
        attribute_cols=[
            "event_type",
            "opportunity_type",
            "person_external_id",
            "new_status",
        ],
        event_date_col="event_ts",
    ),
    EventQueryBuilder(
        event_type=EventType.WORKFLOWS_USER_PAGE,
        description="Event where the officer viewed a particular Workflows page, e.g., PROFILE_VIEWED",
        sql_source=f"""
SELECT * 
FROM `{{project_id}}.analyst_data.workflows_officer_events_materialized`
WHERE event = "{EventType.WORKFLOWS_USER_PAGE.value}" """,
        attribute_cols=[
            "event_type",
            "opportunity_type",
            "person_external_id",
            "new_status",
        ],
        event_date_col="event_ts",
    ),
]

EVENTS_BY_TYPE = {e.event_type: e for e in EVENTS}
