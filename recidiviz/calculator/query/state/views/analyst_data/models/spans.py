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
"""Configures span query builder objects at the person-level."""

from typing import List

from recidiviz.calculator.query.state.views.analyst_data.models.span_query_builder import (
    SpanQueryBuilder,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_type import (
    SpanType,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    USAGE_EVENTS_DICT,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_impact_funnel_status_sessions import (
    WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_user_caseload_access_sessions import (
    WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.justice_impact_sessions import (
    JUSTICE_IMPACT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.person_demographics import (
    PERSON_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_officer_sessions import (
    SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER,
)

SPANS: List[SpanQueryBuilder] = [
    SpanQueryBuilder(
        span_type=SpanType.ASSESSMENT_SCORE_SESSION,
        description="Spans of time between assessment scores of the same type",
        sql_source="""
SELECT *
FROM
    `{project_id}.sessions.assessment_score_sessions_materialized`
WHERE
    assessment_date IS NOT NULL
    AND assessment_type IS NOT NULL
    AND assessment_score IS NOT NULL
""",
        attribute_cols=["assessment_type", "assessment_score", "assessment_level"],
        span_start_date_col="assessment_date",
        span_end_date_col="score_end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.COMPARTMENT_SESSION,
        description="Compartment sessions unique on compartment level 1 & 2 types",
        sql_source=COMPARTMENT_SESSIONS_VIEW_BUILDER.table_for_query,
        attribute_cols=[
            "compartment_level_1",
            "compartment_level_2",
            "case_type_start",
        ],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.COMPARTMENT_SUB_SESSION,
        description="Non-overlapping spans unique on all population attributes",
        sql_source=COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.table_for_query,
        attribute_cols=[
            "compartment_level_1",
            "compartment_level_2",
            "compartment_location",
            "facility",
            "facility_name",
            "supervision_office",
            "supervision_office_name",
            "supervision_district",
            "supervision_district_name",
            "supervision_region_name",
            "correctional_level",
            "correctional_level_raw_text",
            "housing_unit",
            "housing_unit_category",
            "housing_unit_type",
            "housing_unit_type_raw_text",
            "case_type",
            "prioritized_race_or_ethnicity",
            "gender",
            "age",
            "assessment_score",
        ],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.COMPLETED_CONTACT_SESSION,
        description="Spans between completed contact dates and the subsequent contact date to help "
        "identify the most recent completed contact",
        sql_source="""SELECT DISTINCT
    state_code,
    person_id,
    contact_date,
FROM
    `{project_id}.normalized_state.state_supervision_contact`
WHERE
    status = "COMPLETED"
""",
        attribute_cols=[],
        span_start_date_col="contact_date",
        span_end_date_col="LEAD(contact_date) OVER (PARTITION BY person_id ORDER BY contact_date)",
    ),
    SpanQueryBuilder(
        span_type=SpanType.CUSTODY_LEVEL_SESSION,
        description="Non-overlapping spans of time over which a person has a certain custody level",
        sql_source="""SELECT *
FROM
    `{project_id}.sessions.custody_level_sessions_materialized`
WHERE
    custody_level IS NOT NULL""",
        attribute_cols=["custody_level"],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.EMPLOYMENT_PERIOD,
        description="Employment periods -- can be overlapping",
        sql_source="""SELECT *
FROM
    `{project_id}.normalized_state.state_employment_period`
WHERE
    start_date IS NOT NULL
    AND employment_status != "UNEMPLOYED"
""",
        attribute_cols=["employer_name"],
        span_start_date_col="start_date",
        span_end_date_col="end_date",
    ),
    SpanQueryBuilder(
        span_type=SpanType.EMPLOYMENT_STATUS_SESSION,
        description="Non-overlapping spans of time over which a person has a certain employment status",
        sql_source="""SELECT *
FROM
    `{project_id}.sessions.supervision_employment_status_sessions_materialized`
WHERE
    employment_status_start_date IS NOT NULL """,
        attribute_cols=["is_employed"],
        span_start_date_col="employment_status_start_date",
        span_end_date_col="employment_status_end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.HOUSING_TYPE_SESSION,
        description="Non-overlapping spans of time over which a person has a certain housing type",
        sql_source="""SELECT *
FROM
    `{project_id}.sessions.housing_unit_type_sessions_materialized`
WHERE
    housing_unit_type IS NOT NULL""",
        attribute_cols=["housing_unit_type"],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        description="Person days of justice involvement weighted by type",
        sql_source=JUSTICE_IMPACT_SESSIONS_VIEW_BUILDER.table_for_query,
        attribute_cols=[
            "justice_impact_type",
            "justice_impact_weight",
            "unweighted_days_justice_impacted",
            "weighted_days_justice_impacted",
        ],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.PERSON_DEMOGRAPHICS,
        description="Demographics over the span from a person's birth to the present",
        sql_source=PERSON_DEMOGRAPHICS_VIEW_BUILDER.table_for_query,
        attribute_cols=["birthdate", "gender", "prioritized_race_or_ethnicity"],
        span_start_date_col="birthdate",
        span_end_date_col="DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY)",
    ),
    SpanQueryBuilder(
        span_type=SpanType.SENTENCE_SPAN,
        description="Span of attributes of sentences being served",
        sql_source="""SELECT
    spans.state_code,
    spans.person_id,
    spans.start_date,
    spans.end_date_exclusive,

    -- Characteristics from sentence spans
    LOGICAL_OR(sentences.is_drug_uniform) AS any_is_drug_uniform,
    LOGICAL_OR(sentences.is_violent_uniform) AS any_is_violent_uniform,
    LOGICAL_OR(sentences.crime_against_uniform = "Person") AS any_is_crime_against_person,
    LOGICAL_OR(sentences.crime_against_uniform = "Property") AS any_is_crime_against_property,
    LOGICAL_OR(sentences.crime_against_uniform = "Society") AS any_is_crime_against_society,
    MIN(sentences.effective_date) AS effective_date,
    MAX(sentences.parole_eligibility_date) AS parole_eligibility_date,
    MAX(sentences.projected_completion_date_max) AS projected_completion_date_max,
    -- Expected release dates from sentence deadline spans
    MAX(task_deadlines.projected_supervision_release_date) AS projected_supervision_release_snapshot_date,
    MAX(task_deadlines.projected_incarceration_release_date) AS projected_incarceration_release_snapshot_date,
    MAX(task_deadlines.parole_eligibility_date) AS parole_eligibility_snapshot_date,

FROM
    `{project_id}.sessions.sentence_spans_materialized` spans,
    UNNEST(sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id,
    UNNEST(sentence_deadline_id_array) AS sentence_deadline_id
LEFT JOIN
    `{project_id}.sessions.sentences_preprocessed_materialized` sentences
USING
    (person_id, state_code, sentences_preprocessed_id)
LEFT JOIN
    `{project_id}.sessions.sentence_deadline_spans_materialized` task_deadlines
USING
    (person_id, state_code, sentence_deadline_id)
GROUP BY 1, 2, 3, 4
        """,
        attribute_cols=[
            "any_is_crime_against_person",
            "any_is_crime_against_property",
            "any_is_crime_against_society",
            "any_is_drug_uniform",
            "any_is_violent_uniform",
            "effective_date",
            "parole_eligibility_date",
            "parole_eligibility_snapshot_date",
            "projected_completion_date_max",
            "projected_incarceration_release_snapshot_date",
            "projected_supervision_release_snapshot_date",
        ],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE,
        description="Open supervision mismatch (downgrades only), ends when mismatch corrected or supervision period ends",
        sql_source="""SELECT *, "SUPERVISION_DOWNGRADE" AS task_name,
FROM
    `{project_id}.sessions.supervision_downgrade_sessions_materialized`
WHERE
    recommended_supervision_downgrade_level IS NOT NULL""",
        attribute_cols=[
            "task_name",
            "mismatch_corrected",
            "recommended_supervision_downgrade_level",
        ],
        span_start_date_col="start_date",
        span_end_date_col="DATE_ADD(end_date, INTERVAL 1 DAY)",
    ),
    SpanQueryBuilder(
        span_type=SpanType.SUPERVISION_LEVEL_SESSION,
        description="Spans of time over which a client is at a given supervision level",
        sql_source="""SELECT * EXCEPT(supervision_level),
    COALESCE(supervision_level, "INTERNAL_UNKNOWN") AS supervision_level,
FROM `{project_id}.sessions.supervision_level_sessions_materialized`""",
        attribute_cols=["supervision_level"],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.SUPERVISION_OFFICER_INFERRED_LOCATION_SESSION,
        description="Spans of time over which an officer has the majority of their clients in a given location",
        sql_source="SELECT *, supervising_officer_external_id AS officer_id "
        "FROM `{project_id}.sessions.supervision_officer_inferred_location_sessions_materialized`",
        attribute_cols=["primary_office", "primary_district"],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.SUPERVISION_OFFICER_SESSION,
        description="Spans of time over which a client is supervised by a given officer",
        sql_source=SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER.table_for_query,
        attribute_cols=["supervising_officer_external_id"],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
    SpanQueryBuilder(
        span_type=SpanType.TASK_CRITERIA_SPAN,
        description="Spans of time over which a person is eligible for a given criteria",
        sql_source="""SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    "INCARCERATION_PAST_FULL_TERM_RELEASE_DATE" AS criteria,
    meets_criteria,
FROM
    `{project_id}.task_eligibility_criteria_general.incarceration_past_full_term_completion_date_materialized`

UNION ALL

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    "SUPERVISION_PAST_FULL_TERM_RELEASE_DATE" AS criteria,
    meets_criteria,
FROM
    `{project_id}.task_eligibility_criteria_general.supervision_past_full_term_completion_date_materialized`

UNION ALL

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    "INCARCERATION_PAST_PAROLE_ELIGIBILITY_DATE" AS criteria,
    meets_criteria,
FROM
    `{project_id}.task_eligibility_criteria_general.incarceration_past_parole_eligibility_date_materialized`""",
        attribute_cols=[
            "criteria",
            "meets_criteria",
        ],
        span_start_date_col="start_date",
        span_end_date_col="end_date",
    ),
    SpanQueryBuilder(
        span_type=SpanType.TASK_ELIGIBILITY_SESSION,
        description="Task eligibility spans",
        sql_source="""SELECT
    * EXCEPT (ineligible_criteria),
    completion_event_type AS task_type,
    ARRAY_TO_STRING(ineligible_criteria, ",") AS ineligible_criteria,
FROM
    `{project_id}.task_eligibility.all_tasks_materialized`
INNER JOIN
    `{project_id}.reference_views.task_to_completion_event`
USING
    (task_name)""",
        attribute_cols=["task_name", "task_type", "is_eligible", "ineligible_criteria"],
        span_start_date_col="start_date",
        span_end_date_col="end_date",
    ),
    SpanQueryBuilder(
        span_type=SpanType.WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSION,
        description="Spans of time over which a client had a specific usage and eligibility status for a task type",
        sql_source=WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER.table_for_query,
        attribute_cols=[
            "task_type",
            "is_justice_involved",
            "is_eligible",
            "is_almost_eligible",
            "task_completed",
            *[k.lower() for k in USAGE_EVENTS_DICT],
        ],
        span_start_date_col="start_date",
        span_end_date_col="end_date",
    ),
    SpanQueryBuilder(
        span_type=SpanType.WORKFLOWS_USER_CASELOAD_ACCESS_SESSION,
        description="Spans of time over which a workflows user is associated with a given location",
        sql_source=WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_BUILDER.table_for_query,
        attribute_cols=[
            "has_supervision_workflows",
            "has_facilities_workflows",
        ],
        span_start_date_col="start_date",
        span_end_date_col="end_date_exclusive",
    ),
]

SPANS_BY_TYPE = {s.span_type: s for s in SPANS}
