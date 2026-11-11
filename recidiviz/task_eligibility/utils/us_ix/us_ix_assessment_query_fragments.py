# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Helper SQL queries and criteria view builders for Idaho LSI-R and STABLE
assessments on supervision."""

from typing import List, Optional

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    nonnull_start_date_clause,
)
from recidiviz.calculator.query.state.views.tasks.assessment_type import AssessmentType
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.critical_date_query_fragments import (
    critical_date_has_passed_spans_cte,
)

# Days before a supervision session start within which an assessment still counts
# toward that session. Shared by each initial-assessment trigger and its annual
# reassessment criteria so both recognize the same pre-session assessments.
US_IX_LSIR_ASSESSMENT_LOOKBACK_DAYS = 90
US_IX_STABLE_ASSESSMENT_LOOKBACK_DAYS = 60

# Idaho treats an LSI-R done for a pre-sentence investigation (PSI) as valid for a
# year, so a PSI LSI-R counts toward a session that starts up to this many days later.
US_IX_PSI_LSIR_LOOKBACK_DAYS = 365

# LSI-Rs are sometimes entered a few days after the PSI report is marked complete.
_US_IX_PSI_COMPLETED_DATE_GRACE_DAYS = 7

# LSI-R assessments completed as part of a PSI, one row per person and assessment date.
US_IX_PSI_LSIR_ASSESSMENTS_QUERY = f"""
    SELECT DISTINCT
        lsir.state_code,
        lsir.person_id,
        lsir.assessment_date,
    FROM `{{project_id}}.sessions.assessment_score_sessions_materialized` lsir
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` peid
        ON peid.person_id = lsir.person_id
        AND peid.state_code = 'US_IX'
        AND peid.id_type = 'US_IX_DOC'
    INNER JOIN `{{project_id}}.us_ix_raw_data_up_to_date_views.com_PSIReport_latest_materialized` psi
        ON psi.OffenderId = peid.external_id
        AND lsir.assessment_date BETWEEN DATE(psi.OrderDate)
            AND DATE_ADD(DATE(psi.CompletedDate), INTERVAL {_US_IX_PSI_COMPLETED_DATE_GRACE_DAYS} DAY)
    WHERE lsir.state_code = 'US_IX'
        AND lsir.assessment_type = 'LSIR'
        AND lsir.assessment_class = 'RISK'
"""


def lsir_spans() -> str:
    """
    Returns a SQL query that returns LSIR spans in Idaho.

    Note: Confirmed on 12/8/25 that Idaho uses gender (not sex) to classify individual LSIR scores.
    """

    return f"""LSIR_level_gender AS(
  /* This CTE creates a view of LSIR-score level by gender according to updated
  Idaho Supervision Categories (07/21/2020) */
  SELECT
      score.person_id,
      score.state_code,
      score.assessment_date AS score_start_date,
      {nonnull_end_date_clause('score.score_end_date_exclusive')} AS score_end_date,
      ses.start_date AS supervision_start_date,
      ses.end_date_exclusive AS supervision_end_date,
      CASE
          WHEN ((gender != "MALE" OR gender IS NULL) AND assessment_score <=22) THEN "LOW"
          WHEN ((gender != "MALE" OR gender IS NULL)
                                AND (assessment_score BETWEEN 23 AND 30)) THEN "MODERATE"
          WHEN (gender = "MALE" AND assessment_score <=20) THEN "LOW"
          WHEN (gender = "MALE" AND (assessment_score BETWEEN 21 AND 28)) THEN "MODERATE"
          ELSE "HIGH"
          END AS lsir_level
  FROM `{{project_id}}.{{sessions_dataset}}.assessment_score_sessions_materialized` score
  INNER JOIN `{{project_id}}.{{sessions_dataset}}.supervision_super_sessions_materialized`ses
    ON score.state_code = ses.state_code
    AND score.person_id = ses.person_id
    --only consider scores relevant in the supervision session during which they occur 
    AND ses.start_date < {nonnull_end_date_clause('score.score_end_date_exclusive')}
    AND score.assessment_date < {nonnull_end_date_clause('ses.end_date_exclusive')}
     LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person` info
      ON score.state_code = info.state_code
      AND score.person_id = info.person_id
  WHERE score.state_code = 'US_IX' 
      AND assessment_type = 'LSIR'
),
grp_starts as (
   /* This CTE identifies when the LSI-R level changes within a supervision session. 
   grp_start is 1 at the beginning of each new level span and 0 for consecutive periods with the same LSI-R score.*/
  SELECT 
      state_code,
      person_id,
      lsir_level, 
      supervision_start_date,
      supervision_end_date,
      score_start_date,
      score_end_date,
      IF(lsir_level = LAG(lsir_level) OVER supervision_window, 0, 1) AS grp_start
  FROM LSIR_level_gender
  WINDOW supervision_window AS (
    PARTITION BY person_id, supervision_start_date ORDER BY score_start_date
  )
),
grps as (
    /* This CTE sums grp_starts to group together adjacent periods where the LSI-R score is the same */
    SELECT
        state_code,
        person_id,
        lsir_level, 
        supervision_start_date,
        supervision_end_date,
        score_start_date,
        score_end_date,
        SUM(grp_start) OVER(ORDER BY person_id, supervision_start_date, score_start_date) AS grp
    FROM grp_starts
),
lsir_spans AS (
    /* This CTE actually combines adjacent periods where the LSI-R score is the same by 
    choosing min start and max end date.*/
    SELECT 
        state_code, 
        person_id,
        lsir_level,
        MIN(score_start_date) AS score_span_start_date,
        --end span date on supervision session end date, or score end date, whichever comes first
        MAX(LEAST(score_end_date,{nonnull_end_date_clause('supervision_end_date')})) AS score_span_end_date
    FROM grps
    GROUP BY grp, state_code, person_id, lsir_level)"""


def us_ix_supervision_start_spans_query() -> str:
    """Returns a SQL query that selects supervision start spans in Idaho.

    Each row represents a supervision period with the earliest
    compartment_level_1_super_session start date and the
    supervision_super_session end date. Used as the base contact-period
    spans query for initial assessment triggers built with
    non_recurring_contact_compliance_builder.
    """
    return f"""
    SELECT
        css.state_code,
        css.person_id,
        MIN(css.start_date) AS start_date,
        sss.end_date_exclusive AS end_date,
    FROM `{{project_id}}.sessions.compartment_level_1_super_sessions_materialized` css
    LEFT JOIN `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
        ON css.state_code = sss.state_code
            AND css.person_id = sss.person_id
            AND css.start_date BETWEEN sss.start_date AND {nonnull_end_date_clause('sss.end_date')}
    WHERE css.state_code = 'US_IX'
        AND css.start_reason IN ('COURT_SENTENCE', 'RELEASE_FROM_INCARCERATION')
        AND css.compartment_level_1 = 'SUPERVISION'
    GROUP BY css.state_code, css.person_id, sss.end_date_exclusive
"""


def us_ix_annual_assessment_criteria_view_builder(
    criteria_name: str,
    description: str,
    assessment_type: AssessmentType,
    assessment_class: str,
    assessment_lookback_days: int,
    include_psi_lsir_assessments: bool,
    include_supervision_contact_reasons: Optional[List[str]] = None,
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """
    Creates a VIEW_BUILDER for annual assessment criteria in Idaho.

    This criteria shows spans of time for which supervision clients need an assessment
    reassessment. This should happen every 365 days after the last assessment, and is
    triggered 30 days before that date.

    Args:
        criteria_name: The name of the criteria (e.g., 'US_IX_IS_MISSING_ANNUAL_LSIR_ASSESSMENT')
        description: The description/docstring for the criteria
        assessment_type: The compliance AssessmentType this criterion is tied to (e.g.,
            AssessmentType.LSIR, AssessmentType.STABLE). Its value doubles as the
            `state_assessment.assessment_type` value to filter on.
        assessment_class: The assessment class to filter (e.g., 'RISK', 'SEX_OFFENSE')
        assessment_lookback_days: Days before a session start within which an assessment
            still anchors that session's reassessment clock. Mirrors the initial
            trigger's `contact_date_lookback_days`.
        include_psi_lsir_assessments: Whether LSI-Rs done for a pre-sentence
            investigation anchor the reassessment clock of a session that starts up to
            US_IX_PSI_LSIR_LOOKBACK_DAYS later. Only valid for LSI-R assessments.
        include_supervision_contact_reasons: Optional list of supervision contact reasons to also
            consider as valid assessments (e.g., ['LSI_R_REVIEW', 'INITIAL,LSI_R_REVIEW']).
            When provided, supervision contacts with these contact_reasons and status='COMPLETED'
            will be included via UNION ALL.
    """
    if include_psi_lsir_assessments and assessment_type is not AssessmentType.LSIR:
        raise ValueError(
            f"include_psi_lsir_assessments is only valid for LSIR assessments, found "
            f"assessment_type [{assessment_type.value}] for criteria [{criteria_name}]"
        )
    # US_IX's assessment ingest values match their AssessmentType codes.
    state_assessment_type = assessment_type.value

    # Build optional supervision contact query
    supervision_contact_union = ""
    if include_supervision_contact_reasons:
        contact_reasons_str = list_to_query_string(
            include_supervision_contact_reasons, quoted=True, single_quote=True
        )
        supervision_contact_union = f"""
    UNION ALL

    -- Include supervision contacts with the specified contact_reasons as valid assessments
    SELECT
        state_code,
        person_id,
        contact_date AS assessment_date,
        {assessment_lookback_days} AS lookback_days,
    FROM `{{project_id}}.normalized_state.state_supervision_contact`
    WHERE state_code = 'US_IX'
        AND contact_reason_raw_text IN ({contact_reasons_str})
        AND status = 'COMPLETED'"""

    psi_lsir_union = ""
    if include_psi_lsir_assessments:
        psi_lsir_union = f"""
    UNION ALL

    SELECT
        state_code,
        person_id,
        assessment_date,
        {US_IX_PSI_LSIR_LOOKBACK_DAYS} AS lookback_days,
    FROM ({US_IX_PSI_LSIR_ASSESSMENTS_QUERY})"""

    query_template = f"""
WITH supervision_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
        -- Previous session's end, so the lookback below can't reach into a prior
        -- session and attach one assessment to two sessions.
        LAG(end_date_exclusive) OVER (
            PARTITION BY state_code, person_id ORDER BY start_date
        ) AS prev_session_end_date,
    FROM `{{project_id}}.sessions.supervision_super_sessions_materialized` css
    WHERE css.state_code = 'US_IX'
),

assessments AS (
    -- Grouping dedupes dates across sources so a same-day assessment and LSI-R-review
    -- contact don't break the LEAD-based sequencing below. A date from several
    -- sources keeps the longest lookback any of them allows.
    SELECT
        state_code,
        person_id,
        assessment_date,
        MAX(lookback_days) AS lookback_days,
    FROM (
        SELECT
            state_code,
            person_id,
            assessment_date,
            {assessment_lookback_days} AS lookback_days,
        FROM `{{project_id}}.sessions.assessment_score_sessions_materialized`
        WHERE state_code = 'US_IX'
            AND assessment_type = '{state_assessment_type}'
            AND assessment_class = '{assessment_class}'
{supervision_contact_union}
{psi_lsir_union}
    )
    GROUP BY state_code, person_id, assessment_date
),

critical_date_spans AS (
    SELECT *
    FROM (
        SELECT
            ss.state_code,
            ss.person_id,
            -- Clamp to session start so a pre-session assessment doesn't start the span
            -- before supervision begins (critical date still uses the true assessment date).
            GREATEST(
                IFNULL(a.assessment_date, ss.start_date),
                ss.start_date
            ) AS start_datetime,
            -- The end_datetime is either the next assessment date (if it exists) or the end of the
            -- supervision session.
            IF(
                a.assessment_date IS NOT NULL,
                -- next_assessment date OR end_date, whichever is earlier
                LEAST(
                    IFNULL(
                        LEAD(a.assessment_date) OVER (
                            PARTITION BY ss.state_code, ss.person_id
                            ORDER BY a.assessment_date
                        ),
                        '9999-12-31'
                    ),
                    {nonnull_end_date_clause('ss.end_date')}
                ),
                ss.end_date
            ) AS end_datetime,
            DATE_ADD(a.assessment_date, INTERVAL 365 DAY) AS critical_date,
            a.assessment_date AS last_assessment_date,
        FROM supervision_sessions ss
        LEFT JOIN assessments a
            ON ss.person_id = a.person_id
                AND ss.state_code = a.state_code
                -- Reach back up to the assessment's lookback before session start,
                -- but not past the previous session's end.
                AND a.assessment_date BETWEEN GREATEST(
                        DATE_SUB(ss.start_date, INTERVAL a.lookback_days DAY),
                        {nonnull_start_date_clause('ss.prev_session_end_date')}
                    )
                    AND {nonnull_end_date_exclusive_clause('ss.end_date')}
    )
    -- Drop spans collapsed to zero/negative length by the start clamp (a pre-session
    -- assessment superseded by an in-session one); its period is already covered.
    WHERE start_datetime < {nonnull_end_date_exclusive_clause('end_datetime')}
),

{critical_date_has_passed_spans_cte(
    meets_criteria_leading_window_time=30,
    attributes=['last_assessment_date'])}

SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    critical_date_has_passed AS meets_criteria,
    TO_JSON(
        STRUCT(
            critical_date AS assessment_due_date,
            last_assessment_date AS last_assessment_date,
            critical_date_has_passed AS is_missing_annual_assessment,
            '1 EVERY 365 DAYS' AS contact_cadence,
            '{state_assessment_type}' AS assessment_type,
            '{assessment_class}' AS assessment_class
    )) AS reason,
    critical_date AS assessment_due_date,
    last_assessment_date,
    critical_date_has_passed AS is_missing_annual_assessment,
    '1 EVERY 365 DAYS' AS contact_cadence,
    '{state_assessment_type}' AS assessment_type,
    '{assessment_class}' AS assessment_class,
FROM critical_date_has_passed_spans
"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        state_code=StateCode.US_IX,
        criteria_name=criteria_name,
        criteria_spans_query_template=query_template,
        description=description,
        assessment_type=assessment_type,
        reasons_fields=[
            ReasonsField(
                name="assessment_due_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Due date of the assessment.",
            ),
            ReasonsField(
                name="last_assessment_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last assessment.",
            ),
            ReasonsField(
                name="is_missing_annual_assessment",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Whether the client is missing an annual assessment.",
            ),
            ReasonsField(
                name="contact_cadence",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The required cadence for assessments.",
            ),
            ReasonsField(
                name="assessment_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The type of assessment.",
            ),
            ReasonsField(
                name="assessment_class",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="The class of assessment.",
            ),
        ],
    )
