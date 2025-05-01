#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Shared CTE logic for Nebraska for forms, client profiles, etc. in Workflows."""

from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_exclusive_clause,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.criteria.general.no_top_three_severity_level_supervision_violation_within_6_months import (
    VIEW_BUILDER as VIOLATIONS_CRITERION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)


def supervision_level_override_opportunity_query_template(
    tes_task_query_view_builder: SingleTaskEligibilitySpansBigQueryViewBuilder,
) -> str:
    """Generates an opportunity record query for clients eligible for overrides
    to a particular supervision level in Nebraska

    Args:
        tes_task_query_view_builder (SingleTaskEligibilitySpansBigQueryViewBuilder): The
            task eligibility spans view builder for the group of clients being surfaced

    Returns:
        str: An opportunity record query surfacing eligible clients
    """
    collapsed_view_address = build_collapsed_tes_spans_view_materialized_address(
        tes_task_query_view_builder
    ).to_str()

    return f"""
WITH eligible_clients_with_duplicate_external_ids AS (
    {join_current_task_eligibility_spans_with_external_id(state_code= "'US_NE'", 
    tes_task_query_view = f"{tes_task_query_view_builder.view_id}_materialized",
    id_type = "'US_NE_ID_NBR'",
    additional_columns="tes.reasons_v2",
    eligible_only=True)}
)
,
eligible_clients AS (
    SELECT
        eligible.state_code,
        eligible.external_id,
        eligible.person_id,
        eligible.reasons_v2 AS reasons,
        eligible.is_eligible,
        eligible.is_almost_eligible,
        tes_collapsed.start_date AS metadata_eligible_date,
    FROM eligible_clients_with_duplicate_external_ids eligible
    INNER JOIN `{{project_id}}.{{workflows_views_dataset}}.person_id_to_external_id_materialized` pei
        ON eligible.external_id = pei.person_external_id
        AND eligible.state_code = pei.state_code
    INNER JOIN `{{project_id}}.{collapsed_view_address}` tes_collapsed
        ON 
            tes_collapsed.state_code = eligible.state_code
            AND tes_collapsed.person_id = eligible.person_id 
            AND CURRENT_DATE('US/Eastern') BETWEEN tes_collapsed.start_date AND {nonnull_end_date_exclusive_clause('tes_collapsed.end_date')}
)
,
last_four_oras_scores AS (
    SELECT
        state_code,
        person_id,
        TO_JSON(
            ARRAY_AGG(
                STRUCT(
                    assessment_level AS assessment_level,
                    assessment_date AS assessment_date
                )
                ORDER BY assessment_date DESC LIMIT 4
            )
        ) AS last_four_oras_scores,
        MAX(assessment_date) AS latest_assessment_date,
        DATE_ADD(MAX(assessment_date), INTERVAL 6 MONTH) AS next_assessment_date,
    FROM `{{project_id}}.normalized_state.state_assessment`
    WHERE
        state_code = 'US_NE'
        AND assessment_type = 'ORAS_COMMUNITY_SUPERVISION_SCREENING'
    GROUP BY 1,2
)
,
last_case_plan_check_in_as_case_notes AS (
    SELECT
        inmateNumber AS external_id,
        TO_JSON(
            ARRAY_AGG(
                STRUCT(
                    NULL AS note_title, 
                    checkIn AS note_body, 
                    DATE(casePlanDate) AS event_date, 
                    "Latest Case Plan Check-in" AS criteria
                )
                ORDER BY DATE(casePlanDate) DESC, checkIn LIMIT 1
            )
        ) AS case_notes,
    -- TODO(#40952): replace with state_supervision_contact once address periods are ingested
    FROM `{{project_id}}.us_ne_raw_data_up_to_date_views.PIMSCasePlan_latest` case_plan
    GROUP BY 1
)
,
special_conditions AS (
    SELECT
        state_code,
        person_id,
        TO_JSON(
            ARRAY_AGG(
                STRUCT(
                    special_condition_type AS special_condition_type,
                    compliance AS compliance
                )
                ORDER BY special_condition_type ASC
            )
        ) AS special_conditions,
    FROM `{{project_id}}.sessions.us_ne_special_condition_compliance_sessions_materialized`
    WHERE {today_between_start_date_and_nullable_end_date_clause('start_date', 'end_date_exclusive')}
    GROUP BY 1,2
)
,
latest_high_severity_violation AS (
    SELECT
        state_code,
        person_id,
        DATE(JSON_EXTRACT_STRING_ARRAY(reason, '$.latest_violations')[OFFSET(0)]) AS latest_high_severity_violation_date,
    FROM `{{project_id}}.task_eligibility_criteria_general.{VIOLATIONS_CRITERION_VIEW_BUILDER.view_id}_materialized`
    -- Identify the latest high-severity violation criterion row for each person, which has the 
    -- latest violation date (i.e., if a row exists at all, it will include a violation date, by construction
    -- of the criterion.)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code, person_id ORDER BY start_date DESC) = 1
)
SELECT
    person_id,
    external_id,
    state_code,
    reasons,
    is_eligible,
    is_almost_eligible,
    last_case_plan_check_in_as_case_notes.case_notes AS case_notes,
    metadata_eligible_date,
    last_four_oras_scores.last_four_oras_scores AS metadata_recent_oras_scores,
    last_four_oras_scores.latest_assessment_date AS metadata_latest_assessment_date, -- Form + Metadata
    last_four_oras_scores.next_assessment_date AS metadata_next_assessment_date, -- Form + Metadata
    IFNULL(special_conditions.special_conditions, TO_JSON([])) AS metadata_special_conditions, -- Form + Metadata
    latest_high_severity_violation.latest_high_severity_violation_date AS metadata_latest_high_severity_violation_date,
FROM eligible_clients
LEFT JOIN last_four_oras_scores
    USING(state_code, person_id)
LEFT JOIN special_conditions
    USING(state_code, person_id)
LEFT JOIN latest_high_severity_violation
    USING(state_code, person_id)
LEFT JOIN last_case_plan_check_in_as_case_notes
    USING(external_id)
    """
