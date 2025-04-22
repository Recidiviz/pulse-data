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
"""Queries information pertinent to Conditional Low Risk supervision overrides in Nebraska."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_exclusive_clause,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.collapsed_task_eligibility_spans import (
    build_collapsed_tes_spans_view_materialized_address,
)
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.eligibility_spans.us_ne.conditional_low_risk_supervision_override import (
    VIEW_BUILDER as US_NE_CONDITIONAL_LOW_RISK_SUPERVISION_OVERRIDE_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_COLLAPSED_TES_SPANS_ADDRESS_OVERRIDE = (
    build_collapsed_tes_spans_view_materialized_address(
        US_NE_CONDITIONAL_LOW_RISK_SUPERVISION_OVERRIDE_VIEW_BUILDER
    )
)


US_NE_CONDITIONAL_LOW_RISK_OVERRIDE_VIEW_NAME = (
    "us_ne_conditional_low_risk_override_record"
)

US_NE_CONDITIONAL_LOW_RISK_OVERRIDE_QUERY_TEMPLATE = f"""

WITH eligible_clients_with_duplicate_external_ids AS (
    {join_current_task_eligibility_spans_with_external_id(state_code= "'US_NE'", 
    tes_task_query_view = 'conditional_low_risk_supervision_override_materialized',
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
    INNER JOIN `{{project_id}}.{_COLLAPSED_TES_SPANS_ADDRESS_OVERRIDE.to_str()}` tes_collapsed
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
    -- TODO(#40171): Add latest disqualifying violation date to the view once we have a criterion
    -- built specifically for disqualifying (more severe) violations
FROM eligible_clients
LEFT JOIN last_four_oras_scores
    USING(state_code, person_id)
LEFT JOIN special_conditions
    USING(state_code, person_id)
LEFT JOIN last_case_plan_check_in_as_case_notes
    USING(external_id)
"""

US_NE_CONDITIONAL_LOW_RISK_OVERRIDE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_NE_CONDITIONAL_LOW_RISK_OVERRIDE_VIEW_NAME,
    view_query_template=US_NE_CONDITIONAL_LOW_RISK_OVERRIDE_QUERY_TEMPLATE,
    description=__doc__,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_NE
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_NE_CONDITIONAL_LOW_RISK_OVERRIDE_VIEW_BUILDER.build_and_print()
