# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
Queries information needed to surface eligible folks to be early released on a Transition Program Release AZ.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
    opportunity_query_final_select_with_case_notes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_az_query_fragments import (
    agreement_form_status_side_panel_notes,
    almost_eligible_tab_logic,
    functional_literacy_enrollment_side_panel_notes,
    home_plan_information_for_side_panel_notes,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_NAME = (
    "us_az_approaching_acis_or_recidiviz_tpr_request_record"
)

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_DESCRIPTION = """
    Queries information needed to surface eligible folks to be early released on a Transition Program Release AZ.
    """


US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_QUERY_TEMPLATE = f"""
# TODO(#33958) - Add all the relevant components of this opportunity record
WITH recidiviz_tpr_date_approaching AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code="'US_AZ'",
    tes_task_query_view='overdue_for_recidiviz_tpr_request_materialized',
    id_type="'US_AZ_PERSON_ID'",
    almost_eligible_only=True)}
),

acis_tpr_wo_reasons AS (
{join_current_task_eligibility_spans_with_external_id(
    state_code="'US_AZ'",
    tes_task_query_view='overdue_for_acis_tpr_request_materialized',
    id_type="'US_AZ_PERSON_ID'",
    almost_eligible_only=True)}
),

-- We add in the Overdue for Recidiviz reasons blob here by merging it with the Overdue for ACIS reasons blob.
-- The reason we do this is to display the Overdue for Recidiviz TES Criteria on the frontend, in the resident side panel
-- without impacting the eligibility of residents via Overdue for ACIS. We want to display only those eligible for 
-- Overdue for ACIS + the criteria they meet for Overdue for Recidiviz.

acis_tpr_date_approaching AS (
  SELECT
    acis_tpr_wo_reasons.external_id,
    acis_tpr_wo_reasons.person_id,
    acis_tpr_wo_reasons.state_code,
    TO_JSON(ARRAY_CONCAT( 
        JSON_QUERY_ARRAY(acis_tpr_wo_reasons.reasons, '$'), 
        JSON_QUERY_ARRAY(tes.reasons, '$') )) AS reasons,
    ARRAY_CONCAT(acis_tpr_wo_reasons.ineligible_criteria, tes.ineligible_criteria) AS ineligible_criteria,
    acis_tpr_wo_reasons.is_eligible,
    acis_tpr_wo_reasons.is_almost_eligible,
  FROM
    acis_tpr_wo_reasons
  LEFT JOIN
    `{{project_id}}.{{task_eligibility_dataset}}.overdue_for_recidiviz_tpr_request_materialized` tes
  USING
    (person_id,
      state_code)
  WHERE
    CURRENT_DATE('US/Pacific') BETWEEN tes.start_date
    AND IFNULL(DATE_SUB(tes.end_date, INTERVAL 1 DAY), "9999-12-31")
    AND tes.state_code = 'US_AZ'
),

combine_acis_and_recidiviz_tpr_dates AS (
{almost_eligible_tab_logic(opp_name='tpr')}
),

side_panel_notes AS (
    {home_plan_information_for_side_panel_notes()}
    UNION ALL
    {functional_literacy_enrollment_side_panel_notes()}
    UNION ALL
    {agreement_form_status_side_panel_notes('TPR')}
),


array_side_panel_notes_cte AS (
    {array_agg_case_notes_by_external_id(
        left_join_cte="side_panel_notes", 
        from_cte="combine_acis_and_recidiviz_tpr_dates")}
)

{opportunity_query_final_select_with_case_notes(
    from_cte = "combine_acis_and_recidiviz_tpr_dates",
    left_join_cte="array_side_panel_notes_cte",
    additional_columns="metadata_tab_name, metadata_tab_description",)}
"""

US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_NAME,
    view_query_template=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_QUERY_TEMPLATE,
    description=US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_AZ
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_APPROACHING_ACIS_OR_RECIDIVIZ_TPR_REQUEST_RECORD_VIEW_BUILDER.build_and_print()
