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
Queries to understand the JII's LSU eligibility status and their status in the tool
to determine what text to send them
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_LSU_VIEW_NAME = "us_ix_lsu"

US_IX_LSU_DESCRIPTION = """Queries to understand the JII's LSU eligibility status and their status in the tool
to determine what text to send them"""

US_IX_LSU_QUERY_TEMPLATE = f"""
WITH current_opportunity_eligibility AS (
  {join_current_task_eligibility_spans_with_external_id(
    state_code="'US_IX'",
    tes_task_query_view='complete_transfer_to_limited_supervision_jii_form_materialized',
    id_type="'US_IX_DOC'",
    eligible_and_almost_eligible_only=True,
  )}
)
, recent_denials AS (
    SELECT 
      peid.state_code,
      peid.person_id,
      JSON_EXTRACT_SCALAR(reasons) = "FFR" AS fines_and_fees_denials,
    FROM `{{project_id}}.{{workflows_views_dataset}}.clients_opportunity_snoozed_materialized` sn,
    UNNEST(JSON_EXTRACT_ARRAY(reasons)) AS reasons
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
      ON peid.state_code = 'US_IX'
        AND peid.external_id = sn.person_external_id
        AND id_type = 'US_IX_DOC'
    WHERE peid.state_code = 'US_IX'
      AND sn.opportunity_type = 'LSU'
      -- Only denials that happened in the past 3 months
      AND SAFE_CAST(timestamp AS DATE) BETWEEN DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 3 MONTH) AND CURRENT_DATE('US/Eastern')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY peid.state_code, peid.person_id ORDER BY timestamp DESC) = 1
)
, full_information AS (
  SELECT 
    opp.state_code,
    opp.external_id,
    opp.person_id,
    cr.person_name,
    cr.phone_number,
    opp.is_eligible,
    opp.ineligible_criteria,
    cr.officer_id,
    CONCAT(sr.given_names, ' ', sr.surname) AS po_name,
    cr.district,
    IFNULL(rd.fines_and_fees_denials, False) as denied_for_fines_and_fees,
  FROM current_opportunity_eligibility opp
  INNER JOIN  `{{project_id}}.{{workflows_views_dataset}}.client_record_materialized` cr
    ON cr.person_external_id = opp.external_id
      AND cr.state_code = opp.state_code
  LEFT JOIN `{{project_id}}.{{workflows_views_dataset}}.supervision_staff_record_materialized` sr
    ON sr.id = cr.officer_id
      AND sr.state_code = cr.state_code
  LEFT JOIN recent_denials rd
    ON rd.person_id = opp.person_id
      AND rd.state_code = opp.state_code
)
SELECT
  * EXCEPT (is_eligible, ineligible_criteria, denied_for_fines_and_fees),
  NULL AS launch_id,
  CASE
    WHEN is_eligible AND NOT denied_for_fines_and_fees THEN "FULLY_ELIGIBLE"
    WHEN is_eligible AND denied_for_fines_and_fees THEN "ELIGIBLE_MISSING_FINES_AND_FEES"
    WHEN ARRAY_LENGTH(ineligible_criteria) = 2 THEN "TWO_MISSING_CRITERIA"
    WHEN "NEGATIVE_DA_WITHIN_90_DAYS" IN UNNEST(ineligible_criteria) THEN "MISSING_DA"
    WHEN "US_IX_INCOME_VERIFIED_WITHIN_3_MONTHS" IN UNNEST(ineligible_criteria) THEN "MISSING_INCOME_VERIFICATION"
  END AS group_id
FROM full_information
"""

US_IX_LSU_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.JII_TEXTING_DATASET_ID,
    view_id=US_IX_LSU_VIEW_NAME,
    view_query_template=US_IX_LSU_QUERY_TEMPLATE,
    description=US_IX_LSU_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_IX
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_LSU_VIEW_BUILDER.build_and_print()
