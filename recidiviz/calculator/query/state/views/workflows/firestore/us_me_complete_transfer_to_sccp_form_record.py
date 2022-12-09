# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query for relevant information needed to fill out transfer to SCCP form in ME
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_NAME = "us_me_complete_transfer_to_sccp_form_record"

US_ME_TRANSFER_TO_SCCP_RECORD_DESCRIPTION = """
    Query for relevant information to fill out the transfer to SCCP forms in ME
    """
US_ME_TRANSFER_TO_SCCP_RECORD_QUERY_TEMPLATE = f"""

WITH current_incarceration_pop_cte AS (
    SELECT pei.external_id,
        tes.state_code,
        tes.reasons,
        tes.ineligible_criteria,
        tes.is_eligible,
    FROM `{{project_id}}.{{task_eligibility_dataset}}.transfer_to_sccp_materialized` tes
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        USING(person_id)
    WHERE 
      CURRENT_DATE('US/Pacific') BETWEEN tes.start_date AND 
                                         {nonnull_end_date_exclusive_clause('tes.end_date')}
      AND tes.state_code = 'US_ME'
),
json_to_array_cte AS (
  --I transform the reason json column into an arrray for easier manipulation
  SELECT 
      *,
      JSON_QUERY_ARRAY(reasons) AS array_reasons
  FROM current_incarceration_pop_cte

)

-- ELIGIBLE
SELECT * EXCEPT(is_eligible)
FROM current_incarceration_pop_cte
WHERE is_eligible

UNION ALL 

-- ALMOST ELIGIBLE (<6mo remaining)
SELECT * EXCEPT(eligible_date, is_eligible)
FROM (SELECT
      * EXCEPT(array_reasons),
      -- only keep eligible_date for the relevant criteria
      CAST(ARRAY(SELECT JSON_VALUE(x.reason.eligible_date)
            FROM UNNEST(array_reasons) AS x
            WHERE 
                STRING(x.criteria_name) = 'US_ME_X_MONTHS_REMAINING_ON_SENTENCE')[OFFSET(0)]
          AS DATE)  AS eligible_date,
    FROM json_to_array_cte
    WHERE 
      -- keep if only ineligible criteria is time remaining on sentence
      'US_ME_X_MONTHS_REMAINING_ON_SENTENCE' IN UNNEST(ineligible_criteria) 
      AND ARRAY_LENGTH(ineligible_criteria) = 1
      )
WHERE DATE_DIFF(eligible_date, CURRENT_DATE('US/Pacific'), MONTH) < 6

UNION ALL

-- ALMOST ELIGIBLE (one discipline away)
SELECT
* EXCEPT(array_reasons, is_eligible),
FROM json_to_array_cte
WHERE 
  -- keep if only ineligible criteria is a violation
  'US_ME_NO_CLASS_A_OR_B_VIOLATION_FOR_90_DAYS' IN UNNEST(ineligible_criteria) 
  AND ARRAY_LENGTH(ineligible_criteria) = 1
"""

US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_NAME,
    view_query_template=US_ME_TRANSFER_TO_SCCP_RECORD_QUERY_TEMPLATE,
    description=US_ME_TRANSFER_TO_SCCP_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_ME
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_TRANSFER_TO_SCCP_RECORD_VIEW_BUILDER.build_and_print()
