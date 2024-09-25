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
"""Query for relevant metadata needed to support administrative supervision opportunity in PA
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder

# from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
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

US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_NAME = (
    "us_pa_transfer_to_administrative_supervision_form_record"
)

US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support administrative supervision opportunity in PA
    """
US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_QUERY_TEMPLATE = f"""
WITH
  eligible_and_almost_eligible AS (
    {join_current_task_eligibility_spans_with_external_id(
        state_code="'US_PA'",
        tes_task_query_view='complete_transfer_to_administrative_supervision_request_materialized',
        id_type="'US_PA_PBPP'",
        eligible_and_almost_eligible_only=True,
    )}
  ),
  form_information AS (
    SELECT person_id,
        LOGICAL_OR(offense_type = 'DRUGS' 
            OR (description LIKE '%DRUG%' AND offense_type IS NULL)
            OR (description LIKE '%MARIJUANA%' AND offense_type IS NULL)
            OR (description LIKE '%MARA%' AND offense_type IS NULL)
            OR (description LIKE '%COCAINE%' AND offense_type IS NULL)
            OR (description LIKE '%HALLUCINOGEN%' AND offense_type IS NULL)
            ) AS form_information_drug_charge,
        LOGICAL_OR(statute LIKE '%CS13A14%' 
            OR description LIKE '%ADMINISTER/DISPENSE/DELIVERY BY PRACTITIONER%') AS form_information_statue_14,
        LOGICAL_OR(statute LIKE '%CS13A30%' 
            OR description LIKE '%MANUFACTURE/SALE/DELIVER OR POSSESS W/INTENT TO%') AS form_information_statue_30,
        LOGICAL_OR(statute LIKE '%CS13A37%' 
            OR description LIKE '%POSSESS EXCESSIVE AMOUNTS OF STERIODS%') AS form_information_statue_37, 
            -- steroids is misspelled in the data
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_charge`
    WHERE state_code = 'US_PA'
    GROUP BY 1
  )
  SELECT
    eligible_and_almost_eligible.* EXCEPT(is_almost_eligible),
    form_information.* EXCEPT(person_id),
  FROM eligible_and_almost_eligible
  LEFT JOIN form_information
    ON eligible_and_almost_eligible.person_id = form_information.person_id 
"""

US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_NAME,
    view_query_template=US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_QUERY_TEMPLATE,
    description=US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_DESCRIPTION,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_PA
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER.build_and_print()
