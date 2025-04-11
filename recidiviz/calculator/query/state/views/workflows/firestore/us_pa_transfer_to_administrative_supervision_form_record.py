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
# TODO(#37715) - Pull time on supervision from sentencing once sentencing v2 is implemented in PA

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.task_eligibility.utils.us_pa_query_fragments import (
    adm_case_notes_helper,
    adm_form_information_helper,
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
  case_notes_cte AS (
    {adm_case_notes_helper()}
  ),
  array_case_notes_cte AS (
    {array_agg_case_notes_by_external_id()}
  ),
  form_information AS (
    SELECT person_id,
        LOGICAL_OR(form_information_drug_charge_initial AND guilty_charge_indicator) AS form_information_drug_conviction_initial,
        LOGICAL_OR(form_information_statute_14 AND guilty_charge_indicator) AS form_information_statute_14_conviction,
        LOGICAL_OR(form_information_statute_30 AND guilty_charge_indicator) AS form_information_statute_30_conviction,
        LOGICAL_OR(form_information_statute_37 AND guilty_charge_indicator) AS form_information_statute_37_conviction,
        LOGICAL_OR(form_information_drug_charge_initial AND unreported_disposition_indicator) AS form_information_drug_unreported_disposition_initial,
        LOGICAL_OR(form_information_statute_14 AND unreported_disposition_indicator) AS form_information_statute_14_unreported_disposition,
        LOGICAL_OR(form_information_statute_30 AND unreported_disposition_indicator) AS form_information_statute_30_unreported_disposition,
        LOGICAL_OR(form_information_statute_37 AND unreported_disposition_indicator) AS form_information_statute_37_unreported_disposition,
    FROM ({adm_form_information_helper()})
    GROUP BY 1
  )
  SELECT
    eligible_and_almost_eligible.*,
    array_case_notes_cte.case_notes,
    (form_information_statute_14_conviction OR form_information_statute_14_unreported_disposition) AS form_information_statue_14, #TODO(#40577) to remove once front end is updated
    (form_information_statute_14_conviction OR form_information_statute_14_unreported_disposition) AS form_information_statute_14,
    (form_information_statute_30_conviction OR form_information_statute_30_unreported_disposition) AS form_information_statue_30,  #TODO(#40577) to remove once front end is updated
    (form_information_statute_30_conviction OR form_information_statute_30_unreported_disposition) AS form_information_statute_30,
    (form_information_statute_37_conviction OR form_information_statute_37_unreported_disposition) AS form_information_statue_37,  #TODO(#40577) to remove once front end is updated
    (form_information_statute_37_conviction OR form_information_statute_37_unreported_disposition) AS form_information_statute_37,
    (form_information_drug_conviction_initial OR form_information_statute_14_conviction OR form_information_statute_30_conviction OR form_information_statute_37_conviction)
        AS form_information_drug_conviction, -- make sure that if sub-section is checked because of a conviction, the first drug box is checked 
    (form_information_drug_conviction_initial OR form_information_statute_14_conviction OR form_information_statute_30_conviction OR form_information_statute_37_conviction)
        AS form_information_drug_charge, #TODO(#40577) to remove once front end is updated
    (form_information_drug_unreported_disposition_initial OR form_information_statute_14_unreported_disposition OR form_information_statute_30_unreported_disposition OR form_information_statute_37_unreported_disposition)
        AS form_information_drug_unreported_disposition, -- make sure that if sub-section is checked because of an unreported disposition, the second drug box is checked 
  FROM eligible_and_almost_eligible
  LEFT JOIN array_case_notes_cte 
    ON eligible_and_almost_eligible.external_id = array_case_notes_cte.external_id
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
    us_pa_raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_PA, instance=DirectIngestInstance.PRIMARY
    ),
    us_pa_raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_PA, instance=DirectIngestInstance.PRIMARY
    ),
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER.build_and_print()
