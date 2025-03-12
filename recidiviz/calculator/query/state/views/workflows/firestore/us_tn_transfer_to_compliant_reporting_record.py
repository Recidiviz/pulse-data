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
"""Query for relevant metadata needed to support compliant reporting opportunity in Tennessee
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    us_tn_compliant_reporting_shared_opp_record_fragment,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_NAME = (
    "us_tn_transfer_to_compliant_reporting_record"
)

US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_DESCRIPTION = """
    Query for relevant metadata needed to support compliant reporting opportunity in Tennessee
    """
US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_QUERY_TEMPLATE = f"""
    WITH eligible_with_discretion AS (
    -- Keep only the most current span per probation client
    {join_current_task_eligibility_spans_with_external_id(
    state_code="'US_TN'",
    tes_task_query_view='transfer_to_compliant_reporting_with_discretion_materialized',
    id_type="'US_TN_DOC'",
    eligible_and_almost_eligible_only=True,
)}
    ),
    eligible_discretion_and_almost AS (
        -- Eligible with discretion
        SELECT
            external_id,
            person_id,
            state_code,
            reasons,
            ineligible_criteria,
            is_eligible,
            is_almost_eligible,
        FROM
            eligible_with_discretion

        UNION ALL

        -- Eligible without discretion
        SELECT
            external_id,
            person_id,
            state_code,
            reasons,
            ineligible_criteria,
            is_eligible,
            is_almost_eligible,
        FROM
            ({join_current_task_eligibility_spans_with_external_id(
    state_code="'US_TN'",
    tes_task_query_view='transfer_to_compliant_reporting_no_discretion_materialized',
    id_type="'US_TN_DOC'",
    eligible_only=True,
)})
    ),
    /*
    We have two views, transfer_to_compliant_reporting_no_discretion and transfer_to_compliant_reporting_with_discretion to help identify people
    who might be eligible with discretion. This is done so that the people who are eligible with discretion AND require one other
    criteria (e.g. fines/fees) can be easily identified above as have 1 remaining criteria. The previous CTE therefore helps identify the 
    4 following groups:
    1. eligible without discretion or additional criteria
    2. eligible without discretion but requiring additional criteria
    3. eligible with discretion without additional criteria
    4. eligible with discretion and additional criteria

    Once we have those groups, we ultimately want a reasons blob that contains the relevant information for *all* required and
    discretionary criteria. To get this, we join back onto `transfer_to_compliant_reporting_no_discretion` which contains all this
    information. The reasons blob in transfer_to_compliant_reporting_with_discretion does not contain this since it is created
    without including discretionary criteria in order to identify almost-eligible folks.    
    */
    base AS (
        SELECT
            "COMPLIANT_REPORTING" AS metadata_task_name,
            a.external_id,
            a.person_id,
            a.state_code,
            a.ineligible_criteria,
            b.reasons,
            a.is_eligible,
            a.is_almost_eligible,
            NULL AS metadata_eligible_date,
        FROM eligible_discretion_and_almost a
        LEFT JOIN ({join_current_task_eligibility_spans_with_external_id(
    state_code="'US_TN'",
    tes_task_query_view='transfer_to_compliant_reporting_no_discretion_materialized',
    id_type="'US_TN_DOC'"
)}) b
        USING(person_id)   
    )
    SELECT *
    FROM (
        {us_tn_compliant_reporting_shared_opp_record_fragment()}
    )


"""

US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_NAME,
    view_query_template=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_QUERY_TEMPLATE,
    description=US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN
    ),
    should_materialize=True,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.build_and_print()
