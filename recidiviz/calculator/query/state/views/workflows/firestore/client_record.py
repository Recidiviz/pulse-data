#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""View to prepare client records for Workflows for export to the frontend."""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    list_to_query_string,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
    PersonRecordType,
    WorkflowsOpportunityConfig,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record_ctes import (
    full_client_record,
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
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_RECORD_VIEW_NAME = "client_record"

CLIENT_RECORD_DESCRIPTION = """
    Client records to be exported to Firestore to power the Workflows dashboard.
    """

client_record_fields = [
    "person_external_id",
    "display_id",
    "state_code",
    "person_name",
    "officer_id",
    "supervision_type",
    "supervision_level",
    "supervision_level_start",
    "address",
    "phone_number",
    "email_address",
    "supervision_start_date",
    "expiration_date",
    "district",
    "current_employers",
    "all_eligible_opportunities",
    "milestones",
    "current_balance",
    "last_payment_date",
    "last_payment_amount",
    "special_conditions",
    "board_conditions",
]

WORKFLOWS_CONFIGS_WITH_CLIENTS = [
    config
    for config in WORKFLOWS_OPPORTUNITY_CONFIGS
    if config.person_record_type == PersonRecordType.CLIENT
]

WORKFLOWS_SUPERVISION_STATES = sorted(
    {config.state_code.value for config in WORKFLOWS_CONFIGS_WITH_CLIENTS}
) + ["US_TX"]
# Note: TX currently only supports tasks and not any opportunities

WORKFLOWS_MILESTONES_STATES = ["US_IX", "US_MI", "US_CA"]

HASH_VALUE_QUERY_STR = (
    "IF(state_code='US_IX', 'US_ID', state_code) || person_external_id"
)


def get_eligibility_ctes(configs: List[WorkflowsOpportunityConfig]) -> str:
    cte_body = "\n        UNION ALL\n".join(
        [
            f"""
        SELECT
            state_code,
            external_id AS person_external_id,
            "{config.opportunity_type}" AS opportunity_name,
            is_eligible,
            is_almost_eligible,
        FROM `{{project_id}}.{{workflows_dataset}}.{config.opportunity_record_view_name}`
        -- TODO(Recidiviz/recidiviz-dashboards#5003): Remove this conditional
        WHERE external_id IS NOT NULL
        """
            for config in configs
        ]
    )
    return f"""
    opportunities AS (
        {cte_body}
    ),
    opportunities_aggregated AS (
        SELECT
            state_code,
            person_external_id,
            ARRAY_AGG(
                DISTINCT IF(is_eligible OR is_almost_eligible, opportunity_name, NULL)
                IGNORE NULLS
                ORDER BY IF(is_eligible OR is_almost_eligible, opportunity_name, NULL)
            ) AS all_eligible_opportunities
        FROM opportunities
        GROUP BY 1, 2
    )
    """


CLIENT_RECORD_QUERY_TEMPLATE = f"""
    WITH 
        {get_eligibility_ctes(WORKFLOWS_CONFIGS_WITH_CLIENTS)},
        {full_client_record()}
          
        SELECT *,
            {get_pseudonymized_id_query_str(HASH_VALUE_QUERY_STR)} AS pseudonymized_id FROM clients
"""


CLIENT_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENT_RECORD_VIEW_NAME,
    view_query_template=CLIENT_RECORD_QUERY_TEMPLATE,
    description=CLIENT_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    dataflow_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    us_ca_task_eligibility_spans_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_CA
    ),
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
    ),
    us_ca_raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_CA, instance=DirectIngestInstance.PRIMARY
    ),
    us_or_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_OR, instance=DirectIngestInstance.PRIMARY
    ),
    us_pa_raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_PA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
    workflows_supervision_states=list_to_query_string(
        WORKFLOWS_SUPERVISION_STATES, quoted=True
    ),
    workflows_milestones_states=list_to_query_string(
        WORKFLOWS_MILESTONES_STATES, quoted=True
    ),
    static_reference_tables_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_VIEW_BUILDER.build_and_print()
