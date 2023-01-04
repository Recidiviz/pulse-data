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

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record_ctes import (
    full_client_record,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.supervision_clients_template import (
    US_TN_SUPERVISION_CLIENTS_QUERY_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_RECORD_VIEW_NAME = "client_record"

CLIENT_RECORD_DESCRIPTION = """
    Client records to be exported to Firestore to power the Workflows dashboard.
    """

PSEUDONYMIZED_ID = """
    # truncating hash string
    SUBSTRING(
        # hashing external ID to base64url
        REPLACE(
            REPLACE(
                TO_BASE64(SHA256(state_code || person_external_id)), 
                '+', 
                '-'
            ), 
            '/', 
            '_'
        ), 
        1, 
        16
    )"""


@attr.s
class EligibilityQueryConfig:
    state_code: str = attr.ib()
    opportunity_name: str = attr.ib()
    opportunity_record_view: str = attr.ib()

    def eligibility_cte_name(self) -> str:
        return f"{self.state_code.lower()}_{self.opportunity_name}_eligibility"


ELIGIBILITY_QUERY_CONFIGS = [
    EligibilityQueryConfig(
        "US_ID",
        "LSU",
        "us_id_complete_transfer_to_limited_supervision_form_record_materialized",
    ),
    EligibilityQueryConfig(
        "US_ID",
        "earnedDischarge",
        "us_id_complete_discharge_early_from_supervision_request_record_materialized",
    ),
    EligibilityQueryConfig(
        "US_ID",
        "pastFTRD",
        "us_id_complete_full_term_discharge_from_supervision_request_record_materialized",
    ),
    EligibilityQueryConfig(
        "US_IX",
        "LSU",
        "us_ix_complete_transfer_to_limited_supervision_form_record_materialized",
    ),
    EligibilityQueryConfig(
        "US_IX",
        "earnedDischarge",
        "us_ix_complete_discharge_early_from_supervision_request_record_materialized",
    ),
    EligibilityQueryConfig(
        "US_IX",
        "pastFTRD",
        "us_ix_complete_full_term_discharge_from_supervision_request_record_materialized",
    ),
    EligibilityQueryConfig(
        "US_ND",
        "earlyTermination",
        "us_nd_complete_discharge_early_from_supervision_record_materialized",
    ),
]


def get_eligibility_ctes(configs: List[EligibilityQueryConfig]) -> str:
    cte_body = "\n        UNION ALL\n".join(
        [
            f"""
        SELECT
            state_code,
            external_id AS person_external_id,
            "{config.opportunity_name}" AS opportunity_name,
        FROM `{{project_id}}.{{workflows_dataset}}.{config.opportunity_record_view}`
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
            ARRAY_AGG(DISTINCT opportunity_name) AS all_eligible_opportunities
        FROM opportunities
        GROUP BY 1, 2
    )
    """


CLIENT_RECORD_QUERY_TEMPLATE = f"""
    WITH 
        {US_TN_SUPERVISION_CLIENTS_QUERY_TEMPLATE},
        {get_eligibility_ctes(ELIGIBILITY_QUERY_CONFIGS)},
        {full_client_record()}
    SELECT *, {PSEUDONYMIZED_ID} AS pseudonymized_id FROM tn_clients
    UNION ALL
    SELECT *, {PSEUDONYMIZED_ID} AS pseudonymized_id FROM clients
"""


CLIENT_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENT_RECORD_VIEW_NAME,
    view_query_template=CLIENT_RECORD_QUERY_TEMPLATE,
    description=CLIENT_RECORD_DESCRIPTION,
    analyst_views_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    us_nd_raw_data="us_nd_raw_data_up_to_date_views",
    us_id_raw_data="us_id_raw_data_up_to_date_views",
    should_materialize=True,
    state_id_type=state_specific_query_strings.state_specific_external_id_type(
        "sessions"
    ),
    workflows_supervision_states=list_to_query_string(
        ["US_ID", "US_ND", "US_IX"], quoted=True
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_VIEW_BUILDER.build_and_print()
