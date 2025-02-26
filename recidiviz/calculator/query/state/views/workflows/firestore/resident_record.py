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
"""View to prepare resident records for Workflows for export to the frontend."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
    PersonRecordType,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    get_eligibility_ctes,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record_ctes import (
    full_resident_record,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RESIDENT_RECORD_VIEW_NAME = "resident_record"

RESIDENT_RECORD_DESCRIPTION = """
    Resident records to be exported to Firestore to power the Workflows dashboard.
    """

PSEUDONYMIZED_ID = """
    # truncating hash string
    SUBSTRING(
        # hashing external ID to base64url
        REPLACE(
            REPLACE(
                TO_BASE64(SHA256(state_code || person_external_id || wk.value)), 
                '+', 
                '-'
            ), 
            '/', 
            '_'
        ), 
        1, 
        16
    )"""

WORKFLOWS_CONFIGS_WITH_RESIDENTS = [
    config
    for config in WORKFLOWS_OPPORTUNITY_CONFIGS
    if config.person_record_type == PersonRecordType.RESIDENT
]

RESIDENT_RECORD_QUERY_TEMPLATE = f"""
    WITH
        {get_eligibility_ctes(WORKFLOWS_CONFIGS_WITH_RESIDENTS)},
        {full_resident_record()}
    SELECT
        * EXCEPT(key, value),
        {PSEUDONYMIZED_ID} AS pseudonymized_id
    FROM residents
    JOIN {{project_id}}.{{static_reference_dataset}}.workflows_keys wk
        ON wk.key = 'resident_record_salt'
"""

RESIDENT_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=RESIDENT_RECORD_VIEW_NAME,
    view_query_template=RESIDENT_RECORD_QUERY_TEMPLATE,
    description=RESIDENT_RECORD_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    dataflow_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
    ),
    analyst_dataset=ANALYST_VIEWS_DATASET,
    us_me_raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
    workflows_incarceration_states=list_to_query_string(
        ["US_ME", "US_MO"], quoted=True
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RESIDENT_RECORD_VIEW_BUILDER.build_and_print()
