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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.us_nd.supervision_clients_template import (
    US_ND_SUPERVISION_CLIENTS_QUERY_TEMPLATE,
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

CLIENT_RECORD_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH 
        {US_TN_SUPERVISION_CLIENTS_QUERY_TEMPLATE},
        {US_ND_SUPERVISION_CLIENTS_QUERY_TEMPLATE}
    SELECT *, {PSEUDONYMIZED_ID} AS pseudonymized_id FROM tn_clients
    UNION ALL
    SELECT *, {PSEUDONYMIZED_ID} AS pseudonymized_id FROM nd_clients
"""

CLIENT_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENT_RECORD_VIEW_NAME,
    view_query_template=CLIENT_RECORD_QUERY_TEMPLATE,
    description=CLIENT_RECORD_DESCRIPTION,
    analyst_views_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    dataflow_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    us_nd_raw_data="us_nd_raw_data_up_to_date_views",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_VIEW_BUILDER.build_and_print()
