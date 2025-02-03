#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
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
"""View to prepare incarceration staff records for Workflows for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.us_az.incarceration_staff_template import (
    US_AZ_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_ix.incarceration_staff_template import (
    US_IX_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_me.incarceration_staff_template import (
    US_ME_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_nd.incarceration_staff_template import (
    US_ND_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_STAFF_RECORD_VIEW_NAME = "incarceration_staff_record"

INCARCERATION_STAFF_RECORD_DESCRIPTION = """
    Incarceration staff records to be exported to Firestore to power Workflows.
    """

INCARCERATION_STAFF_RECORD_QUERY_TEMPLATE = f"""
    WITH 
        me_staff AS ({US_ME_INCARCERATION_STAFF_TEMPLATE}),
        nd_staff AS ({US_ND_INCARCERATION_STAFF_TEMPLATE}),
        az_staff AS ({US_AZ_INCARCERATION_STAFF_TEMPLATE}),
        ix_staff AS ({US_IX_INCARCERATION_STAFF_TEMPLATE})
    SELECT {{columns}} FROM me_staff    
    UNION ALL
    SELECT {{columns}} FROM nd_staff
    UNION ALL
    SELECT {{columns}} FROM az_staff
    UNION ALL
    SELECT {{columns}} FROM ix_staff
"""

INCARCERATION_STAFF_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=INCARCERATION_STAFF_RECORD_VIEW_NAME,
    view_query_template=INCARCERATION_STAFF_RECORD_QUERY_TEMPLATE,
    description=INCARCERATION_STAFF_RECORD_DESCRIPTION,
    columns=[
        "id",
        "state_code",
        "district",
        "email",
        "given_names",
        "surname",
        "role_subtype",
        "pseudonymized_id",
    ],
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    us_az_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_STAFF_RECORD_VIEW_BUILDER.build_and_print()
