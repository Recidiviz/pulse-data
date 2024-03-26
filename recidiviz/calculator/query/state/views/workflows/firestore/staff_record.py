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
"""View to prepare staff records for Workflows for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.us_ca.supervision_staff_template import (
    US_CA_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_id.supervision_staff_template import (
    US_ID_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_ix.supervision_staff_template import (
    US_IX_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_me.staff_template import (
    US_ME_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_mi.supervision_staff_template import (
    US_MI_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_mo.incarceration_staff_template import (
    US_MO_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_nd.supervision_staff_template import (
    US_ND_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_or.supervision_staff_template import (
    US_OR_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_pa.supervision_staff_template import (
    US_PA_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.incarceration_staff_template import (
    US_TN_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.supervision_staff_template import (
    US_TN_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STAFF_RECORD_VIEW_NAME = "staff_record"

STAFF_RECORD_DESCRIPTION = """
    Staff records to be exported to Firestore to power Workflows.
    """
STAFF_RECORD_QUERY_TEMPLATE = f"""
    WITH 
        tn_staff AS ({US_TN_SUPERVISION_STAFF_TEMPLATE})
        , nd_staff AS ({US_ND_SUPERVISION_STAFF_TEMPLATE})
        , or_staff AS ({US_OR_SUPERVISION_STAFF_TEMPLATE})
        , pa_staff AS ({US_PA_SUPERVISION_STAFF_TEMPLATE})
        , id_staff AS ({US_ID_SUPERVISION_STAFF_TEMPLATE})
        , ix_staff AS ({US_IX_SUPERVISION_STAFF_TEMPLATE})
        , me_staff AS ({US_ME_STAFF_TEMPLATE})
        , mi_staff AS ({US_MI_SUPERVISION_STAFF_TEMPLATE})
        , mo_staff AS ({US_MO_INCARCERATION_STAFF_TEMPLATE})
        , ca_staff AS ({US_CA_SUPERVISION_STAFF_TEMPLATE})
        , tn_facility_staff AS ({US_TN_INCARCERATION_STAFF_TEMPLATE})
    
    SELECT {{columns}} FROM tn_staff
    UNION ALL 
    SELECT {{columns}} FROM nd_staff
    UNION ALL
    SELECT {{columns}} FROM id_staff
    UNION ALL
    SELECT {{columns}} FROM ix_staff
    UNION ALL
    SELECT {{columns}} FROM me_staff
    UNION ALL
    SELECT {{columns}} FROM mi_staff
    UNION ALL
    SELECT {{columns}} FROM mo_staff
    UNION ALL
    SELECT {{columns}} FROM ca_staff
    UNION ALL
    SELECT {{columns}} FROM or_staff
    UNION ALL
    SELECT {{columns}} FROM pa_staff
    UNION ALL
    SELECT {{columns}} FROM tn_facility_staff
"""

# TODO(#25057): Remove staff_record once we are fully using incarceration and supervision staff collections
STAFF_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=STAFF_RECORD_VIEW_NAME,
    view_query_template=STAFF_RECORD_QUERY_TEMPLATE,
    description=STAFF_RECORD_DESCRIPTION,
    columns=[
        "id",
        "state_code",
        "district",
        "email",
        "has_caseload",
        "has_facility_caseload",
        "given_names",
        "surname",
        "role_subtype",
    ],
    static_reference_tables_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    normalized_state_dataset=dataset_config.NORMALIZED_STATE_DATASET,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    vitals_report_dataset=dataset_config.VITALS_REPORT_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    us_ca_raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_CA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STAFF_RECORD_VIEW_BUILDER.build_and_print()
