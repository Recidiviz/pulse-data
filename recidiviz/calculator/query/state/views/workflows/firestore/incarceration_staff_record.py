#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
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
from recidiviz.calculator.query.state.views.workflows.us_me.staff_template import (
    build_us_me_staff_template,
)
from recidiviz.calculator.query.state.views.workflows.us_mi.incarceration_staff_template import (
    US_MI_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_mo.incarceration_staff_template import (
    US_MO_INCARCERATION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.incarceration_staff_template import (
    US_TN_INCARCERATION_STAFF_TEMPLATE,
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
          me_staff AS ({build_us_me_staff_template("resident_record_materialized")})
        , mo_staff AS ({US_MO_INCARCERATION_STAFF_TEMPLATE})
        , mi_staff AS ({US_MI_INCARCERATION_STAFF_TEMPLATE})
        , tn_staff AS ({US_TN_INCARCERATION_STAFF_TEMPLATE})
    SELECT {{columns}} FROM me_staff    
    UNION ALL
    SELECT {{columns}} FROM mo_staff
    UNION ALL
    SELECT {{columns}} FROM mi_staff
    UNION ALL
    SELECT {{columns}} FROM tn_staff
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
    ],
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_STAFF_RECORD_VIEW_BUILDER.build_and_print()
