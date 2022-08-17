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
from recidiviz.calculator.query.state.views.workflows.us_nd.supervision_staff_template import (
    US_ND_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.supervision_staff_template import (
    US_TN_SUPERVISION_STAFF_TEMPLATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.datasets.static_data.config import EXTERNAL_REFERENCE_DATASET
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

STAFF_RECORD_VIEW_NAME = "staff_record"

STAFF_RECORD_DESCRIPTION = """
    Staff records to be exported to Firestore to power Workflows.
    """

STAFF_RECORD_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH 
        tn_staff AS ({US_TN_SUPERVISION_STAFF_TEMPLATE})
        , nd_staff AS ({US_ND_SUPERVISION_STAFF_TEMPLATE})
    
    SELECT {{columns}} FROM tn_staff
    UNION ALL 
    SELECT {{columns}} FROM nd_staff
"""

STAFF_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=STAFF_RECORD_VIEW_NAME,
    view_query_template=STAFF_RECORD_QUERY_TEMPLATE,
    description=STAFF_RECORD_DESCRIPTION,
    columns=["id", "state_code", "name", "district", "email", "has_caseload"],
    static_reference_tables_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    analyst_views_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    external_reference_dataset=EXTERNAL_REFERENCE_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_TN.value
    ),
    vitals_report_dataset=dataset_config.VITALS_REPORT_DATASET,
    workflows_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        STAFF_RECORD_VIEW_BUILDER.build_and_print()
