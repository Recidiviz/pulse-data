#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
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
"""View to prepare Sentencing staff records for PSI tools for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import get_pseudonymized_id_query_str
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.sentencing.us_ix.sentencing_staff_template import (
    US_IX_SENTENCING_STAFF_TEMPLATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCING_STAFF_RECORD_VIEW_NAME = "sentencing_staff_record"

SENTENCING_STAFF_RECORD_DESCRIPTION = """
    Sentencing staff records to be exported to frontend to power PSI tools.
    """

SENTENCING_STAFF_RECORD_QUERY_TEMPLATE = f"""
   WITH 
        ix_staff AS ({US_IX_SENTENCING_STAFF_TEMPLATE}), 

        -- full_query serves as a template for when PSI expands to other states and we union other views
        full_query AS 
        (
            SELECT 
                ix.*
            FROM ix_staff ix
        ),
        -- add pseudonymized Ids to all staff records
        full_query_with_pseudo AS
        (
            SELECT
                *,
                -- Use the same pseudo id function as the client record for consistency
                -- across queries
                {get_pseudonymized_id_query_str("IF(state_code = 'US_IX', 'US_ID', state_code) || external_id")} AS pseudonymized_id,
            FROM full_query
        )
         
    SELECT
        {{columns}}
    FROM full_query_with_pseudo
"""

SENTENCING_STAFF_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    view_id=SENTENCING_STAFF_RECORD_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCING_STAFF_RECORD_QUERY_TEMPLATE,
    description=SENTENCING_STAFF_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
    columns=[
        "state_code",
        "full_name",
        "external_id",
        "email",
        "pseudonymized_id",
        "case_ids",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCING_STAFF_RECORD_VIEW_BUILDER.build_and_print()
