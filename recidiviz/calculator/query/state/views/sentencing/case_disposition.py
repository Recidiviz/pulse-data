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
"""View that associates cases with the PSI staff and the eventual sentencing disposition for use in impact analysis."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.sentencing.us_ix.sentencing_case_disposition_template import (
    US_IX_SENTENCING_CASE_DISPOSITION_TEMPLATE,
)
from recidiviz.calculator.query.state.views.sentencing.us_nd.sentencing_case_disposition_template import (
    US_ND_SENTENCING_CASE_DISPOSITION_TEMPLATE,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCING_CASE_DISPOSITION_VIEW_NAME = "sentencing_case_disposition"

SENTENCING_CASE_DISPOSITION_DESCRIPTION = """
    Sentencing case records associated with the assigned PSI and the eventual sentencing disposition outcome.
    """

SENTENCING_CASE_DISPOSITION_QUERY_TEMPLATE = f"""
WITH 
    ix_cases AS 
        ({US_IX_SENTENCING_CASE_DISPOSITION_TEMPLATE}),  
    nd_cases AS
        ({US_ND_SENTENCING_CASE_DISPOSITION_TEMPLATE}),  
    -- full_query serves as a template for when Sentencing expands to other states and we union other views
    full_query AS 
    (   
        SELECT * FROM ix_cases
        UNION ALL
        SELECT * FROM nd_cases
    ) 
    SELECT
        {{columns}}
    FROM full_query
"""

SENTENCING_CASE_DISPOSITION_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    view_id=SENTENCING_CASE_DISPOSITION_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCING_CASE_DISPOSITION_QUERY_TEMPLATE,
    description=SENTENCING_CASE_DISPOSITION_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
    columns=[
        "state_code",
        "staff_id",
        "client_id",
        "person_id",
        "case_id",
        "psi_email",
        "gender",
        "lsir_score",
        "assessment_score",
        "disposition",
        "location_id",
        "location_name",
        "due_date",
        "completion_date",
        "sentence_date",
        "assigned_date",
        "sentence_start_date",
        "most_severe_description",
        "any_is_violent_uniform",
        "any_is_drug_uniform",
        "any_is_sex_offense",
        "assigned_date_diff_days",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.build_and_print()
