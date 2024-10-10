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
"""View to prepare Sentencing charge records for PSI tools for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.sentencing.us_ix.sentencing_charge_template import (
    US_IX_SENTENCING_CHARGE_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCING_CHARGE_RECORD_VIEW_NAME = "sentencing_charge_record"

SENTENCING_CHARGE_RECORD_DESCRIPTION = """
    Sentencing charge records to be exported to frontend to power PSI tools.
    """

SENTENCING_CHARGE_RECORD_QUERY_TEMPLATE = f"""
WITH 
    ix_charges AS 
        ({US_IX_SENTENCING_CHARGE_TEMPLATE}), 
    -- full_query serves as a template for when Sentencing expands to other states and we union other views
    full_query AS 
    (
        SELECT * FROM ix_charges
    ) 
    SELECT
        {{columns}}
    FROM full_query
"""

SENTENCING_CHARGE_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    view_id=SENTENCING_CHARGE_RECORD_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCING_CHARGE_RECORD_QUERY_TEMPLATE,
    description=SENTENCING_CHARGE_RECORD_DESCRIPTION,
    should_materialize=True,
    columns=["state_code", "charge", "is_sex_offense", "is_violent"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCING_CHARGE_RECORD_VIEW_BUILDER.build_and_print()
