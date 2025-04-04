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
"""View to prepare Sentencing case records for PSI tools for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCING_CASE_RECORD_VIEW_NAME = "sentencing_case_record"

SENTENCING_CASE_RECORD_DESCRIPTION = """
    Sentencing case records to be exported to frontend to power PSI tools.
    """

SENTENCING_CASE_RECORD_QUERY_TEMPLATE = """
    SELECT
        {columns}
    FROM `{project_id}.sentencing_views.sentencing_case_record_historical_materialized`
    WHERE
        -- Make sure that case is either not completed or completed within the last three months 
        (completion_date > DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH) OR completion_date IS NULL)
        -- For US_ND, only keep cases within the past year (there are some very old cases that were never completed)
        AND (state_code != 'US_ND' OR due_date > DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR))
        -- Don't include cases where the staff member is inactive
        AND employee_inactive = "0"
"""

SENTENCING_CASE_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    view_id=SENTENCING_CASE_RECORD_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCING_CASE_RECORD_QUERY_TEMPLATE,
    description=SENTENCING_CASE_RECORD_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "staff_id",
        "client_id",
        "due_date",
        "county",
        "lsir_score",
        "lsir_level",
        "report_type",
        "external_id",
        "district",
        "investigation_status",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCING_CASE_RECORD_VIEW_BUILDER.build_and_print()
