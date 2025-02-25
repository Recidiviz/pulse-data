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
"""View to prepare Sentencing case insight records for PSI tools for export to the frontend."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.sentencing.us_ix.sentencing_case_insights_template import (
    US_IX_SENTENCING_CASE_INSIGHTS_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SENTENCING_CASE_INSIGHTS_RECORD_VIEW_NAME = "case_insights_record"

SENTENCING_CASE_INSIGHTS_RECORD_DESCRIPTION = """
    Sentencing case insight records to be exported to frontend to power PSI tools.
    """

SENTENCING_CASE_INSIGHTS_RECORD_QUERY_TEMPLATE = f"""
   WITH 
    ix_case_insights AS 
        ({US_IX_SENTENCING_CASE_INSIGHTS_TEMPLATE}), 
    -- full_query serves as a template for when Sentencing expands to other states and we union other views
    full_query AS 
    (
        SELECT * FROM ix_case_insights
    ) 
    SELECT
        {{columns}}
    FROM full_query
"""

SENTENCING_CASE_INSIGHTS_RECORD_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    view_id=SENTENCING_CASE_INSIGHTS_RECORD_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=SENTENCING_CASE_INSIGHTS_RECORD_QUERY_TEMPLATE,
    description=SENTENCING_CASE_INSIGHTS_RECORD_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "gender",
        "assessment_score_bucket_start",
        "assessment_score_bucket_end",
        "most_severe_description",
        "recidivism_rollup",
        "recidivism_num_records",
        "recidivism_probation_series",
        "recidivism_rider_series",
        "recidivism_term_series",
        "disposition_num_records",
        "disposition_probation_pc",
        "disposition_rider_pc",
        "disposition_term_pc",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SENTENCING_CASE_INSIGHTS_RECORD_VIEW_BUILDER.build_and_print()
