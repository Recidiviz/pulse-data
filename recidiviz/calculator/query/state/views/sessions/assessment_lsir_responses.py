# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Individual questions and components of the LSI-R assessment, across states"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ASSESSMENT_LSIR_RESPONSES_VIEW_NAME = "assessment_lsir_responses"

ASSESSMENT_LSIR_RESPONSES_VIEW_DESCRIPTION = (
    """Individual questions and components of the LSI-R assessment"""
)

ASSESSMENT_LSIR_RESPONSES_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT * FROM `{project_id}.{sessions_dataset}.us_id_raw_lsir_assessments`
    UNION ALL 
    SELECT * FROM `{project_id}.{sessions_dataset}.us_nd_raw_lsir_assessments`
    """

ASSESSMENT_LSIR_RESPONSES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=ASSESSMENT_LSIR_RESPONSES_VIEW_NAME,
    view_query_template=ASSESSMENT_LSIR_RESPONSES_QUERY_TEMPLATE,
    description=ASSESSMENT_LSIR_RESPONSES_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSESSMENT_LSIR_RESPONSES_VIEW_BUILDER.build_and_print()
