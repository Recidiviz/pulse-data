# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Types of admissions for each state in the matrix views."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_NAME = "admission_types_per_state_for_matrix"

ADMISSION_TYPES_PER_STATE_FOR_MATRIX_DESCRIPTION = """
Types of admissions for each state in the matrix views
"""

ADMISSION_TYPES_PER_STATE_FOR_MATRIX_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        DISTINCT state_code, admission_type
    FROM `{project_id}.{reference_views_dataset}.revocations_matrix_by_person_materialized`
    """

ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_NAME,
    should_materialize=True,
    view_query_template=ADMISSION_TYPES_PER_STATE_FOR_MATRIX_QUERY_TEMPLATE,
    description=ADMISSION_TYPES_PER_STATE_FOR_MATRIX_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_BUILDER.build_and_print()
