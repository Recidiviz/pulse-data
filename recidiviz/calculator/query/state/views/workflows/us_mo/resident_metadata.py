# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Missouri resident metadata"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import WORKFLOWS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_RESIDENT_METADATA_VIEW_NAME = "us_mo_resident_metadata"

US_MO_RESIDENT_METADATA_VIEW_DESCRIPTION = """
Missouri resident metadata
"""


US_MO_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = """
    SELECT
        person_id,
        metadata_all_sanctions AS d1_sanction_info_past_year,
        metadata_num_d1_sanctions_past_year AS num_d1_sanctions_past_year,
        metadata_solitary_assignment_info_past_year AS solitary_assignment_info_past_year,
        metadata_num_solitary_assignments_past_year AS num_solitary_assignments_past_year,
    FROM `{project_id}.analyst_data.us_mo_restrictive_housing_record_materialized`
"""

US_MO_RESIDENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_MO_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_MO_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_MO_RESIDENT_METADATA_VIEW_DESCRIPTION,
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_RESIDENT_METADATA_VIEW_BUILDER.build_and_print()
