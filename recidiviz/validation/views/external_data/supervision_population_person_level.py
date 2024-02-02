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
"""A view containing external data for person level supervision population to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.external_data import regions as external_data_regions

VIEW_ID = "supervision_population_person_level"


def get_supervision_population_person_level_view_builder() -> UnionAllBigQueryViewBuilder:
    """Creates a builder that unions person level supervision validation data from all
    regions.
    """
    region_view_builders = BigQueryViewCollector.collect_view_builders_in_module(
        builder_type=SimpleBigQueryViewBuilder,
        view_dir_module=external_data_regions,
        recurse=True,
        view_builder_attribute_name_regex=".*_VIEW_BUILDER",
    )

    filtered_view_builders = [
        vb for vb in region_view_builders if vb.view_id == VIEW_ID
    ]

    return UnionAllBigQueryViewBuilder(
        dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
        view_id=VIEW_ID,
        description="Contains external data for person level supervision population to "
        "validate against. See http://go/external-validations for instructions on adding "
        "new data.",
        parent_view_builders=filtered_view_builders,
        builder_to_select_statement=lambda _: """SELECT
  region_code,
  person_external_id,
  external_id_type,
  date_of_supervision,
  district,
  supervising_officer,
  supervision_level
""",
    )


if __name__ == "__main__":
    with metadata.local_project_id_override(GCP_PROJECT_STAGING):
        get_supervision_population_person_level_view_builder().build_and_print()
