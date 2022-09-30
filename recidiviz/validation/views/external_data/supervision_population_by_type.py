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
"""A view containing external data for aggregate supervision type populations to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.external_data import regions as external_data_regions

VIEW_ID = "supervision_population_by_type"


def get_supervision_population_by_type_view_builder() -> SimpleBigQueryViewBuilder:
    region_views = BigQueryViewCollector.collect_view_builders_in_module(
        builder_type=SimpleBigQueryViewBuilder,
        view_dir_module=external_data_regions,
        recurse=True,
        view_builder_attribute_name_regex=".*_VIEW_BUILDER",
    )

    region_subqueries = []
    region_dataset_params = {}
    # Gather all region views with a matching view id and union them in.
    for region_view in region_views:
        if region_view.view_id == VIEW_ID and region_view.should_deploy_in_project(
            metadata.project_id()
        ):
            dataset_param = f"{region_view.table_for_query.dataset_id}_dataset"
            region_dataset_params[
                dataset_param
            ] = region_view.table_for_query.dataset_id
            region_subqueries.append(
                f"""
                SELECT
                  region_code, date_of_supervision, supervision_type, population_count
                FROM `{{project_id}}.{{{dataset_param}}}.{region_view.table_for_query.table_id}`
                """
            )

    query_template = "\nUNION ALL\n".join(region_subqueries)

    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
        view_id=VIEW_ID,
        view_query_template=query_template,
        description="Contains external data for aggregate supervision populations by type "
        "to validate against. See http://go/external-validations for instructions on adding "
        "new data.",
        should_materialize=True,
        # Specify default values here so that mypy knows these are not used in the
        # dictionary below.
        projects_to_deploy=None,
        materialized_address_override=None,
        should_deploy_predicate=None,
        clustering_fields=None,
        # Query format arguments
        **region_dataset_params,
    )


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        get_supervision_population_by_type_view_builder().build_and_print()
