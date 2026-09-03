# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Builds the source table repository of all source tables deployed to a project's
BigQuery graph, along with helpers to query its addresses and datasets."""
from functools import cache

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables.collect_all_source_table_configs import (
    collect_source_table_collections_hydrated_outside_view_graphs,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


@cache
def build_source_table_repository_for_collected_schemata(
    project_id: str,
) -> SourceTableRepository:
    """Builds a source table repository for all source tables deployed to the given
    project's BigQuery graph.

    Tables written by Python code (not YAML-managed) must be registered in one
    of the collections in this file — view-graph validation only materializes
    tables found in this repository, so views over an unregistered table fail
    view_graph_validation_test.py. Choose the update_config per the guidance on
    SourceTableCollectionUpdateConfig (regenerable only if the table can be
    rebuilt from its source).
    """
    return SourceTableRepository(
        source_table_collections=collect_source_table_collections_hydrated_outside_view_graphs(
            project_id
        )
    )


@cache
def get_source_table_datasets_to_descriptions(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> dict[str, str]:
    datasets_to_descriptions: dict[str, str] = {}
    for c in build_source_table_repository_for_collected_schemata(
        project_id
    ).source_table_collections:
        if (
            c.dataset_id in datasets_to_descriptions
            and c.description != datasets_to_descriptions[c.dataset_id]
        ):
            raise ValueError(
                f"Found description for dataset {c.dataset_id} [{c.description}] which "
                f"has conflicting versions across source table configurations. "
                f"Conflicting description: [{datasets_to_descriptions[c.dataset_id]}]"
            )

        datasets_to_descriptions[c.dataset_id] = c.description
    return datasets_to_descriptions


@cache
def get_source_table_addresses(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[BigQueryAddress]:
    """Returns the addresses of all the source tables deployed to the given project."""
    return set(
        build_source_table_repository_for_collected_schemata(
            project_id
        ).source_tables.keys()
    )


@environment.local_only
@cache
def get_all_source_table_addresses() -> set[BigQueryAddress]:
    """Returns the addresses of all the source tables deployed across any GCP
    project.
    """
    all_addresses = set()
    for project_id in DATA_PLATFORM_GCP_PROJECTS:
        with local_project_id_override(project_id):
            all_addresses |= get_source_table_addresses(project_id)
    return all_addresses


@cache
def get_source_table_datasets(
    # We require project_id as an argument so that we don't return incorrect cached
    # results when metadata.project_id() changes (e.g. in tests).
    project_id: str,
) -> set[str]:
    """Returns the dataset ids of all the source tables deployed to the given
    project.
    """
    source_table_repository = build_source_table_repository_for_collected_schemata(
        project_id=project_id,
    )
    return {
        source_table_collection.dataset_id
        for source_table_collection in source_table_repository.source_table_collections
    }


@environment.local_only
@cache
def get_all_source_table_datasets() -> set[str]:
    """Returns the dataset ids of all the source tables deployed across any GCP
    project.
    """
    all_datasets = set()
    for project_id in DATA_PLATFORM_GCP_PROJECTS:
        with local_project_id_override(project_id):
            all_datasets |= get_source_table_datasets(project_id)
    return all_datasets


if __name__ == "__main__":
    import pprint

    with local_project_id_override("recidiviz-staging"):
        pprint.pprint(
            build_source_table_repository_for_collected_schemata(metadata.project_id())
        )
