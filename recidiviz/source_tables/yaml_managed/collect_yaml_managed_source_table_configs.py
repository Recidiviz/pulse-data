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
"""Functionality for collecting configs for all source tables whose schemas are managed
by our standard source table update process, with schemas defined in a YAML file in this
directory.
"""
import os
from functools import cache

from recidiviz.source_tables.collect_source_tables_from_yamls import (
    collect_source_tables_from_yamls_by_dataset,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository
from recidiviz.source_tables.yaml_managed.datasets import (
    YAML_MANAGED_DATASETS_TO_DESCRIPTIONS,
)


@cache
def collect_yaml_managed_source_table_collections(
    project_id: str | None,
) -> list[SourceTableCollection]:
    """
    Collects configuration for source tables whose schemas are managed by our standard
    source table update process, with schemas defined in a YAML file in this directory.

    If project_id is None, returns all source tables that exist in any project.
    Otherwise, only returns the collections that are deployed to the given project.
    """

    source_tables_by_dataset = collect_source_tables_from_yamls_by_dataset(
        yamls_root_path=os.path.dirname(__file__)
    )

    return [
        SourceTableCollection(
            dataset_id=dataset_id,
            update_config=SourceTableCollectionUpdateConfig.protected(),
            source_tables_by_address={
                source_table.address: source_table
                for source_table in source_tables
                # Filter project-specific source tables
                if (not project_id or not source_table.deployed_projects)
                or (project_id in source_table.deployed_projects)
            },
            description=YAML_MANAGED_DATASETS_TO_DESCRIPTIONS[dataset_id],
        )
        for dataset_id, source_tables in source_tables_by_dataset.items()
    ]


@cache
def build_source_table_repository_for_yaml_managed_tables(
    project_id: str | None,
) -> SourceTableRepository:
    return SourceTableRepository(
        source_table_collections=[
            *collect_yaml_managed_source_table_collections(project_id=project_id),
        ],
    )
