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
"""Functionality for collecting configs for all externally managed source tables."""
import os
from functools import cache

from recidiviz.calculator.query.state.dataset_config import (
    AUTH0_EVENTS,
    AUTH0_PROD_ACTION_LOGS,
    CASE_PLANNING_PRODUCTION_DATASET,
    EXPORT_ARCHIVES_DATASET,
    PULSE_DASHBOARD_SEGMENT_DATASET,
)
from recidiviz.source_tables.collect_source_tables_from_yamls import (
    collect_source_tables_from_yamls_by_dataset,
)
from recidiviz.source_tables.externally_managed.datasets import (
    EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS,
    JII_TEXTING_DASHBOARDS_DB_US_IX,
    JII_TEXTING_DASHBOARDS_DB_US_TX,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
    SourceTableCollectionValidationConfig,
)
from recidiviz.source_tables.source_table_repository import SourceTableRepository


@cache
def collect_externally_managed_source_table_collections(
    project_id: str | None,
) -> list[SourceTableCollection]:
    """
    Collects configuration for source tables that are managed outside of our standard
    table update process (e.g. via Terraform or via an external process that writes to
    BQ).

    Some of these datasets are created by processes that change the schema frequently.
    In order to avoid updating our YAMLs over and over again, for these datasets we
    validate that only a subset of the fields that we actually use are present.

    If project_id is None, returns all source tables that exist in any project.
    Otherwise, only returns the collections that are deployed to the given project.
    """
    source_tables_by_dataset = collect_source_tables_from_yamls_by_dataset(
        yamls_root_path=os.path.dirname(__file__)
    )

    # "required" columns here means they are required by the view graph and should be
    # validated that the fields exist in BigQuery, not the column mode (REQUIRED vs NULLABLE)
    datasets_to_validation_config = {
        AUTH0_EVENTS: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        AUTH0_PROD_ACTION_LOGS: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        CASE_PLANNING_PRODUCTION_DATASET: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        PULSE_DASHBOARD_SEGMENT_DATASET: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        EXPORT_ARCHIVES_DATASET: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        JII_TEXTING_DASHBOARDS_DB_US_TX: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
        JII_TEXTING_DASHBOARDS_DB_US_IX: SourceTableCollectionValidationConfig(
            only_check_required_columns=True,
        ),
    }

    return [
        SourceTableCollection(
            dataset_id=dataset_id,
            update_config=SourceTableCollectionUpdateConfig.externally_managed(),
            validation_config=datasets_to_validation_config.get(dataset_id, None),
            source_tables_by_address={
                source_table.address: source_table
                for source_table in source_tables
                # Filter project-specific source tables
                if (not project_id or not source_table.deployed_projects)
                or (project_id in source_table.deployed_projects)
            },
            description=EXTERNALLY_MANAGED_DATASETS_TO_DESCRIPTIONS[dataset_id],
        )
        for dataset_id, source_tables in source_tables_by_dataset.items()
    ]


@cache
def build_source_table_repository_for_externally_managed_tables(
    project_id: str | None,
) -> SourceTableRepository:
    return SourceTableRepository(
        source_table_collections=[
            *collect_externally_managed_source_table_collections(project_id=project_id),
        ],
    )
