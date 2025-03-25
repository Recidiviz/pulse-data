# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
This module has functions to find the minimal subset of raw data
for an ingest view.

Example usage:

    python -m recidiviz.tools.ingest.development.pull_root_entity_filtered_raw_data_for_ingest_view \
        --state_code=us_mo \
        --ingest_view_name=sentence \
        --external_id_type="US_MO_DOC" \
        --external_id_value="12345"

"""
from collections import namedtuple
from queue import Queue

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
    DirectIngestViewRawFileDependency,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING

# Used to keep track of JOIN clauses when we search through
# table relationships.
TableJoin = namedtuple("TableJoin", ["config", "previous_join"])


# TODO(#40036): Consider generalizing relationship traversal.
def _join_table_to_external_id_table(
    config: DirectIngestRawFileConfig,
    external_id_type: str,
    external_id_value: str,
    dataset: str,
    project_id: str,
    all_dependencies: dict[str, DirectIngestViewRawFileDependency],
) -> str:
    """
    This function returns the predicate of a SQL query JOINing from the given
    raw file to a file with a column having the given external_id_type.

    We use a breadth first search to build out a query with the fewest joins.
    A refresher is here:
        https://en.wikipedia.org/wiki/Breadth-first_search#Pseudocode
    We search from the given config up to a foreign table with a column
    having the given external_id_type.
    """
    table_queue: Queue[TableJoin] = Queue()
    # The very first table has no previous JOIN clauses
    table_queue.put(TableJoin(config, ""))
    while not table_queue.empty():
        table, previous_join = table_queue.get()
        for relationship in table.table_relationships:
            f_table = all_dependencies[relationship.foreign_table].raw_file_config
            join_clause = " ".join(
                [
                    previous_join,
                    f"JOIN {project_id}.{dataset}.{f_table.file_tag} ON",
                    relationship.join_sql(),
                ]
            ).strip()
            if id_col := f_table.get_external_id_col_with_type(external_id_type):
                return (
                    join_clause + f" WHERE {id_col.name} = '{external_id_value}'"
                ).strip()
            table_queue.put(TableJoin(f_table, join_clause))
    raise ValueError(
        f"Table {config.file_tag} has no primary external ID or table relationships."
    )


def _get_config_filter(
    config: DirectIngestRawFileConfig,
    external_id_type: str,
    external_id_value: str,
    dataset: str,
    project_id: str,
    all_dependencies: dict[str, DirectIngestViewRawFileDependency],
) -> str:
    """Returns a predicate to a SQL query for subsetting raw data."""
    if config.is_code_file:
        return ""
    if id_column := config.get_external_id_col_with_type(external_id_type):
        return f"WHERE {id_column.name} = '{external_id_value}'"
    if config.table_relationships:
        return _join_table_to_external_id_table(
            config,
            external_id_type,
            external_id_value,
            dataset,
            project_id,
            all_dependencies,
        )
    raise ValueError(
        f"Table {config.file_tag} has no primary external ID or table relationships."
    )


def build_root_entity_filtered_raw_data_queries(
    view_builder: DirectIngestViewQueryBuilder,
    external_id_type: str,
    external_id_value: str,
    dataset: str,
    project_id: str,
) -> dict[str, str]:
    """
    Returns a dictionary mapping raw data file tags to queries.
    Each query is for a raw data dependency of the given ingest view,
    where the query finds the subset of raw data where the rows
    link to the given external_id_type.
    """
    all_dependencies = view_builder.raw_table_dependency_configs_by_file_tag
    subset_queries = {}
    for file_tag, raw_file_dependency in all_dependencies.items():
        subset_queries[file_tag] = (
            f"SELECT * FROM {project_id}.{dataset}.{file_tag} "
            + _get_config_filter(
                raw_file_dependency.raw_file_config,
                external_id_type,
                external_id_value,
                dataset,
                project_id,
                all_dependencies,
            )
        ).strip()
    return subset_queries


def main(
    state_code: StateCode,
    ingest_view_name: str,
    external_id_type: str,
    external_id_value: str,
    project_id: str = GCP_PROJECT_STAGING,
) -> None:
    """Builds queries to subset raw data for a given ingest view based on the given external ID."""
    region = direct_ingest_regions.get_direct_ingest_region(state_code.value)
    view_collector = DirectIngestViewQueryBuilderCollector.from_state_code(state_code)
    mapping_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )

    view_builder = view_collector.get_query_builder_by_view_name(ingest_view_name)
    mapping = mapping_collector.ingest_view_to_manifest[ingest_view_name]
    if external_id_type not in mapping.root_entity_external_id_types:
        raise ValueError(
            f"External ID type {external_id_type} not expected for ingest view {ingest_view_name}. "
            f"Expected external ID types: {mapping.root_entity_external_id_types}"
        )
    dataset = raw_tables_dataset_for_region(state_code, DirectIngestInstance.PRIMARY)
    for file_tag, query in build_root_entity_filtered_raw_data_queries(
        view_builder, external_id_type, external_id_value, dataset, project_id
    ).items():
        # TODO(#39680): Execute the query against BigQuery and download.
        # and/or incorporate into the fixture generation script.
        print(f"Query for {file_tag}:\n", query)


# TODO(#39680): Execute the query against BigQuery and download.
# and/or incorporate into the fixture generation script.
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build raw data subset queries for a given ingest view."
    )
    parser.add_argument(
        "--state_code", type=str, help="The state code for the region (e.g., 'US_AZ')."
    )
    parser.add_argument(
        "--ingest_view_name",
        type=str,
        help="The name of the ingest view (e.g., 'state_sentence').",
    )
    parser.add_argument(
        "--external_id_type",
        type=str,
        help="The external ID type to filter on (e.g., 'US_AZ_PERSON_ID').",
    )

    parser.add_argument(
        "--external_id_value",
        type=str,
        help="The external ID value to filter on (e.g., '12345').",
    )
    args = parser.parse_args()

    main(
        state_code=StateCode(args.state_code.upper()),
        ingest_view_name=args.ingest_view_name,
        external_id_type=args.external_id_type,
        external_id_value=args.external_id_value,
    )
