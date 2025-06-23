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

from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
    DirectIngestViewRawFileDependency,
)

# Used to keep track of JOIN clauses when we search through
# table relationships.
TableJoin = namedtuple("TableJoin", ["config", "previous_join"])


def throw_cant_find_id_msg(file_tag: str, external_id_type: str) -> str:
    return (
        f"The raw data file |{file_tag}| could not be linked to any external IDs of type {external_id_type}!\n"
        "You will need to update the raw file configs associated with your ingest view to proceed:\n"
        f"  - If |{file_tag}| is a code file, make sure is_code_file is set to True.\n"
        f"  - If |{file_tag}| has a root entity identifier, make sure external_id_type is set to the correct value.\n"
        f"  - If |{file_tag}| does not have a root entity identifier, make sure the table relationships are set up correctly.\n"
    )


# TODO(#40036): Consider generalizing relationship traversal.
def _join_table_to_external_id_table(
    config: DirectIngestRawFileConfig,
    external_id_type: str,
    external_id_sql_arr: str,
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
            # Only consider relationships relevant to the ingest view
            if relationship.foreign_table not in all_dependencies:
                continue
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
                    join_clause
                    + f" WHERE {f_table.file_tag}.{id_col.name} IN {external_id_sql_arr}"
                ).strip()
            table_queue.put(TableJoin(f_table, join_clause))
    raise ValueError(throw_cant_find_id_msg(config.file_tag, external_id_type))


def _get_config_filter(
    config: DirectIngestRawFileConfig,
    external_id_type: str,
    external_id_sql_arr: str,
    dataset: str,
    project_id: str,
    all_dependencies: dict[str, DirectIngestViewRawFileDependency],
) -> str:
    """Returns a predicate to a SQL query for subsetting raw data."""
    if config.is_code_file:
        return ""
    if id_column := config.get_external_id_col_with_type(external_id_type):
        return f"WHERE {id_column.name} IN {external_id_sql_arr}"
    if config.table_relationships:
        return _join_table_to_external_id_table(
            config,
            external_id_type,
            external_id_sql_arr,
            dataset,
            project_id,
            all_dependencies,
        )
    raise ValueError(throw_cant_find_id_msg(config.file_tag, external_id_type))


def build_root_entity_filtered_raw_data_queries(
    view_builder: DirectIngestViewQueryBuilder,
    external_id_type: str,
    external_id_values: list[str],
    dataset: str,
    project_id: str,
    file_tags_to_skip_with_reason: dict[str, str] | None = None,
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
        if file_tags_to_skip_with_reason and file_tag in file_tags_to_skip_with_reason:
            subset_queries[file_tag] = file_tags_to_skip_with_reason[file_tag]
        else:
            external_id_sql_arr = (
                f"({list_to_query_string(external_id_values, quoted=True)})"
            )
            subset_queries[file_tag] = (
                f"SELECT DISTINCT {file_tag}.* "
                + f"FROM {project_id}.{dataset}.{file_tag} AS {file_tag} "
                + _get_config_filter(
                    raw_file_dependency.raw_file_config,
                    external_id_type,
                    external_id_sql_arr,
                    dataset,
                    project_id,
                    all_dependencies,
                )
            ).strip()
    return subset_queries
