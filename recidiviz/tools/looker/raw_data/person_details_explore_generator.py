# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Code for building LookML explores for raw data tables and writing them to a file.
Used inside person_details_lookml_writer
"""
import os
from collections import defaultdict, deque
from functools import cache
from typing import Dict, List

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawTableRelationshipInfo,
)
from recidiviz.ingest.direct.raw_data.raw_table_relationship_info import (
    ColumnEqualityJoinBooleanClause,
    ColumnFilterJoinBooleanClause,
    JoinBooleanClause,
    JoinColumn,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.looker.lookml_explore import LookMLExplore
from recidiviz.looker.lookml_explore_parameter import (
    ExploreParameterJoin,
    JoinCardinality,
    JoinType,
    LookMLExploreParameter,
    LookMLJoinParameter,
)
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.tools.looker.script_helpers import remove_lookml_files_from
from recidiviz.utils.string import StrictStringFormatter


# TODO(#40036): Consider generalizing relationship traversal.
def _get_parent_child_relationships(
    primary_person_table: DirectIngestRawFileConfig,
    all_tables: Dict[str, DirectIngestRawFileConfig],
) -> Dict[str, List[str]]:
    """
    Do BFS first to generate the hierarchy of table relationships determined by BFS
    ordering from the primary person table.

    Returns a dictionary mapping from table file_tag to list of child table file_tags.
    """
    all_parent_child_relationships: defaultdict[str, list[str]] = defaultdict(list)

    bfs_tables: deque[DirectIngestRawFileConfig] = deque()
    bfs_visited_tables: set[str] = set()
    bfs_tables.append(primary_person_table)
    bfs_visited_tables.add(primary_person_table.file_tag)

    while bfs_tables:
        cur_table = bfs_tables.popleft()
        for relationship in cur_table.table_relationships:
            foreign_tag = relationship.foreign_table
            if foreign_tag not in bfs_visited_tables:
                bfs_visited_tables.add(foreign_tag)
                foreign_table = all_tables[foreign_tag]
                bfs_tables.append(foreign_table)
                all_parent_child_relationships[cur_table.file_tag].append(foreign_tag)
    return all_parent_child_relationships


def get_table_relationship_edges(
    primary_person_table: DirectIngestRawFileConfig,
    all_tables: Dict[str, DirectIngestRawFileConfig],
) -> List[RawTableRelationshipInfo]:
    """
    Return a list of edges in the table relationship tree for the region
    with given primary person table and dict of all tables.
    Edges will be returned in breadth first search order.
    Only edges connecting tables, which have a column that is primary for some
    external ID type, or which have such a column in their subtree,
    will be returned.
    """
    all_parent_child_relationships = _get_parent_child_relationships(
        primary_person_table, all_tables
    )

    @cache
    def _has_primary_person_col_in_subtree(file_tag: str) -> bool:
        """
        Returns true if the given table has a column that is primary for some
        external ID type or one of its child tables does.
        Also requires a dict from file tags to all the tables in the region
        and a dictionary of parent-child relationships in the table relationship tree,
        i.e. table relationships where the parent is the table closer to the
        region's primary person table via BFS.
        """
        table = all_tables[file_tag]
        children = all_parent_child_relationships[file_tag]
        return table.has_primary_person_external_id_col or any(
            _has_primary_person_col_in_subtree(child_tag) for child_tag in children
        )

    # Do BFS again to generate the final list of table relationships
    primary_table_relationships = []
    bfs_tables: deque[DirectIngestRawFileConfig] = deque()
    bfs_tables.append(primary_person_table)
    bfs_visited_tables = set([primary_person_table.file_tag])
    while bfs_tables:
        cur_table = bfs_tables.popleft()
        for relationship in cur_table.table_relationships:
            foreign_tag = relationship.foreign_table
            if foreign_tag not in bfs_visited_tables:
                bfs_visited_tables.add(foreign_tag)
                foreign_table = all_tables[foreign_tag]
                # Skip undocumented tables
                if foreign_table.is_undocumented:
                    continue
                # Only explore subtrees with a primary person col
                if _has_primary_person_col_in_subtree(foreign_tag):
                    bfs_tables.append(foreign_table)
                primary_table_relationships.append(relationship)

    return primary_table_relationships


def _generate_base_explore(
    state_code: StateCode,
    primary_person_table_name: str,
    views_by_file_tag: Dict[str, LookMLView],
) -> LookMLExplore:
    """
    Return an Explore for the given state code that has basic information:
    an extension: required field, fitting description and group label,
    and the given name of the primary person table for that state,
    which is used as the base view name for the Explore.
    """
    state_abbrev = state_code.value.lower()
    explore_name = f"{state_abbrev}_raw_data_template"
    state_name = state_code.get_state().name
    view_name = views_by_file_tag[primary_person_table_name].view_name
    return LookMLExplore(
        explore_name=explore_name,
        parameters=[
            LookMLExploreParameter.description(
                f"Data pertaining to an individual in {state_name}"
            ),
            LookMLExploreParameter.group_label("Raw State Data"),
            LookMLExploreParameter.label(f"{state_abbrev.upper()} Raw Data"),
        ],
        extension_required=True,
        view_name=view_name,
        view_label=view_name,
    )


def _join_clause_to_lookml(
    join_clause: JoinBooleanClause,
    views_by_file_tag: Dict[str, LookMLView],
    transforms: Dict[JoinColumn, str],
) -> str:
    """
    Returns a string version suitable for adding to LookML
    """

    def column_lookml(col: JoinColumn) -> str:
        formatted_col_name = (
            f"${{{views_by_file_tag[col.file_tag].view_name}.{col.column}}}"
        )
        if col in transforms:
            format_str = transforms[col]
            return StrictStringFormatter().format(
                format_str, col_name=formatted_col_name
            )
        return formatted_col_name

    if isinstance(join_clause, ColumnEqualityJoinBooleanClause):
        return f"{column_lookml(join_clause.column_1)} = {column_lookml(join_clause.column_2)}"
    if isinstance(join_clause, ColumnFilterJoinBooleanClause):
        return f"{column_lookml(join_clause.column)} = {join_clause.filter_value}"
    raise ValueError(f"Unexpected JoinBooleanClause type: {type(join_clause)}")


def _build_join_parameter(
    relationship: RawTableRelationshipInfo,
    views_by_file_tag: Dict[str, LookMLView],
) -> ExploreParameterJoin:
    """
    Convert the provided RawTableRelationshipInfo into an ExploreParameterJoin
    """
    col_to_transform = {t.column: t.transformation for t in relationship.transforms}
    sql_on_text = " AND ".join(
        _join_clause_to_lookml(j, views_by_file_tag, col_to_transform)
        for j in relationship.join_clauses
    )
    sql_on = LookMLJoinParameter.sql_on(sql_on_text)

    join_type = LookMLJoinParameter.type(JoinType.FULL_OUTER)
    join_cardinality = LookMLJoinParameter.relationship(
        JoinCardinality[relationship.cardinality.value]
    )
    view_name = views_by_file_tag[relationship.foreign_table].view_name
    view_label = LookMLJoinParameter.view_label(view_name)
    return ExploreParameterJoin(
        view_name=view_name,
        join_params=[sql_on, join_type, join_cardinality, view_label],
    )


def _generate_all_state_explores(
    all_views: Dict[StateCode, Dict[str, LookMLView]]
) -> Dict[StateCode, LookMLExplore]:
    """
    Return a dictionary where keys are StateCodes for states with raw data
    and a primary person table defined, and values are a LookMLExplore for each state
    """
    explores: Dict[StateCode, LookMLExplore] = {}
    for state_code in get_existing_direct_ingest_states():
        region_config = DirectIngestRegionRawFileConfig(region_code=state_code.value)
        if primary_person_table := region_config.get_primary_person_table():
            views_by_file_tag = all_views[state_code]
            relationship_edges = get_table_relationship_edges(
                primary_person_table, region_config.raw_file_configs
            )
            # If there are no relationships defined, we should not make an explore
            if not relationship_edges:
                continue
            join_parameters = [
                _build_join_parameter(relationship, views_by_file_tag)
                for relationship in relationship_edges
            ]
            explore = _generate_base_explore(
                state_code, primary_person_table.file_tag, views_by_file_tag
            )
            explore.parameters.extend(join_parameters)
            explores[state_code] = explore
    return explores


def generate_lookml_explores(
    looker_dir: str, all_views: Dict[StateCode, Dict[str, LookMLView]]
) -> Dict[StateCode, LookMLExplore]:
    """Produce LookML Explore files for all states, writing up-to-date
    .explore.lkml files in looker_dir/explores/raw_data/

    looker_dir: Local path to root directory of the Looker repo
    """

    explore_dir = os.path.join(looker_dir, "explores", "raw_data")
    remove_lookml_files_from(explore_dir)

    all_state_explores = _generate_all_state_explores(all_views)
    for state_code, explore in all_state_explores.items():
        state_dir = os.path.join(explore_dir, state_code.value.lower())
        explore.write(state_dir, source_script_path=__file__)
    return all_state_explores
