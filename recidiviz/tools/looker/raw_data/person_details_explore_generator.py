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
from collections import deque
from typing import Dict, List

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawTableRelationshipInfo,
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
from recidiviz.tools.looker.script_helpers import remove_lookml_files_from


def _get_table_relationship_edges(
    primary_person_table: DirectIngestRawFileConfig,
    all_tables: Dict[str, DirectIngestRawFileConfig],
) -> List[RawTableRelationshipInfo]:
    """
    Return a list of edges in the table relationship tree for the region
    with given primary person table and dict of all tables.
    Edges will be returned in breadth first search order.
    """

    # Do breadth-first-search
    result = []
    tables: deque[DirectIngestRawFileConfig] = deque()
    visited_tables: set[str] = set()
    tables.append(primary_person_table)
    visited_tables.add(primary_person_table.file_tag)
    while tables:
        cur_table = tables.popleft()
        # we don't include any tables that aren't directly linked to a primary table
        if not cur_table.is_primary_person_table:
            continue

        for relationship in cur_table.table_relationships:
            foreign_tag = relationship.foreign_table
            if foreign_tag not in visited_tables:
                visited_tables.add(foreign_tag)
                foreign_table = all_tables[foreign_tag]
                tables.append(foreign_table)

                result.append(relationship)

    return result


def _generate_base_explore(
    state_code: StateCode, primary_person_table_name: str
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
    return LookMLExplore(
        explore_name=explore_name,
        parameters=[
            LookMLExploreParameter.description(
                f"Data pertaining to an individual in {state_name}"
            ),
            LookMLExploreParameter.group_label("Raw State Data"),
        ],
        extension_required=True,
        view_name=primary_person_table_name,
    )


def _build_join_parameter(
    relationship: RawTableRelationshipInfo,
) -> ExploreParameterJoin:
    """
    Convert the provided RawTableRelationshipInfo into an ExploreParameterJoin
    """
    sql_on = LookMLJoinParameter.sql_on(relationship.join_lookml())
    join_type = LookMLJoinParameter.type(JoinType.FULL_OUTER)
    join_cardinality = LookMLJoinParameter.relationship(
        JoinCardinality[relationship.cardinality.value]
    )

    return ExploreParameterJoin(
        view_name=relationship.foreign_table,
        join_params=[sql_on, join_type, join_cardinality],
    )


def _generate_all_state_explores() -> Dict[StateCode, LookMLExplore]:
    """
    Return a dictionary where keys are StateCodes for states with raw data
    and a primary person table defined, and values are a LookMLExplore for each state
    """
    explores: Dict[StateCode, LookMLExplore] = {}
    for state_code in get_existing_direct_ingest_states():
        region_config = DirectIngestRegionRawFileConfig(region_code=state_code.value)
        if primary_person_table := region_config.get_primary_person_table():
            explore = _generate_base_explore(state_code, primary_person_table.file_tag)
            join_info = _get_table_relationship_edges(
                primary_person_table, region_config.raw_file_configs
            )
            join_parameters = map(_build_join_parameter, join_info)
            explore.parameters.extend(join_parameters)
            explores[state_code] = explore
    return explores


def generate_lookml_explores(looker_dir: str) -> None:
    """Produce LookML Explore files for all states, writing up-to-date
    .explore.lkml files in looker_dir/explores/raw_data/

    looker_dir: Local path to root directory of the Looker repo
    """

    explore_dir = os.path.join(looker_dir, "explores", "raw_data")
    remove_lookml_files_from(explore_dir)

    for state_code, explore in _generate_all_state_explores().items():
        state_dir = os.path.join(explore_dir, state_code.value.lower())
        explore.write(state_dir, source_script_path=__file__)
