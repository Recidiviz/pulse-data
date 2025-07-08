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
"""Unit tests for person details LookML Explore generation"""

import unittest
from types import ModuleType
from typing import List

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data.raw_table_relationship_info import (
    ColumnEqualityJoinBooleanClause,
    JoinColumn,
    RawDataJoinCardinality,
    RawTableRelationshipInfo,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.tests.tools.looker.raw_data.raw_data_person_details_generator_test_utils import (
    RawDataPersonDetailsLookMLGeneratorTest,
)
from recidiviz.tools.looker.raw_data import (
    person_details_explore_generator,
    person_details_view_generator,
)
from recidiviz.tools.looker.raw_data.person_details_explore_generator import (
    generate_lookml_explores,
    get_table_relationship_edges,
)
from recidiviz.tools.looker.raw_data.person_details_view_generator import (
    _generate_state_raw_data_views,
)


class RawDataTreeEdgesTest(unittest.TestCase):
    """
    Tests for the function get_table_relationship_edges,
    which aims to return a list of edges in the table relationship tree.
    """

    def setUp(self) -> None:
        self.maxDiff = None
        # Basic raw file info
        # We need at least one column with a description to get tree edges
        self.sparse_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[
                RawTableColumnInfo(
                    name="COL1",
                    state_code=StateCode.US_XX,
                    file_tag="myFile",
                    field_type=RawTableColumnFieldType.STRING,
                    description="test description",
                    is_pii=False,
                )
            ],
            custom_line_terminator=None,
            primary_key_cols=["COL1"],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
            is_code_file=False,
        )
        # Raw file that is a primary person table
        self.primary_person_table_config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="primary column",
                    state_code=StateCode.US_XX,
                    file_tag=self.sparse_config.file_tag,
                    field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
                    is_pii=True,
                    description="test description",
                    external_id_type="US_OZ_EG",
                    is_primary_for_external_id_type=True,
                )
            ],
            primary_key_cols=["primary column"],
            no_valid_primary_keys=False,
            is_primary_person_table=True,
        )
        # Basic relationship between two raw data tables
        self.sparse_relationship = RawTableRelationshipInfo(
            file_tag="myFile",
            foreign_table="myFile2",
            cardinality=RawDataJoinCardinality.MANY_TO_MANY,
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="myFile", column="my_col"),
                    column_2=JoinColumn(file_tag="myFile2", column="my_col"),
                )
            ],
            transforms=[],
        )

    def test_no_relationships(self) -> None:
        # Primary person table has no relationships
        no_relationships = get_table_relationship_edges(
            self.primary_person_table_config,
            {"myFile": self.primary_person_table_config},
        )
        self.assertEqual(no_relationships, [])

    def test_one_relationship(self) -> None:
        # Primary person table has one relationship,
        # unrelated third table exists
        primary_table = attr.evolve(
            self.primary_person_table_config,
            table_relationships=[self.sparse_relationship],
        )
        secondary_table = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            columns=[
                attr.evolve(c, file_tag="myFile2")
                for c in self.sparse_config.all_columns
            ],
            table_relationships=[self.sparse_relationship.invert()],
        )
        third_table = attr.evolve(
            self.sparse_config,
            file_tag="myFile3",
            columns=[
                attr.evolve(c, file_tag="myFile3")
                for c in self.sparse_config.all_columns
            ],
        )
        one_relationship = get_table_relationship_edges(
            primary_table,
            {
                "myFile": primary_table,
                "myFile2": secondary_table,
                "myFile3": third_table,
            },
        )
        self.assertEqual(one_relationship, [self.sparse_relationship])

    def test_multilayer_tree(self) -> None:
        # file1 has children file2 and file3, and file2 has child file4.
        # Should output the first-layer edges before the second-layer one
        file1_file2_edge = self.sparse_relationship
        file1_file3_edge = attr.evolve(
            self.sparse_relationship,
            foreign_table="myFile3",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="myFile", column="my_col"),
                    column_2=JoinColumn(file_tag="myFile3", column="my_col"),
                )
            ],
        )
        file2_file4_edge = attr.evolve(
            self.sparse_relationship,
            file_tag="myFile2",
            foreign_table="myFile4",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="myFile2", column="my_col"),
                    column_2=JoinColumn(file_tag="myFile4", column="my_col"),
                )
            ],
        )
        file1 = attr.evolve(
            self.primary_person_table_config,
            table_relationships=[file1_file2_edge, file1_file3_edge],
        )
        file2 = attr.evolve(
            self.primary_person_table_config,
            file_tag="myFile2",
            columns=[
                attr.evolve(c, file_tag="myFile2")
                for c in self.primary_person_table_config.all_columns
            ],
            table_relationships=[file1_file2_edge.invert(), file2_file4_edge],
        )
        file3 = attr.evolve(
            self.sparse_config,
            file_tag="myFile3",
            columns=[
                attr.evolve(c, file_tag="myFile3")
                for c in self.sparse_config.all_columns
            ],
            table_relationships=[file1_file3_edge.invert()],
        )
        file4 = attr.evolve(
            self.sparse_config,
            file_tag="myFile4",
            columns=[
                attr.evolve(c, file_tag="myFile4")
                for c in self.sparse_config.all_columns
            ],
            table_relationships=[file2_file4_edge.invert()],
        )
        relationships = get_table_relationship_edges(
            file1,
            {"myFile": file1, "myFile2": file2, "myFile3": file3, "myFile4": file4},
        )
        self.assertEqual(
            relationships, [file1_file2_edge, file1_file3_edge, file2_file4_edge]
        )

        # If file2 is not primary, we should no longer see the file2-file4 relationship
        file2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            columns=[
                attr.evolve(c, file_tag="myFile2")
                for c in self.sparse_config.all_columns
            ],
            table_relationships=[file1_file2_edge.invert(), file2_file4_edge],
        )
        relationships = get_table_relationship_edges(
            file1,
            {"myFile": file1, "myFile2": file2, "myFile3": file3, "myFile4": file4},
        )
        self.assertEqual(relationships, [file1_file2_edge, file1_file3_edge])

    def test_cycle(self) -> None:
        # file1 has child file2, file2 has child file3, file3 has child file1
        # should output only the first two relationships since file1 was already visited
        file1_file2_edge = self.sparse_relationship
        file2_file3_edge = attr.evolve(
            self.sparse_relationship,
            file_tag="myFile2",
            foreign_table="myFile3",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="myFile2", column="my_col"),
                    column_2=JoinColumn(file_tag="myFile3", column="my_col"),
                )
            ],
        )
        file1_file3_edge = attr.evolve(
            self.sparse_relationship,
            foreign_table="myFile3",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="myFile", column="my_col"),
                    column_2=JoinColumn(file_tag="myFile3", column="my_col"),
                )
            ],
        )
        file1 = attr.evolve(
            self.primary_person_table_config, table_relationships=[file1_file2_edge]
        )
        file2 = attr.evolve(
            self.primary_person_table_config,
            file_tag="myFile2",
            columns=[
                attr.evolve(c, file_tag="myFile2")
                for c in self.primary_person_table_config.all_columns
            ],
            table_relationships=[file2_file3_edge],
        )
        file3 = attr.evolve(
            self.sparse_config,
            file_tag="myFile3",
            columns=[
                attr.evolve(c, file_tag="myFile3")
                for c in self.sparse_config.all_columns
            ],
            table_relationships=[file1_file3_edge.invert()],
        )
        relationships = get_table_relationship_edges(
            file1, {"myFile": file1, "myFile2": file2, "myFile3": file3}
        )
        self.assertEqual(relationships, [file1_file2_edge, file2_file3_edge])

    def test_no_self_joins(self) -> None:
        # Primary person table has only relationship with itself,
        # which should be filtered out by this function
        self_join = attr.evolve(
            self.sparse_relationship,
            foreign_table="myFile",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="myFile", column="my_col1"),
                    column_2=JoinColumn(file_tag="myFile", column="my_col2"),
                )
            ],
        )
        primary_table = attr.evolve(
            self.primary_person_table_config,
            table_relationships=[self_join],
        )
        no_relationships = get_table_relationship_edges(
            primary_table,
            {
                "myFile": primary_table,
            },
        )
        self.assertEqual(no_relationships, [])

    def test_unlinked_primary_tables(self) -> None:
        # North Dakota-like case: two primary tables with a table between them
        file1_file2_edge = self.sparse_relationship
        file2_file3_edge = attr.evolve(
            self.sparse_relationship,
            file_tag="myFile2",
            foreign_table="myFile3",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(file_tag="myFile2", column="my_col"),
                    column_2=JoinColumn(file_tag="myFile3", column="my_col"),
                )
            ],
        )
        file1 = attr.evolve(
            self.primary_person_table_config, table_relationships=[file1_file2_edge]
        )
        file2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            columns=[
                attr.evolve(c, file_tag="myFile2")
                for c in self.sparse_config.all_columns
            ],
            table_relationships=[file1_file2_edge.invert(), file2_file3_edge],
        )
        file3 = attr.evolve(
            self.primary_person_table_config,
            file_tag="myFile3",
            columns=[
                attr.evolve(c, file_tag="myFile3")
                for c in self.primary_person_table_config.all_columns
            ],
            table_relationships=[file2_file3_edge.invert()],
        )
        relationships = get_table_relationship_edges(
            file1, {"myFile": file1, "myFile2": file2, "myFile3": file3}
        )
        self.assertEqual(relationships, [file1_file2_edge, file2_file3_edge])

    def test_region_structure(self) -> None:
        # Ensure that we get the right relationship edges for every real state
        for state_code in get_existing_direct_ingest_states():
            region_config = DirectIngestRegionRawFileConfig(
                region_code=state_code.value
            )
            all_tables = region_config.raw_file_configs

            # Skip if we don't have any person external id types filled in or a primary table
            all_primary_for_id_type_tables = [
                config.file_tag
                for config in all_tables.values()
                if config.has_primary_person_external_id_col
            ]
            primary_table = region_config.get_primary_person_table()
            if not all_primary_for_id_type_tables or not primary_table:
                continue

            relationships = get_table_relationship_edges(primary_table, all_tables)

            for table in all_tables.values():
                if table.is_undocumented:
                    continue
                is_primary_table = table.has_primary_person_external_id_col
                for relationship in table.table_relationships:
                    foreign_table = all_tables[relationship.foreign_table]
                    if foreign_table.is_undocumented:
                        continue
                    foreign_is_primary_table = (
                        foreign_table.has_primary_person_external_id_col
                    )
                    if is_primary_table or foreign_is_primary_table:
                        self.assertIn(relationship, relationships)


class RawDataPersonDetailsLookMLExploreGeneratorTest(
    RawDataPersonDetailsLookMLGeneratorTest
):
    """Tests LookML explore generation functions"""

    @classmethod
    def generator_modules(cls) -> List[ModuleType]:
        return [person_details_view_generator, person_details_explore_generator]

    def test_generate_lookml_explores(self) -> None:
        all_views = _generate_state_raw_data_views()
        self.generate_files(
            function_to_test=lambda s: generate_lookml_explores(s, all_views),
            filename_filter=".explore.lkml",
        )
