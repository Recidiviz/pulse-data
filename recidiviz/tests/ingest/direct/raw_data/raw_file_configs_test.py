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
"""Tests for classes in raw_file_configs.py."""
import unittest
from typing import Dict

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    RawDataClassification,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data.raw_table_relationship_info import (
    ColumnEqualityJoinBooleanClause,
    JoinColumn,
    RawDataJoinCardinality,
    RawTableRelationshipInfo,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestRawTableColumnInfo(unittest.TestCase):
    """Tests for RawTableColumnInfo"""

    def test_simple(self) -> None:
        column_info = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=False,
            description=None,
            known_values=None,
        )

        self.assertFalse(column_info.is_enum)
        self.assertFalse(column_info.is_datetime)
        self.assertEqual(None, column_info.datetime_sql_parsers)

    def test_known_values_non_string(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Expected field type to be string if known values are present for COL1",
        ):
            _ = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.DATETIME,
                is_pii=False,
                description=None,
                known_values=[ColumnEnumValueInfo(value="test", description=None)],
            )

    def test_datetime_sql_parsers(self) -> None:
        # Valid config, should not crash
        datetime_column_info = RawTableColumnInfo(
            name="COL2",
            field_type=RawTableColumnFieldType.DATETIME,
            is_pii=False,
            description=None,
            known_values=None,
            datetime_sql_parsers=[
                "SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\\:\\d\\d\\d.*', ''))"
            ],
        )

        self.assertFalse(datetime_column_info.is_enum)
        self.assertTrue(datetime_column_info.is_datetime)
        self.assertEqual(
            [
                "SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\\:\\d\\d\\d.*', ''))"
            ],
            datetime_column_info.datetime_sql_parsers,
        )

    def test_valid_external_id_cols(self) -> None:
        # Should run without crashing

        # External id primary column
        _ = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
            is_pii=False,
            description=None,
            known_values=None,
            external_id_type="US_OZ_EG",
            is_primary_for_external_id_type=True,
        )

        # Non-primary column but has an external id type
        _ = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
            is_pii=False,
            description=None,
            known_values=None,
            external_id_type="US_OZ_EG",
        )

    def test_bad_datetime_sql_parsers(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Expected datetime_sql_parsers to be null if is_datetime is False.*",
        ):
            _ = RawTableColumnInfo(
                name="COL2",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                datetime_sql_parsers=[
                    "SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\\:\\d\\d\\d.*', ''))"
                ],
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Expected datetime_sql_parser to have the string literal {col_name}.*",
        ):
            _ = RawTableColumnInfo(
                name="COL2",
                field_type=RawTableColumnFieldType.DATETIME,
                is_pii=False,
                description=None,
                known_values=None,
                datetime_sql_parsers=[
                    "SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE(column, r'\\:\\d\\d\\d.*', ''))"
                ],
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Expected datetime_sql_parser must match expected timestamp parsing formats.*",
        ):
            _ = RawTableColumnInfo(
                name="COL2",
                field_type=RawTableColumnFieldType.DATETIME,
                is_pii=False,
                description=None,
                known_values=None,
                datetime_sql_parsers=[
                    "SAFE_CAST(SAFE.PARSE_TIMESTAMP('%b %e %Y %H:%M:%S', REGEXP_REPLACE({col_name}, r'\\:\\d\\d\\d.*', '')) AS DATETIME),"
                ],
            )

    def test_bad_external_id_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Expected external_id_type to be None and is_primary_for_external_id_type to be False when field_type is string for COL1",
        ):
            _ = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                external_id_type="string",
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Expected external_id_type to be None and is_primary_for_external_id_type to be False when field_type is string for COL1",
        ):
            _ = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                is_primary_for_external_id_type=True,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Expected is_primary_for_external_id_type to be False when external id type is None for COL1*",
        ):
            _ = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
                is_pii=False,
                description=None,
                known_values=None,
                is_primary_for_external_id_type=True,
            )


class TestDirectIngestRawFileConfig(unittest.TestCase):
    """Tests for DirectIngestRawFileConfig"""

    def setUp(self) -> None:
        self.sparse_config = DirectIngestRawFileConfig(
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
        )

    def test_basic_sparse_config(self) -> None:
        """Tests a config with no columns listed."""
        config = self.sparse_config

        self.assertEqual("", config.primary_key_str)
        self.assertEqual(["UTF-8", "ISO-8859-1"], config.encodings_to_try())
        self.assertEqual([], config.documented_columns)
        self.assertEqual([], config.documented_datetime_cols)
        self.assertEqual([], config.documented_non_datetime_cols)
        self.assertEqual([], config.datetime_cols)
        self.assertEqual([], config.non_datetime_cols)
        self.assertFalse(config.has_enums)
        self.assertTrue(config.is_undocumented)
        self.assertEqual(None, config.caps_normalized_col("some_random_column_name"))
        self.assertFalse(config.is_exempt_from_raw_data_pruning())

    def test_column_types(self) -> None:
        """Tests a config with columns of various types / documentation levels."""
        config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                ),
                RawTableColumnInfo(
                    name="Col2",
                    is_pii=False,
                    description="",
                    field_type=RawTableColumnFieldType.STRING,
                ),
                RawTableColumnInfo(
                    name="Col3",
                    description="description 3",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.DATETIME,
                ),
                RawTableColumnInfo(
                    name="Col4",
                    description="",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.DATETIME,
                ),
            ],
            primary_key_cols=["Col1"],
        )

        self.assertEqual("Col1", config.primary_key_str)
        self.assertEqual(["UTF-8", "ISO-8859-1"], config.encodings_to_try())
        self.assertEqual(["Col1", "Col3"], [c.name for c in config.documented_columns])
        self.assertEqual(
            ["Col3"], [name for name, _ in config.documented_datetime_cols]
        )
        self.assertEqual(["Col1"], config.documented_non_datetime_cols)
        self.assertEqual(["Col3", "Col4"], [name for name, _ in config.datetime_cols])
        self.assertEqual(["Col1", "Col2"], config.non_datetime_cols)
        self.assertFalse(config.has_enums)
        self.assertFalse(config.is_undocumented)
        self.assertEqual("Col1", config.caps_normalized_col("col1"))
        self.assertEqual("Col1", config.caps_normalized_col("Col1"))
        self.assertFalse(config.is_exempt_from_raw_data_pruning())

        # Now add an enum column and verify that column-related properties change
        # accordingly.
        config = attr.evolve(
            config,
            columns=[
                *config.columns,
                RawTableColumnInfo(
                    name="Col5",
                    description="description 5",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    known_values=[
                        ColumnEnumValueInfo(value="A", description="A description"),
                        ColumnEnumValueInfo(value="B", description=None),
                    ],
                ),
            ],
        )

        self.assertEqual(
            ["Col1", "Col3", "Col5"], [c.name for c in config.documented_columns]
        )
        self.assertEqual(
            ["Col3"], [name for name, _ in config.documented_datetime_cols]
        )
        self.assertEqual(["Col1", "Col5"], config.documented_non_datetime_cols)
        self.assertEqual(["Col3", "Col4"], [name for name, _ in config.datetime_cols])
        self.assertEqual(["Col1", "Col2", "Col5"], config.non_datetime_cols)
        self.assertTrue(config.has_enums)

    def test_encodings_to_try(self) -> None:
        config = attr.evolve(self.sparse_config, encoding="UTF-8")
        self.assertEqual(["UTF-8", "ISO-8859-1"], config.encodings_to_try())

        config = attr.evolve(self.sparse_config, encoding="ISO-8859-1")
        self.assertEqual(["ISO-8859-1", "UTF-8"], config.encodings_to_try())

        config = attr.evolve(self.sparse_config, encoding="UTF-16")
        self.assertEqual(["UTF-16", "UTF-8", "ISO-8859-1"], config.encodings_to_try())

    def test_no_valid_primary_keys(self) -> None:
        # Cannot set primary_key_cols when no_valid_primary_keys=True
        with self.assertRaisesRegex(ValueError, r"Incorrect primary key setup found"):
            _ = attr.evolve(
                self.sparse_config,
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STRING,
                    )
                ],
                primary_key_cols=["Col1"],
                no_valid_primary_keys=True,
            )

        # However, this setup is valid and should not crash
        _ = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                )
            ],
            primary_key_cols=[],
            no_valid_primary_keys=True,
        )

    # TODO(#19528): remove test once raw data pruning can be done on ContactNoteComment.
    def test_is_exempt_from_raw_data_pruning_contact_note_comment(
        self,
    ) -> None:
        raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=StateCode.US_TN.value
        ).raw_file_configs["ContactNoteComment"]
        self.assertTrue(raw_file_config.is_exempt_from_raw_data_pruning())

    def test_is_exempt_from_raw_data_pruning_no_valid_primary_keys_true(
        self,
    ) -> None:
        """Because the file is not historical, it should be exempt from raw data
        pruning.
        """
        config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                )
            ],
            no_valid_primary_keys=True,
            always_historical_export=False,
        )

        self.assertTrue(config.is_exempt_from_raw_data_pruning())

    def test_raw_data_pruning_exempt_for_file_tag_in_state_no_valid_primary_keys_always_historical(
        self,
    ) -> None:
        """Because the file has no valid primary keys and is historical, it should be
        exempt from raw data pruning."""
        config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                )
            ],
            no_valid_primary_keys=True,
            always_historical_export=True,
        )

        self.assertTrue(config.is_exempt_from_raw_data_pruning())

    def test_raw_data_pruning_exempt_for_file_tag_in_state_valid_primary_keys_always_historical(
        self,
    ) -> None:
        """Because the file has valid primary keys and is always historical, it should
        NOT be exempt from raw data pruning.
        """
        config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                )
            ],
            primary_key_cols=[],
            always_historical_export=True,
        )

        self.assertFalse(config.is_exempt_from_raw_data_pruning())

    def test_raw_data_pruning_exempt_for_file_tag_in_state_valid_primary_keys_not_historical(
        self,
    ) -> None:
        """Because the file has valid primary keys and is not always historical, it
        should be exempt from raw data pruning.
        """
        config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                )
            ],
            primary_key_cols=["Col1"],
            no_valid_primary_keys=False,
            always_historical_export=False,
        )

        self.assertTrue(config.is_exempt_from_raw_data_pruning())

    def test_default_always_historical(self) -> None:
        """Assert that if the file sets always historical to False that is used, even
        if the default has it set to True."""
        config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                )
            ],
            no_valid_primary_keys=True,
            always_historical_export=False,
        )
        self.assertFalse(config.always_historical_export)

    def test_missing_primary_key_columns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Column\(s\) marked as primary keys not listed in columns list"
            r" for file \[myFile\]: \{'Col2'\}$",
        ):
            _ = attr.evolve(
                self.sparse_config,
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STRING,
                    )
                ],
                primary_key_cols=["Col1", "Col2"],
            )

    def test_duplicate_columns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found duplicate columns in raw_file \[myFile\]$",
        ):
            _ = attr.evolve(
                self.sparse_config,
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STRING,
                    ),
                    RawTableColumnInfo(
                        name="Col1",
                        description="some other description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STRING,
                    ),
                ],
                primary_key_cols=["Col1", "Col2"],
            )

    def test_duplicate_table_relationships(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found duplicate table relationships \[myFile1.col1 = myFile2.col2\] and "
            r"\[myFile2.col2\ = myFile1.col1] defined in \[/path/to/myFile1.yaml\]",
        ):
            _ = attr.evolve(
                self.sparse_config,
                file_tag="myFile1",
                file_path="/path/to/myFile1.yaml",
                columns=[
                    RawTableColumnInfo(
                        name="col1",
                        field_type=RawTableColumnFieldType.STRING,
                        is_pii=False,
                        description="col1 description",
                    ),
                    RawTableColumnInfo(
                        name="col2",
                        field_type=RawTableColumnFieldType.DATETIME,
                        is_pii=False,
                        description="col2 description",
                    ),
                ],
                table_relationships=[
                    RawTableRelationshipInfo(
                        file_tag="myFile1",
                        foreign_table="myFile2",
                        cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                        join_clauses=[
                            ColumnEqualityJoinBooleanClause(
                                column_1=JoinColumn(file_tag="myFile1", column="col1"),
                                column_2=JoinColumn(file_tag="myFile2", column="col2"),
                            )
                        ],
                    ),
                    RawTableRelationshipInfo(
                        file_tag="myFile1",
                        foreign_table="myFile2",
                        cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                        join_clauses=[
                            ColumnEqualityJoinBooleanClause(
                                column_1=JoinColumn(file_tag="myFile2", column="col2"),
                                column_2=JoinColumn(file_tag="myFile1", column="col1"),
                            )
                        ],
                    ),
                ],
            )

    def test_external_id_wrong_field_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Expected external_id_type to be None and is_primary_for_external_id_type to be False when field_type is*",
        ):
            _ = attr.evolve(
                self.sparse_config,
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STRING,
                        external_id_type="US_OZ_EG",
                    ),
                ],
            )

    def test_table_relationship_does_not_match_file_tag(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found table_relationship defined for \[myFile1\] with file_tag that does "
            r"not match config file_tag: myFile2.",
        ):
            _ = attr.evolve(
                self.sparse_config,
                file_tag="myFile1",
                file_path="/path/to/myFile1.yaml",
                columns=[
                    RawTableColumnInfo(
                        name="col1",
                        field_type=RawTableColumnFieldType.STRING,
                        is_pii=False,
                        description="col1 description",
                    )
                ],
                table_relationships=[
                    RawTableRelationshipInfo(
                        file_tag="myFile2",
                        foreign_table="myFile1",
                        cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                        join_clauses=[
                            ColumnEqualityJoinBooleanClause(
                                column_1=JoinColumn(file_tag="myFile1", column="col1"),
                                column_2=JoinColumn(file_tag="myFile2", column="col2"),
                            )
                        ],
                    ),
                ],
            )

    def test_duplicate_external_ids(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found duplicate external ID types in raw_file \[myFile\]$",
        ):
            _ = attr.evolve(
                self.sparse_config,
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                    ),
                    RawTableColumnInfo(
                        name="Col2",
                        description="some other description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                    ),
                ],
            )

    def test_is_primary_person_table_without_id(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Table marked as primary person table, but no primary external ID column was specified for file \[myFile\]$",
        ):
            _ = attr.evolve(
                self.sparse_config,
                is_primary_person_table=True,
            )


class TestDirectIngestRegionRawFileConfig(unittest.TestCase):
    """Tests for DirectIngestRegionRawFileConfig"""

    def setUp(self) -> None:
        self.us_xx_region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions,
        )
        self.sparse_config = DirectIngestRawFileConfig(
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
        )

        self.sparse_config = DirectIngestRawFileConfig(
            file_tag="myFile",
            file_path="/path/to/myFile.yaml",
            file_description="This is a raw data file",
            data_classification=RawDataClassification.SOURCE,
            columns=[],
            custom_line_terminator=None,
            primary_key_cols=[],
            supplemental_order_by_clause="",
            encoding="UTF-8",
            separator=",",
            ignore_quotes=False,
            always_historical_export=True,
            no_valid_primary_keys=False,
            import_chunk_size_rows=10,
            infer_columns_from_config=False,
            table_relationships=[],
        )

    def test_missing_configs_for_region(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "^Missing raw data configs for region: us_xy"
        ):
            _ = DirectIngestRegionRawFileConfig(
                region_code="us_xy",
                region_module=fake_regions,
            )

    def test_parse_no_defaults_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "^Missing default raw data configs for region: us_yy"
        ):
            _ = DirectIngestRegionRawFileConfig(
                region_code="us_yy",
                region_module=fake_regions,
            )

    def test_many_primary_person_tables(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^The following tables in region: us_xx are marked as primary"
            " person tables, but only one primary person table is allowed per region",
        ):
            primary_person_table_config1 = attr.evolve(
                self.sparse_config,
                file_tag="root1",
                is_primary_person_table=True,
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                        is_primary_for_external_id_type=True,
                    ),
                ],
            )
            primary_person_table_config2 = attr.evolve(
                self.sparse_config,
                file_tag="root2",
                is_primary_person_table=True,
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                        is_primary_for_external_id_type=True,
                    ),
                ],
            )
            _ = attr.evolve(
                self.us_xx_region_config,
                raw_file_configs={
                    "root1": primary_person_table_config1,
                    "root2": primary_person_table_config2,
                },
            )

    def test_many_person_id_types(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^Duplicate columns marked as primary for external id type US_OZ_EG in region",
        ):
            primary_col_config1 = attr.evolve(
                self.sparse_config,
                file_tag="root1",
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                        is_primary_for_external_id_type=True,
                    ),
                ],
            )
            primary_col_config2 = attr.evolve(
                self.sparse_config,
                file_tag="root2",
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                        is_primary_for_external_id_type=True,
                    ),
                ],
            )
            _ = attr.evolve(
                self.us_xx_region_config,
                raw_file_configs={
                    "root1": primary_col_config1,
                    "root2": primary_col_config2,
                },
            )

    def test_key_mismatch(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^The file tagged myFile was labeled in code as not_matching_key in region",
        ):
            _ = attr.evolve(
                self.us_xx_region_config,
                raw_file_configs={"not_matching_key": self.sparse_config},
            )

    def test_external_id_without_primary(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "^These external ID types are present on columns, without a corresponding"
            " column marked as the primary for that external id type, in region",
        ):
            config_with_external_ids = attr.evolve(
                self.sparse_config,
                file_tag="file1",
                columns=[
                    RawTableColumnInfo(
                        name="Col1",
                        description="description",
                        is_pii=False,
                        field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                    ),
                ],
            )
            _ = attr.evolve(
                self.us_xx_region_config,
                raw_file_configs={"file1": config_with_external_ids},
            )

    def test_valid_external_id(self) -> None:
        # Should run without crashing
        config_with_external_id = attr.evolve(
            self.sparse_config,
            file_tag="root1",
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
                    external_id_type="US_OZ_EG",
                    is_primary_for_external_id_type=True,
                ),
            ],
        )
        _ = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={"root1": config_with_external_id},
        )

    def test_parse_yaml(self) -> None:
        region_config = self.us_xx_region_config
        self.assertEqual(23, len(region_config.raw_file_configs))
        self.assertEqual(
            {
                "file_tag_first",
                "file_tag_second",
                "tagBasicData",
                "tagMoreBasicData",
                "tagColCapsDoNotMatchConfig",
                "tagFullHistoricalExport",
                "tagInvalidCharacters",
                "tagNormalizationConflict",
                "tagCustomLineTerminatorNonUTF8",
                "tagPipeSeparatedNonUTF8",
                "tagDoubleDaggerWINDOWS1252",
                "tagColumnsMissing",
                "tagRowExtraColumns",
                "tagRowMissingColumns",
                "tagFileConfigHeaders",
                "tagInvalidFileConfigHeaders",
                "tagMissingColumnsDefined",
                "tagFileConfigHeadersUnexpectedHeader",
                "tagFileConfigCustomDatetimeSql",
                "tagOneAllNullRow",
                "tagOneAllNullRowTwoGoodRows",
                "singlePrimaryKey",
                "multipleColPrimaryKeyHistorical",
            },
            set(region_config.raw_file_configs.keys()),
        )

        config_1 = region_config.raw_file_configs["file_tag_first"]
        self.assertEqual("file_tag_first", config_1.file_tag)
        self.assertEqual("First raw file.", config_1.file_description)
        self.assertEqual(RawDataClassification.SOURCE, config_1.data_classification)
        self.assertEqual(["col_name_1a", "col_name_1b"], config_1.primary_key_cols)
        self.assertEqual("ISO-456-7", config_1.encoding)
        self.assertEqual(",", config_1.separator)
        self.assertEqual("‡\n", config_1.custom_line_terminator)
        expected_column2_description = (
            "A column description that is long enough to take up\nmultiple lines. This"
            " text block will be interpreted\nliterally and trailing/leading whitespace"
            " is removed."
        )
        expected_columns_config_1 = [
            RawTableColumnInfo(
                name="col_name_1a",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description="First column.",
                known_values=[
                    ColumnEnumValueInfo(value="A", description="A description"),
                    ColumnEnumValueInfo(value="B", description=None),
                ],
            ),
            RawTableColumnInfo(
                name="col_name_1b",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=expected_column2_description,
            ),
            RawTableColumnInfo(
                name="undocumented_column",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
            ),
        ]
        self.assertEqual(expected_columns_config_1, config_1.columns)
        expected_config_1_config_2_relationship = RawTableRelationshipInfo(
            file_tag="file_tag_first",
            foreign_table="file_tag_second",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(
                        file_tag="file_tag_first", column="col_name_1a"
                    ),
                    column_2=JoinColumn(
                        file_tag="file_tag_second", column="col_name_2a"
                    ),
                )
            ],
            cardinality=RawDataJoinCardinality.ONE_TO_MANY,
        )
        expected_config_1_config_3_relationship = RawTableRelationshipInfo(
            file_tag="file_tag_first",
            foreign_table="tagBasicData",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(
                        file_tag="file_tag_first", column="col_name_1a"
                    ),
                    column_2=JoinColumn(file_tag="tagBasicData", column="COL1"),
                )
            ],
            cardinality=RawDataJoinCardinality.MANY_TO_MANY,
        )
        # Tests that a self-join relationship is valid
        expected_config_1_config_1_relationship = RawTableRelationshipInfo(
            file_tag="file_tag_first",
            foreign_table="file_tag_first",
            join_clauses=[
                ColumnEqualityJoinBooleanClause(
                    column_1=JoinColumn(
                        file_tag="file_tag_first", column="col_name_1a"
                    ),
                    column_2=JoinColumn(
                        file_tag="file_tag_first", column="col_name_1b"
                    ),
                )
            ],
            cardinality=RawDataJoinCardinality.MANY_TO_MANY,
        )
        self.assertEqual(
            [
                expected_config_1_config_2_relationship,
                expected_config_1_config_1_relationship,
                expected_config_1_config_3_relationship,
            ],
            config_1.table_relationships,
        )

        config_2 = region_config.raw_file_configs["file_tag_second"]
        expected_file_description_config_2 = (
            "Some special/unusual character's in the description &\nlong enough to"
            " make a second line!\\n Trailing/leading white\nspace is stripped & the"
            " text block is interpreted literally."
        )
        self.assertEqual("file_tag_second", config_2.file_tag)
        self.assertEqual(expected_file_description_config_2, config_2.file_description)
        self.assertEqual(RawDataClassification.VALIDATION, config_2.data_classification)
        self.assertEqual(["col_name_2a"], config_2.primary_key_cols)
        self.assertEqual("UTF-8", config_2.encoding)
        self.assertEqual("$", config_2.separator)
        self.assertEqual(
            [
                RawTableColumnInfo(
                    name="col_name_2a",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="column description",
                )
            ],
            config_2.columns,
        )
        self.assertEqual(
            # This relationship gets added even though it isn't defined reciprocally in
            # the YAML for file_tag_second
            [expected_config_1_config_2_relationship],
            config_2.table_relationships,
        )

        config_3 = region_config.raw_file_configs["tagBasicData"]
        self.assertEqual("tagBasicData", config_3.file_tag)
        self.assertEqual("tagBasicData file description", config_3.file_description)
        self.assertEqual(RawDataClassification.SOURCE, config_3.data_classification)
        self.assertEqual(["COL1"], config_3.primary_key_cols)
        self.assertEqual("UTF-8", config_3.encoding)
        self.assertEqual(",", config_3.separator)
        self.assertEqual(
            [
                RawTableColumnInfo(
                    name="COL1",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="column 1 description",
                    known_values=None,
                ),
                RawTableColumnInfo(
                    name="COL2",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="column 2 description",
                    known_values=None,
                ),
                RawTableColumnInfo(
                    name="COL3",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="column 3 description",
                    known_values=None,
                ),
            ],
            config_3.columns,
        )
        self.assertEqual(
            [expected_config_1_config_3_relationship], config_3.table_relationships
        )

        config_4 = region_config.raw_file_configs["tagPipeSeparatedNonUTF8"]
        self.assertEqual("tagPipeSeparatedNonUTF8", config_4.file_tag)
        self.assertEqual(RawDataClassification.SOURCE, config_4.data_classification)
        self.assertEqual(["PRIMARY_COL1"], config_4.primary_key_cols)
        self.assertEqual("ISO-8859-1", config_4.encoding)
        self.assertEqual("|", config_4.separator)

    def test_default_config_parsing(self) -> None:
        """Makes sure we parse us_xx_default.yaml properly."""
        default_config = self.us_xx_region_config.default_config()
        self.assertEqual("UTF-8", default_config.default_encoding)
        self.assertEqual(",", default_config.default_separator)
        self.assertEqual(False, default_config.default_ignore_quotes)
        self.assertEqual(False, default_config.default_always_historical_export)
        self.assertEqual("‡\n", default_config.default_line_terminator)
        self.assertEqual(False, default_config.default_no_valid_primary_keys)

    def test_parsing_obeys_defaults(self) -> None:
        """Checks that all defaults are applied for a file that does not specify a
        custom line terminator, encoding, etc.
        """
        simple_file_config = self.us_xx_region_config.raw_file_configs[
            "singlePrimaryKey"
        ]
        default_config = self.us_xx_region_config.default_config()
        self.assertEqual(
            simple_file_config.custom_line_terminator,
            default_config.default_line_terminator,
        )
        self.assertEqual(
            simple_file_config.encoding,
            default_config.default_encoding,
        )
        self.assertEqual(
            simple_file_config.separator,
            default_config.default_separator,
        )
        self.assertEqual(
            simple_file_config.always_historical_export,
            default_config.default_always_historical_export,
        )
        self.assertEqual(
            simple_file_config.no_valid_primary_keys,
            default_config.default_no_valid_primary_keys,
        )

    def test_parsing_overrides_defaults(self) -> None:
        """Checks that all defaults are overridden for a file that does specify a
        custom line terminator, encoding, etc.
        """
        default_config = self.us_xx_region_config.default_config()

        # This file has a custom line terminator / encoding / separator
        file_config = self.us_xx_region_config.raw_file_configs[
            "tagPipeSeparatedNonUTF8"
        ]
        self.assertNotEqual(
            file_config.custom_line_terminator, default_config.default_line_terminator
        )
        self.assertEqual("\n", file_config.custom_line_terminator)
        self.assertNotEqual(file_config.encoding, default_config.default_encoding)
        self.assertEqual("ISO-8859-1", file_config.encoding)
        self.assertNotEqual(
            file_config.separator,
            default_config.default_separator,
        )
        self.assertEqual("|", file_config.separator)

        # This file is always a historical export
        file_config = self.us_xx_region_config.raw_file_configs[
            "tagFullHistoricalExport"
        ]
        self.assertNotEqual(
            file_config.always_historical_export,
            default_config.default_always_historical_export,
        )
        self.assertTrue(file_config.always_historical_export)

        # This file has no_valid_primary_keys overridden
        file_config = DirectIngestRegionRawFileConfig(
            region_code="us_ww",
            region_module=fake_regions,
        ).raw_file_configs["tagNotHistorical"]
        self.assertNotEqual(
            file_config.no_valid_primary_keys,
            default_config.default_no_valid_primary_keys,
        )
        self.assertTrue(file_config.no_valid_primary_keys)

    def test_table_relationship_column_does_not_exist(self) -> None:
        config_1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile1",
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="col1 description",
                ),
                RawTableColumnInfo(
                    name="col2",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="col2 description",
                ),
            ],
            table_relationships=[
                RawTableRelationshipInfo(
                    file_tag="myFile1",
                    foreign_table="myFile2",
                    cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                    join_clauses=[
                        ColumnEqualityJoinBooleanClause(
                            column_1=JoinColumn(file_tag="myFile1", column="col1"),
                            # The column col2 does not exist in myFile2, but we can't
                            # know that until we build the DirectIngestRegionRawFileConfig
                            column_2=JoinColumn(file_tag="myFile2", column="col2"),
                        )
                    ],
                )
            ],
        )
        config_2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="col1 description",
                )
            ],
        )

        @attr.s
        class InMemoryDirectIngestRegionRawFileConfig(DirectIngestRegionRawFileConfig):
            def _read_configs_from_disk(self) -> Dict[str, DirectIngestRawFileConfig]:
                return {
                    config_1.file_tag: config_1,
                    config_2.file_tag: config_2,
                }

        with self.assertRaisesRegex(
            ValueError,
            r"Found column \[myFile2.col2\] referenced in join clause "
            r"\[myFile1.col1 = myFile2.col2\] which is not defined in the config for "
            r"\[myFile2\]",
        ):
            InMemoryDirectIngestRegionRawFileConfig(region_code="us_xx")

    def test_different_relationships_between_same_tables_multiple_files(self) -> None:
        config_1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile1",
            file_path="/path/to/myFile1.yaml",
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="col1 description",
                ),
                RawTableColumnInfo(
                    name="col2",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="col2 description",
                ),
            ],
            table_relationships=[
                RawTableRelationshipInfo(
                    file_tag="myFile1",
                    foreign_table="myFile2",
                    cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                    join_clauses=[
                        ColumnEqualityJoinBooleanClause(
                            column_1=JoinColumn(file_tag="myFile1", column="col1"),
                            column_2=JoinColumn(file_tag="myFile2", column="col1"),
                        )
                    ],
                )
            ],
        )
        config_2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            file_path="/path/to/myFile2.yaml",
            columns=[
                RawTableColumnInfo(
                    name="col1",
                    field_type=RawTableColumnFieldType.STRING,
                    is_pii=False,
                    description="col1 description",
                )
            ],
            table_relationships=[
                RawTableRelationshipInfo(
                    file_tag="myFile2",
                    foreign_table="myFile1",
                    cardinality=RawDataJoinCardinality.ONE_TO_MANY,
                    join_clauses=[
                        ColumnEqualityJoinBooleanClause(
                            column_1=JoinColumn(file_tag="myFile1", column="col2"),
                            column_2=JoinColumn(file_tag="myFile2", column="col1"),
                        )
                    ],
                )
            ],
        )

        @attr.s
        class InMemoryDirectIngestRegionRawFileConfig(DirectIngestRegionRawFileConfig):
            def _read_configs_from_disk(self) -> Dict[str, DirectIngestRawFileConfig]:
                return {
                    config_1.file_tag: config_1,
                    config_2.file_tag: config_2,
                }

        with self.assertRaisesRegex(
            ValueError,
            r"Found table_relationship defined in \[/path/to/myFile2.yaml\] between "
            r"tables \('myFile1', 'myFile2'\). There is already a relationship between "
            r"these tables defined in \[/path/to/myFile1.yaml\].",
        ):
            InMemoryDirectIngestRegionRawFileConfig(
                region_code="us_xx",
            )
