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
from datetime import datetime, timezone
from typing import Dict

import attr

from recidiviz.common.constants.csv import DEFAULT_CSV_LINE_TERMINATOR
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    ColumnUpdateInfo,
    ColumnUpdateOperation,
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
    ImportBlockingValidationExemption,
    RawDataClassification,
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
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestColumnChangeInfo(unittest.TestCase):
    """Tests for ColumnChangeInfo"""

    def test_column_update_previous_value(self) -> None:
        # Should not raise any exceptions
        for update_type in [
            ColumnUpdateOperation.ADDITION,
            ColumnUpdateOperation.DELETION,
        ]:
            ColumnUpdateInfo(update_type=update_type, update_datetime=datetime.now())

        ColumnUpdateInfo(
            update_type=ColumnUpdateOperation.RENAME,
            update_datetime=datetime.now(),
            previous_value="old_name",
        )

    def test_column_update_previous_value_error(self) -> None:
        for update_type in [
            ColumnUpdateOperation.ADDITION,
            ColumnUpdateOperation.DELETION,
        ]:
            with self.assertRaisesRegex(
                ValueError,
                r"previous_value must be set if and only if update_type is RENAME",
            ):
                ColumnUpdateInfo(
                    update_type=update_type,
                    update_datetime=datetime.now(),
                    previous_value="old_name",
                )
        with self.assertRaisesRegex(
            ValueError,
            r"previous_value must be set if and only if update_type is RENAME",
        ):
            ColumnUpdateInfo(
                update_type=ColumnUpdateOperation.RENAME,
                update_datetime=datetime.now(),
            )


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
            is_pii=True,
            description=None,
            known_values=None,
            external_id_type="US_OZ_EG",
            is_primary_for_external_id_type=True,
        )

        # Non-primary column but has an external id type
        _ = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
            is_pii=True,
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

        with self.assertRaisesRegex(
            ValueError,
            "Expected all datetime_sql_parsers to parse the same type",
        ):
            _ = RawTableColumnInfo(
                name="COL2",
                field_type=RawTableColumnFieldType.DATETIME,
                is_pii=False,
                description=None,
                known_values=None,
                datetime_sql_parsers=[
                    "SAFE.PARSE_DATETIME('%m/%D/%Y', {col_name})",
                    "SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', {col_name})",
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

    def test_external_id_not_pii(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found field COL1 with external id type person_external_id which is not "
            r"labeled `is_pii: True`.",
        ):
            _ = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.PERSON_EXTERNAL_ID,
                is_pii=False,
                description=None,
                known_values=None,
                external_id_type="US_OZ_EG",
            )

    def test_nonnull_value_validation_exemption(self) -> None:
        exemption = ImportBlockingValidationExemption(
            validation_type=RawDataImportBlockingValidationType.NONNULL_VALUES,
            exemption_reason="reason",
        )
        exempt_column = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=False,
            description=None,
            known_values=None,
            import_blocking_column_validation_exemptions=[exemption],
        )
        self.assertIsNotNone(exempt_column.import_blocking_column_validation_exemptions)
        self.assertTrue(
            ImportBlockingValidationExemption.list_includes_exemption_type(
                exempt_column.import_blocking_column_validation_exemptions,
                RawDataImportBlockingValidationType.NONNULL_VALUES,
            )
        )

    def test_nonnull_value_validation_not_exempt(self) -> None:
        non_exempt_column_default = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=False,
            description=None,
            known_values=None,
        )
        exemption = ImportBlockingValidationExemption(
            validation_type=RawDataImportBlockingValidationType.DATETIME_PARSERS,
            exemption_reason="reason",
        )
        non_exempt_column = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.DATETIME,
            is_pii=False,
            description=None,
            known_values=None,
            import_blocking_column_validation_exemptions=[exemption],
        )

        self.assertIsNone(
            non_exempt_column_default.import_blocking_column_validation_exemptions
        )
        self.assertIsNotNone(
            non_exempt_column.import_blocking_column_validation_exemptions
        )

        self.assertFalse(
            ImportBlockingValidationExemption.list_includes_exemption_type(
                non_exempt_column.import_blocking_column_validation_exemptions,
                RawDataImportBlockingValidationType.NONNULL_VALUES,
            )
        )

    def test_column_multiple_renames(self) -> None:
        column_info = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=False,
            description=None,
            known_values=None,
            update_history=[
                ColumnUpdateInfo(
                    ColumnUpdateOperation.RENAME,
                    update_datetime=datetime(2022, 1, 15),
                    previous_value="OLD_OLD_COL1",
                ),
                ColumnUpdateInfo(
                    update_type=ColumnUpdateOperation.RENAME,
                    update_datetime=datetime(2022, 2, 1),
                    previous_value="OLD_COL1",
                ),
            ],
        )
        self.assertEqual(
            column_info.name_at_datetime(datetime(2022, 1, 1)), "OLD_OLD_COL1"
        )
        self.assertEqual(
            column_info.name_at_datetime(datetime(2022, 1, 15)), "OLD_COL1"
        )
        self.assertEqual(
            column_info.name_at_datetime(datetime(2022, 1, 31)), "OLD_COL1"
        )
        self.assertEqual(column_info.name_at_datetime(datetime(2022, 2, 1)), "COL1")
        self.assertEqual(column_info.name_at_datetime(datetime(2022, 3, 1)), "COL1")

    def test_column_exists_at_datetime(self) -> None:
        column_info = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=False,
            description=None,
            known_values=None,
            update_history=[
                ColumnUpdateInfo(
                    ColumnUpdateOperation.ADDITION,
                    update_datetime=datetime(2022, 1, 15),
                ),
                ColumnUpdateInfo(
                    ColumnUpdateOperation.DELETION,
                    update_datetime=datetime(2022, 2, 1),
                ),
                ColumnUpdateInfo(
                    ColumnUpdateOperation.ADDITION,
                    update_datetime=datetime(2022, 4, 1),
                ),
            ],
        )
        self.assertIsNone(column_info.name_at_datetime(datetime(2022, 1, 1)))

        self.assertEqual(column_info.name_at_datetime(datetime(2022, 1, 15)), "COL1")
        self.assertEqual(column_info.name_at_datetime(datetime(2022, 1, 31)), "COL1")

        self.assertIsNone(column_info.name_at_datetime(datetime(2022, 2, 1)))
        self.assertIsNone(column_info.name_at_datetime(datetime(2022, 3, 1)))

        self.assertEqual(column_info.name_at_datetime(datetime(2022, 4, 1)), "COL1")
        self.assertEqual(column_info.name_at_datetime(datetime(2022, 5, 1)), "COL1")

    def test_valid_update_history(self) -> None:
        # Should not raise an error
        _column_info = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=False,
            description=None,
            known_values=None,
            update_history=[
                ColumnUpdateInfo(
                    update_type=ColumnUpdateOperation.ADDITION,
                    update_datetime=datetime(2022, 1, 15),
                ),
                ColumnUpdateInfo(
                    update_type=ColumnUpdateOperation.RENAME,
                    previous_value="old_name",
                    update_datetime=datetime(2022, 2, 1),
                ),
            ],
        )
        _column_info = RawTableColumnInfo(
            name="COL1",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=False,
            description=None,
            known_values=None,
            update_history=[
                ColumnUpdateInfo(
                    update_type=ColumnUpdateOperation.RENAME,
                    previous_value="old_name",
                    update_datetime=datetime(2022, 1, 15),
                ),
                ColumnUpdateInfo(
                    update_type=ColumnUpdateOperation.DELETION,
                    update_datetime=datetime(2022, 2, 1),
                ),
            ],
        )

    def test_invalid_sequence_update_history(self) -> None:
        with self.assertRaises(ValueError) as context:
            _column_info = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                update_history=[
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.ADDITION,
                        update_datetime=datetime(2022, 1, 15),
                    ),
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.ADDITION,
                        update_datetime=datetime(2022, 2, 1),
                    ),
                ],
            )
        self.assertEqual(
            "Invalid update_history sequence for column [COL1]. Found invalid transition from ADDITION -> ADDITION",
            str(context.exception),
        )

        with self.assertRaises(ValueError) as context:
            _column_info = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                update_history=[
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.DELETION,
                        update_datetime=datetime(2022, 1, 15),
                    ),
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.DELETION,
                        update_datetime=datetime(2022, 2, 1),
                    ),
                ],
            )
        self.assertEqual(
            "Invalid update_history sequence for column [COL1]. Found invalid transition from DELETION -> DELETION",
            str(context.exception),
        )

        with self.assertRaises(ValueError) as context:
            _column_info = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                update_history=[
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.RENAME,
                        previous_value="old_name",
                        update_datetime=datetime(2022, 1, 15),
                    ),
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.RENAME,
                        previous_value="old_name",
                        update_datetime=datetime(2022, 2, 1),
                    ),
                ],
            )
        self.assertEqual(
            "Invalid update_history sequence for column [COL1]. Found two consecutive RENAME updates with the same previous_value [old_name]",
            str(context.exception),
        )

        with self.assertRaises(ValueError) as context:
            _column_info = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                update_history=[
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.RENAME,
                        previous_value="old_name",
                        update_datetime=datetime(2022, 1, 15),
                    ),
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.DELETION,
                        update_datetime=datetime(2022, 1, 15),
                    ),
                ],
            )
        self.assertEqual(
            "Invalid update_history sequence for column [COL1]. Found two updates with the same update_datetime [2022-01-15T00:00:00]",
            str(context.exception),
        )

    def test_unsorted_update_history(self) -> None:
        with self.assertRaises(ValueError) as context:
            _column_info = RawTableColumnInfo(
                name="COL1",
                field_type=RawTableColumnFieldType.STRING,
                is_pii=False,
                description=None,
                known_values=None,
                update_history=[
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.ADDITION,
                        update_datetime=datetime(2022, 2, 1),
                    ),
                    ColumnUpdateInfo(
                        update_type=ColumnUpdateOperation.DELETION,
                        update_datetime=datetime(2022, 1, 15),
                    ),
                ],
            )
        self.assertEqual(
            "Expected update_history to be sorted by update_datetime for column [COL1].",
            str(context.exception),
        )


class TestDirectIngestRawFileConfig(unittest.TestCase):
    """Tests for DirectIngestRawFileConfig"""

    def setUp(self) -> None:
        self.sparse_config = DirectIngestRawFileConfig(
            state_code=StateCode.US_XX,
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
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
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
                        transforms=[],
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
                        transforms=[],
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
                        transforms=[],
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
                        is_pii=True,
                        field_type=RawTableColumnFieldType.STAFF_EXTERNAL_ID,
                        external_id_type="US_OZ_EG",
                    ),
                    RawTableColumnInfo(
                        name="Col2",
                        description="some other description",
                        is_pii=True,
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

    def test_columns_at_datetime(self) -> None:
        config = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.RENAME,
                            update_datetime=datetime(2022, 1, 15, tzinfo=timezone.utc),
                            previous_value="OldCol1",
                        ),
                    ],
                ),
                RawTableColumnInfo(
                    name="Col2",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.ADDITION,
                            update_datetime=datetime(2022, 2, 15, tzinfo=timezone.utc),
                        ),
                    ],
                ),
                RawTableColumnInfo(
                    name="Col3",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.DELETION,
                            update_datetime=datetime(2022, 3, 15, tzinfo=timezone.utc),
                        ),
                    ],
                ),
            ],
        )

        self.assertEqual(
            ["OldCol1", "Col3"],
            config.column_names_at_datetime(datetime(2022, 1, 1, tzinfo=timezone.utc)),
        )
        self.assertEqual(
            ["Col1", "Col3"],
            config.column_names_at_datetime(datetime(2022, 2, 1, tzinfo=timezone.utc)),
        )
        self.assertEqual(
            ["Col1", "Col2", "Col3"],
            config.column_names_at_datetime(datetime(2022, 3, 1, tzinfo=timezone.utc)),
        )
        self.assertEqual(
            ["Col1", "Col2"],
            config.column_names_at_datetime(datetime(2022, 4, 1, tzinfo=timezone.utc)),
        )

    def test_column_mappings_from_datetime_to_current(self) -> None:
        file_upload_datetime = datetime(2021, 1, 11, tzinfo=timezone.utc)
        config = attr.evolve(
            self.sparse_config,
            columns=[
                # Should include column renamed after file_upload_datetime
                RawTableColumnInfo(
                    name="Col1",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.RENAME,
                            update_datetime=datetime(2022, 1, 15, tzinfo=timezone.utc),
                            previous_value="OldCol1",
                        ),
                    ],
                ),
                # Should include columns with no update history
                RawTableColumnInfo(
                    name="Col2",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                ),
                # Should ignore added columns
                RawTableColumnInfo(
                    name="Col3",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.ADDITION,
                            update_datetime=datetime(2022, 3, 15, tzinfo=timezone.utc),
                        ),
                    ],
                ),
                # Should ignore deleted columns
                RawTableColumnInfo(
                    name="Col4",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.DELETION,
                            update_datetime=datetime(2022, 4, 15, tzinfo=timezone.utc),
                        ),
                    ],
                ),
                # Should ignore column rename before file_upload_datetime
                RawTableColumnInfo(
                    name="Col5",
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.STRING,
                    update_history=[
                        ColumnUpdateInfo(
                            update_type=ColumnUpdateOperation.RENAME,
                            update_datetime=datetime(2020, 5, 15, tzinfo=timezone.utc),
                            previous_value="OldCol5",
                        ),
                    ],
                ),
            ],
        )

        mappings = config.column_mapping_from_datetime_to_current(file_upload_datetime)
        self.assertEqual(
            mappings,
            {"OldCol1": "Col1", "Col2": "Col2", "Col5": "Col5"},
        )


class TestDirectIngestRegionRawFileConfig(unittest.TestCase):
    """Tests for DirectIngestRegionRawFileConfig"""

    def setUp(self) -> None:
        self.us_xx_region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions,
        )
        self.sparse_config = DirectIngestRawFileConfig(
            state_code=StateCode(self.us_xx_region_config.region_code.upper()),
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
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
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
                        is_pii=True,
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
                        is_pii=True,
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
                        is_pii=True,
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
                        is_pii=True,
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
                        is_pii=True,
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
                    is_pii=True,
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
        self.assertEqual(28, len(region_config.raw_file_configs))
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
                "tagChunkedFile",
                "tagChunkedFileTwo",
                "tagCustomLineTerminatorNonUTF8",
                "tagPipeSeparatedNonUTF8",
                "tagDoubleDaggerWINDOWS1252",
                "tagColumnsMissing",
                "tagColumnMissingInRawData",
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
                "tagColumnRenamed",
                "tagPipeSeparatedWindows",
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
                import_blocking_column_validation_exemptions=[
                    ImportBlockingValidationExemption(
                        validation_type=RawDataImportBlockingValidationType.NONNULL_VALUES,
                        exemption_reason="reason",
                    )
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
            transforms=[],
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
            transforms=[],
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
            transforms=[],
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

        config_5 = region_config.raw_file_configs["tagColumnRenamed"]
        self.assertEqual("tagColumnRenamed", config_5.file_tag)
        self.assertEqual(
            config_5.column_names_at_datetime(
                datetime(2021, 1, 1, tzinfo=timezone.utc)
            ),
            ["OLD_COL1", "COL2"],
        )
        self.assertEqual(
            config_5.column_names_at_datetime(
                datetime(2023, 1, 1, tzinfo=timezone.utc)
            ),
            ["COL1", "COL2"],
        )

    def test_default_config_parsing(self) -> None:
        """Makes sure we parse us_xx_default.yaml properly."""
        default_config = self.us_xx_region_config.default_config()
        self.assertEqual("UTF-8", default_config.default_encoding)
        self.assertEqual(",", default_config.default_separator)
        self.assertEqual(False, default_config.default_ignore_quotes)
        self.assertEqual(False, default_config.default_always_historical_export)
        self.assertEqual("‡\n", default_config.default_line_terminator)
        self.assertEqual(False, default_config.default_no_valid_primary_keys)
        self.assertTrue(
            ImportBlockingValidationExemption.list_includes_exemption_type(
                default_config.default_import_blocking_validation_exemptions,
                RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS,
            )
        )

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
        self.assertFalse(simple_file_config.is_code_file)
        self.assertFalse(simple_file_config.is_chunked_file)
        self.assertIsNone(simple_file_config.expected_number_of_chunks)
        self.assertTrue(
            ImportBlockingValidationExemption.list_includes_exemption_type(
                default_config.default_import_blocking_validation_exemptions,
                RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS,
            )
        )

    def test_parsing_overrides_defaults(self) -> None:
        """Checks that all defaults are overridden for a file that does specify a
        custom line terminator, encoding, etc.
        """
        default_config = self.us_xx_region_config.default_config()

        # This file has a custom line terminator / encoding / separator
        # and contains import-blocking validation exemption
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
        self.assertEqual(RawDataFileUpdateCadence.DAILY, file_config.update_cadence)
        self.assertTrue(file_config.is_code_file)
        self.assertTrue(file_config.is_chunked_file)
        self.assertIsNotNone(file_config.expected_number_of_chunks)
        # import_blocking_validation_exemptions should be appended to the default_import_blocking_validation_exemptions
        self.assertTrue(
            ImportBlockingValidationExemption.list_includes_exemption_type(
                file_config.import_blocking_validation_exemptions,
                RawDataImportBlockingValidationType.STABLE_HISTORICAL_RAW_DATA_COUNTS,
            )
        )
        self.assertTrue(
            ImportBlockingValidationExemption.list_includes_exemption_type(
                file_config.import_blocking_validation_exemptions,
                RawDataImportBlockingValidationType.NONNULL_VALUES,
            )
        )

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
                    transforms=[],
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
                    transforms=[],
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
                    transforms=[],
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

    def test_get_datetime_parsers(self) -> None:
        parsers = [
            "SAFE.PARSE_DATE('%m/%d/%y', {col_name})",
            "SAFE.PARSE_DATE('%m/%d/%Y', {col_name})",
        ]
        config1 = attr.evolve(
            self.sparse_config,
            columns=[
                RawTableColumnInfo(
                    name="date",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                    datetime_sql_parsers=parsers,
                )
            ],
        )
        # Duplicate parsers
        config2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            columns=[
                RawTableColumnInfo(
                    name="date",
                    field_type=RawTableColumnFieldType.DATETIME,
                    is_pii=False,
                    description="test",
                    datetime_sql_parsers=parsers[:-1],
                )
            ],
        )
        region_with_parsers = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": config1,
                "myFile2": config2,
            },
        )
        self.assertEqual(set(parsers), region_with_parsers.get_datetime_parsers())

    def test_get_no_parsers(self) -> None:
        region_without_parsers = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": self.sparse_config,
            },
        )
        self.assertEqual(set(), region_without_parsers.get_datetime_parsers())

    def test_all_regularly_updated(self) -> None:
        weekly_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile",
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        weekly_config2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        region_with_all_regularly_updated = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": weekly_config1,
                "myFile2": weekly_config2,
            },
        )

        assert (
            region_with_all_regularly_updated.get_configs_with_regularly_updated_data()
            == [
                weekly_config1,
                weekly_config2,
            ]
        )

    def test_regularly_updated_mixed(self) -> None:
        weekly_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile",
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        daily_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            update_cadence=RawDataFileUpdateCadence.DAILY,
        )
        region_with_regularly_updated_mixed = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": weekly_config1,
                "myFile2": daily_config1,
            },
        )

        assert region_with_regularly_updated_mixed.get_configs_with_regularly_updated_data() == [
            weekly_config1,
            daily_config1,
        ]

    def test_some_regularly_updated(self) -> None:
        weekly_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile",
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        irregular_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            update_cadence=RawDataFileUpdateCadence.IRREGULAR,
        )
        region_with_some_regularly_updated = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": weekly_config1,
                "myFile2": irregular_config1,
            },
        )
        assert (
            region_with_some_regularly_updated.get_configs_with_regularly_updated_data()
            == [weekly_config1]
        )

    def test_some_regularly_updated_2(self) -> None:
        weekly_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile",
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )
        irregular_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
            is_code_file=True,
        )
        region_with_some_regularly_updated = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": weekly_config1,
                "myFile2": irregular_config1,
            },
        )
        assert (
            region_with_some_regularly_updated.get_configs_with_regularly_updated_data()
            == [weekly_config1]
        )

    def test_none_regularly_updated(self) -> None:
        irregular_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile",
            update_cadence=RawDataFileUpdateCadence.IRREGULAR,
        )
        irregular_config2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            update_cadence=RawDataFileUpdateCadence.IRREGULAR,
        )
        region_with_none_regularly_updated = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": irregular_config1,
                "myFile2": irregular_config2,
            },
        )
        assert (
            region_with_none_regularly_updated.get_configs_with_regularly_updated_data()
            == []
        )

    def test_none_regularly_updated_2(self) -> None:
        irregular_config1 = attr.evolve(
            self.sparse_config,
            file_tag="myFile",
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
            is_code_file=True,
        )
        irregular_config2 = attr.evolve(
            self.sparse_config,
            file_tag="myFile2",
            update_cadence=RawDataFileUpdateCadence.IRREGULAR,
        )
        region_with_none_regularly_updated = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={
                "myFile": irregular_config1,
                "myFile2": irregular_config2,
            },
        )
        assert (
            region_with_none_regularly_updated.get_configs_with_regularly_updated_data()
            == []
        )

    def test_no_files_regularly_updated(self) -> None:
        region_with_none = attr.evolve(
            self.us_xx_region_config,
            raw_file_configs={},
        )
        assert region_with_none.get_configs_with_regularly_updated_data() == []

    def test_line_terminator_default(self) -> None:
        assert self.sparse_config.line_terminator == DEFAULT_CSV_LINE_TERMINATOR

    def test_line_terminator_custom(self) -> None:
        custom_terminator = attr.evolve(
            self.sparse_config, custom_line_terminator="‡\n"
        )
        assert (
            custom_terminator.line_terminator
            == custom_terminator.custom_line_terminator
        )

    def test_unexpected_number_of_chuks(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Raw data config not marked as is_chunked_file should not have an expected number of chunks: \[\d+\]",
        ):
            attr.evolve(self.sparse_config, expected_number_of_chunks=10)

    def test_no_expected_number_of_chuks(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Raw data config marked as is_chunked_file must have an expected number of chunks",
        ):
            attr.evolve(self.sparse_config, is_chunked_file=True)

    def test_is_exempt_from_validation(self) -> None:
        column_name = "Col1"
        table_validation_exemption_type = (
            RawDataImportBlockingValidationType.NONNULL_VALUES
        )
        column_validation_exemption_type = (
            RawDataImportBlockingValidationType.EXPECTED_TYPE
        )

        exempt_config = attr.evolve(
            self.sparse_config,
            import_blocking_validation_exemptions=[
                ImportBlockingValidationExemption(
                    validation_type=table_validation_exemption_type,
                    exemption_reason="reason",
                )
            ],
            columns=[
                RawTableColumnInfo(
                    name=column_name,
                    description="description",
                    is_pii=False,
                    field_type=RawTableColumnFieldType.INTEGER,
                    import_blocking_column_validation_exemptions=[
                        ImportBlockingValidationExemption(
                            validation_type=column_validation_exemption_type,
                            exemption_reason="reason",
                        )
                    ],
                ),
            ],
        )

        self.assertTrue(
            exempt_config.file_is_exempt_from_validation(
                table_validation_exemption_type
            )
        )
        self.assertTrue(
            exempt_config.column_is_exempt_from_validation(
                column_name, table_validation_exemption_type
            )
        )
        self.assertFalse(
            exempt_config.file_is_exempt_from_validation(
                column_validation_exemption_type
            )
        )
        self.assertTrue(
            exempt_config.column_is_exempt_from_validation(
                column_name, column_validation_exemption_type
            )
        )

    def test_is_exempt_from_column_validation_column_doesnt_exist(self) -> None:
        column_name = "ColDoesntExist"

        with self.assertRaisesRegex(
            ValueError,
            rf"Expected to find exactly one entry for column \[{column_name}\], found: \[\]",
        ):
            self.sparse_config.column_is_exempt_from_validation(
                column_name, RawDataImportBlockingValidationType.NONNULL_VALUES
            )
