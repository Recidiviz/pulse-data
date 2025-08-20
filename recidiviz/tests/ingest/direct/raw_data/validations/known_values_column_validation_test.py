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
"""Unit tests for known_values_column_validation.py."""

import datetime
from typing import Dict, List, Optional, Type
from unittest.mock import patch

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    RawDataClassification,
    RawDataExportLookbackWindow,
    RawDataFileUpdateCadence,
)
from recidiviz.ingest.direct.raw_data.validations.known_values_column_validation import (
    KnownValuesColumnValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.raw_data.validations.column_validation_test_case import (
    ColumnValidationTestCase,
)


class TestKnownValuesColumnValidation(ColumnValidationTestCase):
    """Unit tests for KnownValuesColumnValidation"""

    maxDiff = None

    def setUp(self) -> None:
        super().setUp()
        self.known_values = ["a", "b", "c"]
        self.happy_col = attr.evolve(
            self.happy_col,
            known_values=self._known_values_strings_to_enums(self.known_values),
        )
        self.sad_col = attr.evolve(
            self.sad_col,
            known_values=self._known_values_strings_to_enums(self.known_values),
        )
        self.regions_patcher = patch(
            "recidiviz.ingest.direct.direct_ingest_regions.direct_ingest_regions_module",
            fake_regions,
        )
        self.regions_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.regions_patcher.stop()

    def get_validation_class(self) -> Type[RawDataColumnImportBlockingValidation]:
        return KnownValuesColumnValidation

    def get_test_data(self) -> List[Dict[str, Optional[str]]]:
        return [
            {self.happy_col_name: "a", self.sad_col_name: "b"},
            {self.happy_col_name: "b", self.sad_col_name: "z"},
            {self.happy_col_name: "0000", self.sad_col_name: "a"},
        ]

    def _known_values_strings_to_enums(
        self, known_values: List[str]
    ) -> List[ColumnEnumValueInfo]:
        return [
            ColumnEnumValueInfo(value=x, description="descript") for x in known_values
        ]

    def test_build_query_empty_known_values(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"known_values for {self.sad_col_name} must not be empty",
        ):
            KnownValuesColumnValidation(
                file_tag=self.file_tag,
                project_id=self.project_id,
                temp_table_address=self.temp_table_address,
                column_name=self.sad_col_name,
                known_values=[],
                null_values=None,
                state_code=StateCode.US_XX,
            )

    def test_validation_success(self) -> None:
        self.validation_success_test()

    def test_validation_failure(self) -> None:
        self.file_tag = "tagBasicData"
        quoted_known_values = [f'"{v}"' for v in self.known_values]
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.KNOWN_VALUES,
            validation_query=self.create_validation(self.sad_col).query,
            error_msg=f"Found column [{self.sad_col_name}] on raw file [{self.file_tag}] "
            f"not matching any of the known_values defined in its configuration YAML."
            f"\nDefined known values: [{', '.join(quoted_known_values)}]."
            f'\nValues that did not parse: ["z"].'
            f"\nThe following ingest views reference [tagBasicData]:\n\t-basic\n\t-tagBasicData\nIf this column is used in an ingest enum mapping, adding the new values will help ensure that the enum failure occurs at raw data import time instead of ingest time. If it is not used, you can (1) add the new values to list of known_values for [sad_col] if you want to keep the addition of new values as import-blocking; (2) add an import-blocking exclusion if you want to keep the existing documentation but know that it may quickly become stale; or (3) remove all known_values for [sad_col] from the yaml config to remove its designation as an enum.",
        )

        self.validation_failure_test(expected_error)

    def test_validation_failure_no_known_values(self) -> None:
        self.sad_col = attr.evolve(
            self.sad_col,
            known_values=self._known_values_strings_to_enums([]),
        )

        with self.assertRaisesRegex(
            ValueError, r"known_values for sad_col must not be empty"
        ):
            self.create_validation(self.sad_col)

    def test_validation_failure_multiple_known(self) -> None:
        self.sad_col = attr.evolve(
            self.sad_col,
            known_values=self._known_values_strings_to_enums(["z"]),
        )

        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.KNOWN_VALUES,
            validation_query=self.create_validation(self.sad_col).query,
            error_msg=f"Found column [{self.sad_col_name}] on raw file [{self.file_tag}] "
            f"not matching any of the known_values defined in its configuration YAML."
            f'\nDefined known values: ["z"].'
            f'\nValues that did not parse: ["a", "b"].'
            f"\nNo ingest views references [test_file_tag]. To resolve this error, you can: (1) update the list of known_values for [sad_col] if you want to keep the addition of new values as import-blocking; (2) add an import-blocking exclusion if you want to keep the existing documentation but know that it may quickly become stale; or (3) remove all known_values for [sad_col] from the yaml config to remove its designation as an enum.",
        )

        self.validation_failure_test(expected_error)

    def test_known_values_column_validation_applies_to_column(self) -> None:
        known_values_column = self.happy_col
        non_known_values_column = attr.evolve(self.happy_col, known_values=None)
        raw_file_config = DirectIngestRawFileConfig(
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
            ignore_quotes=True,
            export_lookback_window=RawDataExportLookbackWindow.FULL_HISTORICAL_LOOKBACK,
            no_valid_primary_keys=False,
            infer_columns_from_config=False,
            table_relationships=[],
            update_cadence=RawDataFileUpdateCadence.WEEKLY,
        )

        self.assertTrue(
            KnownValuesColumnValidation.validation_applies_to_column(
                known_values_column, raw_file_config
            )
        )
        self.assertFalse(
            KnownValuesColumnValidation.validation_applies_to_column(
                non_known_values_column, raw_file_config
            )
        )

    def test_query_properly_escapes_backslash(self) -> None:
        known_values = ["a\\b", "c\\d"]
        null_values = ["0000", "\\NA"]
        happy_col = attr.evolve(
            self.happy_col,
            known_values=self._known_values_strings_to_enums(known_values),
            null_values=null_values,
        )
        validation = KnownValuesColumnValidation.create_column_validation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            state_code=StateCode.US_XX,
            temp_table_address=self.temp_table_address,
            file_upload_datetime=datetime.datetime.now(),
            column=happy_col,
        )
        expected_query = r"""
SELECT DISTINCT happy_col
FROM recidiviz-bq-emulator-project.test_dataset.test_table
WHERE happy_col IS NOT NULL
    AND happy_col NOT IN ("a\\b", "c\\d")
    AND happy_col NOT IN ("0000", "\\NA")
"""
        self.assertEqual(validation.build_query(), expected_query)

        known_values = ["\\"]
        happy_col = attr.evolve(
            self.happy_col,
            known_values=self._known_values_strings_to_enums(known_values),
        )
        validation = KnownValuesColumnValidation.create_column_validation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            state_code=StateCode.US_XX,
            temp_table_address=self.temp_table_address,
            file_upload_datetime=datetime.datetime.now(),
            column=happy_col,
        )
        expected_query = r"""
SELECT DISTINCT happy_col
FROM recidiviz-bq-emulator-project.test_dataset.test_table
WHERE happy_col IS NOT NULL
    AND happy_col NOT IN ("\\")
    AND happy_col NOT IN ("0000")
"""
        self.assertEqual(validation.build_query(), expected_query)
