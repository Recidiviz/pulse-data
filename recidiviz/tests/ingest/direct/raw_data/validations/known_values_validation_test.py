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
"""Unit tests for known_values_validation.py."""

from unittest.mock import patch

import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import ColumnEnumValueInfo
from recidiviz.ingest.direct.raw_data.validations.known_values_validation import (
    KnownValuesValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.raw_data.validations.import_blocking_validation_test_case import (
    RawDataImportBlockingValidationTestCase,
)


class TestKnownValuesValidation(RawDataImportBlockingValidationTestCase):
    """Unit tests for KnownValuesValidation"""

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
        self.raw_file_config = attr.evolve(
            self.raw_file_config,
            columns=[self.happy_col, self.sad_col],
        )
        self.context = attr.evolve(self.context, raw_file_config=self.raw_file_config)
        self.validation = KnownValuesValidation.create_validation(context=self.context)

        self.no_known_vals_col = attr.evolve(
            self.sad_col,
            known_values=self._known_values_strings_to_enums([]),
        )
        self.no_known_vals_file_config = attr.evolve(
            self.raw_file_config, columns=[self.no_known_vals_col]
        )
        self.no_known_vals_context = attr.evolve(
            self.context, raw_file_config=self.no_known_vals_file_config
        )

    def tearDown(self) -> None:
        super().tearDown()
        self.regions_patcher.stop()

    @staticmethod
    def _known_values_strings_to_enums(
        known_values: list[str],
    ) -> list[ColumnEnumValueInfo]:
        return [
            ColumnEnumValueInfo(value=x, description="descript") for x in known_values
        ]

    def test_build_query_empty_known_values(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"known_values for \[{self.sad_col_name}\] must not be empty",
        ):
            KnownValuesValidation(
                state_code=self.state_code,
                file_tag=self.file_tag,
                project_id=self.project_id,
                temp_table_address=self.temp_table_address,
                column_name_to_columns={
                    self.no_known_vals_col.name: self.no_known_vals_col
                },
            )

    def test_validation_success(self) -> None:
        valid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "a", self.sad_col_name: "b"},
            {self.happy_col_name: "b", self.sad_col_name: "b"},
            {self.happy_col_name: "0000", self.sad_col_name: "a"},
        ]
        self.validation_success_test(validation=self.validation, test_data=valid_data)

    def test_validation_failure(self) -> None:
        invalid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "a", self.sad_col_name: "b"},
            {self.happy_col_name: "b", self.sad_col_name: "z"},
            {self.happy_col_name: "0000", self.sad_col_name: "a"},
        ]
        file_tag = "tagBasicData"
        context = attr.evolve(self.context, file_tag=file_tag)
        validation = KnownValuesValidation.create_validation(context=context)
        quoted_known_values = [f'"{v}"' for v in self.known_values]
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.KNOWN_VALUES,
            validation_query=validation.build_query(),
            error_msg=f"Found column(s) on raw file [{file_tag}] "
            f"not matching any of the known_values defined in its configuration YAML."
            f"\nColumn name: [{self.sad_col_name}]"
            f"\nDefined known values: [{', '.join(quoted_known_values)}]."
            f'\nValues that did not parse: ["z"].'
            f"\nThe following ingest views reference [tagBasicData]:\n\t-basic\n\t-tagBasicData\nIf this column is used in an ingest enum mapping, adding the new values will help ensure that the enum failure occurs at raw data import time instead of ingest time. If it is not used, you can (1) add the new values to list of known_values for [sad_col] if you want to keep the addition of new values as import-blocking; (2) add an import-blocking exclusion if you want to keep the existing documentation but know that it may quickly become stale; or (3) remove all known_values for [sad_col] from the yaml config to remove its designation as an enum.\n",
        )

        self.validation_failure_test(
            validation=validation,
            test_data=invalid_data,
            expected_error=expected_error,
        )

    def test_validation_failure_no_known_values(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"No columns with configured known_values in \[{self.file_tag}\]",
        ):
            KnownValuesValidation.create_validation(self.no_known_vals_context)

    def test_validation_failure_multiple_known(self) -> None:
        invalid_data: list[dict[str, str | None]] = [
            {self.happy_col_name: "a", self.sad_col_name: "b"},
            {self.happy_col_name: "b", self.sad_col_name: "z"},
            {self.happy_col_name: "0000", self.sad_col_name: "y"},
        ]
        expected_error = RawDataImportBlockingValidationFailure(
            validation_type=RawDataImportBlockingValidationType.KNOWN_VALUES,
            validation_query=self.validation.build_query(),
            error_msg=f"Found column(s) on raw file [{self.file_tag}] "
            f"not matching any of the known_values defined in its configuration YAML."
            f"\nColumn name: [{self.sad_col_name}]"
            f'\nDefined known values: ["a", "b", "c"].'
            f'\nValues that did not parse: ["z", "y"].'
            f"\nNo ingest views references [test_file_tag]. To resolve this error, you can: (1) update the list of known_values for [sad_col] if you want to keep the addition of new values as import-blocking; (2) add an import-blocking exclusion if you want to keep the existing documentation but know that it may quickly become stale; or (3) remove all known_values for [sad_col] from the yaml config to remove its designation as an enum.\n",
        )

        self.validation_failure_test(
            validation=self.validation,
            test_data=invalid_data,
            expected_error=expected_error,
        )

    def test_known_values_column_validation_applies_to_column(self) -> None:
        self.assertTrue(KnownValuesValidation.validation_applies_to_file(self.context))
        self.assertFalse(
            KnownValuesValidation.validation_applies_to_file(self.no_known_vals_context)
        )

    def test_query_properly_escapes_backslash(self) -> None:
        known_values = ["a\\b", "c\\d"]
        null_values = ["0000", "\\NA"]
        happy_col = attr.evolve(
            self.happy_col,
            known_values=self._known_values_strings_to_enums(known_values),
            null_values=null_values,
        )
        validation = KnownValuesValidation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            state_code=self.state_code,
            temp_table_address=self.temp_table_address,
            column_name_to_columns={happy_col.name: happy_col},
        )
        expected_query = r"""
WITH failed_known_values AS (
  SELECT 
            CASE
            WHEN happy_col IS NOT NULL
                AND happy_col NOT IN ("a\\b", "c\\d")
                AND happy_col NOT IN ("0000", "\\NA")
            THEN CAST(happy_col AS STRING)
            ELSE NULL
            END AS happy_col_failed_value
  FROM `recidiviz-bq-emulator-project.test_dataset.test_table`
),
aggregated_failures AS (
  SELECT 
            ARRAY_AGG(DISTINCT happy_col_failed_value IGNORE NULLS LIMIT 10)
            AS happy_col_failures
  FROM failed_known_values
)
SELECT
  column_name,
  failed_values
FROM aggregated_failures
UNPIVOT (
  failed_values FOR column_name IN (happy_col_failures AS 'happy_col')
)
WHERE ARRAY_LENGTH(failed_values) > 0
"""
        self.assertEqual(validation.build_query(), expected_query)

        known_values = ["\\"]
        happy_col = attr.evolve(
            self.happy_col,
            known_values=self._known_values_strings_to_enums(known_values),
        )
        validation = KnownValuesValidation(
            file_tag=self.file_tag,
            project_id=self.project_id,
            state_code=self.state_code,
            temp_table_address=self.temp_table_address,
            column_name_to_columns={happy_col.name: happy_col},
        )
        expected_query = r"""
WITH failed_known_values AS (
  SELECT 
            CASE
            WHEN happy_col IS NOT NULL
                AND happy_col NOT IN ("\\")
                AND happy_col NOT IN ("0000")
            THEN CAST(happy_col AS STRING)
            ELSE NULL
            END AS happy_col_failed_value
  FROM `recidiviz-bq-emulator-project.test_dataset.test_table`
),
aggregated_failures AS (
  SELECT 
            ARRAY_AGG(DISTINCT happy_col_failed_value IGNORE NULLS LIMIT 10)
            AS happy_col_failures
  FROM failed_known_values
)
SELECT
  column_name,
  failed_values
FROM aggregated_failures
UNPIVOT (
  failed_values FOR column_name IN (happy_col_failures AS 'happy_col')
)
WHERE ARRAY_LENGTH(failed_values) > 0
"""
        self.assertEqual(validation.build_query(), expected_query)
