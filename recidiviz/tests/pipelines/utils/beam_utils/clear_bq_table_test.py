# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for the clear_bq_table PTransforms, run against the BQ emulator."""
import apache_beam as beam
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.pipelines.utils.beam_utils.clear_bq_table import (
    ClearBQTable,
    ClearBQTableWhenInputEmpty,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TABLE_ADDRESS = BigQueryAddress(dataset_id="some_dataset", table_id="some_table")
_ID_COL = "id"


class ClearBQTableTestBase(BigQueryEmulatorTestCase):
    """Shared setup for the clear_bq_table PTransform tests: a one-column
    table seeded with one row per test."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            _TABLE_ADDRESS, schema=[bigquery.SchemaField(_ID_COL, "STRING")]
        )
        self.load_rows_into_table(_TABLE_ADDRESS, data=[{_ID_COL: "stale_row"}])
        self.project_specific_address = ProjectSpecificBigQueryAddress(
            project_id=self.project_id,
            dataset_id=_TABLE_ADDRESS.dataset_id,
            table_id=_TABLE_ADDRESS.table_id,
        )

    def run_transform_over_elements(self, elements: list[str]) -> None:
        """Runs the transform under test over a PCollection of the given
        elements (which may be empty)."""
        test_pipeline = create_test_pipeline()
        _ = (
            test_pipeline
            # Create-then-flatten so an empty element list still runs.
            | beam.Create([elements])
            | beam.FlatMap(lambda elements_list: elements_list)
            | self.transform_under_test()
        )
        test_pipeline.run()

    def transform_under_test(self) -> beam.PTransform:
        raise NotImplementedError("Subclasses must supply the transform to test.")

    def assert_table_contents(self, expected_ids: list[str]) -> None:
        self.run_query_test(
            f"SELECT {_ID_COL} FROM `{_TABLE_ADDRESS.to_str()}` ORDER BY {_ID_COL}",
            expected_result=[{_ID_COL: row_id} for row_id in expected_ids],
        )


class ClearBQTableTest(ClearBQTableTestBase):
    """Tests for ClearBQTable."""

    def transform_under_test(self) -> beam.PTransform:
        return ClearBQTable(address=self.project_specific_address)

    def test_clears_with_empty_input(self) -> None:
        self.run_transform_over_elements([])

        self.assert_table_contents([])

    def test_clears_with_nonempty_input(self) -> None:
        self.run_transform_over_elements(["a", "b"])

        self.assert_table_contents([])

    def test_clears_from_pbegin(self) -> None:
        """The PBegin form clears immediately, with no upstream PCollection."""
        test_pipeline = create_test_pipeline()
        _ = test_pipeline | self.transform_under_test()
        test_pipeline.run()

        self.assert_table_contents([])


class ClearBQTableWhenInputEmptyTest(ClearBQTableTestBase):
    """Tests for ClearBQTableWhenInputEmpty."""

    def transform_under_test(self) -> beam.PTransform:
        return ClearBQTableWhenInputEmpty(address=self.project_specific_address)

    def test_clears_with_empty_input(self) -> None:
        self.run_transform_over_elements([])

        self.assert_table_contents([])

    def test_does_not_clear_with_nonempty_input(self) -> None:
        self.run_transform_over_elements(["a", "b"])

        self.assert_table_contents(["stale_row"])
