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
"""Tests for LoadQueryResultsKeyedByColumn"""
from typing import Any, Callable, Dict, Iterable, List, Tuple
from unittest.mock import patch

from apache_beam.testing.util import BeamAssertException, assert_that

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_query_provider import SimpleBigQueryQueryProvider
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.pipelines.utils.beam_utils.load_query_results_keyed_by_column import (
    LoadQueryResultsKeyedByColumn,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.fake_bigquery import FakeReadFromBigQueryWithEmulator

TransformOutputType = Tuple[int, Dict[str, Any]]


class TestLoadQueryResultsKeyedByColumn(BigQueryEmulatorTestCase):
    """Tests for LoadQueryResultsKeyedByColumn"""

    def setUp(self) -> None:
        super().setUp()
        self.read_from_bq_patcher = patch(
            "apache_beam.io.ReadFromBigQuery",
            self.get_fake_read_from_big_query,
        )
        self.read_from_bq_patcher.start()

        self.test_pipeline = create_test_pipeline()

    def tearDown(self) -> None:
        super().tearDown()
        self.read_from_bq_patcher.stop()

    def get_fake_read_from_big_query(
        self,
        query: str,
        # pylint: disable=unused-argument
        use_standard_sql: bool,
        validate: bool,
        bigquery_job_labels: dict[str, str],
    ) -> FakeReadFromBigQueryWithEmulator:
        return FakeReadFromBigQueryWithEmulator(query=query, test_case=self)

    @staticmethod
    def get_expected_output_validator(
        expected_output: Iterable[TransformOutputType],
    ) -> Callable[[Iterable[TransformOutputType]], None]:
        def _validate_output(output: Iterable[TransformOutputType]) -> None:
            sorted_expected = sorted(expected_output, key=lambda t: t[0])
            sorted_output = sorted(output, key=lambda t: t[0])
            if sorted_expected != sorted_output:
                raise BeamAssertException(
                    f"Output does not match expected output. Output: {output}. "
                    f"Expected: {expected_output}."
                )

        return _validate_output

    def test_load_query_results_keyed_by_column(self) -> None:
        table_address = BigQueryAddress(dataset_id="my_dataset", table_id="my_view")
        table_data: List[Dict[str, Any]] = [
            {"primary_key": 101, "value": "A"},
            {"primary_key": 102, "value": "A"},
            {"primary_key": 103, "value": "B"},
            {"primary_key": 103, "value": None},
        ]
        self.create_mock_table(
            address=table_address,
            schema=[
                schema_field_for_type("primary_key", int),
                schema_field_for_type("value", str),
            ],
        )
        self.load_rows_into_table(address=table_address, data=table_data)

        query_provider = SimpleBigQueryQueryProvider(
            query=f"SELECT primary_key, value FROM `{self.project_id}.my_dataset.my_view`"
        )

        output = self.test_pipeline | LoadQueryResultsKeyedByColumn(
            key_column_name="primary_key",
            query_name="my_query",
            query_provider=query_provider,
            resource_labels={},
        )

        expected_output: List[TransformOutputType] = [
            (101, {"primary_key": 101, "value": "A"}),
            (102, {"primary_key": 102, "value": "A"}),
            (103, {"primary_key": 103, "value": "B"}),
            (103, {"primary_key": 103, "value": None}),
        ]

        assert_that(
            output,
            self.get_expected_output_validator(expected_output),
        )
        self.test_pipeline.run()
