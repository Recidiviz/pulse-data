# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for utils/bigquery_io_utils.py."""
import unittest
from typing import Any
from unittest.mock import patch

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
)
from apache_beam.pvalue import PDone
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.pipelines.utils.beam_utils import bigquery_io_utils
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TABLE_ADDRESS = BigQueryAddress(dataset_id="some_dataset", table_id="some_table")
_ID_COL = "id"


class _NoOpSink(beam.PTransform):
    """Stand-in for the real FILE_LOADS sink, which the emulator cannot run.
    Isolates these tests to WriteToBigQuery's clear wiring."""

    # pylint: disable=unused-argument
    def __init__(
        self,
        *,
        output_dataset: str,
        output_table: str,
        write_disposition: str,
        schema: Any = None,
    ) -> None:
        super().__init__()

    def expand(self, input_or_inputs: beam.PCollection) -> PDone:
        _ = input_or_inputs | beam.Map(lambda row: row)
        return PDone(input_or_inputs.pipeline)


class TestBeamUtils(unittest.TestCase):
    """Tests for the beam_utils functions."""

    def testConvertDictToKVTuple(self) -> None:
        test_input = [
            {"key_field": "a", "other_field": "x"},
            {"key_field": "b", "other_field": "y"},
        ]

        test_pipeline = create_test_pipeline()

        output = (
            test_pipeline
            | beam.Create(test_input)
            | "Test ConvertDictToKVTuple"
            >> beam.ParDo(bigquery_io_utils.ConvertDictToKVTuple("key_field"))
        )

        correct_output = [
            ("a", {"key_field": "a", "other_field": "x"}),
            ("b", {"key_field": "b", "other_field": "y"}),
        ]

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testConvertDictToKVTuple_InvalidKey(self) -> None:
        test_input = [
            {"key_field": "a", "other_field": "x"},
            {"key_field": "b", "other_field": "y"},
        ]

        test_pipeline = create_test_pipeline()

        with self.assertRaises(ValueError):
            _ = (
                test_pipeline
                | beam.Create(test_input)
                | "Test ConvertDictToKVTuple"
                >> beam.ParDo(
                    bigquery_io_utils.ConvertDictToKVTuple("not_the_key_field"),
                )
            )

            test_pipeline.run()


class WriteToBigQueryClearsOnEmptyTruncateTest(BigQueryEmulatorTestCase):
    """Tests that WriteToBigQuery clears the table on an empty truncating write
    but not on an empty appending write, run against the BQ emulator. Patches
    out the real FILE_LOADS sink, which the emulator cannot run, leaving the
    clear wiring as the behavior under test."""

    def setUp(self) -> None:
        super().setUp()
        self.create_mock_table(
            _TABLE_ADDRESS, schema=[bigquery.SchemaField(_ID_COL, "STRING")]
        )
        self.load_rows_into_table(_TABLE_ADDRESS, data=[{_ID_COL: "stale_row"}])

    def _run_empty_write(self, write_disposition: str) -> None:
        """Runs WriteToBigQuery over an empty input with the given disposition."""
        options = PipelineOptions()
        options.view_as(GoogleCloudOptions).project = self.project_id
        options.view_as(SetupOptions).save_main_session = False
        test_pipeline = TestPipeline(options=options)
        with patch(f"{bigquery_io_utils.__name__}._WriteToBigQuerySink", _NoOpSink):
            _ = (
                test_pipeline
                | beam.Create([])
                | WriteToBigQuery(
                    output_dataset=_TABLE_ADDRESS.dataset_id,
                    output_table=_TABLE_ADDRESS.table_id,
                    write_disposition=write_disposition,
                )
            )
            test_pipeline.run()

    def test_truncate_clears_stale_rows_on_empty_input(self) -> None:
        self._run_empty_write(beam.io.BigQueryDisposition.WRITE_TRUNCATE)

        self.run_query_test(
            f"SELECT {_ID_COL} FROM `{_TABLE_ADDRESS.to_str()}`", expected_result=[]
        )

    def test_append_keeps_stale_rows_on_empty_input(self) -> None:
        self._run_empty_write(beam.io.BigQueryDisposition.WRITE_APPEND)

        self.run_query_test(
            f"SELECT {_ID_COL} FROM `{_TABLE_ADDRESS.to_str()}`",
            expected_result=[{_ID_COL: "stale_row"}],
        )
