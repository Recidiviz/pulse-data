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
"""Tests for utils/beam_utils.py."""

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from recidiviz.calculator.pipeline.utils import beam_utils


class TestBeamUtils(unittest.TestCase):
    """Tests for the beam_utils functions."""

    def testConvertDictToKVTuple(self):
        test_input = [
            {"key_field": "a", "other_field": "x"},
            {"key_field": "b", "other_field": "y"},
        ]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create(test_input)
            | "Test ConvertDictToKVTuple"
            >> beam.ParDo(beam_utils.ConvertDictToKVTuple(), "key_field")
        )

        correct_output = [
            ("a", {"key_field": "a", "other_field": "x"}),
            ("b", {"key_field": "b", "other_field": "y"}),
        ]

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testConvertDictToKVTuple_InvalidKey(self):
        test_input = [
            {"key_field": "a", "other_field": "x"},
            {"key_field": "b", "other_field": "y"},
        ]

        test_pipeline = TestPipeline()

        with self.assertRaises(ValueError):
            _ = (
                test_pipeline
                | beam.Create(test_input)
                | "Test ConvertDictToKVTuple"
                >> beam.ParDo(beam_utils.ConvertDictToKVTuple(), "not_the_key_field")
            )

            test_pipeline.run()
