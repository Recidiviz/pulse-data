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

from recidiviz.calculator.utils import beam_utils


class TestBeamUtils(unittest.TestCase):
    """Tests for the beam_utils functions."""
    def testRecidivismRateFn(self):
        test_values_dicts = [({'field': 'a'}, 0), ({'field': 'a'}, 0),
                             ({'field': 'a'}, 1),
                             ({'field': 'b'}, 0), ({'field': 'b'}, 1)]

        test_input = []

        for test_dict, value in test_values_dicts:
            test_set = frozenset(test_dict.items())
            test_input.append((test_set, value))

        correct_output = [
            (frozenset({('field', 'a')}), {
                'total_releases': 3,
                'recidivated_releases': 1,
                'recidivism_rate': (1.0 / 3)
            }),
            (frozenset({('field', 'b')}), {
                'total_releases': 2,
                'recidivated_releases': 1,
                'recidivism_rate': 0.5
            })
        ]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(test_input)
                  | 'Test RecidivismRateFn' >>
                  beam.CombinePerKey(beam_utils.RecidivismRateFn()))

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testRecidivismRateFn_EmptyInput(self):
        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([])
                  | 'Test RecidivismRateFn' >>
                  beam.CombinePerKey(beam_utils.RecidivismRateFn()))

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testRecidivismLibertyFn(self):
        test_values_dicts = [({'field': 'a'}, 300), ({'field': 'a'}, 100),
                             ({'field': 'a'}, 200),
                             ({'field': 'b'}, 1000), ({'field': 'b'}, 500)]

        test_input = []

        for test_dict, value in test_values_dicts:
            test_set = frozenset(test_dict.items())
            test_input.append((test_set, value))

        correct_output = [
            (frozenset({('field', 'a')}), {
                'returns': 3,
                'avg_liberty': 200,
            }),
            (frozenset({('field', 'b')}), {
                'returns': 2,
                'avg_liberty': 750,
            })
        ]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(test_input)
                  | 'Test RecidivismLibertyFn' >>
                  beam.CombinePerKey(beam_utils.RecidivismLibertyFn()))

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testRecidivismLibertyFn_NoInput(self):
        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([])
                  | 'Test RecidivismLibertyFn' >>
                  beam.CombinePerKey(beam_utils.RecidivismLibertyFn()))

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testSumFn(self):
        test_values_dicts = [({'field': 'a'}, 1), ({'field': 'a'}, 1),
                             ({'field': 'a'}, 1),
                             ({'field': 'b'}, 1), ({'field': 'b'}, 1)]

        test_input = []

        for test_dict, value in test_values_dicts:
            test_set = frozenset(test_dict.items())
            test_input.append((test_set, value))

        correct_output = [
            (frozenset({('field', 'a')}), 3),
            (frozenset({('field', 'b')}), 2)
        ]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(test_input)
                  | 'Test SumFn' >>
                  beam.CombinePerKey(beam_utils.SumFn()))

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testSumFn_NoInput(self):
        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([])
                  | 'Test SumFn' >>
                  beam.CombinePerKey(beam_utils.SumFn()))

        assert_that(output, equal_to([]))

        test_pipeline.run()
