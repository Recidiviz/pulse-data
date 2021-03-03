# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests the trace various utilities"""

from typing import List
import unittest

from mock import ANY, Mock, call, patch
from parameterized import parameterized
import pytest

from recidiviz.utils import trace


@trace.span
def some_method(arg1: int, arg2: List[str]) -> List[str]:
    return arg1 * arg2


class SomeClass:
    @trace.span
    def recursive(self, depth: int):
        if depth:
            self.recursive(depth - 1)


class TestSpan(unittest.TestCase):
    """Tests trace span functionality"""

    @patch("opencensus.trace.execution_context.get_opencensus_tracer")
    def test_call_createsSpan(self, mock_get_tracer):
        # Arrange
        mock_tracer = mock_get_tracer.return_value
        mock_span = mock_tracer.span.return_value.__enter__.return_value

        # Act
        res = some_method(2, arg2=["1", "2"])

        # Assert
        self.assertEqual(["1", "2", "1", "2"], res)
        mock_tracer.span.assert_called_with(name="some_method")
        mock_span.add_attribute.assert_has_calls(
            [
                call("recidiviz.function.module", "recidiviz.tests.utils.trace_test"),
                call("recidiviz.function.args", "(2,)"),
                call("recidiviz.function.kwargs", "{'arg2': ['1', '2']}"),
            ]
        )

    @patch("recidiviz.utils.monitoring.measurements")
    @patch("time.perf_counter")
    def test_call_recordsTime(self, mock_time, mock_measurements_method):
        # Arrange
        mock_time.side_effect = [2.1, 2.25]
        mock_measurements = mock_measurements_method.return_value.__enter__.return_value

        # Act
        res = some_method(2, arg2=["1", "2"])

        # Assert
        self.assertEqual(["1", "2", "1", "2"], res)
        mock_measurements_method.assert_called_with(
            {
                "module": "recidiviz.tests.utils.trace_test",
                "function": "some_method",
                "recursion_depth": 0,
            }
        )
        self.assertEqual(
            [call(ANY, pytest.approx(0.15))],
            mock_measurements.measure_float_put.mock_calls,
        )

    @patch("recidiviz.utils.monitoring.measurements")
    def test_recursive_countsDepth(self, mock_measurements_method):
        # Act
        SomeClass().recursive(3)

        # Assert
        mock_measurements_method.assert_has_calls(
            [
                call(
                    {
                        "module": "recidiviz.tests.utils.trace_test",
                        "function": "SomeClass.recursive",
                        "recursion_depth": 0,
                    }
                ),
                call().__enter__(),
                call(
                    {
                        "module": "recidiviz.tests.utils.trace_test",
                        "function": "SomeClass.recursive",
                        "recursion_depth": 1,
                    }
                ),
                call().__enter__(),
                call(
                    {
                        "module": "recidiviz.tests.utils.trace_test",
                        "function": "SomeClass.recursive",
                        "recursion_depth": 2,
                    }
                ),
                call().__enter__(),
                call(
                    {
                        "module": "recidiviz.tests.utils.trace_test",
                        "function": "SomeClass.recursive",
                        "recursion_depth": 3,
                    }
                ),
                call().__enter__(),
            ]
        )


class TestCompositeSampler(unittest.TestCase):
    """Tests composite sampler functionality"""

    @parameterized.expand(
        [
            ("aa", "/a/a", 1),
            ("a", "/a", 3),
            ("bb", "/b/b", 2),
            ("b", "/b", 2),
            ("c", "/c", 3),
        ]
    )
    @patch("recidiviz.utils.trace.request")
    def test_compositeSampler_picksCorrect(
        self, _name, test_input, expected, mock_request
    ):
        # Arrange
        mock_1 = Mock()
        mock_1.should_sample.return_value = 1
        mock_2 = Mock()
        mock_2.should_sample.return_value = 2
        mock_3 = Mock()
        mock_3.should_sample.return_value = 3

        composite = trace.CompositeSampler(
            {
                "/a/": mock_1,
                "/b": mock_2,
            },
            mock_3,
        )
        mock_request.path = test_input

        # Act / Assert
        self.assertEqual(composite.should_sample(None), expected)
