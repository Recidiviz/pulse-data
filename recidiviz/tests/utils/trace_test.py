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

import unittest
from concurrent import futures
from typing import List, cast

import pytest
from flask import Flask
from mock import Mock, patch
from more_itertools import one
from opentelemetry.baggage import get_baggage
from opentelemetry.sdk.metrics.export import Histogram, HistogramDataPoint
from parameterized import parameterized

from recidiviz.monitoring import trace
from recidiviz.monitoring.context import push_region_context
from recidiviz.monitoring.keys import AttributeKey, HistogramInstrumentKey
from recidiviz.tests.utils.monitoring_test_utils import OTLMock
from recidiviz.utils.structured_logging import with_context


@trace.span
def some_method(arg1: int, arg2: List[str]) -> List[str]:
    return arg1 * arg2


class SomeClass:
    @trace.span
    def recursive(self, depth: int) -> None:
        if depth:
            self.recursive(depth - 1)


class TestWithContext(unittest.TestCase):
    def setUp(self) -> None:
        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

    def tearDown(self) -> None:
        self.otl_mock.tear_down()

    def test_with_context_works_correctly(self) -> None:
        def job() -> str:
            return str(get_baggage(AttributeKey.REGION))

        tracer = self.otl_mock.tracer_provider.get_tracer("test_tracer")

        with tracer.start_span(name="root span"):
            with push_region_context("us_xx", "PRIMARY"):
                results = []
                with futures.ThreadPoolExecutor(max_workers=1) as executor:
                    job_futures = [executor.submit(with_context(job))]
                    for f in futures.as_completed(job_futures):
                        results.append(f.result())

        self.assertEqual(results, ["us_xx"])


class TestSpan(unittest.TestCase):
    """Tests trace span functionality"""

    def setUp(self) -> None:
        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

    def tearDown(self) -> None:
        self.otl_mock.tear_down()

    def test_call_createsSpan(self) -> None:
        # Act
        res = some_method(2, arg2=["1", "2"])

        # Assert
        self.assertEqual(["1", "2", "1", "2"], res)

        span = self.otl_mock.get_latest_span()

        self.assertEqual(span.name, "some_method")
        self.assertEqual(
            span.attributes,
            {
                AttributeKey.MODULE: "recidiviz.tests.utils.trace_test",
                AttributeKey.ARGS: "(2,)",
                AttributeKey.KWARGS: "{'arg2': ['1', '2']}",
            },
        )

    @patch("time.perf_counter")
    def test_call_recordsTime(self, mock_time: Mock) -> None:
        # Arrange
        mock_time.side_effect = [2.1, 2.25]

        # Actr
        res = some_method(2, arg2=["1", "2"])

        # Assert
        self.assertEqual(["1", "2", "1", "2"], res)

        metric = cast(
            Histogram,
            self.otl_mock.get_metric_data(
                metric_name=HistogramInstrumentKey.FUNCTION_DURATION
            ),
        )

        data_point: HistogramDataPoint = one(metric.data_points)

        self.assertEqual(
            data_point.attributes,
            {
                AttributeKey.MODULE: "recidiviz.tests.utils.trace_test",
                AttributeKey.FUNCTION: "some_method",
                AttributeKey.RECURSION_DEPTH: 1,
            },
        )

        self.assertEqual(data_point.sum, pytest.approx(0.15))

    def test_recursive_countsDepth(self) -> None:
        # Act
        SomeClass().recursive(3)

        histogram = cast(
            Histogram,
            self.otl_mock.get_metric_data(
                metric_name=HistogramInstrumentKey.FUNCTION_DURATION
            ),
        )

        # Assert
        self.assertEqual(
            [data_point.attributes for data_point in reversed(histogram.data_points)],
            [
                {
                    AttributeKey.MODULE: "recidiviz.tests.utils.trace_test",
                    AttributeKey.FUNCTION: "SomeClass.recursive",
                    AttributeKey.RECURSION_DEPTH: 1,
                },
                {
                    AttributeKey.MODULE: "recidiviz.tests.utils.trace_test",
                    AttributeKey.FUNCTION: "SomeClass.recursive",
                    AttributeKey.RECURSION_DEPTH: 2,
                },
                {
                    AttributeKey.MODULE: "recidiviz.tests.utils.trace_test",
                    AttributeKey.FUNCTION: "SomeClass.recursive",
                    AttributeKey.RECURSION_DEPTH: 3,
                },
                {
                    AttributeKey.MODULE: "recidiviz.tests.utils.trace_test",
                    AttributeKey.FUNCTION: "SomeClass.recursive",
                    AttributeKey.RECURSION_DEPTH: 4,
                },
            ],
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
    def test_compositeSampler_picksCorrect(
        self, _name: str, test_input: str, expected: int
    ) -> None:
        test_app = Flask("test_app")

        with test_app.test_request_context(path=test_input):
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

            # Act / Assert
            self.assertEqual(
                composite.should_sample(
                    parent_context=None, trace_id=123, name="route"
                ),
                expected,
            )
