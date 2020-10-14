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
"""Tests for class ProtobufBuilder."""
import unittest

from google.cloud.tasks_v2.proto import queue_pb2
from google.protobuf import duration_pb2
from recidiviz.common.google_cloud.protobuf_builder import ProtobufBuilder


class TestProtobufBuilder(unittest.TestCase):
    """Tests for class ProtobufBuilder."""

    def test_simple_build(self):
        duration = ProtobufBuilder(duration_pb2.Duration).build()
        self.assertEqual(type(duration), duration_pb2.Duration)

    def test_compose_simple(self):
        duration1 = duration_pb2.Duration(seconds=1, nanos=2)
        duration2 = duration_pb2.Duration(nanos=3)
        duration = ProtobufBuilder(duration_pb2.Duration).compose(
            duration1
        ).compose(
            duration2
        ).build()

        self.assertEqual(duration, duration_pb2.Duration(seconds=1, nanos=3))

        duration = ProtobufBuilder(duration_pb2.Duration).compose(
            duration2
        ).compose(
            duration1
        ).build()

        self.assertEqual(duration, duration_pb2.Duration(seconds=1, nanos=2))

    def test_update_args(self):
        duration = ProtobufBuilder(duration_pb2.Duration).update_args(
            seconds=1,
            nanos=2,
        ).update_args(
            nanos=3
        ).build()

        self.assertEqual(duration, duration_pb2.Duration(seconds=1, nanos=3))

        duration = ProtobufBuilder(duration_pb2.Duration).update_args(
            nanos=3
        ).update_args(
            seconds=1,
            nanos=2,
        ).build()

        self.assertEqual(duration, duration_pb2.Duration(seconds=1, nanos=2))

    def test_build_multiple_ways(self):
        duration1 = duration_pb2.Duration(seconds=1, nanos=2)
        duration = ProtobufBuilder(duration_pb2.Duration).compose(
            duration1
        ).update_args(
            nanos=3,
        ).build()

        self.assertEqual(duration, duration_pb2.Duration(seconds=1, nanos=3))

        duration2 = duration_pb2.Duration(nanos=2)
        duration = ProtobufBuilder(duration_pb2.Duration).update_args(
            seconds=1,
        ).compose(
            duration2
        ).build()

        self.assertEqual(duration, duration_pb2.Duration(seconds=1, nanos=2))

    def test_multilevel_compose(self):
        base_queue = queue_pb2.Queue(
            rate_limits=queue_pb2.RateLimits(
                max_dispatches_per_second=100,
                max_concurrent_dispatches=1,
            ),
            retry_config=queue_pb2.RetryConfig(
                max_attempts=5,
            )
        )

        queue = ProtobufBuilder(queue_pb2.Queue).compose(
            base_queue
        ).compose(
            queue_pb2.Queue(
                rate_limits=queue_pb2.RateLimits(
                    max_dispatches_per_second=50,
                ),
            )
        ).update_args(
            retry_config=queue_pb2.RetryConfig(
                max_doublings=4,
            )
        ).build()

        expected_queue = queue_pb2.Queue(
            rate_limits=queue_pb2.RateLimits(
                max_dispatches_per_second=50,
                max_concurrent_dispatches=1,
            ),
            retry_config=queue_pb2.RetryConfig(
                max_attempts=5,
                max_doublings=4,
            )
        )

        self.assertEqual(queue, expected_queue)

    def test_bad_type_raises_error(self):
        duration_builder = ProtobufBuilder(duration_pb2.Duration)
        with self.assertRaises(TypeError):
            duration_builder.compose(queue_pb2.Queue())

    def test_bad_arg_raises_error(self):
        duration_builder = ProtobufBuilder(duration_pb2.Duration)
        with self.assertRaises(ValueError):
            duration_builder.update_args(name='this-field-does-not-exist')
