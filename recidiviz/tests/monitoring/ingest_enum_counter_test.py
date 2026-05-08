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
"""Tests for ingest_enum_counter.py"""
import enum
import unittest
from typing import cast

from more_itertools import one
from opentelemetry.sdk.metrics.export import NumberDataPoint, Sum

from recidiviz.monitoring.ingest_enum_counter import (
    emit_enum_mapping_heartbeat,
    log_unmapped_enum,
)
from recidiviz.monitoring.keys import AttributeKey, CounterInstrumentKey
from recidiviz.tests.utils.monitoring_test_utils import OTLMock


class _FakeEnum(enum.Enum):
    VALUE_A = "VALUE_A"
    INTERNAL_UNKNOWN = "INTERNAL_UNKNOWN"


class LogUnmappedEnumTest(unittest.TestCase):
    """Tests for log_unmapped_enum()."""

    def setUp(self) -> None:
        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

    def tearDown(self) -> None:
        self.otl_mock.tear_down()

    def test_increments_counter_with_correct_attributes(self) -> None:
        log_unmapped_enum(
            state_code="US_XX",
            enum_cls=_FakeEnum,
            field_name="my_enum_field",
            ingest_view_name="my_ingest_view",
            raw_text="SOME_RAW_VALUE",
        )

        metric_data = cast(
            Sum,
            self.otl_mock.get_metric_data(
                metric_name=CounterInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE
            ),
        )
        data_point: NumberDataPoint = one(metric_data.data_points)

        self.assertEqual(data_point.value, 1)
        self.assertEqual(
            data_point.attributes,
            {
                AttributeKey.REGION: "US_XX",
                AttributeKey.ENUM_TYPE: "_FakeEnum",
                AttributeKey.ENUM_FIELD_NAME: "my_enum_field",
                AttributeKey.INGEST_VIEW_NAME: "my_ingest_view",
            },
        )

    def test_multiple_calls_same_attributes_accumulate(self) -> None:
        for _ in range(3):
            log_unmapped_enum(
                state_code="US_XX",
                enum_cls=_FakeEnum,
                field_name="my_enum_field",
                ingest_view_name="my_ingest_view",
                raw_text="SOME_RAW_VALUE",
            )

        metric_data = cast(
            Sum,
            self.otl_mock.get_metric_data(
                metric_name=CounterInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE
            ),
        )
        data_point: NumberDataPoint = one(metric_data.data_points)
        self.assertEqual(data_point.value, 3)

    def test_different_attributes_create_separate_data_points(self) -> None:
        log_unmapped_enum(
            state_code="US_XX",
            enum_cls=_FakeEnum,
            field_name="field_a",
            ingest_view_name="view_a",
            raw_text="RAW_1",
        )
        log_unmapped_enum(
            state_code="US_YY",
            enum_cls=_FakeEnum,
            field_name="field_b",
            ingest_view_name="view_b",
            raw_text="RAW_2",
        )

        metric_data = cast(
            Sum,
            self.otl_mock.get_metric_data(
                metric_name=CounterInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE
            ),
        )
        self.assertEqual(len(metric_data.data_points), 2)

        points_by_region = {
            dp.attributes[AttributeKey.REGION]: dp
            for dp in metric_data.data_points
            if dp.attributes is not None
        }
        self.assertEqual(points_by_region["US_XX"].value, 1)
        assert points_by_region["US_XX"].attributes is not None
        self.assertEqual(
            points_by_region["US_XX"].attributes[AttributeKey.ENUM_FIELD_NAME],
            "field_a",
        )
        self.assertEqual(points_by_region["US_YY"].value, 1)
        assert points_by_region["US_YY"].attributes is not None
        self.assertEqual(
            points_by_region["US_YY"].attributes[AttributeKey.ENUM_FIELD_NAME],
            "field_b",
        )


class EmitEnumMappingHeartbeatTest(unittest.TestCase):
    """Tests for emit_enum_mapping_heartbeat()."""

    def setUp(self) -> None:
        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

    def tearDown(self) -> None:
        self.otl_mock.tear_down()

    def test_emits_zero_with_correct_attributes(self) -> None:
        emit_enum_mapping_heartbeat(
            state_code="US_XX",
            enum_cls=_FakeEnum,
            field_name="my_enum_field",
            ingest_view_name="my_ingest_view",
        )

        metric_data = cast(
            Sum,
            self.otl_mock.get_metric_data(
                metric_name=CounterInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE
            ),
        )
        data_point: NumberDataPoint = one(metric_data.data_points)

        self.assertEqual(data_point.value, 0)
        self.assertEqual(
            data_point.attributes,
            {
                AttributeKey.REGION: "US_XX",
                AttributeKey.ENUM_TYPE: "_FakeEnum",
                AttributeKey.ENUM_FIELD_NAME: "my_enum_field",
                AttributeKey.INGEST_VIEW_NAME: "my_ingest_view",
            },
        )

    def test_heartbeat_does_not_overwrite_real_counter(self) -> None:
        log_unmapped_enum(
            state_code="US_XX",
            enum_cls=_FakeEnum,
            field_name="my_field",
            ingest_view_name="my_view",
            raw_text="BAD_VALUE",
        )

        emit_enum_mapping_heartbeat(
            state_code="US_XX",
            enum_cls=_FakeEnum,
            field_name="my_field",
            ingest_view_name="my_view",
        )

        metric_data = cast(
            Sum,
            self.otl_mock.get_metric_data(
                metric_name=CounterInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE
            ),
        )
        data_point: NumberDataPoint = one(metric_data.data_points)
        self.assertEqual(data_point.value, 1)
