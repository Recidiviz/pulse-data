# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for serialization methods regarding entities."""
import datetime
import unittest

from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.persistence.entity.serialization import json_serializable_dict
from recidiviz.pipelines.metrics.utils.metric_utils import (
    json_serializable_list_value_handler,
)


class TestJsonSerializableDict(unittest.TestCase):
    """Tests using the json_serializable_dict function with the
    json_serializable_list_value_handler built for handling lists in metric values."""

    def test_json_serializable_dict(self) -> None:
        metric_key = {
            "gender": StateGender.MALE,
            "year": 1999,
            "month": 3,
            "state_code": "CA",
        }

        expected_output = {
            "gender": "MALE",
            "year": 1999,
            "month": 3,
            "state_code": "CA",
        }

        updated_metric_key = json_serializable_dict(
            metric_key, list_serializer=json_serializable_list_value_handler
        )

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_dict_date_handling(self) -> None:
        metric_key = {
            "date_field": datetime.date(2000, 1, 2),
            "datetime_field": datetime.datetime(2000, 1, 2, 3, 4, 5),
            "datetime_field_midnight": datetime.datetime(2000, 1, 2, 0, 0, 0, 0),
            "datetime_field_with_millis": datetime.datetime(2000, 1, 2, 3, 4, 5, 6),
            "null_date_field": None,
            "null_datetime_field": None,
        }

        expected_output = {
            "date_field": "2000-01-02",
            "datetime_field": "2000-01-02T03:04:05",
            "datetime_field_midnight": "2000-01-02T00:00:00",
            "datetime_field_with_millis": "2000-01-02T03:04:05.000006",
            "null_date_field": None,
            "null_datetime_field": None,
        }

        updated_metric_key = json_serializable_dict(
            metric_key, list_serializer=json_serializable_list_value_handler
        )

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_dict_ViolationTypeFrequencyCounter(self) -> None:
        metric_key = {
            "gender": StateGender.MALE,
            "year": 1999,
            "month": 3,
            "state_code": "CA",
            "violation_type_frequency_counter": [
                ["TECHNICAL"],
                ["ASC", "EMP", "TECHNICAL"],
            ],
        }

        expected_output = {
            "gender": "MALE",
            "year": 1999,
            "month": 3,
            "state_code": "CA",
            "violation_type_frequency_counter": "[ASC, EMP, TECHNICAL],[TECHNICAL]",
        }

        updated_metric_key = json_serializable_dict(
            metric_key, list_serializer=json_serializable_list_value_handler
        )

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_dict_InvalidList(self) -> None:
        metric_key = {"invalid_list_key": ["list", "values"]}

        with self.assertRaisesRegex(
            ValueError, "^Unexpected list in metric_key for key: invalid_list_key$"
        ):
            json_serializable_dict(
                metric_key, list_serializer=json_serializable_list_value_handler
            )
