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
"""Tests for methods in common_utils.py."""
import datetime
import unittest

from recidiviz.common.common_utils import (
    convert_nested_dictionary_keys,
    create_generated_id,
    create_synthetic_id,
    date_intersects_with_span,
    date_spans_overlap_exclusive,
    date_spans_overlap_inclusive,
    get_external_id,
    is_generated_id,
)
from recidiviz.common.str_field_utils import snake_to_camel

_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)


class CommonUtilsTest(unittest.TestCase):
    """Tests for common_utils.py."""

    def test_create_generated_id(self) -> None:
        generated_id = create_generated_id()
        self.assertTrue(generated_id.endswith("_GENERATE"))

    def test_is_generated_id(self) -> None:
        id_str = "id_str_GENERATE"
        self.assertTrue(is_generated_id(id_str))

    def test_is_not_generated_id(self) -> None:
        id_str = "id_str"
        self.assertFalse(is_generated_id(id_str))

    def test_create_synthetic_id(self) -> None:
        id_str = "id_str"
        id_type = "id_type"
        self.assertEqual(
            "id_type:id_str", create_synthetic_id(external_id=id_str, id_type=id_type)
        )

    def test_get_external_id(self) -> None:
        synthetic_id = "id_type:11:111"
        self.assertEqual("11:111", get_external_id(synthetic_id=synthetic_id))

    def test_dateIntersectsWithSpan(self) -> None:
        self.assertFalse(
            date_intersects_with_span(
                point_in_time=_DATE_1, start_date=_DATE_2, end_date=_DATE_4
            )
        )
        self.assertTrue(
            date_intersects_with_span(
                point_in_time=_DATE_2, start_date=_DATE_2, end_date=_DATE_4
            )
        )
        self.assertTrue(
            date_intersects_with_span(
                point_in_time=_DATE_3, start_date=_DATE_2, end_date=_DATE_4
            )
        )
        self.assertFalse(
            date_intersects_with_span(
                point_in_time=_DATE_4, start_date=_DATE_2, end_date=_DATE_4
            )
        )
        self.assertFalse(
            date_intersects_with_span(
                point_in_time=_DATE_5, start_date=_DATE_2, end_date=_DATE_4
            )
        )

    def test_dateSpansOverlapExclusive(self) -> None:
        # Spans intersect partially
        self.assertTrue(
            date_spans_overlap_exclusive(
                start_1=_DATE_1, end_1=_DATE_3, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # One span completely overshadows the other
        self.assertTrue(
            date_spans_overlap_exclusive(
                start_1=_DATE_1, end_1=_DATE_5, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Single day span start date
        self.assertTrue(
            date_spans_overlap_exclusive(
                start_1=_DATE_2, end_1=_DATE_2, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Single day span middle
        self.assertTrue(
            date_spans_overlap_exclusive(
                start_1=_DATE_3, end_1=_DATE_3, start_2=_DATE_2, end_2=_DATE_4
            )
        )

        # Spans are distinct
        self.assertFalse(
            date_spans_overlap_exclusive(
                start_1=_DATE_5, end_1=_DATE_6, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Span end span start
        self.assertFalse(
            date_spans_overlap_exclusive(
                start_1=_DATE_1, end_1=_DATE_2, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Single day span end date
        self.assertFalse(
            date_spans_overlap_exclusive(
                start_1=_DATE_4, end_1=_DATE_4, start_2=_DATE_2, end_2=_DATE_4
            )
        )

    def test_dateSpansOverlapInclusive(self) -> None:
        # Spans intersect partially
        self.assertTrue(
            date_spans_overlap_inclusive(
                start_1=_DATE_1, end_1=_DATE_3, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # One span completely overshadows the other
        self.assertTrue(
            date_spans_overlap_inclusive(
                start_1=_DATE_1, end_1=_DATE_5, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Span end span start
        self.assertTrue(
            date_spans_overlap_inclusive(
                start_1=_DATE_1, end_1=_DATE_2, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Single day span start date
        self.assertTrue(
            date_spans_overlap_inclusive(
                start_1=_DATE_2, end_1=_DATE_2, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Single day span end date
        self.assertTrue(
            date_spans_overlap_inclusive(
                start_1=_DATE_4, end_1=_DATE_4, start_2=_DATE_2, end_2=_DATE_4
            )
        )
        # Single day span middle
        self.assertTrue(
            date_spans_overlap_inclusive(
                start_1=_DATE_3, end_1=_DATE_3, start_2=_DATE_2, end_2=_DATE_4
            )
        )

        # Spans are distinct
        self.assertFalse(
            date_spans_overlap_inclusive(
                start_1=_DATE_5, end_1=_DATE_6, start_2=_DATE_2, end_2=_DATE_4
            )
        )

    def test_convert_nested_dictionary_keys(self) -> None:
        data = {
            "external_id": 123,
            "nested_dict": {"where_is_my_mind": 500},
            "list_of_stuff": [
                "hello",
                123,
                {"second_key": 1, "third_key": {"fourth_key": [1, 2, 3]}},
            ],
        }
        converted_dict = convert_nested_dictionary_keys(data, snake_to_camel)
        self.assertEqual(
            {
                "externalId": 123,
                "nestedDict": {"whereIsMyMind": 500},
                "listOfStuff": [
                    "hello",
                    123,
                    {"secondKey": 1, "thirdKey": {"fourthKey": [1, 2, 3]}},
                ],
            },
            converted_dict,
        )
