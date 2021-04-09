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
from mock import MagicMock, call

from google.api_core import exceptions  # pylint: disable=no-name-in-module

from recidiviz.common.common_utils import (
    create_generated_id,
    is_generated_id,
    retry_grpc,
    create_synthetic_id,
    get_external_id,
    date_intersects_with_span,
    date_spans_overlap_exclusive,
    date_spans_overlap_inclusive,
)

GO_AWAY_ERROR = exceptions.InternalServerError("500 GOAWAY received")
DEADLINE_EXCEEDED_ERROR = exceptions.InternalServerError("504 Deadline " "Exceeded")
OTHER_ERROR = exceptions.InternalServerError("500 received")
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_DATE_6 = datetime.date(year=2019, month=6, day=1)
_DATE_7 = datetime.date(year=2019, month=7, day=1)


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

    def test_retry_grpc_no_raise(self) -> None:
        fn = MagicMock()
        # Two GOAWAY errors, 1 DEADLINE_EXCEEDED, then works
        fn.side_effect = [GO_AWAY_ERROR] * 2 + [DEADLINE_EXCEEDED_ERROR] + [3]  # type: ignore[list-item]

        result = retry_grpc(3, fn, 1, b=2)

        self.assertEqual(result, 3)
        fn.assert_has_calls([call(1, b=2)] * 3)

    def test_retry_grpc_raises(self) -> None:
        fn = MagicMock()
        # Always a GOAWAY error
        fn.side_effect = GO_AWAY_ERROR

        with self.assertRaises(exceptions.InternalServerError):
            retry_grpc(3, fn, 1, b=2)

        fn.assert_has_calls([call(1, b=2)] * 4)

    def test_retry_grpc_raises_no_goaway(self) -> None:
        fn = MagicMock()
        # Always a different error
        fn.side_effect = OTHER_ERROR

        with self.assertRaises(exceptions.InternalServerError):
            retry_grpc(3, fn, 1, b=2)

        fn.assert_has_calls([call(1, b=2)])

    def test_retry_grpc_raises_deadline_exceeded(self) -> None:
        fn = MagicMock()
        # Always a DEADLINE_EXCEEDED error
        fn.side_effect = DEADLINE_EXCEEDED_ERROR

        with self.assertRaises(exceptions.InternalServerError):
            retry_grpc(3, fn, 1, b=2)

        fn.assert_has_calls([call(1, b=2)] * 4)

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
