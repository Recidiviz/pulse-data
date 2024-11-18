# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for big_query_attr_validators"""
import re
import unittest
from typing import Any

import attr

from recidiviz.common import big_query_attr_validators


class BQAttrValidatorsTest(unittest.TestCase):
    """Tests for big_query_attr_validators"""

    def test_is_valid_bq_label(self) -> None:
        @attr.s
        class _TestClass:
            key: str = attr.ib(
                validator=big_query_attr_validators.is_valid_bq_label_key
            )
            value: str = attr.ib(
                validator=big_query_attr_validators.is_valid_bq_label_value
            )

        # type validation
        wrong_type: Any
        for wrong_type in [None, 1, []]:
            with self.assertRaisesRegex(
                TypeError, r"Expected \[value\] to be a string, found \[.*\]"
            ):
                _ = _TestClass(key="key", value=wrong_type)  # type: ignore[arg-type]

            with self.assertRaisesRegex(
                TypeError, r"Expected \[key\] to be a string, found \[.*\]"
            ):
                _ = _TestClass(key=wrong_type, value="value")  # type: ignore[arg-type]

        # invalid characters
        for failing_label, correct_label in [
            ("A", "a"),
            ("a..a", "a--a"),
            ("US_XX", "us_xx"),
        ]:
            with self.assertRaisesRegex(
                TypeError,
                re.escape(
                    f"[value] is not a valid big query label, found [{failing_label}]. Please change to: [{correct_label}]"
                ),
            ):
                _ = _TestClass(key="key", value=failing_label)  # type: ignore[arg-type]

            with self.assertRaisesRegex(
                TypeError,
                re.escape(
                    f"[key] is not a valid big query label, found [{failing_label}]. Please change to: [{correct_label}]"
                ),
            ):
                _ = _TestClass(key=failing_label, value="value")  # type: ignore[arg-type]

        # min length
        _ok = _TestClass(key="empty_value_ok", value="")
        with self.assertRaisesRegex(
            TypeError,
            re.escape("Expected [key] to be at least 1 character."),
        ):
            _ = _TestClass(key="", value="empty_key_not_ok")  # type: ignore[arg-type]

        # passes
        _ok = _TestClass(
            key="celebration", value="wahoooooooooooooooooooooo_____---11--_"
        )
        _ok = _TestClass(key="underscore", value="____")
        _ok = _TestClass(key="state_code", value="us_xx")
