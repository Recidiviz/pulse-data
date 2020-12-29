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
"""Tests for attr_validators.py."""
import datetime
from typing import Optional, List

import unittest
import attr

from recidiviz.common import attr_validators


class AttrValidatorsTest(unittest.TestCase):
    """Tests for attr_validators.py."""

    def test_str_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_str: str = attr.ib(validator=attr_validators.is_str)
            my_optional_str: Optional[str] = attr.ib(validator=attr_validators.is_opt_str, default=None)

        with self.assertRaises(TypeError) as e:
            _ = _TestClass()  # type: ignore[call-arg]

        self.assertEqual(
            "__init__() missing 1 required positional argument: 'my_required_str'",
            str(e.exception.args[0]))

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_str=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_str' must be <class 'str'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]))

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_str='foo', my_optional_str=True)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_str' must be <class 'str'> (got True that is a <class 'bool'>).",
            str(e.exception.args[0]))

        # These don't crash
        _ = _TestClass(my_required_str='foo', my_optional_str=None)
        _ = _TestClass(my_required_str='foo', my_optional_str='bar')

    def test_bool_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_bool: bool = attr.ib(validator=attr_validators.is_bool)
            my_optional_bool: Optional[bool] = attr.ib(validator=attr_validators.is_opt_bool, default=None)

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_bool=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_bool' must be <class 'bool'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]))

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_bool=True, my_optional_bool='True')  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_bool' must be <class 'bool'> (got 'True' that is a <class 'str'>).",
            str(e.exception.args[0]))

        # These don't crash
        _ = _TestClass(my_required_bool=False, my_optional_bool=None)
        _ = _TestClass(my_required_bool=True, my_optional_bool=False)

    def test_int_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_int: int = attr.ib(validator=attr_validators.is_int)
            my_optional_int: Optional[int] = attr.ib(validator=attr_validators.is_opt_int, default=None)

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_int=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_int' must be <class 'int'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]))

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_int=19, my_optional_int='True')  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_int' must be <class 'int'> (got 'True' that is a <class 'str'>).",
            str(e.exception.args[0]))

        # These don't crash
        _ = _TestClass(my_required_int=10, my_optional_int=None)
        _ = _TestClass(my_required_int=1000, my_optional_int=3000)

    def test_date_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_date: datetime.date = attr.ib(validator=attr_validators.is_date)
            my_optional_date: Optional[datetime.date] = attr.ib(validator=attr_validators.is_opt_date, default=None)

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_date=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_date' must be <class 'datetime.date'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]))

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_date=datetime.date.today(), my_optional_date='True')  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_date' must be <class 'datetime.date'> (got 'True' that is a <class 'str'>).",
            str(e.exception.args[0]))

        # These don't crash
        _ = _TestClass(my_required_date=datetime.date.today(), my_optional_date=None)
        _ = _TestClass(my_required_date=datetime.date.today(), my_optional_date=datetime.date.today())
        _ = _TestClass(my_required_date=datetime.datetime.now(), my_optional_date=datetime.datetime.now())

    def test_list_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_list: List['_TestChildClass'] = attr.ib(validator=attr_validators.is_list, factory=list)

        @attr.s
        class _TestChildClass:
            my_field: str = attr.ib(validator=attr_validators.is_str)

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_list=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_list' must be <class 'list'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]))

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_list={})  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_list' must be <class 'list'> (got {} that is a <class 'dict'>).",
            str(e.exception.args[0]))

        # These don't crash
        _ = _TestClass()
        _ = _TestClass(my_required_list=[])
        _ = _TestClass(my_required_list=[
            _TestChildClass(my_field='foobar')
        ])
