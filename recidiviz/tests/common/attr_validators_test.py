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
import re
import unittest
from typing import List, Optional

import attr
import pytz

from recidiviz.common import attr_validators


class AttrValidatorsTest(unittest.TestCase):
    """Tests for attr_validators.py."""

    def test_str_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_str: str = attr.ib(validator=attr_validators.is_str)
            my_optional_str: Optional[str] = attr.ib(
                validator=attr_validators.is_opt_str, default=None
            )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass()  # type: ignore[call-arg]

        self.assertEqual(
            "AttrValidatorsTest.test_str_validators.<locals>._TestClass.__init__() "
            "missing 1 required positional argument: 'my_required_str'",
            str(e.exception.args[0]),
        )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_str=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_str' must be <class 'str'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]),
        )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_str="foo", my_optional_str=True)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_str' must be <class 'str'> (got True that is a <class 'bool'>).",
            str(e.exception.args[0]),
        )

        # These don't crash
        _ = _TestClass(my_required_str="foo", my_optional_str=None)
        _ = _TestClass(my_required_str="foo", my_optional_str="bar")

    def test_bool_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_bool: bool = attr.ib(validator=attr_validators.is_bool)
            my_optional_bool: Optional[bool] = attr.ib(
                validator=attr_validators.is_opt_bool, default=None
            )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_bool=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_bool' must be <class 'bool'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]),
        )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_bool=True, my_optional_bool="True")  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_bool' must be <class 'bool'> (got 'True' that is a <class 'str'>).",
            str(e.exception.args[0]),
        )

        # These don't crash
        _ = _TestClass(my_required_bool=False, my_optional_bool=None)
        _ = _TestClass(my_required_bool=True, my_optional_bool=False)

    def test_int_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_int: int = attr.ib(validator=attr_validators.is_int)
            my_optional_int: Optional[int] = attr.ib(
                validator=attr_validators.is_opt_int, default=None
            )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_int=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_int' must be <class 'int'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]),
        )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_int=19, my_optional_int="True")  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_int' must be <class 'int'> (got 'True' that is a <class 'str'>).",
            str(e.exception.args[0]),
        )

        # These don't crash
        _ = _TestClass(my_required_int=10, my_optional_int=None)
        _ = _TestClass(my_required_int=1000, my_optional_int=3000)

    def test_date_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_date: datetime.date = attr.ib(validator=attr_validators.is_date)
            my_optional_date: Optional[datetime.date] = attr.ib(
                validator=attr_validators.is_opt_date, default=None
            )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_date=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_date' must be <class 'datetime.date'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]),
        )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_date=datetime.date.today(), my_optional_date="True")  # type: ignore[arg-type]

        self.assertEqual(
            "'my_optional_date' must be <class 'datetime.date'> (got 'True' that is a <class 'str'>).",
            str(e.exception.args[0]),
        )

        # These don't crash
        _ = _TestClass(my_required_date=datetime.date.today(), my_optional_date=None)
        _ = _TestClass(
            my_required_date=datetime.date.today(),
            my_optional_date=datetime.date.today(),
        )
        _ = _TestClass(
            my_required_date=datetime.datetime.now(),
            my_optional_date=datetime.datetime.now(),
        )

    def test_list_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_list: List["_TestChildClass"] = attr.ib(
                validator=attr_validators.is_list, factory=list
            )

        @attr.s
        class _TestChildClass:
            my_field: str = attr.ib(validator=attr_validators.is_str)

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_list=None)  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_list' must be <class 'list'> (got None that is a <class 'NoneType'>).",
            str(e.exception.args[0]),
        )

        with self.assertRaises(TypeError) as e:
            _ = _TestClass(my_required_list={})  # type: ignore[arg-type]

        self.assertEqual(
            "'my_required_list' must be <class 'list'> (got {} that is a <class 'dict'>).",
            str(e.exception.args[0]),
        )

        # These don't crash
        _ = _TestClass()
        _ = _TestClass(my_required_list=[])
        _ = _TestClass(my_required_list=[_TestChildClass(my_field="foobar")])

    def test_not_future_datetime_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_not_future_date: datetime.datetime = attr.ib(
                validator=attr_validators.is_not_future_datetime
            )
            my_optional_not_future_date: Optional[datetime.datetime] = attr.ib(
                validator=attr_validators.is_opt_not_future_datetime, default=None
            )

        now_aware = datetime.datetime.now(tz=pytz.UTC)
        now_naive = datetime.datetime.now()

        # Throw when in the future, timezone aware.
        with self.assertRaises(ValueError):
            future = now_aware + datetime.timedelta(days=7)
            _ = _TestClass(my_required_not_future_date=future)  # type: ignore[arg-type]

        # Throw when in the future, timezone naive.
        with self.assertRaises(ValueError):
            future = now_naive + datetime.timedelta(days=7)
            _ = _TestClass(my_required_not_future_date=future)

        # Throw when optional value is in the future.
        with self.assertRaises(ValueError):
            future = now_aware + datetime.timedelta(days=7)
            past = now_aware - datetime.timedelta(days=7)
            _ = _TestClass(
                my_required_not_future_date=past, my_optional_not_future_date=future
            )

        # Throw when optional value is in the future, timezone naive.
        with self.assertRaises(ValueError):
            future = now_naive + datetime.timedelta(days=7)
            past = now_naive - datetime.timedelta(days=7)
            _ = _TestClass(
                my_required_not_future_date=past, my_optional_not_future_date=future
            )

        # These don't crash
        ok = now_aware - datetime.timedelta(days=7)
        _ = _TestClass(my_required_not_future_date=ok, my_optional_not_future_date=ok)
        ok = now_naive - datetime.timedelta(days=7)
        _ = _TestClass(my_required_not_future_date=ok, my_optional_not_future_date=ok)

    def test_not_future_date_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_not_future_date: datetime.date = attr.ib(
                validator=attr_validators.is_not_future_date
            )
            my_optional_not_future_date: Optional[datetime.date] = attr.ib(
                validator=attr_validators.is_opt_not_future_date, default=None
            )

        today = datetime.date.today()

        # Throw when in the future, timezone aware.
        with self.assertRaises(ValueError):
            future = today + datetime.timedelta(days=7)
            _ = _TestClass(my_required_not_future_date=future)  # type: ignore[arg-type]

        # Throw when optional value is in the future.
        with self.assertRaises(ValueError):
            future = today + datetime.timedelta(days=7)
            past = today - datetime.timedelta(days=7)
            _ = _TestClass(
                my_required_not_future_date=past, my_optional_not_future_date=future
            )

        # These don't crash
        ok = today - datetime.timedelta(days=7)
        _ = _TestClass(my_required_not_future_date=ok, my_optional_not_future_date=ok)


@attr.s
class _TestEmailClass:
    """
    Used in TestEmailValidator
    """

    my_email: str = attr.ib(validator=attr_validators.is_valid_email)
    my_opt_email: Optional[str] = attr.ib(validator=attr_validators.is_opt_valid_email)


class TestEmailValidator(unittest.TestCase):
    """Tests for is_valid email and is_opt_valid_emai"""

    _TestClass: type[_TestEmailClass]
    valid_email: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.valid_email = "test@example.com"
        cls._TestClass = _TestEmailClass

    def test_email_without_at(self) -> None:
        email_value = "testexample.com"
        with self.assertRaisesRegex(
            ValueError,
            f"Incorrect format:Email field with {email_value} missing '@' symbol",
        ):
            self._TestClass(my_email=email_value, my_opt_email=None)

        # Optional value has missing @ symbol
        with self.assertRaisesRegex(
            ValueError,
            f"Incorrect format:Email field with {email_value} missing '@' symbol",
        ):
            self._TestClass(my_email=self.valid_email, my_opt_email=email_value)

    def test_email_with_mul_at(self) -> None:
        email_value = "test@example@.com"
        with self.assertRaisesRegex(
            ValueError,
            f"Incorrect format:Email field with {email_value} has more than one '@' symbol",
        ):
            self._TestClass(my_email=email_value, my_opt_email=None)

        # optional value has more than one '@' symbol
        with self.assertRaisesRegex(
            ValueError,
            f"Incorrect format:Email field with {email_value} has more than one '@' symbol",
        ):
            self._TestClass(my_email=self.valid_email, my_opt_email=email_value)

    def test_email_no_before(self) -> None:
        email_value = "@example.com"
        with self.assertRaisesRegex(
            ValueError,
            f"Incorrect format:Email field with {email_value} has no text before '@' symbol",
        ):
            self._TestClass(my_email=email_value, my_opt_email=None)

        # optional value has no text before @
        with self.assertRaisesRegex(
            ValueError,
            f"Incorrect format:Email field with {email_value} has no text before '@' symbol",
        ):
            self._TestClass(my_email=self.valid_email, my_opt_email=email_value)

    def test_email_whitespace(self) -> None:
        email_value = "test\t@example.com"  # tests an email with a tab
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format:Email field with {email_value} contains whitespace"
            ),
        ):
            self._TestClass(my_email=email_value, my_opt_email=None)

        # Optional value has whitespace
        with self.assertRaisesRegex(
            ValueError,
            f"Incorrect format:Email field with {email_value} contains whitespace",
        ):
            self._TestClass(my_email=self.valid_email, my_opt_email=email_value)

    def test_email_character(self) -> None:
        invalid_chars = r"<,<,\,:"
        email_value = r"<<test\:@example.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Email field with {email_value} contains invalid character {invalid_chars}"
            ),
        ):
            self._TestClass(my_email=email_value, my_opt_email=None)

        # Optional value has an invalid character
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Email field with {email_value} contains invalid character {invalid_chars}"
            ),
        ):
            self._TestClass(my_email=self.valid_email, my_opt_email=email_value)

    def test_email_local_part(self) -> None:
        email_value = "x@example.com"
        with self.assertRaisesRegex(
            ValueError, f"Email has a suspicious username {email_value.split('@',1)[0]}"
        ):
            self._TestClass(my_email="x@example.com", my_opt_email=None)

        # Optional value has suspicious username
        with self.assertRaisesRegex(
            ValueError, f"Email has a suspicious username {email_value.split('@',1)[0]}"
        ):
            self._TestClass(my_email="tex@example.com", my_opt_email=email_value)
