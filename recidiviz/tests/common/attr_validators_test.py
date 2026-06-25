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
import freezegun
import pytz

from recidiviz.common import attr_validators
from recidiviz.common.attr_validators import (
    is_dict_of,
    is_list_of,
    is_not_set_along_with,
    is_set_of,
    is_tuple_of,
)


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

    def test_is_snake_case(self) -> None:
        @attr.s
        class _TestClass:
            value: str = attr.ib(validator=attr_validators.is_snake_case)

        # Valid snake_case strings don't crash.
        for valid in ["employer_name", "status", "foo2", "a_b_c"]:
            _ = _TestClass(value=valid)

        # Uppercase, leading digit, leading underscore, dashes, and empty all fail.
        for invalid in ["EmployerName", "Status", "1foo", "_foo", "foo-bar", ""]:
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    f"Field [value] must be snake_case (matching "
                    f"[^[a-z][a-z0-9_]*$]), received: [{invalid}]"
                ),
            ):
                _ = _TestClass(value=invalid)

        with self.assertRaisesRegex(
            ValueError, re.escape("Expected value type str, found <class 'int'>.")
        ):
            _ = _TestClass(value=5)  # type: ignore[arg-type]

    def test_is_upper_snake_case(self) -> None:
        @attr.s
        class _TestClass:
            value: str = attr.ib(validator=attr_validators.is_upper_snake_case)

        # Valid UPPER_SNAKE_CASE strings don't crash.
        for valid in ["EMPLOYER_NAME", "STATUS", "FOO2", "A_B_C"]:
            _ = _TestClass(value=valid)

        # Lowercase, mixed-case, leading digit, leading underscore, and empty fail.
        for invalid in ["employer_name", "Employer_Name", "1FOO", "_FOO", ""]:
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    f"Field [value] must be UPPER_SNAKE_CASE (matching "
                    f"[^[A-Z][A-Z0-9_]*$]), received: [{invalid}]"
                ),
            ):
                _ = _TestClass(value=invalid)

        with self.assertRaisesRegex(
            ValueError, re.escape("Expected value type str, found <class 'int'>.")
        ):
            _ = _TestClass(value=5)  # type: ignore[arg-type]

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

    def test_is_int_strict(self) -> None:
        @attr.s
        class _TestClass:
            my_strict_int: int = attr.ib(validator=attr_validators.is_int_strict)

        with self.assertRaises(ValueError) as e:
            _ = _TestClass(my_strict_int=True)

        self.assertEqual(
            "Field [my_strict_int] on [_TestClass] must be an int. Found value "
            "[True] of type [<class 'bool'>].",
            str(e.exception.args[0]),
        )

        with self.assertRaises(ValueError) as e:
            _ = _TestClass(my_strict_int=1.5)  # type: ignore[arg-type]

        self.assertEqual(
            "Field [my_strict_int] on [_TestClass] must be an int. Found value "
            "[1.5] of type [<class 'float'>].",
            str(e.exception.args[0]),
        )

        with self.assertRaises(ValueError):
            _ = _TestClass(my_strict_int="3")  # type: ignore[arg-type]

        # These don't crash
        _ = _TestClass(my_strict_int=0)
        _ = _TestClass(my_strict_int=-10)

    def test_is_numerical_strict(self) -> None:
        @attr.s
        class _TestClass:
            my_strict_numerical: float = attr.ib(
                validator=attr_validators.is_numerical_strict
            )

        with self.assertRaises(ValueError) as e:
            _ = _TestClass(my_strict_numerical=True)

        self.assertEqual(
            "Field [my_strict_numerical] on [_TestClass] must be an int or "
            "float. Found value [True] of type [<class 'bool'>].",
            str(e.exception.args[0]),
        )

        with self.assertRaises(ValueError):
            _ = _TestClass(my_strict_numerical="1.5")  # type: ignore[arg-type]

        # These don't crash — both ints and floats are numerical
        _ = _TestClass(my_strict_numerical=1.5)
        _ = _TestClass(my_strict_numerical=2)
        _ = _TestClass(my_strict_numerical=-0.5)

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

        with self.assertRaisesRegex(
            TypeError,
            r"Found datetime value \[2020-01-02 03:04:05.000006\] on field "
            r"\[my_required_date\] on class \[_TestClass\].",
        ):
            _ = _TestClass(
                my_required_date=datetime.datetime(2020, 1, 2, 3, 4, 5, 6),
                my_optional_date=None,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found datetime value \[2020-01-02 03:04:05.000006\] on field "
            r"\[my_optional_date\] on class \[_TestClass\].",
        ):
            _ = _TestClass(
                my_required_date=datetime.date.today(),
                my_optional_date=datetime.datetime(2020, 1, 2, 3, 4, 5, 6),
            )

        # These don't crash
        _ = _TestClass(my_required_date=datetime.date.today(), my_optional_date=None)
        _ = _TestClass(
            my_required_date=datetime.date.today(),
            my_optional_date=datetime.date.today(),
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

    def test_is_utc_timezone_aware_datetimes_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_utc_aware_datetime: datetime.date = attr.ib(
                validator=attr_validators.is_utc_timezone_aware_datetime
            )
            my_optional_utc_aware_datetime: Optional[datetime.date] = attr.ib(
                validator=attr_validators.is_opt_utc_timezone_aware_datetime,
                default=None,
            )

        utc_tz = datetime.datetime.now(tz=datetime.UTC)
        non_utc = datetime.datetime.now(tz=datetime.timezone.max)

        # throw when non utc timezone
        with self.assertRaises(ValueError):
            _ = _TestClass(my_required_utc_aware_datetime=non_utc)

        with self.assertRaises(ValueError):
            _ = _TestClass(
                my_required_utc_aware_datetime=utc_tz,
                my_optional_utc_aware_datetime=non_utc,
            )

        no_tz = datetime.datetime.now(tz=None)

        # throw when no timezone
        with self.assertRaises(ValueError):
            _ = _TestClass(my_required_utc_aware_datetime=no_tz)

        with self.assertRaises(ValueError):
            _ = _TestClass(
                my_required_utc_aware_datetime=utc_tz,
                my_optional_utc_aware_datetime=no_tz,
            )

        # throw when not a date
        with self.assertRaises(ValueError):
            _ = _TestClass(my_required_utc_aware_datetime=1)  # type: ignore[arg-type]

        with self.assertRaises(ValueError):
            _ = _TestClass(
                my_required_utc_aware_datetime=utc_tz,
                my_optional_utc_aware_datetime=1,  # type: ignore[arg-type]
            )

        # doesn't crash
        _ok = _TestClass(
            my_required_utc_aware_datetime=utc_tz, my_optional_utc_aware_datetime=utc_tz
        )

    def test_is_positive_integer_validators(self) -> None:
        @attr.s
        class _TestClass:
            my_required_int: int = attr.ib(validator=attr_validators.is_positive_int)
            my_optional_int: Optional[int] = attr.ib(
                validator=attr_validators.is_opt_positive_int, default=None
            )

        with self.assertRaisesRegex(
            TypeError,
            r"'my_required_int' must be <class 'int'> "
            r"\(got None that is a <class 'NoneType'>\).",
        ):
            _ = _TestClass(my_required_int=None)  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            TypeError,
            r"'my_optional_int' must be <class 'int'> "
            r"\(got 'True' that is a <class 'str'>\).",
        ):
            _ = _TestClass(my_required_int=19, my_optional_int="True")  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            ValueError,
            r"Field \[my_required_int\] on \[_TestClass\] must be a positive integer. "
            r"Found value \[-1\]",
        ):
            _ = _TestClass(my_required_int=-1)

        with self.assertRaisesRegex(
            ValueError,
            r"Field \[my_required_int\] on \[_TestClass\] must be a positive integer. "
            r"Found value \[0\]",
        ):
            _ = _TestClass(my_required_int=0)

        with self.assertRaisesRegex(
            ValueError,
            r"Field \[my_optional_int\] on \[_TestClass\] must be a positive integer. "
            r"Found value \[-1\]",
        ):
            _ = _TestClass(my_required_int=1, my_optional_int=-1)

        with self.assertRaisesRegex(
            ValueError,
            r"Field \[my_optional_int\] on \[_TestClass\] must be a positive integer. "
            r"Found value \[0\]",
        ):
            _ = _TestClass(my_required_int=1, my_optional_int=0)

        # These don't crash
        _ = _TestClass(my_required_int=1, my_optional_int=None)
        _ = _TestClass(my_required_int=1000, my_optional_int=3000)

    def test_is_non_negative_integer_validator(self) -> None:
        @attr.s
        class _TestClass:
            my_int: int = attr.ib(validator=attr_validators.is_non_negative_int)

        with self.assertRaisesRegex(
            TypeError,
            r"'my_int' must be <class 'int'> "
            r"\(got None that is a <class 'NoneType'>\).",
        ):
            _ = _TestClass(my_int=None)  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            ValueError,
            r"Field \[my_int\] on \[_TestClass\] must be a non-negative integer. "
            r"Found value \[-1\]",
        ):
            _ = _TestClass(my_int=-1)

        # Zero and positive values do not crash.
        _ = _TestClass(my_int=0)
        _ = _TestClass(my_int=1000)

    def test_is_subclass_of(self) -> None:
        class _Base:
            pass

        class _Child(_Base):
            pass

        class _Unrelated:
            pass

        @attr.s
        class _TestClass:
            my_type: type = attr.ib(validator=attr_validators.is_subclass_of(_Base))

        # The class itself and subclasses do not crash.
        _ = _TestClass(my_type=_Base)
        _ = _TestClass(my_type=_Child)

        with self.assertRaisesRegex(
            ValueError,
            r"\[my_type\] .* which is not a subclass of \[_Base\]: \[<class .*_Unrelated'>\].",
        ):
            _ = _TestClass(my_type=_Unrelated)

        # An instance (not a class) is rejected.
        with self.assertRaisesRegex(
            ValueError,
            r"\[my_type\] .* which is not a subclass of \[_Base\]:",
        ):
            _ = _TestClass(my_type=_Child())  # type: ignore[arg-type]

        # A non-type value is rejected.
        with self.assertRaisesRegex(
            ValueError,
            r"\[my_type\] .* which is not a subclass of \[_Base\]:",
        ):
            _ = _TestClass(my_type="not a type")  # type: ignore[arg-type]

    def test_is_list_of_non_empty_str(self) -> None:
        @attr.s
        class _TestClass:
            my_list: list = attr.ib(validator=attr_validators.is_list_of_non_empty_str)

        # Empty list and lists of non-empty strings do not crash.
        _ = _TestClass(my_list=[])
        _ = _TestClass(my_list=["a", "b"])

        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            _ = _TestClass(my_list=["a", ""])

        with self.assertRaisesRegex(ValueError, "Expected value type str"):
            _ = _TestClass(my_list=["a", 1])

        with self.assertRaises((TypeError, ValueError)):
            _ = _TestClass(my_list="not a list")  # type: ignore[arg-type]

    def test_is_list_where_each(self) -> None:
        @attr.s
        class _SingleValidatorClass:
            my_list: list = attr.ib(
                validator=attr_validators.is_list_where_each(
                    attr_validators.is_non_empty_str
                )
            )

        _ = _SingleValidatorClass(my_list=["a", "b"])
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            _ = _SingleValidatorClass(my_list=[""])

        # A sequence of member validators is combined — all must pass per element.
        @attr.s
        class _MultiValidatorClass:
            my_list: list = attr.ib(
                validator=attr_validators.is_list_where_each(
                    [attr_validators.is_non_empty_str, attr_validators.is_snake_case]
                )
            )

        _ = _MultiValidatorClass(my_list=["foo_bar", "baz"])
        # Fails the non-empty validator.
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            _ = _MultiValidatorClass(my_list=[""])
        # Fails the snake_case validator.
        with self.assertRaisesRegex(ValueError, "must be snake_case"):
            _ = _MultiValidatorClass(my_list=["NotSnake"])

    def test_is_dict_where_each(self) -> None:
        @attr.s
        class _TestClass:
            my_dict: dict = attr.ib(
                validator=attr_validators.is_dict_where_each(
                    key_validator=attr_validators.is_str,
                    value_validator=attr_validators.is_non_empty_str,
                )
            )

        _ = _TestClass(my_dict={"a": "x"})

        # Bad value.
        with self.assertRaisesRegex(ValueError, "String value should not be empty."):
            _ = _TestClass(my_dict={"a": ""})

        # Bad key.
        with self.assertRaises((TypeError, ValueError)):
            _ = _TestClass(my_dict={1: "x"})  # type: ignore[dict-item]


@attr.s(frozen=True)
class _TestEmailClass:
    """
    Used in TestEmailValidator
    """

    my_email: str = attr.ib(validator=attr_validators.is_valid_email_legacy)
    my_opt_email: Optional[str] = attr.ib(
        validator=attr_validators.is_opt_valid_email_legacy
    )


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


@attr.s(frozen=True)
class _TestNewEmailClass:
    """
    Used in TestNewEmailValidator for testing the new is_valid_email / is_opt_valid_email validators.
    """

    my_email: str = attr.ib(validator=attr_validators.is_valid_email)
    my_opt_email: str | None = attr.ib(validator=attr_validators.is_opt_valid_email)


class TestNewEmailValidator(unittest.TestCase):
    """Tests for the new stricter is_valid_email and is_opt_valid_email validators"""

    valid_email: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.valid_email = "valid@example.com"

    def test_valid_email(self) -> None:
        """Test that valid emails pass validation."""
        instance = _TestNewEmailClass(
            my_email=self.valid_email, my_opt_email="another@test.org"
        )
        self.assertEqual(instance.my_email, self.valid_email)
        self.assertEqual(instance.my_opt_email, "another@test.org")

    def test_optional_none(self) -> None:
        """Test that None is valid for optional email field."""
        instance = _TestNewEmailClass(my_email=self.valid_email, my_opt_email=None)
        self.assertIsNone(instance.my_opt_email)

    def test_email_without_at(self) -> None:
        """Test that email without @ symbol fails."""
        email_value = "testexample.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]: missing '@' symbol"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

        # Optional value missing @
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_opt_email] value [{email_value}] on "
                f"[_TestNewEmailClass]: missing '@' symbol"
            ),
        ):
            _TestNewEmailClass(my_email=self.valid_email, my_opt_email=email_value)

    def test_email_with_multiple_at(self) -> None:
        """Test that email with more than one @ symbol fails."""
        email_value = "test@example@.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: more than one '@' symbol"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_no_text_before_at(self) -> None:
        """Test that email with no text before @ fails."""
        email_value = "@example.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: has no text before '@' symbol"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_no_text_after_at(self) -> None:
        """Test that email with no text after @ fails."""
        email_value = "test@"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: has no text after '@' symbol"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_no_dot_in_domain(self) -> None:
        """Test that email with no . in domain fails."""
        email_value = "test@examplecom"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: has no '.' in domain"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_domain_starts_with_dot(self) -> None:
        """Test that email with domain starting with . fails."""
        email_value = "test@.example.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: domain starts with '.'"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_domain_ends_with_dot(self) -> None:
        """Test that email with domain ending with . fails."""
        email_value = "test@example.com."
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: domain ends with '.'"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_with_whitespace(self) -> None:
        """Test that email with whitespace fails."""
        email_value = "test @example.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: contains whitespace"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

        # Test with tab character
        email_value_tab = "test\t@example.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value_tab}] on "
                f"[_TestNewEmailClass]:: contains whitespace"
            ),
        ):
            _TestNewEmailClass(my_email=email_value_tab, my_opt_email=None)

    def test_email_with_invalid_characters(self) -> None:
        """Test that email with invalid characters fails."""
        email_value = r"<<test\:@example.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Email field with {email_value} contains invalid character"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_suspicious_usernames(self) -> None:
        """Test that emails with suspicious usernames fail."""
        suspicious_usernames = ["x", "unknown", "none", "noname", "nobody", "test"]
        for username in suspicious_usernames:
            email_value = f"{username}@example.com"
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    f"Incorrectly formatted [my_email] value [{email_value}] on "
                    f"[_TestNewEmailClass]:: has a suspicious username {username}"
                ),
            ):
                _TestNewEmailClass(my_email=email_value, my_opt_email=None)

    def test_email_suspicious_username_case_insensitive(self) -> None:
        """Test that suspicious username check is case insensitive."""
        email_value = "UNKNOWN@example.com"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrectly formatted [my_email] value [{email_value}] on "
                f"[_TestNewEmailClass]:: has a suspicious username UNKNOWN"
            ),
        ):
            _TestNewEmailClass(my_email=email_value, my_opt_email=None)


@attr.s(frozen=True)
class _TestPhoneNumberClass:
    """
    Used in TestPhoneNumberValidator
    """

    my_phone: str = attr.ib(validator=attr_validators.is_valid_phone_number)
    my_opt_phone: Optional[str] = attr.ib(
        validator=attr_validators.is_opt_valid_phone_number
    )


class TestPhoneNumberValidator(unittest.TestCase):
    """Tests for is_valid_phone_number and is_opt_valid_phone_number"""

    _TestClass: type[_TestPhoneNumberClass]

    @classmethod
    def setUpClass(cls) -> None:
        cls._TestClass = _TestPhoneNumberClass

    def test_valid_phone_10_digits(self) -> None:
        """Test that a 10-digit phone number is valid."""
        valid_phone = "2345678901"
        instance = self._TestClass(my_phone=valid_phone, my_opt_phone=None)
        self.assertEqual(instance.my_phone, valid_phone)

    def test_valid_phone_11_digits_with_1(self) -> None:
        """Test that an 11-digit phone number starting with 1 is valid."""
        valid_phone = "12345678901"
        instance = self._TestClass(my_phone=valid_phone, my_opt_phone=None)
        self.assertEqual(instance.my_phone, valid_phone)

    def test_invalid_phone_11_digits_not_starting_with_1(self) -> None:
        """Test that an 11-digit phone number not starting with 1 is invalid."""
        invalid_phone = "22345678901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_with_plus(self) -> None:
        """Test that a phone number with +1 prefix is invalid."""
        invalid_phone = "+12345678901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_with_formatting(self) -> None:
        """Test that a formatted phone number (with parens and dashes) is invalid."""
        invalid_phone = "(234) 567-8901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_with_dashes(self) -> None:
        """Test that a phone number with dashes is invalid."""
        invalid_phone = "234-567-8901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_with_dots(self) -> None:
        """Test that a phone number with dots is invalid."""
        invalid_phone = "234.567.8901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_with_spaces(self) -> None:
        """Test that a phone number with spaces is invalid."""
        invalid_phone = "234 567 8901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_too_few_digits(self) -> None:
        """Test that a phone number with too few digits is invalid."""
        invalid_phone = "123456789"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_too_many_digits(self) -> None:
        """Test that a phone number with too many digits is invalid."""
        invalid_phone = "123456789012"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_empty_string(self) -> None:
        """Test that an empty string is invalid."""
        invalid_phone = ""
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_area_code_ending_in_11(self) -> None:
        """Test that a phone number with area code ending in 11 is invalid."""
        invalid_phone = "2115678901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} has invalid area code"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_exchange_code_ending_in_11(self) -> None:
        """Test that a phone number with exchange code ending in 11 is invalid."""
        invalid_phone = "2345118901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} has invalid exchange code"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_both_area_and_exchange_ending_in_11(self) -> None:
        """Test that a phone number with both area and exchange ending in 11 is invalid (area code checked first)."""
        invalid_phone = "2115118901"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} has invalid area code"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_11_digits_area_code_ending_in_11(self) -> None:
        """Test that 11-digit phone with area code ending in 11 is rejected."""
        invalid_phone = "12115678901"  # 1 + area code 211
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} has invalid area code"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_invalid_phone_11_digits_exchange_code_ending_in_11(self) -> None:
        """Test that 11-digit phone with exchange code ending in 11 is rejected."""
        invalid_phone = "12345118901"  # 1 + valid area + exchange ending in 11
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} has invalid exchange code"
            ),
        ):
            self._TestClass(my_phone=invalid_phone, my_opt_phone=None)

    def test_validator_does_not_enforce_full_nanp_first_digit_rules(self) -> None:
        """Test that the validator does NOT enforce NANP N=2-9 rules for first digits.

        According to NANP, area code and exchange code first digits should be 2-9,
        but the current implementation only validates:
        - Length is exactly 10 or 11 digits
        - If 11 digits, first digit is '1'
        - Area code doesn't end in '11'
        - Exchange code doesn't end in '11'

        This test documents that phones with 0 or 1 as first digits are accepted.
        """
        phones_violating_nanp_but_accepted = [
            "0125678901",  # Area code 012 (first digit 0, not 2-9)
            "2340128901",  # Exchange code 012 (first digit 0, not 2-9)
        ]
        for phone in phones_violating_nanp_but_accepted:
            instance = self._TestClass(my_phone=phone, my_opt_phone=None)
            self.assertEqual(instance.my_phone, phone)

    def test_optional_phone_with_none(self) -> None:
        """Test that None is valid for optional phone number."""
        valid_phone = "2345678901"
        instance = self._TestClass(my_phone=valid_phone, my_opt_phone=None)
        self.assertIsNone(instance.my_opt_phone)

    def test_optional_phone_with_valid_value(self) -> None:
        """Test that a valid phone number is accepted for optional field."""
        required_phone = "2345678901"
        optional_phone = "9876543210"
        instance = self._TestClass(my_phone=required_phone, my_opt_phone=optional_phone)
        self.assertEqual(instance.my_opt_phone, optional_phone)

    def test_optional_phone_with_invalid_value(self) -> None:
        """Test that an invalid phone number raises error for optional field."""
        required_phone = "2345678901"
        invalid_phone = "12345"
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                f"Incorrect format: Phone number field with {invalid_phone} is not valid"
            ),
        ):
            self._TestClass(my_phone=required_phone, my_opt_phone=invalid_phone)


class TestIsListOfValidator(unittest.TestCase):
    """Tests for the is_list_of() validator."""

    @attr.define
    class TestClass:
        list_of_str_field: List[str] = attr.ib(validator=is_list_of(str))
        list_of_class_field: List[_TestEmailClass] = attr.ib(
            validator=is_list_of(_TestEmailClass)
        )

    def test_list_of_validator_correct_values(self) -> None:
        _ = self.TestClass(
            list_of_str_field=["a", "b"],
            list_of_class_field=[
                _TestEmailClass(my_email="valid@example.com", my_opt_email=None)
            ],
        )

    def test_list_of_validator_empty_lists(self) -> None:
        _ = self.TestClass(list_of_str_field=[], list_of_class_field=[])

    def test_list_of_validator_bad_types(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found item in list type field [list_of_str_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsListOfValidator.TestClass'>] "
                "which is not the expected type [<class 'str'>]: <class 'int'>",
            ),
        ):
            _ = self.TestClass(
                list_of_str_field=[1],  # type: ignore[list-item]
                list_of_class_field=[],
            )

    def test_list_of_validator_none_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found item in list type field [list_of_class_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsListOfValidator.TestClass'>] "
                "which is not the expected type "
                "[<class 'recidiviz.tests.common.attr_validators_test._TestEmailClass'>]: "
                "<class 'NoneType'>",
            ),
        ):
            _ = self.TestClass(
                list_of_str_field=[],
                list_of_class_field=[None],  # type: ignore[list-item]
            )


class TestIsTupleOfValidator(unittest.TestCase):
    """Tests for the is_tuple_of() validator."""

    @attr.define
    class TestClass:
        tuple_of_str_field: tuple[str, ...] = attr.ib(validator=is_tuple_of(str))
        tuple_of_class_field: tuple[_TestEmailClass, ...] = attr.ib(
            validator=is_tuple_of(_TestEmailClass)
        )

    def test_tuple_of_validator_correct_values(self) -> None:
        _ = self.TestClass(
            tuple_of_str_field=("a", "b"),
            tuple_of_class_field=(
                _TestEmailClass(my_email="valid@example.com", my_opt_email=None),
            ),
        )

    def test_tuple_of_validator_empty_tuples(self) -> None:
        _ = self.TestClass(tuple_of_str_field=(), tuple_of_class_field=())

    def test_tuple_of_validator_bad_types(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found item in tuple type field [tuple_of_str_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsTupleOfValidator.TestClass'>] "
                "which is not the expected type [<class 'str'>]: <class 'int'>",
            ),
        ):
            _ = self.TestClass(
                tuple_of_str_field=(1,),  # type: ignore[arg-type]
                tuple_of_class_field=(),
            )

    def test_tuple_of_validator_none_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found item in tuple type field [tuple_of_class_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsTupleOfValidator.TestClass'>] "
                "which is not the expected type "
                "[<class 'recidiviz.tests.common.attr_validators_test._TestEmailClass'>]: "
                "<class 'NoneType'>",
            ),
        ):
            _ = self.TestClass(
                tuple_of_str_field=(),
                tuple_of_class_field=(None,),  # type: ignore[arg-type]
            )

    def test_tuple_of_validator_non_tuple_input(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found value for tuple type field [tuple_of_str_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsTupleOfValidator.TestClass'>] "
                "which has non-tuple type [<class 'list'>]."
            ),
        ):
            _ = self.TestClass(
                tuple_of_str_field=["a"],  # type: ignore[arg-type]
                tuple_of_class_field=(),
            )


class TestIsSetOfValidator(unittest.TestCase):
    """Tests for the is_set_of() validator."""

    @attr.define
    class TestClass:
        set_of_str_field: set[str] = attr.ib(validator=is_set_of(str))
        set_of_class_field: set[_TestEmailClass] = attr.ib(
            validator=is_set_of(_TestEmailClass)
        )

    def test_set_of_validator_correct_values(self) -> None:
        _ = self.TestClass(
            set_of_str_field={"a", "b"},
            set_of_class_field={
                _TestEmailClass(my_email="valid@example.com", my_opt_email=None)
            },
        )

    def test_set_of_validator_empty_lists(self) -> None:
        _ = self.TestClass(set_of_str_field=set(), set_of_class_field=set())

    def test_set_of_validator_bad_types(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found item in set type field [set_of_str_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsSetOfValidator.TestClass'>] "
                "which is not the expected type [<class 'str'>]: <class 'int'>",
            ),
        ):
            _ = self.TestClass(
                set_of_str_field={1},  # type: ignore[arg-type]
                set_of_class_field=set(),
            )

    def test_set_of_validator_none_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found item in set type field [set_of_class_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsSetOfValidator.TestClass'>] "
                "which is not the expected type "
                "[<class 'recidiviz.tests.common.attr_validators_test._TestEmailClass'>]: "
                "<class 'NoneType'>",
            ),
        ):
            _ = self.TestClass(
                set_of_str_field=set(),
                set_of_class_field={None},  # type: ignore[arg-type]
            )


class TestIsDictOfValidator(unittest.TestCase):
    """Tests for the is_dict_of() validator."""

    @attr.define
    class TestClass:
        dict_of_str_to_int_field: dict[str, int] = attr.ib(
            validator=is_dict_of(str, int)
        )
        dict_of_str_to_class_field: dict[str, _TestEmailClass] = attr.ib(
            validator=is_dict_of(str, _TestEmailClass)
        )

    def test_dict_of_validator_correct_values(self) -> None:
        _ = self.TestClass(
            dict_of_str_to_int_field={"a": 1, "b": 2},
            dict_of_str_to_class_field={
                "a": _TestEmailClass(my_email="valid@example.com", my_opt_email=None)
            },
        )

    def test_dict_of_validator_empty_dicts(self) -> None:
        _ = self.TestClass(dict_of_str_to_int_field={}, dict_of_str_to_class_field={})

    def test_dict_of_validator_non_dict(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found value for dict type field [dict_of_str_to_int_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsDictOfValidator.TestClass'>] "
                "which has non-dict type [<class 'list'>].",
            ),
        ):
            _ = self.TestClass(
                dict_of_str_to_int_field=[],  # type: ignore[arg-type]
                dict_of_str_to_class_field={},
            )

    def test_dict_of_validator_bad_key_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found key in dict type field [dict_of_str_to_int_field] on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsDictOfValidator.TestClass'>] "
                "which is not the expected type [<class 'str'>]: <class 'int'>",
            ),
        ):
            _ = self.TestClass(
                dict_of_str_to_int_field={1: 1},  # type: ignore[dict-item]
                dict_of_str_to_class_field={},
            )

    def test_dict_of_validator_bad_value_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Found value for key [a] in dict type field [dict_of_str_to_int_field] "
                "on class "
                "[<class 'recidiviz.tests.common.attr_validators_test.TestIsDictOfValidator.TestClass'>] "
                "which is not the expected type [<class 'int'>]: <class 'str'>",
            ),
        ):
            _ = self.TestClass(
                dict_of_str_to_int_field={"a": "1"},  # type: ignore[dict-item]
                dict_of_str_to_class_field={},
            )


class TestIsReasonableDateValidator(unittest.TestCase):
    """Tests for the IsReasonable*Date* validators"""

    # Freeze time to midnight, UTC
    @freezegun.freeze_time("2022-02-02T00:00:00Z")
    def test_non_optional_fields(self) -> None:

        lower_bound_date = datetime.date(2011, 1, 1)
        upper_bound_date = datetime.date(2033, 3, 3)
        reasonable_past_date = datetime.date(2012, 1, 1)
        reasonable_future_date = datetime.date(2030, 1, 1)

        lower_bound_datetime = datetime.datetime(2011, 1, 1)
        reasonable_past_datetime = datetime.datetime(2012, 1, 1)

        @attr.s
        class _TestClass:
            my_required_date: datetime.date = attr.ib(
                validator=attr_validators.is_reasonable_date(
                    min_allowed_date_inclusive=lower_bound_date,
                    max_allowed_date_exclusive=upper_bound_date,
                )
            )
            my_required_past_date: datetime.date = attr.ib(
                validator=attr_validators.is_reasonable_past_date(
                    min_allowed_date_inclusive=lower_bound_date
                )
            )
            my_required_past_datetime: datetime.date = attr.ib(
                validator=attr_validators.is_reasonable_past_datetime(
                    min_allowed_datetime_inclusive=lower_bound_datetime
                )
            )

        # Tests that we fail for None values
        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_required_date\] value on class \[_TestClass\] with unexpected "
            r"type \[NoneType\]: None",
        ):
            _ = _TestClass(
                my_required_date=None,  # type: ignore[arg-type]
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_required_past_date\] value on class \[_TestClass\] with "
            r"unexpected type \[NoneType\]: None",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=None,  # type: ignore[arg-type]
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_required_past_datetime\] value on class \[_TestClass\] with "
            r"unexpected type \[NoneType\]: None",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=None,  # type: ignore[arg-type]
            )

        # Tests that we fail for non-date values
        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_required_date\] value on class \[_TestClass\] with unexpected "
            r"type \[bool\]: True",
        ):
            _ = _TestClass(
                my_required_date=True,  # type: ignore[arg-type]
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_required_past_date\] value on class \[_TestClass\] with "
            r"unexpected type \[bool\]: True",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=True,  # type: ignore[arg-type]
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_required_past_datetime\] value on class \[_TestClass\] with "
            r"unexpected type \[bool\]: True",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=True,  # type: ignore[arg-type]
            )

        # Tests that we fail for bad lower bound dates
        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_required_date\] value on class \[_TestClass\] with value "
            r"\[2010-12-31\] which is less than \[2011-01-01\], the \(inclusive\) min "
            r"allowed date.",
        ):
            _ = _TestClass(
                my_required_date=(lower_bound_date - datetime.timedelta(days=1)),
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_required_past_date\] value on class \[_TestClass\] with value "
            r"\[2010-12-31\] which is less than \[2011-01-01\], the \(inclusive\) min "
            r"allowed date.",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=(lower_bound_date - datetime.timedelta(days=1)),
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_required_past_datetime\] value on class \[_TestClass\] with "
            r"value \[2010-12-31T00:00:00\] which is less than "
            r"\[2011-01-01T00:00:00\], the \(inclusive\) min allowed date.",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=(
                    lower_bound_datetime - datetime.timedelta(days=1)
                ),
            )

        # Tests that we fail for bad upper bound dates
        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_required_date\] value on class \[_TestClass\] with value "
            r"\[2033-03-03\] which is greater than or equal to \[2033-03-03\], the "
            r"\(exclusive\) max allowed date.",
        ):
            _ = _TestClass(
                my_required_date=upper_bound_date,
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_required_past_date\] value on class \[_TestClass\] with value "
            r"\[2022-02-03\] which is greater than or equal to \[2022-02-03\], the "
            r"\(exclusive\) max allowed date.",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=(
                    datetime.date.today() + datetime.timedelta(days=1)
                ),
                my_required_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_required_past_datetime\] value on class \[_TestClass\] with "
            r"value \[2022-02-12T00:00:00\] which is greater than or equal to "
            r"\[2022-02-02T00:00:00\], the \(exclusive\) max allowed date.",
        ):
            _ = _TestClass(
                my_required_date=reasonable_past_date,
                my_required_past_date=reasonable_past_date,
                my_required_past_datetime=(
                    datetime.datetime.now() + datetime.timedelta(days=10)
                ),
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found datetime value \[2011-01-01 00:00:00\] on field "
            r"\[my_required_date\] on class \[_TestClass\]",
        ):
            _ = _TestClass(
                my_required_date=lower_bound_datetime,
                my_required_past_date=lower_bound_date,
                my_required_past_datetime=lower_bound_datetime,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found datetime value \[2011-01-01 00:00:00\] on field "
            r"\[my_required_past_date\] on class \[_TestClass\]",
        ):
            _ = _TestClass(
                my_required_date=lower_bound_date,
                my_required_past_date=lower_bound_datetime,
                my_required_past_datetime=lower_bound_datetime,
            )

        # These don't crash
        _ = _TestClass(
            my_required_date=lower_bound_date,
            my_required_past_date=lower_bound_date,
            my_required_past_datetime=lower_bound_datetime,
        )

        _ = _TestClass(
            my_required_date=upper_bound_date - datetime.timedelta(days=1),
            my_required_past_date=datetime.date.today(),
            my_required_past_datetime=(
                datetime.datetime.now() - datetime.timedelta(seconds=1)
            ),
        )

        _ = _TestClass(
            my_required_date=reasonable_future_date,
            my_required_past_date=reasonable_past_date,
            my_required_past_datetime=reasonable_past_datetime,
        )

    # Freeze time to midnight, UTC
    @freezegun.freeze_time("2022-02-02T00:00:00Z")
    def test_optional_fields(self) -> None:

        lower_bound_date = datetime.date(2011, 1, 1)
        upper_bound_date = datetime.date(2033, 3, 3)
        reasonable_past_date = datetime.date(2012, 1, 1)
        reasonable_future_date = datetime.date(2030, 1, 1)

        lower_bound_datetime = datetime.datetime(2011, 1, 1)
        reasonable_past_datetime = datetime.datetime(2012, 1, 1)

        @attr.s
        class _TestClass:
            my_optional_date: datetime.date | None = attr.ib(
                validator=attr_validators.is_opt_reasonable_date(
                    min_allowed_date_inclusive=lower_bound_date,
                    max_allowed_date_exclusive=upper_bound_date,
                )
            )
            my_optional_past_date: datetime.date | None = attr.ib(
                validator=attr_validators.is_opt_reasonable_past_date(
                    min_allowed_date_inclusive=lower_bound_date
                )
            )
            my_optional_past_datetime: datetime.date | None = attr.ib(
                validator=attr_validators.is_opt_reasonable_past_datetime(
                    min_allowed_datetime_inclusive=lower_bound_datetime
                )
            )

        # Tests that we do not fail for None values
        _ = _TestClass(
            my_optional_date=None,
            my_optional_past_date=reasonable_past_date,
            my_optional_past_datetime=reasonable_past_datetime,
        )

        _ = _TestClass(
            my_optional_date=reasonable_past_date,
            my_optional_past_date=None,
            my_optional_past_datetime=reasonable_past_datetime,
        )

        _ = _TestClass(
            my_optional_date=reasonable_past_date,
            my_optional_past_date=reasonable_past_date,
            my_optional_past_datetime=None,
        )

        # Tests that we fail for non-date values
        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_optional_date\] value on class \[_TestClass\] with unexpected "
            r"type \[bool\]: True",
        ):
            _ = _TestClass(
                my_optional_date=True,  # type: ignore[arg-type]
                my_optional_past_date=reasonable_past_date,
                my_optional_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_optional_past_date\] value on class \[_TestClass\] with "
            r"unexpected type \[bool\]: True",
        ):
            _ = _TestClass(
                my_optional_date=reasonable_past_date,
                my_optional_past_date=True,  # type: ignore[arg-type]
                my_optional_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            TypeError,
            r"Found \[my_optional_past_datetime\] value on class \[_TestClass\] with "
            r"unexpected type \[bool\]: True",
        ):
            _ = _TestClass(
                my_optional_date=reasonable_past_date,
                my_optional_past_date=reasonable_past_date,
                my_optional_past_datetime=True,  # type: ignore[arg-type]
            )

        # Tests that we fail for bad lower bound dates
        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_optional_date\] value on class \[_TestClass\] with value "
            r"\[2010-12-31\] which is less than \[2011-01-01\], the \(inclusive\) min "
            r"allowed date.",
        ):
            _ = _TestClass(
                my_optional_date=(lower_bound_date - datetime.timedelta(days=1)),
                my_optional_past_date=reasonable_past_date,
                my_optional_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_optional_past_date\] value on class \[_TestClass\] with value "
            r"\[2010-12-31\] which is less than \[2011-01-01\], the \(inclusive\) min "
            r"allowed date.",
        ):
            _ = _TestClass(
                my_optional_date=reasonable_past_date,
                my_optional_past_date=(lower_bound_date - datetime.timedelta(days=1)),
                my_optional_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_optional_past_datetime\] value on class \[_TestClass\] with "
            r"value \[2010-12-31T00:00:00\] which is less than "
            r"\[2011-01-01T00:00:00\], the \(inclusive\) min allowed date.",
        ):
            _ = _TestClass(
                my_optional_date=reasonable_past_date,
                my_optional_past_date=reasonable_past_date,
                my_optional_past_datetime=(
                    lower_bound_datetime - datetime.timedelta(days=1)
                ),
            )

        # Tests that we fail for bad upper bound dates
        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_optional_date\] value on class \[_TestClass\] with value "
            r"\[2033-03-03\] which is greater than or equal to \[2033-03-03\], the "
            r"\(exclusive\) max allowed date.",
        ):
            _ = _TestClass(
                my_optional_date=upper_bound_date,
                my_optional_past_date=reasonable_past_date,
                my_optional_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_optional_past_date\] value on class \[_TestClass\] with value "
            r"\[2022-02-03\] which is greater than or equal to \[2022-02-03\], the "
            r"\(exclusive\) max allowed date.",
        ):
            _ = _TestClass(
                my_optional_date=reasonable_past_date,
                my_optional_past_date=(
                    datetime.date.today() + datetime.timedelta(days=1)
                ),
                my_optional_past_datetime=reasonable_past_datetime,
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Found \[my_optional_past_datetime\] value on class \[_TestClass\] with "
            r"value \[2022-02-12T00:00:00\] which is greater than or equal to "
            r"\[2022-02-02T00:00:00\], the \(exclusive\) max allowed date.",
        ):
            _ = _TestClass(
                my_optional_date=reasonable_past_date,
                my_optional_past_date=reasonable_past_date,
                my_optional_past_datetime=(
                    datetime.datetime.now() + datetime.timedelta(days=10)
                ),
            )

        # These don't crash
        _ = _TestClass(
            my_optional_date=lower_bound_date,
            my_optional_past_date=lower_bound_date,
            my_optional_past_datetime=lower_bound_datetime,
        )

        _ = _TestClass(
            my_optional_date=upper_bound_date - datetime.timedelta(days=1),
            my_optional_past_date=datetime.date.today(),
            my_optional_past_datetime=(
                datetime.datetime.now() - datetime.timedelta(seconds=1)
            ),
        )

        _ = _TestClass(
            my_optional_date=reasonable_future_date,
            my_optional_past_date=reasonable_past_date,
            my_optional_past_datetime=reasonable_past_datetime,
        )


class TestIsNotSetAlongWithValidator(unittest.TestCase):
    """Tests for the is_not_set_along_with() validator."""

    @attr.define
    class _TestClass:
        first_field: str | None = attr.ib(
            default=None, validator=is_not_set_along_with("second_field")
        )
        second_field: str | None = attr.ib(default=None)

    def test_is_not_set_along_with_validator_correct_values(self) -> None:
        _ = self._TestClass(first_field="anything")
        _ = self._TestClass(second_field="anything")

    def test_is_not_set_along_with_validator_both_none(self) -> None:
        _ = self._TestClass(first_field=None, second_field=None)

    def test_is_not_set_along_with_validator_both_set(self) -> None:
        with self.assertRaisesRegex(
            TypeError,
            r"first_field and second_field cannot both be set on class \[_TestClass\]",
        ):
            _ = self._TestClass(first_field="value 1", second_field="value 2")


class TestIsNonEmptyList(unittest.TestCase):
    """Tests for the is_non_empty_list() validator."""

    @attr.define
    class _TestClass:
        items: list = attr.ib(validator=attr_validators.is_non_empty_list)

    def test_non_empty_list(self) -> None:
        _ = self._TestClass(items=["a", "b"])

    def test_single_element_list(self) -> None:
        _ = self._TestClass(items=[1])

    def test_empty_list(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be a non-empty list"):
            _ = self._TestClass(items=[])

    def test_not_a_list(self) -> None:
        with self.assertRaises((TypeError, ValueError)):
            _ = self._TestClass(items="not a list")  # type: ignore[arg-type]


class TestIsNone(unittest.TestCase):
    """Tests for the is_none() validator."""

    @attr.define
    class _TestClass:
        field: str | None = attr.ib(default=None, validator=attr_validators.is_none)

    def test_none(self) -> None:
        _ = self._TestClass()

    def test_explicit_none(self) -> None:
        _ = self._TestClass(field=None)

    def test_string_value_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "Expected field to always be None"):
            _ = self._TestClass(field="value")  # type: ignore[arg-type]

    def test_empty_string_raises(self) -> None:
        with self.assertRaisesRegex(ValueError, "Expected field to always be None"):
            _ = self._TestClass(field="")  # type: ignore[arg-type]


@attr.s(frozen=True)
class _TestNamePartClass:
    """Used in TestNamePartValidator."""

    my_name: str = attr.ib(validator=attr_validators.is_valid_name_part)
    my_opt_name: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_valid_name_part
    )


class TestNamePartValidator(unittest.TestCase):
    """Tests for is_valid_name_part and is_opt_valid_name_part."""

    def test_valid_alpha_name(self) -> None:
        instance = _TestNamePartClass(my_name="John")
        self.assertEqual(instance.my_name, "John")

    def test_valid_name_with_hyphen_and_apostrophe(self) -> None:
        instance = _TestNamePartClass(my_name="O'Brien-Smith")
        self.assertEqual(instance.my_name, "O'Brien-Smith")

    def test_valid_unicode_name(self) -> None:
        instance = _TestNamePartClass(my_name="José")
        self.assertEqual(instance.my_name, "José")

    def test_valid_name_with_period(self) -> None:
        for name in ["J.", "L.R.", "St. Pierre"]:
            instance = _TestNamePartClass(my_name=name)
            self.assertEqual(instance.my_name, name)

    def test_valid_name_with_internal_whitespace(self) -> None:
        instance = _TestNamePartClass(my_name="Mary Anne")
        self.assertEqual(instance.my_name, "Mary Anne")

    def test_valid_empty_string(self) -> None:
        instance = _TestNamePartClass(my_name="")
        self.assertEqual(instance.my_name, "")

    def test_opt_name_allows_none(self) -> None:
        instance = _TestNamePartClass(my_name="John", my_opt_name=None)
        self.assertIsNone(instance.my_opt_name)

    def test_opt_name_accepts_valid_string(self) -> None:
        instance = _TestNamePartClass(my_name="John", my_opt_name="Doe")
        self.assertEqual(instance.my_opt_name, "Doe")

    def test_rejects_digit_in_name(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"must contain only letters, hyphens, apostrophes, periods, and "
            r"whitespace. Found value \[J0HN\]",
        ):
            _ = _TestNamePartClass(my_name="J0HN")

    def test_rejects_trailing_digit(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="John2")

    def test_rejects_opt_name_with_digit(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="John", my_opt_name="Smith1")

    def test_rejects_non_string(self) -> None:
        with self.assertRaisesRegex(TypeError, r"Expected str for \[my_name\]"):
            _ = _TestNamePartClass(my_name=123)  # type: ignore[arg-type]

    def test_rejects_pure_numeric_string(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="12345")

    def test_rejects_parentheses(self) -> None:
        for name in ["JOHN (RUSTY)", "SMITH (JONES)", "(DECEASED)"]:
            with self.assertRaisesRegex(ValueError, r"must contain only letters"):
                _ = _TestNamePartClass(my_name=name)

    def test_rejects_comma(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="RAY, JR")

    def test_rejects_slash(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="JONATHAN/JOSHUA")

    def test_rejects_asterisk(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="MICHAEL***")

    def test_rejects_double_quote(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name='"SONNY"')

    def test_rejects_backtick(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="ELIZABETH`")

    def test_rejects_underscore(self) -> None:
        with self.assertRaisesRegex(ValueError, r"must contain only letters"):
            _ = _TestNamePartClass(my_name="UT_COTTONWOOD")

    def test_rejects_other_special_chars(self) -> None:
        for name in ["JOHN+", "JOHN;", "JOHN]", "JOHN&", "JOHN?", "JOHN@"]:
            with self.assertRaisesRegex(ValueError, r"must contain only letters"):
                _ = _TestNamePartClass(my_name=name)


@attr.s(frozen=True)
class _TestNameSuffixClass:
    """Used in TestNameSuffixValidator."""

    my_suffix: str = attr.ib(validator=attr_validators.is_valid_name_suffix)
    my_opt_suffix: str | None = attr.ib(
        default=None, validator=attr_validators.is_opt_valid_name_suffix
    )


class TestNameSuffixValidator(unittest.TestCase):
    """Tests for is_valid_name_suffix and is_opt_valid_name_suffix."""

    def test_opt_allows_none(self) -> None:
        instance = _TestNameSuffixClass(my_suffix="Jr", my_opt_suffix=None)
        self.assertIsNone(instance.my_opt_suffix)

    def test_allows_empty_string(self) -> None:
        instance = _TestNameSuffixClass(my_suffix="")
        self.assertEqual(instance.my_suffix, "")

    def test_allows_common_generational_variants(self) -> None:
        for suffix in ["Jr", "Jr.", "JR", "JR.", "Sr", "Sr.", "III", "iii", "VIII"]:
            instance = _TestNameSuffixClass(my_suffix=suffix)
            self.assertEqual(instance.my_suffix, suffix)

    def test_allows_ordinal_variants(self) -> None:
        for suffix in ["2nd", "3rd", "4th"]:
            instance = _TestNameSuffixClass(my_suffix=suffix)
            self.assertEqual(instance.my_suffix, suffix)

    def test_allows_bare_digit(self) -> None:
        instance = _TestNameSuffixClass(my_suffix="2")
        self.assertEqual(instance.my_suffix, "2")

    def test_allows_professional_credentials(self) -> None:
        for suffix in ["MD", "M.D.", "PhD", "Esq.", "Jr., MD"]:
            instance = _TestNameSuffixClass(my_suffix=suffix)
            self.assertEqual(instance.my_suffix, suffix)

    def test_allows_unicode_letters(self) -> None:
        # "Père" / "Fils" are the French equivalents of "Sr." / "Jr.".
        for suffix in ["Père", "Fils"]:
            instance = _TestNameSuffixClass(my_suffix=suffix)
            self.assertEqual(instance.my_suffix, suffix)

    def test_rejects_underscore(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"must contain only letters, digits, periods, commas, hyphens, "
            r"and whitespace",
        ):
            _ = _TestNameSuffixClass(my_suffix="Jr_5")

    def test_rejects_long_id_number(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"must be no more than {attr_validators.MAX_NAME_SUFFIX_LENGTH} characters",
        ):
            _ = _TestNameSuffixClass(my_suffix="12345678901")

    def test_rejects_free_text(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            rf"must be no more than {attr_validators.MAX_NAME_SUFFIX_LENGTH} characters",
        ):
            _ = _TestNameSuffixClass(my_suffix="Father of John")

    def test_rejects_disallowed_characters(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"must contain only letters, digits, periods, commas, hyphens, "
            r"and whitespace",
        ):
            _ = _TestNameSuffixClass(my_suffix="Jr<br>")

    def test_rejects_special_characters(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"must contain only letters, digits, periods, commas, hyphens, "
            r"and whitespace",
        ):
            _ = _TestNameSuffixClass(my_suffix="@#$")

    def test_rejects_non_string(self) -> None:
        with self.assertRaisesRegex(TypeError, r"Expected str for \[my_suffix\]"):
            _ = _TestNameSuffixClass(my_suffix=2)  # type: ignore[arg-type]
