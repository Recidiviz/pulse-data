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
    is_list_of,
    is_not_set_along_with,
    is_set_of,
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


@attr.s(frozen=True)
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
