# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Test decorator for mocking secrets """
import functools
from typing import Any, Callable, Dict, List, Type, Union, overload
from unittest import TestCase
from unittest.mock import patch

TestSuiteOrMethod = Union[Callable, Type[TestCase]]


def decorate_with_secrets(test: Callable, secrets: Dict[str, Any]) -> Callable:
    @functools.wraps(test)
    def inner(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
        patcher = patch("recidiviz.utils.secrets.get_secret", side_effect=secrets.get)
        patcher.start()

        value = test(*args, *kwargs)

        patcher.stop()

        return value

    return inner


def with_secrets(secrets: Dict[str, Any]) -> Callable:
    @overload
    def wrapped(test: Callable) -> Callable:
        ...

    @overload
    def wrapped(test: Type[TestCase]) -> Type[TestCase]:
        ...

    def wrapped(test: Any) -> Any:
        if isinstance(test, type) and issubclass(test, TestCase):
            test_method_names = [attr for attr in dir(test) if attr.startswith("test_")]

            for test_method_name in test_method_names:
                test_method = getattr(test, test_method_name)

                setattr(
                    test,
                    test_method_name,
                    decorate_with_secrets(test_method, secrets),
                )

            return test

        if callable(test):
            return decorate_with_secrets(test, secrets)

        raise ValueError("Not passed a TestCase or test  method")

    return wrapped
