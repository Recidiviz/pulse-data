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
"""General use mypy types."""

from typing import Any, Iterable, Optional, Type, TypeVar

# A type variable intended for use in generic class methods that return an object with the `cls` type.
# For example:
#
# class MySuperClassOrMixin:
#     @classmethod
#     def make_object(cls: Type[ClsT]) -> ClsT:
#         return cls()
#
# class MySubClass(MySuperClassOrMixin):
#     def print_foo(self):
#         print('foo')
#
# obj = MySubClass.make_object()
# obj.print_foo()  <- mypy understand the type of obj is MySubClass and doesn't complain here
ClsT = TypeVar("ClsT", bound=object)

# A Generic type where the generic can be any object
T = TypeVar("T")

# A Generic type where the generic can be any object
U = TypeVar("U")


def non_optional(v: Optional[T]) -> T:
    """Converts the type of a value from optional to non-optional, throwing if it is
    None.
    """
    if v is None:
        raise ValueError("Expected non-null value.")
    return v


def assert_type(v: Optional[Any], expected_type: Type[T]) -> T:
    """Asserts that the type of a value is a particular type, throwing if it is not."""
    non_optional_v: Any = non_optional(v)
    if not isinstance(non_optional_v, expected_type):
        raise ValueError(
            f"Expected type [{expected_type}], found [{v}] with type [{type(v)}]."
        )
    return non_optional_v


def assert_type_list(v: Iterable[Any], expected_type: Type[T]) -> Iterable[T]:
    """Asserts that the type of a value is a list of a particular type, throwing if any
    of the elements are not that type."""
    return [assert_type(element, expected_type) for element in v]


def assert_subclass(v_type: Type, expected_type: Type[T]) -> Type[T]:
    """Asserts that the value is a class that is a subclass of the expected type."""
    if issubclass(v_type, expected_type):
        return v_type
    raise ValueError(f"Expected subclass of [{expected_type}], found [{v_type}].")


def assert_subclass_list(
    v: Iterable[Type], expected_type: Type[T]
) -> Iterable[Type[T]]:
    """Asserts that the type of a value is a list of classes that are subclasses of a
    particular type, throwing if any of the elements are not subclasses of that type.
    """
    return [assert_subclass(cls, expected_type) for cls in v]
