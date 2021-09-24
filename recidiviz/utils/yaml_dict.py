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
"""Functionality for working with objects parsed from YAML."""
import copy
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

import yaml

# Represents a dictionary parsed from YAML, where values in the dictionary can only contain strings, numbers, or nested
# dictionaries, but not lists.
#
# Mypy's new type engine does not support recursive types yet, so for now this does not actually provide type safety.
YAMLDictType = Dict[str, Union[str, float, "YAMLDictType"]]  # type: ignore

T = TypeVar("T")


class YAMLDict:
    """Wraps a dict parsed from YAML and provides type safety when accessing items within the dict."""

    def __init__(self, raw_yaml: YAMLDictType):
        self.raw_yaml = raw_yaml

    @classmethod
    def from_path(cls, yaml_path: str) -> "YAMLDict":
        with open(yaml_path, encoding="utf-8") as yaml_file:
            loaded_raw_yaml = yaml.safe_load(yaml_file)
            if not isinstance(loaded_raw_yaml, dict):
                raise ValueError(
                    f"Expected manifest to contain a top-level dictionary, but "
                    f"received: {type(loaded_raw_yaml)} at path [{yaml_path}]."
                )
            return YAMLDict(loaded_raw_yaml)

    @classmethod
    def _assert_type(cls, field: str, value: Any, value_type: Type[T]) -> T:
        if value is None or not isinstance(value, value_type):
            raise ValueError(
                f"The field [{field}] must be of type [{value_type}]. Invalid "
                f"[{field}] value, expected type [{value_type}] but received: "
                f"{type(value)}"
            )
        return value

    def pop_optional(self, field: str, value_type: Type[T]) -> Optional[T]:
        """Returns the object at the given key |field| without popping it from the
        YAMLDict. Will return None if the field does not exist or if the value at that
        field is None. Throws if the value is nonnull but the type is not the expected
        |value_type|.
        """
        value = self.raw_yaml.pop(field, None)
        if value is None:
            return None
        return self._assert_type(field, value, value_type)

    def pop(self, field: str, value_type: Type[T]) -> T:
        """Returns the object at the given key |field| after popping it from the
        YAMLDict. Throws if the value is nonnull but the type is not the expected
        |value_type|, or if the field does not exist, or if the value at that field is
        None.
        """
        try:
            value = self.raw_yaml.pop(field)
        except KeyError as e:
            raise KeyError(
                f"Expected nonnull [{field}] in input: {self.raw_yaml}"
            ) from e
        return self._assert_type(field, value, value_type)

    def pop_dict(self, field: str) -> "YAMLDict":
        """Returns the dictionary at the given key |field| after popping it from the
        YAMLDict. Throws if the value is nonnull but the type is not a dictionary, or
        if the field does not exist, or if the value at that field is None.
        """
        return YAMLDict(self.pop(field, dict))

    def pop_dict_optional(self, field: str) -> Optional["YAMLDict"]:
        """Returns the dictionary at the given key |field| without popping it from the
        YAMLDict. Will return None if the field does not exist or if the value at that
        field is None. Throws if the value is nonnull but is not a dictionary.
        """
        try:
            raw_yaml = self.pop(field, dict)
            return YAMLDict(raw_yaml)
        except (KeyError, ValueError):
            return None

    @classmethod
    def _transform_dicts(cls, field: str, raw_yamls: List) -> List["YAMLDict"]:
        dicts = []
        for raw_yaml in raw_yamls:
            raw_yaml = cls._assert_type(field, raw_yaml, dict)
            dicts.append(YAMLDict(raw_yaml))
        return dicts

    def pop_dicts(self, field: str) -> List["YAMLDict"]:
        """Returns the list of dictionaries at the given key |field| after popping it
        from the YAMLDict. Throws if the value is nonnull but the type is not a
        list, if any of the list values are not dictionaries, if the field does not
        exist, or if the value at that field is None.
        """
        return self._transform_dicts(field, self.pop(field, list))

    def pop_dicts_optional(self, field: str) -> Optional[List["YAMLDict"]]:
        """Returns the list of dictionaries at the given key |field| after popping it
        from the YAMLDict. Throws if the value is nonnull but the type is not a
        list or if any of the list values are not dictionaries. If the field does not
        exist, or if the value at that field is None, returns None.
        """
        try:
            raw_yamls = self.pop(field, list)
        except (KeyError, ValueError):
            return None
        return self._transform_dicts(field, raw_yamls)

    def pop_list(self, field: str, list_values_type: Type[T]) -> List[T]:
        """Returns the list of dictionaries at the given key |field| after popping it
        from the YAMLDict. Throws if the value is nonnull but the type is not a
        list, if any of the list values are not the expected |list_values_type|, if the
        field does not exist, or if the value at that field is None.
        """
        raw_yamls = self.pop(field, list)
        return [
            self._assert_type(field, raw_val, list_values_type) for raw_val in raw_yamls
        ]

    def pop_list_optional(
        self, field: str, list_values_type: Type[T]
    ) -> Optional[List[T]]:
        """Returns the list of dictionaries at the given key |field| after popping it
        from the YAMLDict. Throws if the value is nonnull but the type is not a
        list or if any of the list values are not the expected |list_values_type|. If
        the field does not exist, or if the value at that field is None, returns None.
        """
        try:
            raw_yamls = self.pop(field, list)
        except (KeyError, ValueError):
            return None
        return [
            self._assert_type(field, raw_val, list_values_type) for raw_val in raw_yamls
        ]

    def peek_optional(self, field: str, value_type: Type[T]) -> Optional[T]:
        """Returns the object at the given key |field| without popping it from the
        YAMLDict. Will return None if the field does not exist or if the value at that
        field is None.
        """
        try:
            return self.peek(field, value_type)
        except (KeyError, ValueError):
            return None

    def peek(self, field: str, value_type: Type[T]) -> T:
        """Returns the object at the given key |field| without popping it from the
        YAMLDict. Throws if the field does not exist or if the value at that field is
        None.
        """
        try:
            value = self.raw_yaml[field]
        except KeyError as e:
            raise KeyError(
                f"Expected nonnull [{field}] in input: {self.raw_yaml}"
            ) from e
        return self._assert_type(field, value, value_type)

    def peek_type(self, field: str) -> Type:
        """Returns the type of the object at |field|. Throws if no such field exists or
        if the value at |field| is None.
        """
        return type(self.peek(field, object))

    def __len__(self) -> int:
        return len(self.raw_yaml)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, YAMLDict):
            return False

        return self.get() == other.get()

    def __repr__(self) -> str:
        return str(self.get())

    def get(self) -> YAMLDictType:
        """Returns the underlying raw dictionary representation of the YAML."""
        return self.raw_yaml

    def keys(self) -> List[str]:
        """Returns a list of keys in this YAMLDict."""
        return list(self.raw_yaml.keys())

    def copy(self) -> "YAMLDict":
        """Returns a deep copy of this YamlDict."""
        return YAMLDict(copy.deepcopy(self.raw_yaml))
