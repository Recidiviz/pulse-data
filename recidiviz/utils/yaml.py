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

from typing import Any, Dict, List, Optional, Type, TypeVar, Union

# Represents a dictionary parsed from YAML, where values in the dictionary can only contain strings, numbers, or nested
# dictionaries, but not lists.
#
# Mypy's new type engine does not support recursive types yet, so for now this does not actually provide type safety.
YAMLDictType = Dict[str, Union[str, float, 'YAMLDictType']]  # type: ignore

T = TypeVar('T')

class YAMLDict:
    """Wraps a dict parsed from YAML and provides type safety when accessing items within the dict."""
    def __init__(self, yaml: YAMLDictType):
        self.yaml = yaml

    @classmethod
    def _assert_type(cls, field: str, value: Any, value_type: Type[T]) -> Optional[T]:
        if value is not None and not isinstance(value, value_type):
            raise ValueError(f"Invalid {field}, expected {value_type} but received: {repr(value)}")
        return value

    def pop_optional(self, field: str, value_type: Type[T]) -> Optional[T]:
        return self._assert_type(field, self.yaml.pop(field, None), value_type)

    def pop(self, field: str, value_type: Type[T]) -> T:
        value = self.pop_optional(field, value_type)
        if value is None:
            raise KeyError(f"Expected {field} in input: {repr(self.yaml)}")
        return value

    def pop_dict(self, field: str) -> 'YAMLDict':
        return YAMLDict(self.pop(field, dict))

    def pop_dict_optional(self, field: str) -> Optional['YAMLDict']:
        yaml = self.pop_optional(field, dict)
        if not yaml:
            return None
        return YAMLDict(yaml)

    @classmethod
    def _transform_dicts(cls, field: str, yamls: List) -> List['YAMLDict']:
        dicts = []
        for yaml in yamls:
            yaml = cls._assert_type(field, yaml, dict)
            if yaml is None:
                raise ValueError(f"Received entry in list that is None: {yamls}")
            dicts.append(YAMLDict(yaml))
        return dicts

    def pop_dicts(self, field: str) -> List['YAMLDict']:
        return self._transform_dicts(field, self.pop(field, list))

    def pop_dicts_optional(self, field: str) -> Optional[List['YAMLDict']]:
        yamls = self.pop_optional(field, list)
        if not yamls:
            return None
        return self._transform_dicts(field, yamls)

    def __len__(self) -> int:
        return len(self.yaml)

    def get(self) -> YAMLDictType:
        return self.yaml
