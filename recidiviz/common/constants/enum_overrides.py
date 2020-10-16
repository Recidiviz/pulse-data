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
"""Contains logic related to EnumOverrides."""

from collections import defaultdict
from typing import Callable, Set
from typing import Dict, Optional

import attr

from recidiviz.common.str_field_utils import normalize
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta

EnumMapper = Callable[[str], Optional[EntityEnum]]
EnumIgnorePredicate = Callable[[str], bool]

# pylint doesn't support custom decorators, so these attributes can't be subscripted.
# https://github.com/PyCQA/pylint/issues/1694
# pylint: disable=unsubscriptable-object
@attr.s(frozen=True)
class EnumOverrides:
    """Contains scraper-specific mappings from string keys to EntityEnum values. EnumOverrides objects should be
    created using EnumOverrides.Builder.
    """
    _str_mappings_dict: Dict[EntityEnumMeta, Dict[str, EntityEnum]] = attr.ib()
    _mappers_dict: Dict[EntityEnumMeta, Set['EnumMapper']] = attr.ib()
    _ignores: Dict[EntityEnumMeta, Set[str]] = attr.ib()
    _ignore_predicates_dict: Dict[EntityEnumMeta, Set[EnumIgnorePredicate]] = attr.ib()

    def should_ignore(self, label: str, enum_class: EntityEnumMeta) -> bool:
        label = normalize(label, remove_punctuation=True)
        predicate_calls = (predicate(label) for predicate in self._ignore_predicates_dict[enum_class])
        return label in self._ignores[enum_class] or any(predicate_calls)

    def parse(self,
              label: str,
              enum_class: EntityEnumMeta) -> Optional[EntityEnum]:
        label = normalize(label, remove_punctuation=True)
        if self.should_ignore(label, enum_class):
            return None

        direct_lookup = self._str_mappings_dict[enum_class].get(label)
        if direct_lookup:
            return direct_lookup

        matches = {mapper(label) for mapper in self._mappers_dict[enum_class] if mapper(label) is not None}
        if len(matches) > 1:
            raise ValueError("Overrides map matched too many values from label {}: [{}]".format(label, matches))
        if matches:
            return matches.pop()
        return None

    # pylint: disable=protected-access
    def to_builder(self) -> 'Builder':
        builder = self.Builder()
        builder._str_mappings_dict = self._str_mappings_dict
        builder._mappers_dict = self._mappers_dict
        builder._ignores = self._ignores
        builder._ignore_predicates_dict = self._ignore_predicates_dict
        return builder

    @classmethod
    def empty(cls) -> 'EnumOverrides':
        return cls.Builder().build()

    class Builder:
        """Builder for EnumOverrides objects."""

        def __init__(self) -> None:
            self._str_mappings_dict: Dict[EntityEnumMeta, Dict[str, EntityEnum]] = defaultdict(dict)
            self._mappers_dict: Dict[EntityEnumMeta, Set[EnumMapper]] = defaultdict(set)
            self._ignores: Dict[EntityEnumMeta, Set[str]] = defaultdict(set)
            self._ignore_predicates_dict: Dict[EntityEnumMeta, Set[EnumIgnorePredicate]] = defaultdict(set)

        def build(self) -> 'EnumOverrides':
            return EnumOverrides(self._str_mappings_dict,
                                 self._mappers_dict,
                                 self._ignores,
                                 self._ignore_predicates_dict)

        def add_mapper(self,
                       mapper: EnumMapper,
                       mapped_cls: EntityEnumMeta,
                       from_field: EntityEnumMeta = None) -> 'EnumOverrides.Builder':
            """Adds a |mapper| which maps field values to enums within the |mapped_cls|. |mapper| must be a
            Callable which, given a string value, returns an enum of class |mapped_cls| or None.

            Optionally, the |from_field| parameter allows values to be mapped across fields. For example:
            `add_mapper(bond_status_mapper, BondStatus, BondType)` remaps the bond_type field to a bond_status when
            the bond_status_mapper returns an enum value. Mappings *between* entity types are not allowed.

            Note: take care not to add multiple mappers which map the same field value to different enums, as
            EnumOverrides.parse will throw an exception.
            """
            if from_field is None:
                from_field = mapped_cls
            self._mappers_dict[from_field].add(mapper)
            return self

        def add(self,
                label: str,
                mapped_enum: EntityEnum,
                from_field: EntityEnumMeta = None) -> 'EnumOverrides.Builder':
            """Adds a mapping from |label| to |mapped_enum|. As |label| must be a string, the provided field value must
            match the string exactly to constitute a match.

            Optionally, the |from_field| parameter allows values to be mapped across fields. For example:
            `add('PENDING', BondStatus.PENDING, BondType)` remaps the bond_type field to a bond_status when the
            bond_type is set to 'PENDING'. Mappings *between* entity types are not allowed.
            """
            if from_field is None:
                from_field = mapped_enum.__class__
            label = normalize(label, remove_punctuation=True)
            self._str_mappings_dict[from_field][label] = mapped_enum
            return self

        def ignore_with_predicate(self,
                                  predicate: EnumIgnorePredicate,
                                  from_field: EntityEnumMeta) -> 'EnumOverrides.Builder':
            """Marks strings matching |predicate| as ignored values for |from_field| enum class."""
            self._ignore_predicates_dict[from_field].add(predicate)
            return self

        def ignore(self, label: str, from_field: EntityEnumMeta) -> 'EnumOverrides.Builder':
            """Marks strings matching |label| as ignored values for |from_field| enum class."""
            label = normalize(label, remove_punctuation=True)
            self._ignores[from_field].add(label)

            return self
