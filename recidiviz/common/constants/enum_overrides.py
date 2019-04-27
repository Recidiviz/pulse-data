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
from typing import Callable, Set, Union
from typing import Dict, Optional

import attr

from recidiviz.common.str_field_utils import normalize
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


# pylint doesn't support custom decorators, so these attributes can't be
# subscripted. https://github.com/PyCQA/pylint/issues/1694
# pylint: disable=unsubscriptable-object
@attr.s(frozen=True)
class EnumOverrides:
    """Contains scraper-specific mappings from string keys to EntityEnum
    values. EnumOverrides objects should be created using EnumOverrides.Builder.
    """
    _maps: Dict[EntityEnumMeta, Dict[str, EntityEnum]] = attr.ib()
    _predicate_maps: Dict[EntityEnumMeta, Set['_EnumMatcher']] = attr.ib()
    _ignores: Dict[EntityEnumMeta, Set[str]] = attr.ib()
    _predicate_ignores: Dict[EntityEnumMeta,
                             Set[Callable[[str], bool]]] = attr.ib()

    def should_ignore(self, label: str, enum_class: EntityEnumMeta) -> bool:
        label = normalize(label, remove_punctuation=True)
        predicate_calls = (predicate(label) for predicate in
                           self._predicate_ignores[enum_class])
        return label in self._ignores[enum_class] or any(predicate_calls)

    def parse(self,
              label: str,
              enum_class: EntityEnumMeta) -> Optional[EntityEnum]:
        label = normalize(label, remove_punctuation=True)
        if self.should_ignore(label, enum_class):
            return None

        direct_lookup = self._maps[enum_class].get(label)
        if direct_lookup:
            return direct_lookup

        matches = {matcher.value for matcher in self._predicate_maps[enum_class]
                   if matcher.predicate(label)}
        if len(matches) > 1:
            raise ValueError("Overrides map matched too many values from label"
                             " {}: [{}]".format(label, matches))
        if matches:
            return matches.pop()

        return None

    # pylint: disable=protected-access
    def to_builder(self) -> 'Builder':
        builder = self.Builder()
        builder._maps = self._maps
        builder._predicate_maps = self._predicate_maps
        builder._ignores = self._ignores
        builder._predicate_ignores = self._predicate_ignores
        return builder

    @classmethod
    def empty(cls) -> 'EnumOverrides':
        return cls.Builder().build()

    class Builder:
        """Builder for EnumOverrides objects."""

        def __init__(self):
            self._maps: Dict[EntityEnumMeta,
                             Dict[str, EntityEnum]] = defaultdict(dict)
            self._predicate_maps: Dict[EntityEnumMeta,
                                       Set[_EnumMatcher]] = defaultdict(set)
            self._ignores: Dict[EntityEnumMeta, Set[str]] = defaultdict(set)
            self._predicate_ignores: \
                Dict[EntityEnumMeta, Set[Callable[[str], bool]]] = \
                defaultdict(set)

        def build(self) -> 'EnumOverrides':
            return EnumOverrides(self._maps, self._predicate_maps,
                                 self._ignores, self._predicate_ignores)

        def add(self,
                label_or_predicate: Union[str, Callable[[str], bool]],
                mapped_enum: EntityEnum,
                from_field: EntityEnumMeta = None) -> None:
            """Adds a mapping from |match| to |mapped_enum|. |match| can be
            either a string value, in which case the field value must match the
            string exactly, or it can be a predicate specifying which strings
            constitute a match. Optionally, the |from_field| parameter allows
            values to be mapped accross fields. For example:
            `add('PENDING', BondStatus.PENDING, BondType)` remaps the bond_type
            field to a bond_status when the bond_type is set to 'PENDING'.
            Mappings *between* entity types are not allowed.
            Note: take care not to add multiple predicates which are properties
            of the same string, as EnumOverrides.parse will throw an exception
            if too many matches are found.
            """
            if from_field is None:
                from_field = mapped_enum.__class__
            if isinstance(label_or_predicate, str):
                label = normalize(label_or_predicate, remove_punctuation=True)
                self._maps[from_field][label] = mapped_enum
            else:
                predicate = label_or_predicate
                self._predicate_maps[from_field].add(
                    _EnumMatcher(predicate, mapped_enum))

        def ignore(self,
                   label_or_predicate: Union[str, Callable[[str], bool]],
                   from_field: EntityEnumMeta) -> None:
            """Marks strings matching |label_or_predicate| as ignored
            values for |enum_class|."""
            if isinstance(label_or_predicate, str):
                label = normalize(label_or_predicate, remove_punctuation=True)
                self._ignores[from_field].add(label)
            else:
                predicate = label_or_predicate
                self._predicate_ignores[from_field].add(predicate)


@attr.s(frozen=True)
class _EnumMatcher:
    predicate: Callable[[str], bool] = attr.ib()
    value: EntityEnum = attr.ib()
