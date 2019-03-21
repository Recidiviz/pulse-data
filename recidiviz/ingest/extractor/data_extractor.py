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

"""A module which takes any content and extracts from it key/value pairs that
a user might care about.  The extracted information is put into the ingest data
model and returned.
"""

import abc
from typing import Union, List, Optional, Sequence, Dict, Set

import yaml
from lxml.html import HtmlElement

from recidiviz.ingest.models.ingest_info import HIERARCHY_MAP, PLURALS, \
    IngestInfo, IngestObject


class DataExtractor(metaclass=abc.ABCMeta):
    """Base class for automatically extracting data from a file."""

    def __init__(self, key_mapping_file=None):
        """The init of the data extractor.

        Args:
            key_mapping_file: a yaml file defining the mappings that the
                data extractor uses to find relevant keys.
        """
        if key_mapping_file:
            with open(key_mapping_file, 'r') as ymlfile:
                self.manifest = yaml.load(ymlfile)
            self.keys = self.manifest['key_mappings']
            self.multi_keys = self.manifest.get('multi_key_mapping', {})

            # We want to know which of the classes are multi keys as this helps
            # us with behaviour when we set the values.
            self.multi_key_classes = set(
                value.split('.')[0] if isinstance(value, str)
                else value[0].split('.')[0]
                for value in self.multi_keys.values())

    @abc.abstractmethod
    def extract_and_populate_data(self, content: HtmlElement,
                                  ingest_info: IngestInfo = None) -> IngestInfo:
        pass

    def _set_or_create_object(self, ingest_info: IngestInfo,
                              lookup_keys: Union[str, List[str]],
                              values: List[Optional[str]],
                              seen_map: Dict[int, Set[str]]) -> None:
        """Contains the logic to set or create a field on an ingest object.
        The logic here is that we check if we have a most recent class already
        to check if the field is already set.  If the field we care about is
        already set, we say that we need to create a new object and set the
        field.

        Args:
            ingest_info: The top level IngestInfo object to set the data on.
            lookup_keys: A list of strings or single string, formatted as
                <class_name>.<field_name>, i.e. 'person.surname'
            values: The list of values that the field name will be set to.
                Each element of the list is a different write and might incur
                objects being created/updated.
            seen_map: A dict keyed by object ids, containing sets of field
            names that have already been set on the object. We mark fields as
            set on objects so that setting the field again will create a new
            object, even if the last object's field was set to None. This method
            modifies the map when it sets fields.
        """
        lookup_keys = lookup_keys if isinstance(lookup_keys, list) \
            else [lookup_keys]
        for lookup_key in lookup_keys:
            class_to_set, ingest_key = lookup_key.split('.')
            is_multi_key = class_to_set in self.multi_key_classes
            for i, value in enumerate(values):
                parent = self._get_parent(ingest_info, class_to_set, i,
                                          is_multi_key)
                object_to_set = self._get_object_to_set(class_to_set, parent,
                                                        i, is_multi_key)
                # If the object we are trying to operate on is None, or it has
                # already set the ingest_key then we know we need to create a
                # new one.
                if object_to_set is None or \
                        ingest_key in seen_map[id(object_to_set)]:
                    object_to_set = _create(parent, class_to_set)

                setattr(object_to_set, ingest_key, value)
                seen_map[id(object_to_set)].add(ingest_key)

    def _get_parent(
            self, ingest_info: IngestInfo, class_to_set: str, index: int,
            is_multi_key: bool) -> IngestObject:
        """Finds or creates the parent of the object we are going to set, which
        may need to have its own parent created if it is a hold or charge in a
        multi-key column."""
        # Multi-keys may need to be indexed by their parent, i.e. a bond at
        # index 3 has the parent charge at index 3.
        if is_multi_key and class_to_set != 'person':
            parent_cls_to_set = HIERARCHY_MAP[class_to_set][-1]
            grandparent_cls_to_set = HIERARCHY_MAP[parent_cls_to_set][-1] if \
                HIERARCHY_MAP[parent_cls_to_set] else None
            grandparent = self._find_parent_ingest_info(
                ingest_info, HIERARCHY_MAP[parent_cls_to_set], index)
            recent = _get_recent(grandparent, parent_cls_to_set)

            # If |index| was used to index the grandparent, use the most
            # recent parent if one exists.
            if parent_cls_to_set in self.multi_key_classes \
                    and grandparent_cls_to_set in self.multi_key_classes \
                    and recent:
                return recent

            # If |index| should be used to index the parent, find the parent.
            if class_to_set not in PLURALS or class_to_set == 'booking':
                list_of_parents = getattr(grandparent,
                                          PLURALS[parent_cls_to_set])
                if index < len(list_of_parents):
                    return list_of_parents[index]

            # If the parent doesn't exist, create it.
            if class_to_set not in PLURALS or \
                    parent_cls_to_set in self.multi_key_classes:
                return _create(grandparent, parent_cls_to_set)

        return self._find_parent_ingest_info(ingest_info,
                                             HIERARCHY_MAP[class_to_set], index)

    def _get_object_to_set(
            self, class_to_set: str, parent: IngestObject, index: int,
            is_multi_key: bool) -> IngestObject:
        """Finds or creates the object we are going to set, which may already
        exist in the multi-key case."""
        if is_multi_key and class_to_set in PLURALS:
            list_of_class_to_set = getattr(parent, PLURALS[class_to_set])
            if list_of_class_to_set is not None \
                    and isinstance(list_of_class_to_set, list) \
                    and len(list_of_class_to_set) > index:
                return list_of_class_to_set[index]
        return _get_recent(parent, class_to_set)

    def _find_parent_ingest_info(self, ingest_info: IngestInfo,
                                 hierarchy: Sequence[str],
                                 val_index: int) -> IngestObject:
        """Find the parent object to set the value on

        Args:
            ingest_info: an IngestInfo object
            hierarchy: the hierarchy of the data structure to help us
                walk the data object.
            val_index: the index of the value we are setting, note that this is
                unused if it is a multi key.
        Returns:
            the parent object to operate on.
        """
        parent = ingest_info
        for hier_class in hierarchy:
            # If we are a multi key class instead of getting the most recent
            # object as a parent, we use the index of the value we found, if it
            # exists.
            if hier_class in self.multi_key_classes and \
                    len(getattr(parent, PLURALS[hier_class])) > val_index:
                parent = getattr(parent, PLURALS[hier_class])[val_index]
            else:
                old_parent = parent
                parent = _get_recent(old_parent, hier_class)
                if parent is None:
                    parent = _create(old_parent, hier_class)
        return parent


def _get_recent(
        parent: IngestObject, class_name: str):
    return getattr(parent, 'get_recent_' + class_name)()


def _create(parent: IngestObject, class_name: str):
    return getattr(parent, 'create_' + class_name)()
