# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
from typing import Union, List, Optional, Sequence

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
                              values: List[Optional[str]]) -> None:
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
        """
        lookup_keys = lookup_keys if isinstance(lookup_keys, list) \
            else [lookup_keys]
        for lookup_key in lookup_keys:
            class_to_set, ingest_key = lookup_key.split('.')
            is_multi_key = class_to_set in self.multi_key_classes
            for i, value in enumerate(values):
                if value is None or value == '' or value.isspace():
                    continue
                # The first task is to find the parent.
                parent = self._find_parent_ingest_info(
                    ingest_info, HIERARCHY_MAP[class_to_set], i)

                get_recent_name = 'get_recent_' + class_to_set
                object_to_set = getattr(parent, get_recent_name)()
                # If we are in multikey, we need to pull out the correct index
                # of class type we want to set
                if is_multi_key and class_to_set in PLURALS:
                    attr = getattr(parent, PLURALS[class_to_set])
                    parent_cls_to_set = HIERARCHY_MAP[class_to_set][-1]
                    if i != 0 and parent_cls_to_set in self.multi_key_classes:
                        grandparent = self._find_parent_ingest_info(
                            ingest_info, HIERARCHY_MAP[parent_cls_to_set], i)
                        create_name = 'create_' + parent_cls_to_set
                        create_func = getattr(grandparent, create_name)
                        parent = create_func()
                    if attr is not None \
                            and isinstance(attr, list) \
                            and len(attr) > i:
                        object_to_set = attr[i]

                create_name = 'create_' + class_to_set
                create_func = getattr(parent, create_name)
                # If the object we are trying to operate on is None, or it has
                # already set the ingest_key then we know we need to create a
                # new one.
                if (object_to_set is None or
                        getattr(object_to_set, ingest_key) is not None):
                    object_to_set = create_func()

                setattr(object_to_set, ingest_key, value)

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
                get_recent_name = 'get_recent_' + hier_class
                create_func = 'create_' + hier_class
                old_parent = parent
                parent = getattr(old_parent, get_recent_name)()
                if parent is None:
                    parent = getattr(old_parent, create_func)()
        return parent
