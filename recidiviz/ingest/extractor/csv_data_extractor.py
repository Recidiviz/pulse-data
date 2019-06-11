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

"""A module which takes CSV text and extracts from it rows that a user
might care about. The extracted information is put into the ingest data
model and returned.
"""
import csv
import logging
from collections import defaultdict
from typing import Dict, Set, List, Callable, Optional

import more_itertools

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.extractor.data_extractor import DataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, IngestObject


class IngestFieldCoordinates:
    """A thin object describing the "coordinates" of a particular ingest object
     and field, i.e. the class being referred to, the field being referred to,
     and the value of that field. This is used to locate objects that we want to
     update, create, or attach objects to."""

    def __init__(self, class_name, field_name, field_value):
        self.class_name = class_name
        self.field_name = field_name
        self.field_value = field_value

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __str__(self):
        return f'IngestFieldCoordinates[class_name: {self.class_name}, ' \
               f'field_name: {self.field_name}, ' \
               f'field_value: {self.field_value}]'


class CsvDataExtractor(DataExtractor):
    """Data extractor for CSV text."""

    def __init__(self,
                 key_mapping_file=None,
                 row_post_hooks=None,
                 ancestor_key_override_callback=None,
                 primary_key_override_callback=None,
                 system_level=SystemLevel.COUNTY,
                 set_with_empty_value=False,
                 should_cache=False):
        """
        Args:
            key_mapping_file: the path to the yaml file with key mappings
            row_post_hooks: an ordered list of hooks for post-processing on each
                ingested row in the CSV file
            ancestor_key_override_callback: a callable that returns the key for
                an ancestor of the object being constructed by a row in the file
            primary_key_override_callback: a callable that returns the key for
                the primary object being constructed by a row in the file
            system_level: whether this is a COUNTY- or STATE-level ingest
            set_with_empty_value: a flag that specifies whether rows whose
                non-ignored columns are all empty should still instantiate a new
                object. Defaults to False. Typically should only be True if
                there is a file that predominantly relies on hooks or callbacks
                and may conceivably have rows with mostly empty columns.
            should_cache: a flag that specifies whether we should proactively
                cache each created object by its primary key id. Defaults to
                False because it's typically not necessary unless you're
                stitching together IngestInfo object graphs where id mappings
                don't suffice.
        """
        super().__init__(key_mapping_file, should_cache=should_cache)

        self.keys_to_ignore: List[str] = self.manifest.get('keys_to_ignore', [])
        self.ancestor_keys: Dict[str, str] = \
            self.manifest.get('ancestor_keys', {})
        self.primary_key: Dict[str, str] = \
            self.manifest.get('primary_key', {})
        self.child_keys: Dict[str, str] = \
            self.manifest.get('child_key_mappings', {})
        self.enforced_ancestor_types: Dict[str, str] = \
            self.manifest.get('enforced_ancestor_types', {})

        if row_post_hooks is None:
            row_post_hooks = []
        self.row_post_hooks: List[Callable] = row_post_hooks

        self.ancestor_key_override_callback: Callable = \
            ancestor_key_override_callback
        self.primary_key_override_callback: Callable = \
            primary_key_override_callback
        self.system_level: SystemLevel = system_level
        self.set_with_empty_value: bool = set_with_empty_value

        self.keys.update(self.child_keys)

        self.all_keys: List[str] = set(self.keys.keys()) | set(
            self.child_keys.keys()) | set(self.keys_to_ignore) | set(
                self.ancestor_keys.keys()) | set(self.primary_key.keys())

    def extract_and_populate_data(self,
                                  content: str,
                                  ingest_info: IngestInfo = None) -> IngestInfo:
        """This function does all the work of taking the users yaml file
        and content and returning a populated data class.  This function
        iterates through every field in the object and builds a model based on
        the keys that it sees.

        Args:
            content: CSV-formatted text (string, not a file object)
            ingest_info: An IngestInfo object to use, if None we create a new
                one by default

        Returns:
            A populated ingest data model for a scrape.
        """
        if ingest_info is None:
            ingest_info = IngestInfo()
        self._extract(content, ingest_info)
        return ingest_info.prune()

    def _extract(self, content: str, ingest_info: IngestInfo):
        """Converts entries in |content| and adds data to |ingest_info|."""
        if not isinstance(content, str):
            logging.error("%r is not a string", content)
            return

        rows = csv.DictReader(content.splitlines())
        seen_map: Dict[int, Set[str]] = defaultdict(set)
        for row in rows:
            primary_coordinates = self._primary_coordinates(row)
            ancestor_chain: Dict[str, str] = self._ancestor_chain(
                row, primary_coordinates)

            # TODO(1839): Generalize instantiation so we don't special-case this
            if not ancestor_chain \
                    or not ancestor_chain.get('person') \
                    or not ancestor_chain.get('state_person'):
                # If there's no person at the top of this tree, create one now
                self._instantiate_person(ingest_info)

            extracted_objects_for_row = []
            for k, v in row.items():
                if k not in self.all_keys:
                    raise ValueError("Unmapped key: [%s]" % k)

                if not v and not self.set_with_empty_value:
                    continue

                create_args = self._get_creation_args(row, k)

                column_ancestor_chain = ancestor_chain.copy()
                if k in self.child_keys and primary_coordinates:
                    # If we have a key for a child object in this row, include
                    # the primary key of the ancestor from the row
                    column_ancestor_chain[primary_coordinates.class_name] = \
                        primary_coordinates.field_value

                extracted_objects_for_column = \
                    self._set_value_if_key_exists(k, v,
                                                  ingest_info,
                                                  seen_map,
                                                  column_ancestor_chain,
                                                  **create_args)
                extracted_objects_for_row.extend(extracted_objects_for_column)

            self._post_process_row(row, extracted_objects_for_row)

    def _set_value_if_key_exists(self,
                                 lookup_key: str, value: str,
                                 ingest_info: IngestInfo,
                                 seen_map: Dict[int, Set[str]],
                                 ancestor_chain: Dict[str, str] = None,
                                 **create_args) -> List[IngestObject]:
        """If the column key is one we want to set on the object, set it on the
        appropriate object instance."""
        if lookup_key in self.keys:
            return self._set_or_create_object(
                ingest_info, self.keys[lookup_key], [value], seen_map,
                ancestor_chain, self.enforced_ancestor_types, **create_args)

        return []

    def _instantiate_person(self, ingest_info: IngestInfo):
        if self.system_level == SystemLevel.COUNTY:
            ingest_info.create_person()
        else:
            ingest_info.create_state_person()

    def _post_process_row(self, row: Dict[str, str],
                          extracted_objects: List[IngestObject]):
        """Applies post-processing based on the given row, for the list of
        ingest objects that were extracted during extraction of that row."""
        unique_extracted_objects: Dict[int, IngestObject] = {}
        for extracted_object in extracted_objects:
            object_id = id(extracted_object)
            if object_id not in unique_extracted_objects:
                unique_extracted_objects[object_id] = extracted_object

        for hook in self.row_post_hooks:
            hook(row, unique_extracted_objects.values(),
                 self.ingest_object_cache)

    def _get_creation_args(self, row: Dict[str, str], lookup_key: str) \
            -> Dict[str, str]:
        """Gets arguments needed to create a new entity, if necessary.

        For now, this just returns the primary key-esque id for the entity to
        be created or updated, to help with assembling object graphs when a
        row contains data for multiple entities.
        """

        def _current_field_is_from_class(current_field_key: str,
                                         class_name: str) -> bool:
            current_class_name, _current_field_name = \
                current_field_key.split('.')
            return current_class_name == class_name

        current_field = self.keys.get(lookup_key, None)
        if not current_field:
            return {}

        primary_coordinates = self._primary_coordinates(row)

        if primary_coordinates and _current_field_is_from_class(
                current_field, primary_coordinates.class_name):
            return {
                primary_coordinates.field_name: primary_coordinates.field_value
            }

        return {}

    def _ancestor_chain(self, row: Dict[str, str],
                        primary_coordinates: Optional[IngestFieldCoordinates]) \
            -> Dict[str, str]:
        """Returns the ancestor chain for this row, i.e. a mapping from ancestor
        class to the id of the ancestor corresponding to this row.

        This consists of any ancestor key mappings for the row, and any ancestor
        key override callback. If the mapping and the callback each provide an
        id for a particular ancestor class, the callback wins.
        """
        ancestor_chain = {}

        if self.ancestor_keys:
            ancestor_coordinates = self._get_coordinates_from_mapping(
                self.ancestor_keys, row)
            for coordinate in ancestor_coordinates:
                ancestor_chain[coordinate.class_name] = coordinate.field_value

        if self.ancestor_key_override_callback and primary_coordinates:
            ancestor_addition = self.ancestor_key_override_callback(
                primary_coordinates)
            ancestor_chain.update(ancestor_addition)

        return ancestor_chain

    def _primary_coordinates(self, row: Dict[str, str]) \
            -> Optional[IngestFieldCoordinates]:
        """Returns the coordinates of the primary key for the main entity being
        extracted for this row.

        Unlike ancestor chain functionality which layers the ancestor key
        override atop anything in the ancestor key mapping, this provides only
        a single set of coordinates. As such, if there is a callback, we return
        its output. Otherwise, we return the primary key mapping or None.
        """
        if self.primary_key_override_callback:
            return self.primary_key_override_callback(row)

        if self.primary_key:
            coordinates = self._get_coordinates_from_mapping(self.primary_key,
                                                             row)
            return more_itertools.one(coordinates)

        return None

    @staticmethod
    def _get_coordinates_from_mapping(key_mapping: Dict[str, str],
                                      row: Dict[str, str]) \
            -> List[IngestFieldCoordinates]:
        coordinates = []

        for lookup_key, cls_and_field in key_mapping.items():
            if not cls_and_field:
                raise TypeError(f"Expected truthy key mapping [{key_mapping}] "
                                f"to have a value inside")
            cls, field = cls_and_field.split('.')
            id_value = row.get(lookup_key)
            coordinates.append(IngestFieldCoordinates(cls, field, id_value))

        return coordinates
