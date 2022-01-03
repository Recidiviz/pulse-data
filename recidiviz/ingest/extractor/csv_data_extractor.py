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
import collections.abc
import csv
import logging
from collections import OrderedDict, defaultdict
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

import more_itertools

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.extractor.data_extractor import DataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, IngestObject
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.ingest.models.ingest_object_hierarchy import get_ancestor_class_sequence


class IngestFieldCoordinates:
    """A thin object describing the "coordinates" of a particular ingest object
    and field, i.e. the class being referred to, the field being referred to,
    and the value of that field. This is used to locate objects that we want to
    update, create, or attach objects to."""

    def __init__(self, class_name: str, field_name: str, field_value: str):
        self.class_name = class_name
        self.field_name = field_name
        self.field_value = field_value

    def __eq__(self, other: object) -> bool:
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __str__(self) -> str:
        return (
            f"IngestFieldCoordinates[class_name: {self.class_name}, "
            f"field_name: {self.field_name}, "
            f"field_value: {self.field_value}]"
        )


_DUMMY_KEY_PREFIX = "CSV_EXTRACTOR_DUMMY_KEY"

HookContextType = TypeVar("HookContextType")

RowType = Dict[str, str]
RowPrehookCallable = Callable[[HookContextType, RowType], None]
RowPosthookCallable = Callable[
    [HookContextType, RowType, List[IngestObject], IngestObjectCache], None
]
FilePostprocessorCallable = Callable[
    [HookContextType, IngestInfo, Optional[IngestObjectCache]], None
]
PrimaryKeyOverrideCallable = Callable[
    [HookContextType, RowType], IngestFieldCoordinates
]
AncestorChainOverridesCallable = Callable[[HookContextType, RowType], Dict[str, str]]


class CsvDataExtractor(Generic[HookContextType], DataExtractor):
    """Data extractor for CSV text."""

    def __init__(
        self,
        key_mapping_file: str,
        # TODO(#8905): Consider significantly simplifying this class to remove all
        #  callback / hook codepaths once all direct ingest usages have been eliminated.
        hook_context: HookContextType,
        row_pre_hooks: Optional[List[RowPrehookCallable]] = None,
        row_post_hooks: Optional[List[RowPosthookCallable]] = None,
        file_post_hooks: Optional[List[FilePostprocessorCallable]] = None,
        ancestor_chain_overrides_callback: Optional[
            AncestorChainOverridesCallable
        ] = None,
        primary_key_override_callback: Optional[PrimaryKeyOverrideCallable] = None,
        system_level: SystemLevel = SystemLevel.COUNTY,
        set_with_empty_value: bool = False,
    ) -> None:
        """
        Args:
            key_mapping_file: the path to the yaml file with key mappings
            row_pre_hooks: an ordered list of hooks for pre-processing on each
                ingested row in the CSV file
            row_post_hooks: an ordered list of hooks for post-processing on each
                ingested row in the CSV file
            ancestor_chain_overrides_callback: a callable that returns the key
            for an ancestor of the object being constructed by a row in the file
            primary_key_override_callback: a callable that returns the key for
                the primary object being constructed by a row in the file
            system_level: whether this is a COUNTY- or STATE-level ingest
            set_with_empty_value: a flag that specifies whether rows whose
                non-ignored columns are all empty should still instantiate a new
                object. Defaults to False. Typically should only be True if
                there is a file that predominantly relies on hooks or callbacks
                and may conceivably have rows with mostly empty columns.
        """
        super().__init__(key_mapping_file, should_cache=True)

        self.keys_to_ignore: List[str] = self.manifest.get("keys_to_ignore", [])
        self.ancestor_keys: Dict[str, str] = self.manifest.get("ancestor_keys", {})
        self.primary_key: Dict[str, str] = self.manifest.get("primary_key", {})
        self.child_keys: Dict[str, str] = self.manifest.get("child_key_mappings", {})
        self.enforced_ancestor_types: Dict[str, str] = self.manifest.get(
            "enforced_ancestor_types", {}
        )
        self.hook_context = hook_context
        if row_pre_hooks is None:
            row_pre_hooks = []
        self.row_pre_hooks: List[RowPrehookCallable] = row_pre_hooks

        if row_post_hooks is None:
            row_post_hooks = []
        self.row_post_hooks: List[RowPosthookCallable] = row_post_hooks

        if file_post_hooks is None:
            file_post_hooks = []
        self.file_post_hooks: List[FilePostprocessorCallable] = file_post_hooks

        self.ancestor_chain_overrides_callback: Optional[
            AncestorChainOverridesCallable
        ] = ancestor_chain_overrides_callback
        self.primary_key_override_callback: Optional[
            PrimaryKeyOverrideCallable
        ] = primary_key_override_callback
        self.system_level: SystemLevel = system_level
        self.set_with_empty_value: bool = set_with_empty_value

        self.keys.update(self.child_keys)

        self.all_keys: Set[str] = (
            set(self.keys.keys())
            | set(self.child_keys.keys())
            | set(self.keys_to_ignore)
            | set(self.ancestor_keys.keys())
            | set(self.primary_key.keys())
        )

    def extract_and_populate_data(
        self, content: Union[str, Iterable[str]], ingest_info: IngestInfo = None
    ) -> IngestInfo:
        """This function does all the work of taking the users yaml file
        and content and returning a populated data class.  This function
        iterates through every field in the object and builds a model based on
        the keys that it sees.

        Args:
            content: CSV-formatted text (Either a string with the full file
                contents, or an Interable where each element is a single line of
                contents. Not a file object.)
            ingest_info: An IngestInfo object to use, if None we create a new
                one by default

        Returns:
            A populated ingest data model for a scrape.
        """
        if ingest_info is None:
            ingest_info = IngestInfo()
        self._extract(content, ingest_info)
        self._run_file_post_hooks(ingest_info)
        return ingest_info.prune()

    def _extract(
        self, content: Union[str, Iterable[str]], ingest_info: IngestInfo
    ) -> None:
        """Converts entries in |content| and adds data to |ingest_info|."""
        if isinstance(content, str):
            rows = csv.DictReader(content.splitlines())
        elif isinstance(content, collections.abc.Iterable):
            rows = csv.DictReader(content)
        else:
            logging.error("%r is not a string or an Iterable", content)
            return

        self._extract_rows(rows, ingest_info)

    def _extract_rows(self, rows: csv.DictReader, ingest_info: IngestInfo) -> None:
        """Converts entries in |rows| and adds data to |ingest_info|."""

        if self.ingest_object_cache is None:
            raise ValueError("Ingest object cache unexpectedly None")

        seen_map: Dict[int, Set[str]] = defaultdict(set)
        for row in rows:
            row_with_stripped_cols = OrderedDict()
            for key in row:
                row_with_stripped_cols[key.strip()] = row[key]
            row = row_with_stripped_cols

            self._pre_process_row(row)
            primary_coordinates = self._primary_coordinates(row)
            ancestor_chain: Dict[str, str] = self._ancestor_chain(row)

            extracted_objects_for_row = []
            for k, v in row.items():
                k = k.strip()
                if k not in self.all_keys:
                    raise ValueError(f"Unmapped key: [{k}]")

                if k in self.keys:
                    cls, field = self.keys[k].split(".")
                    if (
                        cls == primary_coordinates.class_name
                        and field == primary_coordinates.field_name
                    ):
                        # It's possible that the primary key field has been listed in key_mappings in the YAML to make
                        # it so that section is not empty. However, if there is a primary coordinates override, we want
                        # the value to match the overridden value so we don't skip this field if the row value is empty.
                        v = primary_coordinates.field_value

                if not v and not self.set_with_empty_value:
                    continue

                column_ancestor_chain = ancestor_chain.copy()
                if k in self.child_keys:
                    child_class_to_set, _ = self.child_keys[k].split(".")
                    self._update_column_ancestor_chain_for_child_object(
                        row,
                        primary_coordinates,
                        child_class_to_set,
                        column_ancestor_chain,
                    )

                create_args = self._get_creation_args(row, k, column_ancestor_chain)

                extracted_objects_for_column = self._set_value_if_key_exists(
                    k, v, ingest_info, seen_map, column_ancestor_chain, **create_args
                )
                extracted_objects_for_row.extend(extracted_objects_for_column)

            self._post_process_row(row, extracted_objects_for_row)

        for obj_dict in self.ingest_object_cache.object_by_id_cache.values():
            for obj in obj_dict.values():
                self._clear_dummy_id(obj)

    def _update_column_ancestor_chain_for_child_object(
        self,
        row: Dict[str, str],
        primary_coordinates: IngestFieldCoordinates,
        child_class_to_set: str,
        column_ancestor_chain: Dict[str, str],
    ) -> None:
        """
        The ancestor chain for a column starts with just id values for
        ancestors of the primary object in this row. This function adds id
        values for all classes in the ancestor chain of the object
        represented by this column.

        For example, for a file with primary object state_incarceration_sentence, the
        ancestor chain would start as: {'state_person': 'my_person_id'}

        For a column corresponding to child of class
        'state_court_case', the ancestor chain might be updated to be:
        {'state_person': 'my_person_id',
         'state_incarceration_sentence': 'my_booking_id',
         'state_charge': 'DUMMY_GENERATED_ID'}
        """
        # Add primary object id to the ancestor chain for this child object
        column_ancestor_chain[
            primary_coordinates.class_name
        ] = primary_coordinates.field_value

        ancestor_class_sequence = get_ancestor_class_sequence(
            child_class_to_set, column_ancestor_chain, self.enforced_ancestor_types
        )
        i = ancestor_class_sequence.index(primary_coordinates.class_name)
        ancestor_class_sequence_below_primary = ancestor_class_sequence[i + 1 :]

        # For all children below the primary object and above this child object,
        # add to the column ancestor chain.
        for child_ancestor_class in ancestor_class_sequence_below_primary:
            child_coordinates = self._child_primary_coordinates(
                row, child_ancestor_class, column_ancestor_chain
            )
            column_ancestor_chain[
                child_coordinates.class_name
            ] = child_coordinates.field_value

    def _clear_dummy_id(self, obj: IngestObject) -> None:
        id_field_name = f"{obj.class_name()}_id"
        id_value = getattr(obj, id_field_name)
        if id_value and id_value.startswith(_DUMMY_KEY_PREFIX):
            obj.__setattr__(id_field_name, None)

    def _run_file_post_hooks(self, ingest_info: IngestInfo) -> None:
        for post_hook in self.file_post_hooks:
            post_hook(self.hook_context, ingest_info, self.ingest_object_cache)

    def _set_value_if_key_exists(
        self,
        lookup_key: str,
        value: str,
        ingest_info: IngestInfo,
        seen_map: Dict[int, Set[str]],
        ancestor_chain: Dict[str, str] = None,
        **create_args: Any,
    ) -> List[IngestObject]:
        """If the column key is one we want to set on the object, set it on the
        appropriate object instance."""
        if lookup_key in self.keys:
            return self._set_or_create_object(
                ingest_info,
                self.keys[lookup_key],
                [value],
                seen_map,
                ancestor_chain,
                self.enforced_ancestor_types,
                **create_args,
            )

        return []

    def _instantiate_person(self, ingest_info: IngestInfo) -> None:
        if self.system_level == SystemLevel.COUNTY:
            ingest_info.create_person()
        else:
            ingest_info.create_state_person()

    def _pre_process_row(self, row: Dict[str, str]) -> None:
        """Applies pre-processing on the data in the given row, before we
        try to extract ingested objects."""
        for hook in self.row_pre_hooks:
            hook(self.hook_context, row)

    def _post_process_row(
        self, row: Dict[str, str], extracted_objects: List[IngestObject]
    ) -> None:
        """Applies post-processing based on the given row, for the list of
        ingest objects that were extracted during extraction of that row."""
        unique_extracted_objects: Dict[int, IngestObject] = {}
        for extracted_object in extracted_objects:
            object_id = id(extracted_object)
            if object_id not in unique_extracted_objects:
                unique_extracted_objects[object_id] = extracted_object

        for hook in self.row_post_hooks:
            if not self.ingest_object_cache:
                raise ValueError("Must have ingest object cache to run row posthook.")
            hook(
                self.hook_context,
                row,
                list(unique_extracted_objects.values()),
                self.ingest_object_cache,
            )

    def _build_dummy_child_primary_key(
        self,
        row: Dict[str, str],
        child_class_name: str,
        column_ancestor_chain: Dict[str, str],
    ) -> str:

        child_primary_key_parts = [_DUMMY_KEY_PREFIX]

        # Append all ids of parent objects, ordered by CSV column name
        for _field_name, field_value in sorted(column_ancestor_chain.items()):
            child_primary_key_parts.append(field_value)

        # Append all values in this row that are relevant to this child object,
        # ordered by CSV column name
        cols_for_id = []
        for col, field in self.child_keys.items():
            this_col_class_name, _ = field.split(".")
            if child_class_name == this_col_class_name:
                cols_for_id.append(col)
        child_primary_key_parts += [row[col.strip()] for col in sorted(cols_for_id)]

        return "|".join(child_primary_key_parts)

    def _child_primary_coordinates(
        self,
        row: Dict[str, str],
        child_class_name: str,
        column_ancestor_chain: Dict[str, str],
    ) -> IngestFieldCoordinates:
        child_primary_key_name = f"{child_class_name}_id"
        child_primary_key_value = self._build_dummy_child_primary_key(
            row, child_class_name, column_ancestor_chain
        )
        return IngestFieldCoordinates(
            child_class_name, child_primary_key_name, child_primary_key_value
        )

    def _get_creation_args(
        self,
        row: Dict[str, str],
        lookup_key: str,
        column_ancestor_chain: Dict[str, str],
    ) -> Dict[str, str]:
        """Gets arguments needed to create a new entity, if necessary.

        For now, this just returns the primary key-esque id for the entity to
        be created or updated, to help with assembling object graphs when a
        row contains data for multiple entities.
        """

        current_field = self.keys.get(lookup_key, None)
        if not current_field:
            return {}

        primary_coordinates = self._primary_coordinates(row)

        current_class_name, _current_field_name = current_field.split(".")
        if current_class_name == primary_coordinates.class_name:
            return {primary_coordinates.field_name: primary_coordinates.field_value}

        child_primary_key_coords = self._child_primary_coordinates(
            row, current_class_name, column_ancestor_chain
        )

        return {
            child_primary_key_coords.field_name: child_primary_key_coords.field_value
        }

    def _ancestor_chain(self, row: Dict[str, str]) -> Dict[str, str]:
        """Returns the ancestor chain for this row, i.e. a mapping from ancestor
        class to the id of the ancestor corresponding to this row.

        This consists of any ancestor key mappings for the row, and any ancestor
        key override callback. If the mapping and the callback each provide an
        id for a particular ancestor class, the callback wins.
        """
        ancestor_chain = {}

        if self.ancestor_keys:
            ancestor_coordinates = self._get_coordinates_from_mapping(
                self.ancestor_keys, row
            )
            for coordinate in ancestor_coordinates:
                ancestor_chain[coordinate.class_name] = coordinate.field_value

        if self.ancestor_chain_overrides_callback:
            ancestor_addition = self.ancestor_chain_overrides_callback(
                self.hook_context, row
            )
            ancestor_chain.update(ancestor_addition)

        return ancestor_chain

    def _primary_coordinates(self, row: Dict[str, str]) -> IngestFieldCoordinates:
        """Returns the coordinates of the primary key for the main entity being
        extracted for this row.

        Unlike ancestor chain functionality which layers the ancestor key
        override atop anything in the ancestor key mapping, this provides only
        a single set of coordinates. As such, if there is a callback, we return
        its output. Otherwise, we return the primary key mapping or None.
        """
        if self.primary_key_override_callback:
            return self.primary_key_override_callback(self.hook_context, row)

        if self.primary_key:
            coordinates_list = self._get_coordinates_from_mapping(self.primary_key, row)
            coordinates = more_itertools.one(coordinates_list)
            if not coordinates.field_value:
                raise ValueError(
                    f"Found empty primary coordinates mapping in col [{self.primary_key}] for "
                    f"[{coordinates.class_name}.{coordinates.field_name}]"
                )
            return coordinates

        raise ValueError(
            "Must either define a primary_key or primary_key_override_callback,"
            "but neither found."
        )

    @staticmethod
    def _get_coordinates_from_mapping(
        key_mapping: Dict[str, str], row: Dict[str, str]
    ) -> List[IngestFieldCoordinates]:
        coordinates = []

        for lookup_key, cls_and_field in key_mapping.items():
            if not cls_and_field:
                raise TypeError(
                    f"Expected truthy key mapping [{key_mapping}] to have a value inside"
                )

            cls, field = cls_and_field.split(".")
            id_value = row[lookup_key.strip()]

            coordinates.append(IngestFieldCoordinates(cls, field, id_value))

        return coordinates
