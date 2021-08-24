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

DataExtractors are initialized with two categories of keys: single-value keys
(|self.keys|) and multi-keys (|self.multi_keys|). Single-value keys have one
value per unit of data: for example, a page might have one 'DOB' field per
table. Multi-key values are extracted from the content in a sequence: for
example, a page might have a table with 'Charge Name' and 'Bond Amount' columns,
such that the association of charge name and bond amount is inferred by their
order (so the fifth bond amount corresponds to the fifth charge).

Implementations of the DataExtractor walk through structured content and call
_set_or_create_object, which builds an IngestInfo incrementally. The behavior of
this method is different for single-key values and multi-key values, and will
either set a single value or a list of values on the most recent object or
list of objects; or create new objects as needed.
"""

import abc
import logging
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import yaml
from lxml.html import HtmlElement

from recidiviz.ingest.models.ingest_info import PLURALS, IngestInfo, IngestObject
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.ingest.models.ingest_object_hierarchy import get_ancestor_class_sequence


class DataExtractor(metaclass=abc.ABCMeta):
    """Base class for automatically extracting data from a file."""

    def __init__(self, key_mapping_file: str, should_cache: bool = False):
        """The init of the data extractor.

        Args:
            key_mapping_file: a yaml file defining the mappings that the
                data extractor uses to find relevant keys.
            should_cache: a flag that specifies whether we should proactively
                cache each created object by its primary key id. Defaults to
                False because it's typically not necessary unless you're
                stitching together IngestInfo object graphs where id mappings
                don't suffice.
        """
        with open(key_mapping_file, "r", encoding="utf-8") as ymlfile:
            self.manifest = yaml.full_load(ymlfile)
        self.keys = self.manifest.get("key_mappings", {})
        self.multi_keys = self.manifest.get("multi_key_mapping", {})

        if not self.keys:
            self.keys = {}

        self.ingest_object_cache: Optional[IngestObjectCache] = (
            IngestObjectCache() if should_cache else None
        )

        # We want to know which of the classes are multi keys as this helps
        # us with behaviour when we set the values.
        self.multi_key_classes = set(
            value.split(".")[0] if isinstance(value, str) else value[0].split(".")[0]
            for value in self.multi_keys.values()
        )

    @abc.abstractmethod
    def extract_and_populate_data(
        self, content: HtmlElement, ingest_info: IngestInfo = None
    ) -> IngestInfo:
        pass

    def _set_or_create_object(
        self,
        ingest_info: IngestInfo,
        lookup_keys: Union[str, List[str]],
        values: List[Optional[str]],
        seen_map: Dict[int, Set[str]],
        ancestor_chain: Dict[str, str] = None,
        enforced_ancestor_types: Dict[str, str] = None,
        **create_args: Any,
    ) -> List[IngestObject]:
        """Contains the logic to set or create a field on an ingest object.
        The logic here is that we check if we have an object with a matching id
        already to check if the field is already set.  If the field we care
        about is already set, we say that we need to create a new object and set
        the field.

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
        lookup_keys = lookup_keys if isinstance(lookup_keys, list) else [lookup_keys]
        extracted_objects = []

        if not ancestor_chain:
            ancestor_chain = {}

        if not enforced_ancestor_types:
            enforced_ancestor_types = {}

        for lookup_key in lookup_keys:
            class_to_set, ingest_key = lookup_key.split(".")
            is_multi_key = class_to_set in self.multi_key_classes
            for i, value in enumerate(values):
                object_to_set = self._get_object_to_set(
                    ingest_info,
                    seen_map,
                    class_to_set,
                    ingest_key,
                    i,
                    is_multi_key,
                    ancestor_chain,
                    enforced_ancestor_types,
                    **create_args,
                )

                logging.debug(
                    "Setting value [%s] on field [%s] of object [%s]",
                    value,
                    ingest_key,
                    object_to_set,
                )
                setattr(object_to_set, ingest_key, value)
                extracted_objects.append(object_to_set)
                seen_map[id(object_to_set)].add(ingest_key)

        return extracted_objects

    def _get_parent(
        self,
        ingest_info: IngestInfo,
        class_to_set: str,
        index: int,
        is_multi_key: bool,
        ancestor_chain: Dict[str, str],
        enforced_ancestor_types: Dict[str, str],
        **create_args: Any,
    ) -> IngestObject:
        """Finds or creates the parent of the object we are going to set, which
        may need to have its own parent created if it is a hold or charge in a
        multi-key column."""
        ancestor_class_sequence = get_ancestor_class_sequence(
            class_to_set, ancestor_chain, enforced_ancestor_types
        )

        # Multi-keys may need to be indexed by their parent, i.e. a bond at
        # index 3 has the parent charge at index 3.
        if is_multi_key and ancestor_class_sequence:
            parent_cls_to_set = ancestor_class_sequence[-1]
            parent_ancestor_class_sequence = ancestor_class_sequence[:-1]
            grandparent_cls_to_set = (
                parent_ancestor_class_sequence[-1]
                if parent_ancestor_class_sequence
                else None
            )
            grandparent = self._find_parent_ingest_object(
                ingest_info, parent_ancestor_class_sequence, index, ancestor_chain
            )

            recent = _get_by_id_or_recent_if_no_cache(
                self.ingest_object_cache, grandparent, parent_cls_to_set, ancestor_chain
            )

            # If |index| was used to index the grandparent, use the most
            # recent parent if one exists.
            if (
                parent_cls_to_set in self.multi_key_classes
                and grandparent_cls_to_set in self.multi_key_classes
                and recent
            ):
                return recent

            # If |index| should be used to index the parent, find the parent.
            if class_to_set not in PLURALS or class_to_set == "booking":
                list_of_parents = getattr(grandparent, PLURALS[parent_cls_to_set])
                if index < len(list_of_parents):
                    return list_of_parents[index]

            # If the parent doesn't exist, create it.
            if (
                class_to_set not in PLURALS
                or parent_cls_to_set in self.multi_key_classes
            ):
                return self._create(grandparent, parent_cls_to_set)

        if self.ingest_object_cache:
            parent = self._get_cached_parent(
                ingest_info, class_to_set, ancestor_chain, ancestor_class_sequence
            )

            if parent is None:
                raise ValueError(f"_get_cached_parent returned none: {class_to_set}")
            return parent

        return self._find_parent_ingest_object(
            ingest_info, ancestor_class_sequence, index, ancestor_chain, **create_args
        )

    def _get_object_to_set(
        self,
        ingest_info: IngestInfo,
        seen_map: Dict[int, Set[str]],
        class_to_set: str,
        ingest_key: str,
        index: int,
        is_multi_key: bool,
        ancestor_chain: Dict[str, str],
        enforced_ancestor_types: Dict[str, str],
        **create_args: Any,
    ) -> IngestObject:
        """Finds or creates the object we are going to set, which may already
        exist in the multi-key case."""

        if create_args and self.ingest_object_cache:
            # First, check the cache for an object with a matching ID
            obj_to_set_id_name = f"{class_to_set}_id"
            if obj_to_set_id_name in create_args:
                obj_to_set_id = create_args[obj_to_set_id_name]
                cached_object_to_set = self.ingest_object_cache.get_object_by_id(
                    class_to_set, obj_to_set_id
                )

                if cached_object_to_set is not None:
                    return cached_object_to_set

        object_to_set = None

        # If we don't find a cached object with a matching id, look on the
        # parent object for a potential match.
        parent = self._get_parent(
            ingest_info,
            class_to_set,
            index,
            is_multi_key,
            ancestor_chain,
            enforced_ancestor_types,
        )

        if is_multi_key and class_to_set in PLURALS:
            list_of_class_to_set = getattr(parent, PLURALS[class_to_set])
            if (
                list_of_class_to_set is not None
                and isinstance(list_of_class_to_set, list)
                and len(list_of_class_to_set) > index
            ):
                object_to_set = list_of_class_to_set[index]
            else:
                object_to_set = _get_by_id_or_recent_if_no_cache(
                    self.ingest_object_cache, parent, class_to_set, ancestor_chain
                )
        else:
            object_to_set = _get_by_id_or_recent_if_no_cache(
                self.ingest_object_cache, parent, class_to_set, ancestor_chain
            )

        if object_to_set is not None and ingest_key not in seen_map[id(object_to_set)]:
            return object_to_set

        # If the object we are trying to operate on is None, or it has
        # already set the ingest_key then we know we need to create a
        # new one.
        logging.debug("Creating new [%s] on parent object [%s]", class_to_set, parent)
        object_to_set = self._create(parent, class_to_set, **create_args)

        return object_to_set

    def _find_parent_ingest_object(
        self,
        ingest_info: IngestInfo,
        hierarchy: Sequence[str],
        val_index: int,
        ancestor_chain: Optional[Dict[str, str]] = None,
        **create_args: Any,
    ) -> IngestObject:
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
        if ancestor_chain is None:
            ancestor_chain = {}

        parent = ingest_info
        for hier_class in hierarchy:
            # If we are a multi key class instead of getting the parent object
            # by id, we use the index of the value we found, if it exists.
            if (
                hier_class in self.multi_key_classes
                and len(getattr(parent, PLURALS[hier_class])) > val_index
            ):
                parent = getattr(parent, PLURALS[hier_class])[val_index]
            else:
                old_parent = parent

                parent = _get_by_id_or_recent_if_no_cache(
                    self.ingest_object_cache, old_parent, hier_class, ancestor_chain
                )

                if parent is None:
                    # If we are creating a parent where we have a parent id that
                    # doesn't appear to point to an existing object, initialize
                    # that parent with the provided parent id.
                    parent_id = ancestor_chain.get(hier_class)
                    parent_create_args = {
                        **create_args,
                        **{hier_class + "_id": parent_id},
                    }
                    parent = self._create(old_parent, hier_class, **parent_create_args)

        return parent

    def _get_cached_parent(
        self,
        ingest_info: IngestInfo,
        class_to_set: str,
        ancestor_chain: Dict[str, str],
        hierarchy_sequence: Sequence[str],
    ) -> IngestObject:
        """Retrieves the cached parent instance of the object we are trying to
        set, i.e. the object at the end of the |hierarchy_sequence|, based on
        the given |ancestor_chain|. If there are gaps in the chain of parents up
        from this class, i.e. the grandparent is known but not the direct
        parent, then those gaps are proactively filled by creating entities in
        the chain."""
        (
            closest_ancestor_class,
            closest_ancestor,
            remaining_ancestors,
        ) = self._get_closest_cached_ancestor(ancestor_chain, hierarchy_sequence)

        if not closest_ancestor_class:
            ancestor_objs: List[Tuple[str, IngestObject]] = [
                ("ingest_info", ingest_info)
            ]
        else:
            ancestor_objs = [(closest_ancestor_class, closest_ancestor)]

        for next_ancestor_class in remaining_ancestors:
            _, last_ancestor = ancestor_objs[-1]

            next_ancestor = _get_by_id_or_recent_if_no_cache(
                self.ingest_object_cache,
                last_ancestor,
                next_ancestor_class,
                ancestor_chain,
            )

            if next_ancestor is None:
                self._overwrite_last_ancestor_obj_if_cannot_create_next_child(
                    ancestor_objs, next_ancestor_class
                )
                _, last_ancestor = ancestor_objs[-1]

                next_ancestor_id = ancestor_chain.get(next_ancestor_class)
                next_ancestor = self._create(
                    last_ancestor,
                    next_ancestor_class,
                    **{next_ancestor_class + "_id": next_ancestor_id},
                )

            ancestor_objs.append((next_ancestor_class, next_ancestor))

        self._overwrite_last_ancestor_obj_if_cannot_create_next_child(
            ancestor_objs, class_to_set
        )

        # The parent is the last ancestor object in the chain
        return ancestor_objs[-1][1]

    def _overwrite_last_ancestor_obj_if_cannot_create_next_child(
        self, ancestor_objs: List[Tuple[str, IngestObject]], next_child_class: str
    ) -> None:
        """Our ingest algorithm generates dummy placeholder objects with null
        ids. We may add multiple child objects to this dummy object when in
        reality, the dummy object will eventually be split into multiple
        objects. In the case that the schema does not allow multiple child
        objects of a given type (e.g. Charge -> CourtCase), we need to create
        a new dummy object to attach a child object to if the matched dummy
        parent already has an object of that type. This function does that work.
        """
        last_ancestor_class, last_ancestor = ancestor_objs[-1]

        # This is a hack that assumes that singular child fields will match the
        # name of the child class.
        if (
            len(ancestor_objs) > 1
            and hasattr(last_ancestor, next_child_class)
            and getattr(last_ancestor, next_child_class) is not None
        ):
            _, grandparent = ancestor_objs[-2]
            last_ancestor = self._create(
                grandparent, last_ancestor_class, **{last_ancestor_class + "_id": None}
            )

            ancestor_objs[-1] = (last_ancestor_class, last_ancestor)

    def _get_closest_cached_ancestor(
        self, ancestor_chain: Dict[str, str], hierarchy_sequence: Sequence[str]
    ):
        """Finds the ancestor "closest" to the |class_to_set| that is currently
        in the cache, returning a tuple of that ancestor's class name, the
        ancestor instance itself, and then the remaining sequence of descendants
        of ancestor, running to the |class_to_set|.

        For example, if the class to set is sentence, and the booking in the
        given |ancestor_chain| is in the cache, then this returns a tuple of
        'booking', the booking object, and then a sequence of ('charge'). If the
        booking is not in the cache, this returns a tuple of 'person', the
        person object, and then a sequence of ('booking', 'charge').
        """
        for i, hier_class in enumerate(reversed(hierarchy_sequence)):
            # hier_class may not be in ancestor_chain if the ancestor chain
            # derived from the content being extracted is partial
            if hier_class in ancestor_chain:
                if not self.ingest_object_cache:
                    raise ValueError("The ingest_object_cache must not be null.")
                parent = self.ingest_object_cache.get_object_by_id(
                    hier_class, ancestor_chain[hier_class]
                )
                if parent:
                    remaining_uncached_ancestors = (
                        hierarchy_sequence[-i:] if i > 0 else []
                    )

                    return hier_class, parent, remaining_uncached_ancestors
        return None, None, hierarchy_sequence

    def _create(self, parent: IngestObject, class_name: str, **create_args: Any):
        created_obj = getattr(parent, "create_" + class_name)(**create_args)

        if self.ingest_object_cache:
            id_field = f"{class_name}_id"
            # It is possible that we do not have an id to proactively set on the
            # newly instantiated object, namely if there is no primary key
            # identified for the extraction content
            if create_args and id_field in create_args:
                self.ingest_object_cache.cache_object_by_id(
                    class_name, create_args[id_field], created_obj
                )

        return created_obj


def _get_by_id_or_recent_if_no_cache(
    ingest_object_cache: Optional[IngestObjectCache],
    parent: IngestObject,
    class_name: str,
    ancestor_chain: Dict[str, str],
):
    class_id = ancestor_chain.get(class_name)
    if ingest_object_cache is not None:
        if class_id is not None:
            return ingest_object_cache.get_object_by_id(class_name, class_id)

        return _get_by_id(parent, class_name, None)

    if class_id is not None:
        return _get_by_id(parent, class_name, class_id)

    return _get_recent(parent, class_name)


def _get_recent(parent: IngestObject, class_name: str):
    return getattr(parent, "get_recent_" + class_name)()


def _get_by_id(parent: IngestObject, class_name: str, class_id: Optional[str]):
    return getattr(parent, "get_" + class_name + "_by_id")(class_id)
