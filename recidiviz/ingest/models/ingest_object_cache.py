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

"""Caching utilities for IngestInfo."""
from collections import defaultdict
from typing import Dict, Any, Optional


def _recursive_defaultdict():
    return defaultdict(_recursive_defaultdict)


class IngestObjectCache:
    """A cache of ingest objects.

    The cache is a dictionary of dictionaries. The outer key is the name of the
    class, e.g. `booking`. The inner key is the primary key id of the object.
    The inner value is the object itself.

    Each layer is a default dictionary, so one can safely make GET and PUT
    references without worrying that an intermediate key may not be present.
    """

    def __init__(self):
        self.object_by_id_cache: Dict[str, Dict[str, Any]] = _recursive_defaultdict()

    def cache_object_by_id(self, class_name: str, obj_id: Optional[str], obj: Any):
        if obj_id is None:
            return
        self.object_by_id_cache[class_name][obj_id] = obj

    def get_object_by_id(self, class_name: str, obj_id: Optional[str]) -> Any:
        if class_name not in self.object_by_id_cache:
            return None

        if not obj_id or obj_id not in self.object_by_id_cache[class_name]:
            return None

        return self.object_by_id_cache[class_name][obj_id]

    def clear_object_by_id(self, class_name: str, obj_id: Optional[str]):
        if obj_id is None:
            return

        if (
            class_name in self.object_by_id_cache
            and obj_id in self.object_by_id_cache[class_name]
        ):
            del self.object_by_id_cache[class_name][obj_id]

    def get_objects_of_type(self, class_name: str):
        if class_name not in self.object_by_id_cache:
            return []
        return self.object_by_id_cache[class_name].values()
