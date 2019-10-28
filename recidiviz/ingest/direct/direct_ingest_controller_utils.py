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
"""Util functions shared across multiple types of hooks in the direct
ingest controllers."""
from recidiviz.ingest.models.ingest_info import IngestObject


def create_if_not_exists(obj: IngestObject,
                         parent_obj: IngestObject,
                         objs_field_name: str):
    """Create an object on |parent_obj| if an identical object does not
    already exist.
    """

    existing_objects = getattr(parent_obj, objs_field_name) or []
    if isinstance(existing_objects, IngestObject):
        existing_objects = [existing_objects]

    for existing_obj in existing_objects:
        if obj == existing_obj:
            return
    create_func = getattr(parent_obj, f'create_{obj.class_name()}')
    create_func(**obj.__dict__)
