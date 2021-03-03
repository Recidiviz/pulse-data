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
"""Helper functions to serialize and deserialize arbitrary attrs types."""

import datetime
import importlib
from typing import Dict, Any

import attr
import cattr


def datetime_to_serializable(dt: datetime.datetime) -> str:
    return dt.isoformat()


def serializable_to_datetime(date_str: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(date_str)


# pylint:disable=unnecessary-lambda
def attr_to_json_dict(attr_obj: attr.Attribute) -> Dict[str, Any]:
    """Converts an attr-defined object to a JSON dict. The resulting dict should
    be unstructured using |attr_from_json_dict| below, which uses the __module__
    and __classname__ fields to reconstruct the object using the same Converter
    used to unstructure it here."""
    converter = cattr.Converter()
    converter.register_unstructure_hook(  # type: ignore[misc]
        datetime.datetime, lambda d: datetime_to_serializable(d)
    )
    attr_dict = cattr.unstructure(attr_obj)
    attr_dict["__classname__"] = attr_obj.__class__.__name__
    attr_dict["__module__"] = attr_obj.__module__
    return attr_dict


def attr_from_json_dict(attr_dict: Dict[str, Any]) -> attr.Attribute:
    """Converts a JSON dict created by |attr_to_json_dict| above into the attr
    object it was originally created from."""
    module = importlib.import_module(attr_dict.pop("__module__"))
    cls = getattr(module, attr_dict.pop("__classname__"))

    converter = cattr.Converter()
    converter.register_structure_hook(
        datetime.datetime, lambda date_str, _: serializable_to_datetime(date_str)
    )
    return converter.structure(attr_dict, cls)
