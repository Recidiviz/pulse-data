# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements stubs for jsonschema library."""
from collections.abc import Mapping
from typing import Any

from referencing import jsonschema

from recidiviz.utils.yaml_dict import YAMLDictType

class Draft202012Validator:
    def __init__(
        self, registry: jsonschema.Registry, schema: Mapping[str, Any]
    ) -> None: ...
    def validate(self, yaml_dict: YAMLDictType) -> None: ...
