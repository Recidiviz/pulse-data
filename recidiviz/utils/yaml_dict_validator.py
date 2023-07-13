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
"""Functionality to validate YAML dicts in tests."""

import json
import os
from urllib.parse import urlparse

import jsonschema
from referencing import Registry, Resource

from recidiviz.utils.yaml_dict import YAMLDict


def validate_yaml_matches_schema(yaml_dict: YAMLDict, json_schema_path: str) -> None:
    """Validates that the YAML dict matches the JSON schema at the provided path. If
    validation fails, raises jsonschema.exceptions.ValidationError."""
    with open(json_schema_path, encoding="utf-8") as f:
        schema = json.load(f)

    json_schema_dir = os.path.dirname(os.path.abspath(json_schema_path))

    def handle_file(uri: str) -> Resource:
        # Find relative path of URI to base schema URI
        rel_path = os.path.relpath(
            urlparse(uri).path, os.path.dirname(urlparse(schema["$id"]).path)
        )

        # Build local path to file referenced by the URI
        reference_path = os.path.normpath(os.path.join(json_schema_dir, rel_path))
        with open(reference_path, encoding="utf-8") as reference_file:
            contents = json.load(reference_file)
            return Resource.from_contents(contents=contents)

    registry: Registry = Registry(retrieve=handle_file)
    validator = jsonschema.Draft202012Validator(schema=schema, registry=registry)

    validator.validate(yaml_dict.raw_yaml)
