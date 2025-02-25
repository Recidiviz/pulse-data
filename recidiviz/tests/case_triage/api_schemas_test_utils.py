# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Implements tests for the Case Triage Flask server."""
from typing import Any, Callable, Dict, List, Optional, Type
from unittest import TestCase

from marshmallow import Schema, ValidationError

from recidiviz.common.str_field_utils import snake_to_camel


class SchemaTestCase(TestCase):
    camel_case: bool = True
    schema: Type[Schema]


def valid_schema_test(data: Dict[Any, Any]) -> Callable:
    def inner(self: SchemaTestCase) -> None:
        schema = self.schema()
        self.assertIsNotNone(schema.load(data), schema.validate(data))

    return inner


def invalid_schema_test(
    data: Dict[Any, Any], invalid_keys: Optional[List[str]] = None
) -> Callable:
    invalid_keys = [] if invalid_keys is None else invalid_keys

    def inner(self: SchemaTestCase) -> None:
        schema = self.schema()
        with self.assertRaises(ValidationError) as exception_context:
            schema.load(data)

        if invalid_keys:
            for key in invalid_keys:
                # Catch keys in tests that have not been updated when fields are renamed
                self.assertIn(key, schema.fields.keys())
                self.assertIn(
                    snake_to_camel(key) if self.camel_case else key,
                    exception_context.exception.messages,
                )

    return inner
