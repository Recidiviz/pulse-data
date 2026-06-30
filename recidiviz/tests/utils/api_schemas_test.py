# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for recidiviz/utils/api_schemas.py."""
import json
import unittest
from http import HTTPStatus

from flask import Flask, Response, g, jsonify
from marshmallow import ValidationError, fields
from pydantic import BaseModel
from werkzeug.test import TestResponse

from recidiviz.utils.api_schemas import (
    CamelCaseSchema,
    CamelOrSnakeCaseSchema,
    load_api_schema,
    requires_api_schema,
    requires_pydantic_schema,
)

app = Flask(__name__)
app.config["TESTING"] = True
app.config["WTF_CSRF_ENABLED"] = False


class _PersonSchema(CamelCaseSchema):
    first_name = fields.Str(required=True)
    last_name = fields.Str(required=True)


class _FlexSchema(CamelOrSnakeCaseSchema):
    first_name = fields.Str(required=True)


class _PersonModel(BaseModel):
    first_name: str
    last_name: str


@app.route("/marshmallow", methods=["POST"])
@requires_api_schema(_PersonSchema)
def _marshmallow_route() -> Response:
    return jsonify(g.api_data)


@app.route("/pydantic", methods=["POST"])
@requires_pydantic_schema(_PersonModel)
def _pydantic_route(parsed_request_body: _PersonModel) -> Response:
    return jsonify({"first_name": parsed_request_body.first_name})


class CamelCaseSchemaTest(unittest.TestCase):
    def test_load_camel_case_input(self) -> None:
        result = _PersonSchema().load({"firstName": "Frodo", "lastName": "Baggins"})
        self.assertEqual({"first_name": "Frodo", "last_name": "Baggins"}, result)

    def test_dump_produces_camel_case_keys(self) -> None:
        result = _PersonSchema().dump({"first_name": "Frodo", "last_name": "Baggins"})
        self.assertIn("firstName", result)
        self.assertIn("lastName", result)
        self.assertNotIn("first_name", result)

    def test_unknown_fields_raise(self) -> None:
        with self.assertRaises(ValidationError):
            _PersonSchema().load(
                {"firstName": "Frodo", "lastName": "Baggins", "age": 50}
            )


class CamelOrSnakeCaseSchemaTest(unittest.TestCase):
    def test_load_camel_case_input(self) -> None:
        result = _FlexSchema().load({"firstName": "Frodo"})
        self.assertEqual({"first_name": "Frodo"}, result)

    def test_load_snake_case_input(self) -> None:
        result = _FlexSchema().load({"first_name": "Frodo"})
        self.assertEqual({"first_name": "Frodo"}, result)


class LoadApiSchemaTest(unittest.TestCase):
    def test_loads_dict(self) -> None:
        result = load_api_schema(
            _PersonSchema, {"firstName": "Frodo", "lastName": "Baggins"}
        )
        self.assertEqual({"first_name": "Frodo", "last_name": "Baggins"}, result)

    def test_unknown_fields_raise(self) -> None:
        with self.assertRaises(ValidationError):
            load_api_schema(
                _PersonSchema,
                {"firstName": "Frodo", "lastName": "Baggins", "age": 50},
            )


class RequiresApiSchemaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = app.test_client()

    def _post(self, data: dict) -> TestResponse:
        return self.client.post(
            "/marshmallow",
            data=json.dumps(data),
            content_type="application/json",
        )

    def test_valid_body_is_loaded(self) -> None:
        resp = self._post({"firstName": "Frodo", "lastName": "Baggins"})
        self.assertEqual(HTTPStatus.OK, resp.status_code)
        self.assertEqual(
            {"first_name": "Frodo", "last_name": "Baggins"}, resp.get_json()
        )

    def test_missing_required_field_raises(self) -> None:
        with self.assertRaises(ValidationError):
            self._post({"firstName": "Frodo"})


class RequiresPydanticSchemaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = app.test_client()

    def _post(self, data: dict) -> TestResponse:
        return self.client.post(
            "/pydantic",
            data=json.dumps(data),
            content_type="application/json",
        )

    def test_valid_body_is_passed_as_kwarg(self) -> None:
        resp = self._post({"first_name": "Frodo", "last_name": "Baggins"})
        self.assertEqual(HTTPStatus.OK, resp.status_code)
        self.assertEqual({"first_name": "Frodo"}, resp.get_json())

    def test_invalid_body_returns_400(self) -> None:
        resp = self._post({"last_name": "Baggins"})
        self.assertEqual(HTTPStatus.BAD_REQUEST, resp.status_code)
