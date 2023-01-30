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
""" Contains utils for API Marshmallow schemas"""
from functools import wraps
from typing import Any, Callable, Dict, Iterable, List, Mapping, Type, Union

from flask import g, request
from marshmallow import RAISE, Schema, pre_load
from marshmallow.fields import Field

from recidiviz.common.str_field_utils import snake_to_camel, to_snake_case
from recidiviz.utils.types import assert_type


class CamelCaseSchema(Schema):
    """
    Schema that uses camel-case for its external representation
    and snake-case for its internal representation.
    """

    def on_bind_field(self, field_name: str, field_obj: Field) -> None:
        field_obj.data_key = snake_to_camel(field_obj.data_key or field_name)


class CamelOrSnakeCaseSchema(Schema):
    """
    Schema that deserializes top-level keys from camel or snake-case and serializes to snake case.
    """

    @pre_load
    def preprocess_keys(
        self, data: Dict[str, Any], **_kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        return {to_snake_case(k): v for k, v in data.items()}


def load_api_schema(api_schema: Union[type, Type[Schema]], source_data: Any) -> Dict:
    data: Union[Mapping[str, Any], Iterable[Mapping[str, Any]]]

    try:
        data = assert_type(source_data, dict)
    except ValueError:
        data = assert_type(source_data, list)

    return api_schema(unknown=RAISE).load(data)


def requires_api_schema(api_schema: Type[Schema]) -> Callable:
    def inner(route: Callable) -> Callable:
        @wraps(route)
        def decorated(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            g.api_data = load_api_schema(api_schema, request.json)

            return route(*args, **kwargs)

        return decorated

    return inner
