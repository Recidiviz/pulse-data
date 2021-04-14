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
""" Contains Marshmallow schemas for our API """
from typing import Dict, Type

from flask import request
from marshmallow import fields, validate, Schema
from marshmallow.fields import Field

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType


def camelcase(s: str) -> str:
    parts = iter(s.split("_"))
    return next(parts) + "".join(i.title() for i in parts)


class CamelCaseSchema(Schema):
    """Schema that uses camel-case for its external representation
    and snake-case for its internal representation.
    """

    def on_bind_field(self, field_name: str, field_obj: Field) -> None:
        field_obj.data_key = camelcase(field_obj.data_key or field_name)


class PolicyRequirementsSchema(CamelCaseSchema):
    """ Schema for retrieving policy requirements """

    state = fields.Str(validate=validate.OneOf(["US_ID"]), required=True)


class CaseUpdateSchema(CamelCaseSchema):
    """ Schema for submitting case updates """

    person_external_id = fields.Str(required=True)
    actions = fields.List(
        fields.Str(
            validate=validate.OneOf(
                [action_type.value for action_type in CaseUpdateActionType]
            ),
        ),
        required=True,
    )
    other_text = fields.Str(default=None)


def load_api_schema(api_schema: Type[CamelCaseSchema]) -> Dict:
    return api_schema().load(request.json)
