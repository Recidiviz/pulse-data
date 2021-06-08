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
from functools import wraps
from typing import Any, Callable, Dict, List, Type

from flask import g, request
from marshmallow import Schema, ValidationError, fields, validate
from marshmallow.fields import Field
from marshmallow_enum import EnumField

from recidiviz.case_triage.case_updates.types import CaseUpdateActionType
from recidiviz.case_triage.client_info.types import PreferredContactMethod
from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityType,
)


def camelcase(s: str) -> str:
    parts = iter(s.split("_"))
    return next(parts) + "".join(i.title() for i in parts)


def non_empty_string(data: str) -> None:
    if not data:
        raise ValidationError("Field must be non-empty.")


class CamelCaseSchema(Schema):
    """Schema that uses camel-case for its external representation
    and snake-case for its internal representation.
    """

    def on_bind_field(self, field_name: str, field_obj: Field) -> None:
        field_obj.data_key = camelcase(field_obj.data_key or field_name)


class PolicyRequirementsSchema(CamelCaseSchema):
    state = fields.Str(validate=validate.OneOf(["US_ID"]), required=True)


class CaseUpdateSchema(CamelCaseSchema):
    person_external_id = fields.Str(required=True)
    action_type = EnumField(CaseUpdateActionType, by_value=True, required=True)
    comment = fields.Str(default=None)


class DeferOpportunitySchema(CamelCaseSchema):
    person_external_id = fields.Str(required=True)
    opportunity_type = EnumField(OpportunityType, by_value=True, required=True)
    deferral_type = EnumField(OpportunityDeferralType, by_value=True, required=True)
    defer_until = fields.DateTime(required=True)
    request_reminder = fields.Boolean(required=True)


class PreferredContactMethodSchema(CamelCaseSchema):
    person_external_id = fields.Str(required=True)
    contact_method = EnumField(PreferredContactMethod, by_value=True, required=True)


class PreferredNameSchema(CamelCaseSchema):
    person_external_id = fields.Str(required=True)
    name = fields.Str(required=True, allow_none=True)


class CreateNoteSchema(CamelCaseSchema):
    person_external_id = fields.Str(required=True)
    text = fields.Str(required=True, validate=non_empty_string)


class ResolveNoteSchema(CamelCaseSchema):
    note_id = fields.Str(required=True)


class UpdateNoteSchema(CamelCaseSchema):
    note_id = fields.Str(required=True)
    text = fields.Str(required=True, validate=non_empty_string)


def requires_api_schema(api_schema: Type[Schema]) -> Callable:
    def inner(route: Callable) -> Callable:
        @wraps(route)
        def decorated(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
            g.api_data = api_schema().load(request.json)

            return route(*args, **kwargs)

        return decorated

    return inner
