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
"""Marshmallow schemas for the auth api"""

from datetime import timezone
from typing import Any, Dict

from flask_smorest.fields import Upload
from marshmallow import fields, post_load, pre_dump
from sqlalchemy.engine.row import Row

from recidiviz.case_triage.api_schemas_utils import (
    CamelCaseSchema,
    CamelOrSnakeCaseSchema,
)


class UserSchema(CamelCaseSchema):
    """Schema expected by /auth endpoints that take or return a user or list of users."""

    email_address = fields.Email(required=True)
    state_code = fields.Str(required=True)
    # Set allow_none on all non-required fields to allow returning instances of UserOverride where
    # column values may be None
    external_id = fields.Str(allow_none=True)
    roles = fields.List(fields.Str, allow_none=True)
    district = fields.Str(allow_none=True)
    first_name = fields.Str(allow_none=True)
    last_name = fields.Str(allow_none=True)
    user_hash = fields.Str(allow_none=True)
    pseudonymized_id = fields.Str(allow_none=True)
    blocked_on = fields.DateTime(allow_none=True)

    @post_load
    # pylint: disable=unused-argument
    def process_input(
        self, data: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process input after it has been deserialized (converted from its external JSON
        representation into python data types), but before it gets passed to our endpoints.
        """
        # Enforce casing for columns where we have a preference.
        # Even though email_address and state_code are defined as required here, they might not be
        # set if the input to an endpoint declares the schema as partial (like in a PATCH)
        if email_address := data.get("email_address"):
            data["email_address"] = email_address.lower()

        if state_code := data.get("state_code"):
            data["state_code"] = state_code.upper()

        if external_id := data.get("external_id"):
            data["external_id"] = external_id.upper()

        return data

    @pre_dump
    # pylint: disable=unused-argument
    def process_output(
        self, data: Dict | Row, **kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process response before it gets serialized (converted from Python data types to
        JSON).
        """
        if not isinstance(data, dict):
            data = dict(data.__dict__)

        if blocked_on := data.get("blocked_on"):
            data["blocked_on"] = blocked_on.astimezone(timezone.utc)

        return data


class ReasonSchema(CamelCaseSchema):
    reason = fields.Str()


class UserRequestSchema(UserSchema, ReasonSchema):
    pass


class PermissionsSchema(CamelCaseSchema):
    routes = fields.Dict(keys=fields.Str(), values=fields.Bool())
    feature_variants = fields.Dict(keys=fields.Str(), values=fields.Raw())
    allowed_apps = fields.Dict(keys=fields.Str(), values=fields.Bool())
    jii_permissions = fields.Dict(keys=fields.Str(), values=fields.Bool())


class PermissionsRequestSchema(PermissionsSchema, ReasonSchema):
    pass


class PermissionsResponseSchema(PermissionsSchema, CamelCaseSchema):
    email_address = fields.Email(required=True)


class FullUserSchema(UserSchema, PermissionsSchema, CamelCaseSchema):
    @pre_dump
    def process_input(
        self, data: dict | Row, **kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Process input before serialization to convert to dict if not already so
        that we can access the attributes with the .get() method below.
        """
        if isinstance(data, dict):
            return data
        return dict(data.__dict__)

    allowed_supervision_location_ids = fields.Function(
        (lambda user: user.get("district") if user.get("state_code") == "US_MO" else "")
    )
    allowed_supervision_location_level = fields.Function(
        lambda user: (
            "level_1_supervision_location"
            if user.get("state_code") == "US_MO" and user.get("district") is not None
            else ""
        )
    )


class StateCodeSchema(CamelOrSnakeCaseSchema):
    state_code = fields.Str(required=True)

    @post_load
    # pylint: disable=unused-argument
    def process_input(
        self, data: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process input after it has been deserialized (converted from its external JSON
        representation into python data types), but before it gets passed to our endpoints.
        """
        data["state_code"] = data["state_code"].upper()
        return data


class UploadSchema(CamelCaseSchema):
    file = Upload()
