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

from typing import Any, Dict

from marshmallow import fields, post_load

from recidiviz.case_triage.api_schemas_utils import CamelCaseSchema


class UserSchema(CamelCaseSchema):
    email_address = fields.Email(required=True)
    state_code = fields.Str(required=True)
    # Set allow_none on all non-required fields to allow returning instances of UserOverride where
    # column values may be None
    external_id = fields.Str(allow_none=True)
    role = fields.Str(allow_none=True)
    district = fields.Str(allow_none=True)
    first_name = fields.Str(allow_none=True)
    last_name = fields.Str(allow_none=True)
    user_hash = fields.Str(allow_none=True)

    @post_load
    # pylint: disable=unused-argument
    def process_input(
        self, data: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process input after it has been deserialized (converted from its external JSON
        representation into python data types), but before it gets passed to our endpoints."""
        # Enforce casing for columns where we have a preference.
        data["email_address"] = data["email_address"].lower()
        data["state_code"] = data["state_code"].upper()
        data["external_id"] = (
            data["external_id"].upper() if data.get("external_id") is not None else None
        )
        return data


class ReasonMixin:
    reason = fields.Str()


class UserRequestSchema(UserSchema, ReasonMixin):
    pass


class FullUserSchema(UserSchema, CamelCaseSchema):
    allowed_supervision_location_ids = fields.Function(
        (lambda user: user.district if user.state_code == "US_MO" else "")
    )
    allowed_supervision_location_level = fields.Function(
        lambda user: "level_1_supervision_location"
        if user.state_code == "US_MO" and user.district is not None
        else ""
    )
    routes = fields.Dict(keys=fields.Str(), values=fields.Bool())
    feature_variants = fields.Dict(keys=fields.Str(), values=fields.Raw())
    blocked = fields.Bool()
