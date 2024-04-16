#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Marshmallow API schemas for Workflows admin panel routes"""

from marshmallow import fields

from recidiviz.case_triage.api_schemas_utils import CamelCaseSchema


class StateCodeSchema(CamelCaseSchema):
    """Schema that represents information for states needed in the admin panel."""

    code = fields.Str(required=True)
    name = fields.Str(required=True)


class OpportunitySchema(CamelCaseSchema):
    """Schema representing the base information about an opportunity."""

    state_code = fields.Str(required=True)
    name = fields.Str(required=True)
    system = fields.Str(required=True)
    url = fields.Str(required=True)
    completion_event = fields.Str(required=True)
    experiment_id = fields.Str(required=True)
    last_updated_at = fields.Str(required=True)
    last_updated_by = fields.Str(required=True)

    feature_variant = fields.Str(required=False)
