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

from recidiviz.case_triage.api_schemas_utils import (
    CamelCaseSchema,
    CamelOrSnakeCaseSchema,
)
from recidiviz.case_triage.workflows.api_schemas import WorkflowsConfigSchema
from recidiviz.persistence.database.schema.workflows.schema import OpportunityStatus


class StateCodeSchema(CamelCaseSchema):
    """Schema that represents information for states needed in the admin panel."""

    code = fields.Str(required=True)
    name = fields.Str(required=True)


class OpportunitySchema(CamelCaseSchema):
    """Schema representing the base information about an opportunity."""

    state_code = fields.Str(required=True)
    opportunity_type = fields.Str(required=True)
    system_type = fields.Str(required=True)
    url_section = fields.Str(required=True)
    completion_event = fields.Str(required=True)
    experiment_id = fields.Str(required=True)
    last_updated_at = fields.Str(required=True)
    last_updated_by = fields.Str(required=True)

    gating_feature_variant = fields.Str(required=False)


class OpportunityConfigurationRequestSchema(WorkflowsConfigSchema):
    """
    Schema representing an opportunity configuration to add to the database.
    Contains additional metadata not shown in the tool.
    """

    description = fields.Str(required=True)
    status = fields.Enum(OpportunityStatus, required=True)


class OpportunityConfigurationResponseSchema(OpportunityConfigurationRequestSchema):
    """
    Schema representing an opportunity configuration in the database with additional
    metadata to be displayed in the admin panel.
    """

    created_at = fields.Str(required=True)
    created_by = fields.Str(required=True)


class OpportunityConfigurationsQueryArgs(CamelOrSnakeCaseSchema):
    """
    Schema representing query args for OpportunityConfigurationsAPI
    """

    offset = fields.Int(required=False)
    status = fields.Enum(OpportunityStatus, required=False)
