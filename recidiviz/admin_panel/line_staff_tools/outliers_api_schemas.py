# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Marshmallow API schemas for Outliers endpoints at admin_panel/routes/line_staff_tools.py."""


from marshmallow import fields

from recidiviz.case_triage.api_schemas_utils import CamelCaseSchema


class ConfigurationSchema(CamelCaseSchema):
    """
    Schema expected by /admin/outliers endpoints that take or
    return a Configuration or list of Configurations
    """

    feature_variant = fields.Str(allow_none=True)
    updated_by = fields.Email(allow_none=True)
    supervision_officer_label = fields.Str(required=True)
    supervision_district_label = fields.Str(required=True)
    supervision_unit_label = fields.Str(required=True)
    supervision_supervisor_label = fields.Str(required=True)
    supervision_district_manager_label = fields.Str(required=True)
    supervision_jii_label = fields.Str(required=True)
    supervisor_has_no_outlier_officers_label = fields.Str(required=True)
    officer_has_no_outlier_metrics_label = fields.Str(required=True)
    supervisor_has_no_officers_with_eligible_clients_label = fields.Str(required=True)
    officer_has_no_eligible_clients_label = fields.Str(required=True)
    learn_more_url = fields.Str(required=True)
    none_are_outliers_label = fields.Str(required=True)
    worse_than_rate_label = fields.Str(required=True)
    exclusion_reason_description = fields.Str(required=True)
    slightly_worse_than_rate_label = fields.Str(required=True)
    at_or_below_rate_label = fields.Str(required=True)
    absconders_label = fields.Str(required=True)
    at_or_above_rate_label = fields.Str(required=True)
    outliers_hover = fields.Str(required=True)
    vitals_metrics_methodology_url = fields.Str(required=True)
    action_strategy_copy = fields.Dict(
        keys=fields.Str(),
        values=fields.Dict(keys=fields.Str(), values=fields.Str()),
        required=True,
    )


class FullConfigurationSchema(ConfigurationSchema):
    id = fields.Int(required=True)
    updated_at = fields.DateTime(required=True)
    status = fields.Str(required=True)


class StateCodeSchema(CamelCaseSchema):
    """Schema that represents information for states needed in the admin panel."""

    code = fields.Str(required=True)
    name = fields.Str(required=True)
