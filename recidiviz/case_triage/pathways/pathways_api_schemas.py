# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
# ============================================================================
"""Contains Marshmallow schemas for the Pathways API """
import attr
import marshmallow.fields
from marshmallow import Schema, fields, validate

from recidiviz.case_triage.pathways.metrics.metric_query_builders import ALL_METRICS
from recidiviz.case_triage.shared_pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.shared_pathways.dimensions.dimension_mapping import (
    DimensionOperation,
)

FETCH_METRIC_SCHEMAS_BY_NAME = {}


for metric_class in ALL_METRICS:
    dimension_mapping_collection = metric_class.dimension_mapping_collection

    schema_fields: dict[str, marshmallow.fields.Field] = {
        "filters": fields.Dict(
            fields.Enum(
                Dimension,
                by_value=True,
                validate=validate.OneOf(
                    dimension_mapping_collection.operable_map[
                        DimensionOperation.FILTER
                    ].keys()
                ),
            ),
            fields.List(fields.Str),
        ),
        "demo": fields.Boolean(),
    }
    if any(
        field.name == "group"
        for field in list(attr.fields(metric_class.get_params_class()))
    ):
        schema_fields["group"] = fields.Enum(
            Dimension,
            by_value=True,
            required=True,
            validate=validate.OneOf(
                dimension_mapping_collection.operable_map[
                    DimensionOperation.GROUP
                ].keys()
            ),
        )

    FETCH_METRIC_SCHEMAS_BY_NAME[metric_class.name] = Schema.from_dict(schema_fields)
