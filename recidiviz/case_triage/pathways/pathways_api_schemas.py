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
from typing import Dict, Union

import attr
import marshmallow.fields
from marshmallow import Schema, fields, validate
from marshmallow_enum import EnumField

from recidiviz.case_triage.pathways.dimensions.dimension import Dimension
from recidiviz.case_triage.pathways.dimensions.dimension_mapping import (
    DimensionOperation,
)
from recidiviz.case_triage.pathways.metrics.metric_query_builders import ALL_METRICS

FETCH_METRIC_SCHEMAS_BY_NAME = {}


for metric_class in ALL_METRICS:
    dimension_mapping_collection = metric_class.dimension_mapping_collection

    schema_fields: Dict[str, Union[marshmallow.fields.Field, type]] = {
        "filters": fields.Dict(
            EnumField(
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
    }
    if any(
        field.name == "group" for field in attr.fields(metric_class.get_params_class())
    ):
        schema_fields["group"] = EnumField(
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
