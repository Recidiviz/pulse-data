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
import datetime

from marshmallow import Schema, ValidationError, fields, validate
from marshmallow_enum import EnumField

from recidiviz.case_triage.pathways.metrics import (
    COMPOSITE_DIMENSIONS,
    ENABLED_METRICS_BY_STATE,
    Dimension,
)

FETCH_METRIC_SCHEMAS_BY_NAME = {}


def is_date_string(value: str) -> None:
    try:
        datetime.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        raise ValidationError("Not a date string in YYYY-MM-DD format") from e


for enabled_metrics in ENABLED_METRICS_BY_STATE.values():
    for metric_name, metric_class in enabled_metrics.items():
        FETCH_METRIC_SCHEMAS_BY_NAME[metric_name] = Schema.from_dict(
            {
                "since": fields.String(allow_none=True, validate=is_date_string),
                "group": EnumField(
                    Dimension,
                    by_value=True,
                    required=True,
                    validate=validate.OneOf(metric_class.dimensions),
                ),
                "filters": fields.Dict(
                    EnumField(
                        Dimension,
                        by_value=True,
                        validate=[
                            validate.NoneOf(
                                COMPOSITE_DIMENSIONS,
                                error="Cannot filter on composite fields",
                            ),
                            validate.OneOf(
                                list(
                                    set(metric_class.dimensions)
                                    - set(COMPOSITE_DIMENSIONS)
                                )
                            ),
                        ],
                    ),
                    fields.List(fields.Str),
                ),
            }
        )
