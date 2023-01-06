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
# =============================================================================
"""Utilities for working with the Datapoint model."""

from typing import List, Optional, Tuple

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.persistence.database.schema.justice_counts import schema


def get_dimension(datapoint: schema.Datapoint) -> Tuple[Optional[DimensionBase], bool]:
    """Each datapoint in the DB has a JSON `dimension_identifier_to_member`
    dictionary that looks like `{"metric/prisons/staff/type": "SECURITY"}`.
    This dictionary tells us that the datapoint is filtered on the dimension
    class with identifier "metric/prisons/staff/type" (i.e. PrisonsStaffType)
    and that the value of that filter is PrisonsStaffType.SECURITY. This
    method parses the JSON blob to ultimately return this enum member --
    -- e.g. in this case the return value is PrisonsStaffType.SECURITY.

    This method turns a tuple of (Optional[enum member], success). If
    `dimension_identifier_to_member` is None, then the datapoint is an
    aggregate value, in which case we return (None, True). If
    `dimension_identifier_to_member` references a deprecated dimension
    identifier or enum member, we fail to parse, and we return (None, False).
    Else, we return (enum member, True).
    """
    if datapoint.dimension_identifier_to_member is None:
        return None, True
    if len(datapoint.dimension_identifier_to_member) != 1:
        raise ValueError(
            f"datapoint with id: {datapoint.id} has more than one disaggregation, which is currently not supported"
        )
    # example: dimension_member = "MALE"
    dimension_id, dimension_member = list(
        datapoint.dimension_identifier_to_member.items()
    ).pop()

    dimension_class = None
    try:
        dimension_class = DIMENSION_IDENTIFIER_TO_DIMENSION[
            dimension_id
        ]  # example: dimension_class = GenderRestricted
    except KeyError:
        return None, False

    dimension_enum_member = None
    if dimension_class is not None:
        try:
            # example: dimension_enum_member = GenderRestricted.MALE
            dimension_enum_member = dimension_class[dimension_member]  # type: ignore[misc]
        except KeyError:
            return None, False

    return dimension_enum_member, True


def is_datapoint_deprecated(datapoint: schema.Datapoint) -> bool:
    """Return True if the metric is not in the metric registry or if
    the `dimension_identifier_to_member` references a deprecated dimension
    identifier or enum member. These datapoints will be filtered out after fetching
    from the database.
    """
    metric = METRIC_KEY_TO_METRIC.get(datapoint.metric_definition_key)
    if metric is None:
        return True
    _, success = get_dimension(datapoint)
    return not success


def filter_deprecated_datapoints(
    datapoints: List[schema.Datapoint],
) -> List[schema.Datapoint]:
    "Filter out deprecated datapoints from the given list."
    return [dp for dp in datapoints if not is_datapoint_deprecated(datapoint=dp)]
