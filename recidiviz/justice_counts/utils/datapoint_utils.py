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

from typing import Any, List, Optional, Tuple

from recidiviz.common.constants.justice_counts import ValueType
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
    # Filter out report context datapoints
    if datapoint.context_key is not None and datapoint.report_id is not None:
        return True
    _, success = get_dimension(datapoint)
    return not success


def filter_deprecated_datapoints(
    datapoints: List[schema.Datapoint],
) -> List[schema.Datapoint]:
    "Filter out deprecated datapoints from the given list."
    return [dp for dp in datapoints if not is_datapoint_deprecated(datapoint=dp)]


def get_value(
    datapoint: schema.Datapoint,
    use_value: Optional[str] = None,
) -> Any:
    """This function converts the value of a datapoint to it's
    correct type. All datapoint values are stored as strings
    within the database, but we use the datapoint's `value_type`
    field to identify whether the true type is a string or number.
    If the value is number, we check if the number is "integer-like",
    i.e. ends in .0, and if so we convert it to an int.

    If use_value is not None, then get_value will return the value of
    the use_value parameter, not the datapoint. This functionality is
    used to cast datapoint history values to their correct type.
    Basically, this lets us re-use the casting functionality of
    this method on something besides a datapoint.
    """
    value = datapoint.value if use_value is None else use_value

    try:
        status = datapoint.report.status
    except AttributeError:
        # not an instantiated datapoint
        status = None

    if value is None:
        return value

    if datapoint.context_key is None or datapoint.value_type == ValueType.NUMBER:
        try:
            float_value = float(value)
        except ValueError as e:
            if status == schema.ReportStatus.PUBLISHED and use_value is None:
                raise ValueError(
                    f"Datapoint represents a float value, but is a string. Datapoint value: {value}",
                ) from e
            return value

        if float_value.is_integer():
            return int(float_value)

        return float_value

    return value


def get_dimension_id_and_member(
    datapoint: schema.Datapoint,
) -> Tuple[Optional[str], Optional[str]]:
    dimension_identifier_to_member = datapoint.dimension_identifier_to_member
    if dimension_identifier_to_member is None:
        return (None, None)
    if len(dimension_identifier_to_member) > 1:
        raise ValueError(
            "Datapoints with more than one dimension are not currently supported."
        )
    return list(dimension_identifier_to_member.items())[0]


def get_dimension_id(
    datapoint: schema.Datapoint,
) -> Optional[str]:
    id_and_member = get_dimension_id_and_member(datapoint=datapoint)
    if id_and_member is None:
        return None
    return id_and_member[0]


def get_dimension_member(
    datapoint: schema.Datapoint,
) -> Optional[str]:
    id_and_member = get_dimension_id_and_member(datapoint=datapoint)
    if id_and_member is None:
        return None
    return id_and_member[1]
