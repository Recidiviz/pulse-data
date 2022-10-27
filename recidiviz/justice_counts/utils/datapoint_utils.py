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

import logging
from typing import Optional

from recidiviz.justice_counts.dimensions.base import DimensionBase
from recidiviz.justice_counts.dimensions.dimension_registry import (
    DIMENSION_IDENTIFIER_TO_DIMENSION,
)
from recidiviz.persistence.database.schema.justice_counts import schema


def get_dimension(datapoint: schema.Datapoint) -> Optional[DimensionBase]:
    """Each datapoint in the DB has a JSON `dimension_identifier_to_member`
    dictionary that looks like `{"metric/prisons/staff/type": "SECURITY"}`.
    This dictionary tells us that the datapoint is filtered on the dimension
    class with identifier "metric/prisons/staff/type" (i.e. PrisonsStaffType)
    and that the value of that filter is PrisonsStaffType.SECURITY. This
    method parses the JSON blob to ultimately return this enum member --
    -- e.g. in this case the return value is PrisonsStaffType.SECURITY.
    """
    if datapoint.dimension_identifier_to_member is None:
        return None
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
        logging.warning("Dimension identifier %s not found.", dimension_id)

    dimension_enum_member = None
    if dimension_class is not None:
        try:
            # example: dimension_enum_member = GenderRestricted.MALE
            dimension_enum_member = dimension_class[dimension_member]
        except KeyError:
            logging.warning(
                "Dimension member %s not found in class %s",
                dimension_member,
                dimension_class,
            )
    return dimension_enum_member
