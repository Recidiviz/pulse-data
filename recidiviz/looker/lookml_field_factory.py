# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Factory for creating reusable LookML fields."""
from typing import List, Optional

from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldType,
    MeasureLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldParameter

COUNT_FIELD = "count"


class LookMLFieldFactory:
    """Factory for creating reusable LookML fields."""

    @staticmethod
    def count_measure(
        drill_fields: Optional[List[str]] = None,
    ) -> MeasureLookMLViewField:
        """
        Creates a LookML count measure.

        This measure counts all rows in the dataset. It optionally supports
        drill fields, allowing users to see detailed data when drilling into
        the measure.

        Args:
            drill_fields (Optional[List[str]]): A list of field names that
                can be used for drill-down exploration. Defaults to an empty list.

        Returns:
            MeasureLookMLViewField: A LookML measure field for counting rows.
        """
        return MeasureLookMLViewField(
            field_name=COUNT_FIELD,
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.COUNT),
                LookMLFieldParameter.drill_fields(drill_fields or []),
            ],
        )

    @staticmethod
    def count_field_measure(field: str) -> MeasureLookMLViewField:
        """
        Creates a LookML count measure for a specific field.

        This measure counts the number of non-null values in the specified field.

        Args:
            field (str): The name of the field to count.

        Returns:
            MeasureLookMLViewField: A LookML measure field for counting
                non-null values in the specified field.
        """
        return MeasureLookMLViewField(
            field_name=f"count_{field}",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.sql(f"COUNT(${{{field}}})"),
            ],
        )

    @staticmethod
    def average_measure(field: str) -> MeasureLookMLViewField:
        """
        Creates a LookML average measure for a specific field.

        This measure calculates the average value of the specified field.

        Args:
            field (str): The name of the field to compute the average for.

        Returns:
            MeasureLookMLViewField: A LookML measure field for computing
                the average of the specified field.
        """
        return MeasureLookMLViewField(
            field_name=f"average_{field}",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.AVERAGE),
                LookMLFieldParameter.sql(f"${{{field}}}"),
            ],
        )

    @staticmethod
    def sum_measure(field: str) -> MeasureLookMLViewField:
        """
        Creates a LookML sum measure for a specific field.

        This measure calculates the total sum of the specified field.

        Args:
            field (str): The name of the field to sum.

        Returns:
            MeasureLookMLViewField: A LookML measure field for computing
                the sum of the specified field.
        """
        return MeasureLookMLViewField(
            field_name=f"total_{field}",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.SUM),
                LookMLFieldParameter.sql(f"${{{field}}}"),
            ],
        )

    @staticmethod
    def hidden_dimension(field: str) -> DimensionLookMLViewField:
        """
        Creates a hidden LookML dimension for a specific field.

        This dimension is hidden from the user interface and can be used
        for internal calculations or filtering.

        Args:
            field (str): The name of the field to create a hidden dimension for.

        Returns:
            DimensionLookMLViewField: A hidden LookML dimension field for the
                specified field.
        """
        return DimensionLookMLViewField(
            field_name=field,
            parameters=[
                LookMLFieldParameter.hidden(is_hidden=True),
            ],
        )

    @staticmethod
    def list_field_measure(field: str) -> MeasureLookMLViewField:
        """
        Creates a LookML list measure for a specific field.

        This is useful for aggregating a list of values for the specified field.

        Args:
            field (str): The name of the field to create a list for.

        Returns:
            MeasureLookMLViewField: A LookML measure field for creating a list
                of values for the specified field.
        """
        return MeasureLookMLViewField(
            field_name=f"list_{field}",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.LIST),
                LookMLFieldParameter.list_field(field),
            ],
        )
