# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Creates LookMLViewField object and associated functions"""
import abc
from typing import List, Optional, TypeVar

import attr
from more_itertools import one

from recidiviz.looker.lookml_view_field_parameter import (
    FieldParameterAllowedValue,
    FieldParameterDatatype,
    FieldParameterTimeframes,
    FieldParameterType,
    FieldParameterViewLabel,
    LookMLFieldCategory,
    LookMLFieldDatatype,
    LookMLFieldParameter,
    LookMLFieldType,
    LookMLTimeframesOption,
)
from recidiviz.utils.string import StrictStringFormatter

FIELD_TEMPLATE = """
  {field_type}: {field_name} {{
    {parameter_declarations}
  }}"""

LookMLViewFieldT = TypeVar("LookMLViewFieldT", bound="LookMLViewField")


def enforce_has_datatype_if_date_field(
    field_parameters: List[LookMLFieldParameter],
) -> None:
    """Enforce that datatype exists when type is date"""
    if any(
        isinstance(param, FieldParameterType)
        and param.field_type is LookMLFieldType.DATE
        for param in field_parameters
    ) and not any(
        isinstance(param, FieldParameterDatatype) for param in field_parameters
    ):
        raise ValueError(
            "Datatype parameter must be supplied when type parameter is `date`."
        )


@attr.define
class LookMLViewField:
    """Produces a LookML view field clause that satisfies the syntax described in
    https://cloud.google.com/looker/docs/reference/param-field. Not all field syntax
    is supported.
    Although not required by Looker, consider providing a `description` and `group_label` parameter where possible.
    """

    field_name: str
    parameters: List[LookMLFieldParameter]

    @property
    @abc.abstractmethod
    def field_category(self) -> LookMLFieldCategory:
        pass

    def __attrs_post_init__(self) -> None:
        disallowed_parameters = sorted(
            {
                p.key
                for p in self.parameters
                if not p.allowed_for_category(self.field_category)
            }
        )

        if disallowed_parameters:
            raise ValueError(
                f"The following parameter types are not allowed for "
                f"[{self.field_category.value}] fields: {disallowed_parameters}"
            )

        # Enforce that there aren't multiple parameters with the same key (except allowed_values)
        single_value_keys = [
            param.key
            for param in self.parameters
            if not isinstance(param, FieldParameterAllowedValue)
        ]

        if len(single_value_keys) != len(set(single_value_keys)):
            raise ValueError(
                f"Defined field parameters contain repeated key: {single_value_keys}"
            )

    def build(self) -> str:
        parameter_declarations = "\n    ".join(
            [param.build() for param in self.parameters]
        )
        return StrictStringFormatter().format(
            FIELD_TEMPLATE,
            field_type=self.field_category.value,
            field_name=self.field_name,
            parameter_declarations=parameter_declarations,
        )

    def extend(
        self: LookMLViewFieldT, additional_parameters: List[LookMLFieldParameter]
    ) -> LookMLViewFieldT:
        """Returns a new view field of the same type with additional parameters added"""
        return self.__class__(self.field_name, self.parameters + additional_parameters)

    def view_label(self) -> FieldParameterViewLabel:
        return one(p for p in self.parameters if isinstance(p, FieldParameterViewLabel))


@attr.define
class DimensionLookMLViewField(LookMLViewField):
    """Defines a LookML dimension field object."""

    field_category = LookMLFieldCategory.DIMENSION

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)

    @classmethod
    def for_column(
        cls,
        column_name: str,
        field_type: LookMLFieldType = LookMLFieldType.STRING,
        custom_params: Optional[List[LookMLFieldParameter]] = None,
    ) -> "DimensionLookMLViewField":
        """
        Generates simple dimension referencing a column name present in the view source table.
        """
        additional_params: List[LookMLFieldParameter] = custom_params or []
        if field_type is LookMLFieldType.DATE:
            additional_params.append(
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE)
            )

        return DimensionLookMLViewField(
            field_name=column_name,
            parameters=[
                LookMLFieldParameter.type(field_type),
                *additional_params,
                LookMLFieldParameter.sql(f"${{TABLE}}.{column_name}"),
            ],
        )

    @classmethod
    def for_days_in_period(
        cls,
        start_date_column_name: str,
        end_date_column_name: str,
        view_label: Optional[str] = None,
    ) -> "DimensionLookMLViewField":
        """Generates a dimension calculating the difference in days between two date columns"""
        additional_params: List[LookMLFieldParameter] = []
        if view_label is not None:
            additional_params.append(LookMLFieldParameter.view_label(view_label))

        return DimensionLookMLViewField(
            field_name="days_in_period",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                *additional_params,
                LookMLFieldParameter.sql(
                    f"DATE_DIFF(${{TABLE}}.{end_date_column_name}, ${{TABLE}}.{start_date_column_name}, DAY)"
                ),
            ],
        )


@attr.define
class FilterLookMLViewField(LookMLViewField):
    field_category = LookMLFieldCategory.FILTER

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)


@attr.define
class MeasureLookMLViewField(LookMLViewField):
    field_category = LookMLFieldCategory.MEASURE

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)


@attr.define
class ParameterLookMLViewField(LookMLViewField):
    field_category = LookMLFieldCategory.PARAMETER

    def allowed_values(self) -> List[FieldParameterAllowedValue]:
        return [p for p in self.parameters if isinstance(p, FieldParameterAllowedValue)]


@attr.define
class DimensionGroupLookMLViewField(LookMLViewField):
    """Defines a LookML dimension group field object."""

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        # Enforce that type is always time or duration
        has_type_time = any(
            isinstance(param, FieldParameterType)
            and param.field_type is LookMLFieldType.TIME
            for param in self.parameters
        )

        has_type_duration = any(
            isinstance(param, FieldParameterType)
            and param.field_type is LookMLFieldType.DURATION
            for param in self.parameters
        )

        if not has_type_time and not has_type_duration:
            raise ValueError(
                "Type parameter must be `duration` or `time` for a `dimension_group`."
            )

        # Enforce that timeframes is always used with type: time
        if (
            any(
                isinstance(param, FieldParameterTimeframes) for param in self.parameters
            )
            and not has_type_time
        ):
            raise ValueError(
                "`timeframes` may only be used when type parameter is `time`."
            )

        # Enforce that datatype exists when type is date
        enforce_has_datatype_if_date_field(self.parameters)

    field_category = LookMLFieldCategory.DIMENSION_GROUP

    @classmethod
    def for_datetime_column(
        cls,
        column_name: str,
        custom_params: List[LookMLFieldParameter] | None = None,
    ) -> "DimensionGroupLookMLViewField":
        """
        Generates a dimension group for a datetime column.
        """
        return DimensionGroupLookMLViewField(
            field_name=column_name,
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.TIME),
                LookMLFieldParameter.timeframes(
                    [
                        LookMLTimeframesOption.RAW,
                        LookMLTimeframesOption.TIME,
                        LookMLTimeframesOption.DATE,
                        LookMLTimeframesOption.WEEK,
                        LookMLTimeframesOption.MONTH,
                        LookMLTimeframesOption.QUARTER,
                        LookMLTimeframesOption.YEAR,
                    ]
                ),
                LookMLFieldParameter.convert_tz(False),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATETIME),
                LookMLFieldParameter.sql(f"${{TABLE}}.{column_name}"),
            ]
            + (custom_params or []),
        )

    @classmethod
    def for_date_column(
        cls,
        column_name: str,
        custom_params: List[LookMLFieldParameter] | None = None,
    ) -> "DimensionGroupLookMLViewField":
        """
        Generates a dimension group for a date column.
        """
        return DimensionGroupLookMLViewField(
            field_name=column_name,
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.TIME),
                LookMLFieldParameter.timeframes(
                    [
                        LookMLTimeframesOption.RAW,
                        LookMLTimeframesOption.DATE,
                        LookMLTimeframesOption.WEEK,
                        LookMLTimeframesOption.MONTH,
                        LookMLTimeframesOption.QUARTER,
                        LookMLTimeframesOption.YEAR,
                    ]
                ),
                LookMLFieldParameter.convert_tz(False),
                LookMLFieldParameter.datatype(LookMLFieldDatatype.DATE),
                LookMLFieldParameter.sql(f"${{TABLE}}.{column_name}"),
            ]
            + (custom_params or []),
        )
